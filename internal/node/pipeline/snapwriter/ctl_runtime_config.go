package snapwriter

import (
	"context"
	"errors"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/pipeline/amqpin"
	"github.com/xmapst/xdts/internal/node/pipeline/mysqlout"
	"github.com/xmapst/xhive"
	"github.com/xmapst/xhive/chanrpc"
)

type snapshotRuntimeUpdate struct {
	request iproto.SnapshotTargetRuntimeUpdate
	reply   *chanrpc.Replier
}

func (m *Module) onRuntimeUpdate(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
	req, ok := ci.Request.(iproto.SnapshotTargetRuntimeUpdate)
	u := req.Update
	if !ok || u.Context == nil || u.PipelineID != m.opts.Run.PipelineID || u.RunID != m.opts.Run.RunID || u.InstanceID == "" || u.InstanceID != m.opts.InstanceID || u.Barrier != nil || m.runtimeFailed.Load() || m.runtimeEnded.Load() {
		return &chanrpc.RetInfo{Err: errors.New("snapwriter: invalid runtime update identity or state")}
	}
	if err := u.Context.Err(); err != nil {
		return &chanrpc.RetInfo{Err: err}
	}
	if req.WriteConcurrency > 128 {
		return &chanrpc.RetInfo{Err: errors.New("snapwriter: write concurrency exceeds 128")}
	}
	if err := iproto.ValidateTargetErrorPolicy(u.Config.ErrorPolicy); err != nil {
		return &chanrpc.RetInfo{Err: err}
	}
	run := m.opts.Run
	run.MaxRows, run.MaxBytes = req.MaxRows, req.MaxBytes
	if err := run.Validate(); err != nil {
		return &chanrpc.RetInfo{Err: err}
	}
	if err := mysqlout.ValidateRuntimeMapping(u.Config); err != nil {
		return &chanrpc.RetInfo{Err: err}
	}
	update := snapshotRuntimeUpdate{request: req, reply: ci.Hold()}
	select {
	case m.runtimeUpdates <- update:
	default:
		_ = update.reply.Ret(&chanrpc.RetInfo{Err: errors.New("snapwriter: runtime update already pending")})
	}
	return nil
}

// Called by the coordinator after all workers returned and old deliveries were
// acknowledged. No writer state is accessed while another owner can use it.
func (m *Module) updateRuntime(ctx context.Context, req iproto.SnapshotTargetRuntimeUpdate, writers []*mysqlout.Writer) error {
	if err := req.Update.Context.Err(); err != nil {
		return err
	}
	if req.Update.Config.ConfigVersion < m.runtimeVersion.Load() {
		return errors.New("snapwriter: stale runtime configuration")
	}
	if err := mysqlout.ValidateRuntimeMapping(req.Update.Config); err != nil {
		return err
	}
	ready, done, commit := make(chan error, 1), make(chan error, 1), make(chan struct{})
	request := req.Update
	// Cancellation before commit aborts preparation. After commit we wait for
	// its result, rather than exposing a successful one-sided mapping change.
	prepareCtx, cancel := context.WithCancel(req.Update.Context)
	defer cancel()
	stop := context.AfterFunc(ctx, cancel)
	defer stop()
	request.Context = prepareCtx
	request.Barrier = &iproto.TargetRuntimeBarrier{Ready: ready, Done: done, Commit: commit}
	client := chanrpc.NewClient()
	defer client.Close()
	response := make(chan error, 1)
	if err := client.AsyncCall(xhive.ChanRPC(amqpin.ModuleName(request.PipelineID)), request, func(ret *chanrpc.RetInfo) { response <- ret.Err }); err != nil {
		return err
	}
	prepared := false
	for !prepared {
		select {
		case err := <-ready:
			if err != nil {
				return err
			}
			if err := prepareCtx.Err(); err != nil {
				return err
			}
			prepared = true
		case <-client.NotEmpty():
			if ret, ok := client.Pop(); ok {
				client.AsyncCallback(ret)
				return <-response
			}
		case <-prepareCtx.Done():
			return prepareCtx.Err()
		}
	}
	close(commit)
	// A missing post-commit response must stop this pipeline. Continuing old
	// snapshot mappings after an unknown incremental commit would mix plans.
	committedCtx, stopCommit := context.WithTimeout(ctx, 30*time.Second)
	defer stopCommit()
	haveDone, haveReply := false, false
	for !haveDone || !haveReply {
		select {
		case err := <-done:
			if err != nil {
				return runtimeCommitFailure{err}
			}
			haveDone = true
		case <-client.NotEmpty():
			if ret, ok := client.Pop(); ok {
				client.AsyncCallback(ret)
				if err := <-response; err != nil {
					return runtimeCommitFailure{err}
				}
				haveReply = true
			}
		case <-committedCtx.Done():
			return runtimeCommitFailure{committedCtx.Err()}
		}
	}
	fullConfig := req.Update.Config
	fullConfig.WritePolicy = nil
	for _, writer := range writers {
		if err := writer.UpdateRuntimeMapping(fullConfig); err != nil {
			return runtimeCommitFailure{err}
		}
	}
	m.runtimeVersion.Store(req.Update.Config.ConfigVersion)
	return nil
}

func (m *Module) rejectRuntimeUpdates() {
	for {
		select {
		case update := <-m.runtimeUpdates:
			_ = update.reply.Ret(&chanrpc.RetInfo{Err: errors.New("snapwriter: workflow ended before runtime update")})
		default:
			return
		}
	}
}

// A commit failure is terminal: the companion may already use the new plan.
type runtimeCommitFailure struct{ error }
