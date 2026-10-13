package snapreader

import (
	"context"
	"errors"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/pipeline/binlogin"
	"github.com/xmapst/xdts/internal/node/snapshot"
	"github.com/xmapst/xhive"
	"github.com/xmapst/xhive/chanrpc"
)

func (m *Module) onRuntimeUpdate(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
	req, ok := ci.Request.(iproto.SnapshotSourceRuntimeUpdate)
	u := req.Update
	if !ok || u.Context == nil || u.PipelineID != m.opts.PipelineID || u.RunID != m.opts.RunID || u.InstanceID == "" || u.InstanceID != m.opts.InstanceID || u.Barrier != nil || m.opts.Reader == nil || m.runtimeFailed.Load() || m.runtimeUpdating || m.runtimeCtx.Err() != nil {
		return &chanrpc.RetInfo{Err: errors.New("snapreader: invalid runtime identity or state")}
	}
	if err := snapshot.ValidateRuntimeConfig(req); err != nil {
		return &chanrpc.RetInfo{Err: err}
	}
	if err := u.Context.Err(); err != nil {
		return &chanrpc.RetInfo{Err: err}
	}
	if u.Config.ConfigVersion < m.runtimeVersion.Load() {
		return &chanrpc.RetInfo{Err: errors.New("snapreader: stale runtime configuration")}
	}
	m.runtimeUpdating = true
	reply := ci.Hold()
	var updateErr error
	m.AsyncGo("snapreader.runtime_update", func() { updateErr = m.updateRuntime(req) }, func() {
		m.runtimeUpdating = false
		if updateErr == nil {
			m.runtimeVersion.Store(u.Config.ConfigVersion)
		}
		if _, ok := errors.AsType[runtimeCommitFailure](updateErr); ok {
			m.runtimeFailed.Store(true)
		}
		_ = reply.Ret(&chanrpc.RetInfo{Err: updateErr})
	})
	return nil
}

type runtimeCommitFailure struct{ error }

func (m *Module) updateRuntime(req iproto.SnapshotSourceRuntimeUpdate) (updateErr error) {
	prepareCtx, cancel := context.WithCancel(req.Update.Context)
	defer cancel()
	stop := context.AfterFunc(m.runtimeCtx, cancel)
	defer stop()
	if err := m.opts.Reader.PauseRuntime(prepareCtx); err != nil {
		return err
	}
	defer func() {
		if recover() != nil {
			updateErr = runtimeCommitFailure{errors.New("snapreader: runtime workflow panicked")}
		}
		if _, ok := errors.AsType[runtimeCommitFailure](updateErr); ok {
			m.runtimeFailed.Store(true)
			m.runtimeCancel()
			if m.opts.OnFailure != nil {
				m.opts.OnFailure()
			}
		}
		m.opts.Reader.ResumeRuntime()
	}()
	ready, done, commit := make(chan error, 1), make(chan error, 1), make(chan struct{})
	request := req.Update
	request.Context = prepareCtx
	request.Barrier = &iproto.SourceRuntimeBarrier{Ready: ready, Commit: commit, Done: done}
	client := chanrpc.NewClient()
	defer client.Close()
	response := make(chan error, 1)
	if err := client.AsyncCall(xhive.ChanRPC(binlogin.ModuleName(request.PipelineID)), request, func(ret *chanrpc.RetInfo) { response <- ret.Err }); err != nil {
		return err
	}
	for {
		select {
		case err := <-ready:
			if err != nil {
				return err
			}
			if err := prepareCtx.Err(); err != nil {
				return err
			}
			close(commit)
			return m.finishRuntimeCommit(client, response, done, req)
		case <-client.NotEmpty():
			if ret, ok := client.Pop(); ok {
				client.AsyncCallback(ret)
				return <-response
			}
		case <-prepareCtx.Done():
			return prepareCtx.Err()
		}
	}
}

func (m *Module) finishRuntimeCommit(client *chanrpc.Client, response <-chan error, done <-chan error, req iproto.SnapshotSourceRuntimeUpdate) error {
	ctx, cancel := context.WithTimeout(m.runtimeCtx, 30*time.Second)
	defer cancel()
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
		case <-ctx.Done():
			return runtimeCommitFailure{ctx.Err()}
		}
	}
	if err := m.opts.Reader.InstallRuntime(req); err != nil {
		return runtimeCommitFailure{err}
	}
	m.runtimeVersion.Store(req.Update.Config.ConfigVersion)
	return nil
}
