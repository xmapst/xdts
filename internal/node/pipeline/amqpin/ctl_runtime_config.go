package amqpin

import (
	"errors"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/pipeline/mysqlout"
	"github.com/xmapst/xhive/chanrpc"
	"google.golang.org/protobuf/proto"
)

type iRuntimeMappingApplier interface {
	UpdateRuntimeMapping(iproto.TargetRuntimeConfig) error
}
type targetRuntimeUpdate struct {
	request  iproto.TargetRuntimeUpdate
	reply    *chanrpc.Replier
	prepared bool
	resume   chan struct{}
}

func (m *Module) onRuntimeUpdate(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
	req, ok := ci.Request.(iproto.TargetRuntimeUpdate)
	if !ok || req.Context == nil || req.PipelineID != m.opts.PipelineID || req.RunID != m.opts.RunID || req.InstanceID == "" || req.InstanceID != m.opts.InstanceID {
		return &chanrpc.RetInfo{Err: errors.New("amqpin: invalid runtime update identity")}
	}
	if err := req.Context.Err(); err != nil {
		return &chanrpc.RetInfo{Err: err}
	}
	m.handlerMu.Lock()
	defer m.handlerMu.Unlock()
	if m.handlerClosing || (!m.runtimeReady && m.opts.SnapshotReady == nil) || m.isFrozen() || m.shutdownCtx == nil || m.shutdownCtx.Err() != nil {
		return &chanrpc.RetInfo{Err: errors.New("amqpin: target is not ready for runtime update")}
	}
	if m.runtimePending != nil {
		return &chanrpc.RetInfo{Err: errors.New("amqpin: runtime update already pending")}
	}
	if req.Barrier != nil && (req.Barrier.Ready == nil || req.Barrier.Commit == nil || req.Barrier.Done == nil) {
		return &chanrpc.RetInfo{Err: errors.New("amqpin: incomplete runtime barrier")}
	}
	if err := mysqlout.ValidateRuntimeMapping(req.Config); err != nil {
		return &chanrpc.RetInfo{Err: err}
	}
	if err := req.Config.DatabaseRoute.Validate(); err != nil {
		return &chanrpc.RetInfo{Err: err}
	}
	if req.Config.DatabaseRoute.TargetServerUUID != m.opts.DatabaseRoute.TargetServerUUID {
		return &chanrpc.RetInfo{Err: errors.New("amqpin: target database change requires rebuild")}
	}
	if err := iproto.ValidateTargetErrorPolicy(req.Config.ErrorPolicy); err != nil {
		return &chanrpc.RetInfo{Err: err}
	}
	_, asyncStore := m.opts.Store.(iCriticalAsyncStore)
	if req.Config.ErrorPolicy.SkipUnretryable && !asyncStore {
		return &chanrpc.RetInfo{Err: errors.New("amqpin: skipping errors requires durable store")}
	}
	limits, err := iproto.NormalizePublicationWindow(req.Config.PublicationWindow)
	if err != nil {
		return &chanrpc.RetInfo{Err: err}
	}
	if limits.Batches > m.opts.MaxReassemblyChunks || (limits.Batches > 1 && !asyncStore) {
		return &chanrpc.RetInfo{Err: errors.New("amqpin: publication window requires sufficient prefetch and durable store")}
	}
	req.Config.PublicationWindow = iproto.ClonePublicationWindow(req.Config.PublicationWindow)
	m.runtimePending = &targetRuntimeUpdate{request: req, reply: ci.Hold(), resume: make(chan struct{})}
	m.finishRuntimeUpdateLocked()
	return nil
}

// handlerMu grants exclusive execution ownership when the serial callback is
// inactive. It is never held during SQL IO, ack, retry sleep or other external IO.
func (m *Module) finishRuntimeUpdateLocked() {
	update := m.runtimePending
	if update == nil {
		return
	}
	committed := false
	if update.prepared {
		select {
		case <-update.request.Barrier.Commit:
			committed = true
		default:
		}
	}
	var err error
	if !committed {
		err = update.request.Context.Err()
	}
	if err == nil && (m.handlerClosing || m.isFrozen()) {
		err = errors.New("amqpin: target stopped before runtime update")
	}
	if err == nil && (m.handlerActive || m.reassembly != nil || len(m.heldHandles) > 0) {
		return
	}
	if err == nil && update.request.Config.ConfigVersion < m.runtimeVersion.Load() {
		err = errors.New("amqpin: stale runtime configuration")
	}
	if err == nil && update.request.Barrier != nil && !committed {
		if !update.prepared {
			select {
			case update.request.Barrier.Ready <- nil:
				update.prepared = true
			default:
				err = errors.New("amqpin: runtime barrier has no ready receiver")
			}
		}
		if err == nil {
			return
		}
	}
	if err == nil {
		if resource := update.request.Resource; resource != nil {
			writer, ok := m.opts.Applier.(*mysqlout.Writer)
			if !ok {
				err = errors.New("amqpin: writer cannot install prepared connections")
			} else {
				err = resource.Commit(update.request.Context, func(candidate iproto.ITargetRuntimeResource) (iproto.ITargetRuntimeResource, error) {
					return writer.InstallRuntimeResource(candidate, update.request.Config)
				})
			}
		} else {
			applier, ok := m.opts.Applier.(iRuntimeMappingApplier)
			if !ok {
				err = errors.New("amqpin: writer cannot update runtime mapping")
			} else {
				err = applyRuntimeMappingSafely(applier, update.request.Config)
			}
		}
	}
	if err == nil {
		m.runtimeMaxOriginHops = update.request.Config.MaxOriginHops
		m.runtimeDatabaseRoute = update.request.Config.DatabaseRoute
		m.runtimeErrorPolicy = update.request.Config.ErrorPolicy
		m.runtimeWritePolicy = nil
		if update.request.Config.WritePolicy != nil {
			m.runtimeWritePolicy = proto.Clone(update.request.Config.WritePolicy).(*iproto.WritePolicy)
		}
		m.runtimePublicationWindow = iproto.ClonePublicationWindow(update.request.Config.PublicationWindow)
		m.runtimeVersion.Store(update.request.Config.ConfigVersion)
		if m.fullOnlyDone {
			// A configuration can finish while the completion report is in flight.
			// Republish the latched result at the newly installed version so the
			// Manager can commit STOP without accepting a stale observation.
			m.reportStatus(iproto.PipelineState_PIPELINE_STATE_RUNNING, "")
		}
	}
	m.runtimePending = nil
	close(update.resume)
	if update.request.Barrier != nil {
		if !update.prepared {
			select {
			case update.request.Barrier.Ready <- err:
			default:
			}
		}
		select {
		case update.request.Barrier.Done <- err:
		default:
		}
	}
	_ = update.reply.Ret(&chanrpc.RetInfo{Err: err})
}

// Both cold catchup initialization and consumer callbacks acquire the same
// execution ownership. Waiting happens outside the mutex and is cancellable.
func (m *Module) beginRuntimeWork() bool {
	for {
		m.handlerMu.Lock()
		if m.handlerClosing || m.shutdownCtx.Err() != nil {
			m.handlerMu.Unlock()
			return false
		}
		m.finishRuntimeUpdateLocked()
		if update := m.runtimePending; update != nil && update.prepared {
			resume := update.resume
			m.handlerMu.Unlock()
			select {
			case <-resume:
			case <-m.shutdownCtx.Done():
				return false
			}
			continue
		}
		m.handlerActive = true
		m.handlerMu.Unlock()
		return true
	}
}

func (m *Module) endRuntimeWork() {
	m.handlerMu.Lock()
	defer m.handlerMu.Unlock()
	m.handlerActive = false
	m.finishRuntimeUpdateLocked()
}

func applyRuntimeMappingSafely(applier iRuntimeMappingApplier, config iproto.TargetRuntimeConfig) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("amqpin: runtime mapping update panicked")
		}
	}()
	return applier.UpdateRuntimeMapping(config)
}

func (m *Module) onRuntimeUpdateTimer(_ int64, _ map[string]string) {
	m.handlerMu.Lock()
	defer m.handlerMu.Unlock()
	m.finishRuntimeUpdateLocked()
}
