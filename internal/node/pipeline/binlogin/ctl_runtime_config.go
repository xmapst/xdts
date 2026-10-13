package binlogin

import (
	"errors"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xhive/chanrpc"
)

type runtimeUpdate struct {
	request iproto.SourceRuntimeUpdate
	reply   *chanrpc.Replier
}

func (m *Module) onRuntimeUpdate(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
	req, ok := ci.Request.(iproto.SourceRuntimeUpdate)
	if !ok || req.Context == nil || req.PipelineID != m.opts.PipelineID || req.RunID != m.opts.RunID || req.InstanceID == "" || req.InstanceID != m.opts.InstanceID || req.Config.TableFilter == nil || req.Config.ColumnFilter == nil || req.Config.OriginResolver == nil {
		return &chanrpc.RetInfo{Err: errors.New("binlogin: invalid runtime update identity or plan")}
	}
	if err := req.Context.Err(); err != nil {
		return &chanrpc.RetInfo{Err: err}
	}
	if err := iproto.ValidateDDLColumnPolicy(req.Config.DDLColumnPolicy); err != nil {
		return &chanrpc.RetInfo{Err: err}
	}
	if _, err := iproto.NormalizePublicationWindow(req.Config.PublicationWindow); err != nil {
		return &chanrpc.RetInfo{Err: err}
	}
	if err := req.Config.DatabaseRoute.Validate(); err != nil {
		return &chanrpc.RetInfo{Err: err}
	}
	if req.Config.DatabaseRoute.SourceServerUUID != m.opts.DatabaseRoute.SourceServerUUID {
		return &chanrpc.RetInfo{Err: errors.New("binlogin: source database change requires rebuild")}
	}
	if req.Config.DatabaseRoute.Persistent != m.opts.DatabaseRoute.Persistent {
		return &chanrpc.RetInfo{Err: errors.New("binlogin: lineage storage change requires rebuild")}
	}
	req.Config.DDLColumnPolicy = iproto.CloneDDLColumnPolicy(req.Config.DDLColumnPolicy)
	req.Config.PublicationWindow = iproto.ClonePublicationWindow(req.Config.PublicationWindow)
	if req.Barrier != nil && (req.Barrier.Ready == nil || req.Barrier.Commit == nil || req.Barrier.Done == nil) {
		return &chanrpc.RetInfo{Err: errors.New("binlogin: incomplete runtime barrier")}
	}
	if m.stopping || m.sessionCtx == nil || m.sessionCtx.Err() != nil {
		return &chanrpc.RetInfo{Err: errors.New("binlogin: source instance is stopping")}
	}
	update := runtimeUpdate{request: req, reply: ci.Hold()}
	select {
	case m.runtimeUpdates <- update:
	default:
		_ = update.reply.Ret(&chanrpc.RetInfo{Err: errors.New("binlogin: runtime update already pending")})
	}
	return nil
}

// Only the serial session calls this, before the next event at a transaction
// boundary. Fragment publication is synchronous and completes before return
// to this boundary; tables are re-evaluated from the next TableMap event.
func (m *Module) applyRuntimeUpdate(agg *aggregator) error {
	if agg.state != stateIdle {
		return nil
	}
	select {
	case update := <-m.runtimeUpdates:
		req := update.request
		err := req.Context.Err()
		if err == nil && req.Config.ConfigVersion < m.runtimeVersion.Load() {
			err = errors.New("binlogin: stale runtime configuration")
		}
		var publicationErr error
		if err == nil {
			publicationErr = m.drainPublicationWindow(req.Context)
			err = publicationErr
		}
		if req.Barrier != nil {
			select {
			case req.Barrier.Ready <- err:
			default:
				err = errors.New("binlogin: runtime barrier has no ready receiver")
			}
			if err == nil {
				select {
				case <-req.Barrier.Commit:
				case <-req.Context.Done():
					err = req.Context.Err()
				case <-m.sessionCtx.Done():
					err = m.sessionCtx.Err()
				}
				// A commit already authorized before cancellation wins over an
				// expired preparation; the coordinator waits for this outcome.
				select {
				case <-req.Barrier.Commit:
					err = nil
				default:
				}
			}
		}
		if err == nil {
			m.sourceRuntime = req.Config
			agg.tableFilter = req.Config.TableFilter
			agg.columnFilter = generatedColumnFilter{user: req.Config.ColumnFilter, schema: agg.schemaTrack}
			agg.rowFilter = req.Config.RowFilter
			agg.transforms = req.Config.Transforms
			agg.ddlColumnPolicy = iproto.CloneDDLColumnPolicy(req.Config.DDLColumnPolicy)
			agg.origin = req.Config.OriginResolver
			agg.databaseRoute = req.Config.DatabaseRoute
			clear(agg.tables)
			m.limiter.reconfigure(req.Config.RowsPerSec, req.Config.BytesPerSec)
			m.runtimeVersion.Store(req.Config.ConfigVersion)
		}
		if req.Barrier != nil {
			select {
			case req.Barrier.Done <- err:
			default:
			}
		}
		_ = update.reply.Ret(&chanrpc.RetInfo{Err: err})
		if publicationErr != nil && req.Context.Err() == nil {
			return publicationErr
		}
	default:
	}
	return nil
}

func (m *Module) rejectPendingRuntimeUpdates() {
	for {
		select {
		case update := <-m.runtimeUpdates:
			err := errors.New("binlogin: source session ended before runtime update")
			if barrier := update.request.Barrier; barrier != nil {
				select {
				case barrier.Ready <- err:
				default:
				}
				select {
				case barrier.Done <- err:
				default:
				}
			}
			_ = update.reply.Ret(&chanrpc.RetInfo{Err: err})
		default:
			return
		}
	}
}
