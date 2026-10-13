package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xmapst/xdoc"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/store"
	"github.com/xmapst/xdts/internal/transport"
)

// Each role has an independent immutable tombstone document. Writing it cannot
// overwrite another role or an unrelated applied snapshot in flight.
type pipelineDeletionRecord struct {
	PipelineID    string
	Role          int32
	ConfigVersion uint64
}

func pipelineDeletionID(key appliedKey) string {
	return "deleted-" + key.pipelineID + "-" + fmt.Sprint(int32(key.role))
}

func confirmPipelineDeletion(ctx context.Context, st iStoreAccess, item *iproto.PipelineConfigItem) error {
	key := keyOf(item)
	doc := pipelineDeletionRecord{PipelineID: key.pipelineID, Role: int32(key.role), ConfigVersion: item.GetConfigVersion()}
	return st.PutCritical(ctx, store.Op{Collection: appliedStateCollection, ID: pipelineDeletionID(key), Doc: doc})
}

func restorePipelineDeletions(ctx context.Context, st iStoreAccess, prior map[appliedKey]appliedPipeline) (map[appliedKey]uint64, error) {
	deleted := make(map[appliedKey]uint64)
	for key := range prior {
		var doc pipelineDeletionRecord
		err := st.Get(ctx, appliedStateCollection, pipelineDeletionID(key), &doc)
		if errors.Is(err, xdoc.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if doc.PipelineID != key.pipelineID || doc.Role != int32(key.role) {
			return nil, fmt.Errorf("controller: invalid persisted pipeline deletion identity")
		}
		deleted[key] = doc.ConfigVersion
	}
	return deleted, nil
}

func (m *Module) startPipelineDeletion(item *iproto.PipelineConfigItem) {
	id, role := item.GetPipelineId(), item.GetRole()
	if item.GetDeleted() {
		m.deletingRoles[keyOf(item)] = true
	}
	prior, exists := m.applied[keyOf(item)]
	if !exists {
		prior = m.priorRunApplied[keyOf(item)]
	}
	_, durablyDeleted := m.deletedRoleVersions[keyOf(item)]
	resumeTargetDrain := (!exists || !prior.running) && !durablyDeleted && role == iproto.PipelineRole_PIPELINE_ROLE_TARGET && prior.PipelineConfigItem != nil
	needsUnload := exists && (prior.running || prior.unloadUncertain)
	physicalUnloaded := !needsUnload
	loadedTarget := false
	deletionCommitted := false
	var unloadErr, publishErr error
	m.AsyncGo("controller.unload."+id, func() {
		if role == iproto.PipelineRole_PIPELINE_ROLE_TARGET && item.GetDeleted() {
			if !item.GetCleanupSourceStopped() {
				unloadErr = fmt.Errorf("等待源端确认停止，再处理目标端剩余数据")
			} else {
				if resumeTargetDrain && m.opts.Conn != nil {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					var pending bool
					pending, unloadErr = pipelineQueuesHaveMessages(ctx, m.opts.Conn, id)
					cancel()
					if unloadErr == nil && pending {
						unloadErr = m.loader.LoadTarget(context.Background(), prior.PipelineConfigItem)
						loadedTarget = unloadErr == nil
						needsUnload = loadedTarget
						physicalUnloaded = !loadedTarget
					}
				}
				if unloadErr == nil && m.opts.Conn != nil {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					unloadErr = waitPipelineQueuesDrained(ctx, m.opts.Conn, id)
					cancel()
				}
			}
		}
		if needsUnload && unloadErr == nil {
			unloadErr = m.loader.Unload(id, role)
			physicalUnloaded = unloadErr == nil
		}
		// The target owns the data queues in its own center. A Manager-side
		// lookup on another Broker must never stand in for their reclamation.
		if unloadErr == nil && role == iproto.PipelineRole_PIPELINE_ROLE_TARGET && item.GetDeleted() && m.opts.Conn != nil {
			dc, err := transport.NewDeclarator(m.opts.Conn)
			if err != nil {
				unloadErr = err
			} else {
				dataErr := dc.DeleteDataQueue(transport.PipelineDataQueue(id))
				snapErr := dc.DeleteDataQueue(transport.PipelineSnapshotQueue(id))
				unloadErr = errors.Join(dataErr, snapErr)
				dc.Close()
			}
		}
		// Target shutdown can requeue an in-flight delivery. Keep its last
		// confirmed config available for another drain until queues are reclaimed.
		if unloadErr == nil && item.GetOwnershipReleased() && role == iproto.PipelineRole_PIPELINE_ROLE_SOURCE {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			unloadErr = releaseLocalSourceEpochs(ctx, m.opts.Store, id)
			cancel()
		}
		if unloadErr == nil && item.GetDeleted() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			unloadErr = confirmPipelineDeletion(ctx, m.opts.Store, item)
			cancel()
			deletionCommitted = unloadErr == nil
		}
		publishErr = m.publishApplyResult(context.Background(), buildApplyResult(item, changeUnload, unloadErr))
	}, func() {
		if loadedTarget && !physicalUnloaded {
			m.applied[keyOf(item)] = appliedPipeline{PipelineConfigItem: prior.PipelineConfigItem, running: true}
		}
		if physicalUnloaded && unloadErr != nil {
			if ap, exists := m.applied[keyOf(item)]; exists {
				ap.running, ap.paused, ap.unloadUncertain = false, false, false
				m.applied[keyOf(item)] = ap
			}
		}
		if deletionCommitted {
			m.deletedRoleVersions[keyOf(item)] = item.GetConfigVersion()
			delete(m.deletingRoles, keyOf(item))
		}
		m.finishUnload(item, unloadErr, publishErr)
	})
}

func pipelineQueuesHaveMessages(ctx context.Context, conn *transport.Conn, id string) (bool, error) {
	for _, name := range []string{transport.PipelineDataQueue(id), transport.PipelineSnapshotQueue(id)} {
		stats, err := conn.InspectQueue(ctx, name)
		if transport.IsQueueNotFound(err) {
			continue
		}
		if err != nil {
			return false, err
		}
		if stats.Messages > 0 {
			return true, nil
		}
	}
	return false, nil
}

func waitPipelineQueuesDrained(ctx context.Context, conn *transport.Conn, id string) error {
	for _, name := range []string{transport.PipelineDataQueue(id), transport.PipelineSnapshotQueue(id)} {
		for {
			stats, err := conn.InspectQueue(ctx, name)
			if transport.IsQueueNotFound(err) || err == nil && stats.Messages == 0 {
				break
			}
			if err != nil && !errors.Is(err, transport.ErrQueueInspectBusy) {
				return fmt.Errorf("查询待处理数据失败: %w", err)
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("等待目标端处理剩余数据: %w", ctx.Err())
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	return nil
}
