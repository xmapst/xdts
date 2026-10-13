package controller

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/xmapst/xdts/internal/iproto"
)

func (m *Module) startHotUpdate(item *iproto.PipelineConfigItem, updater iPipelineHotUpdater, kind changeKind) {
	key := keyOf(item)
	instance := m.runtimeInstances[key].InstanceID
	m.runtimeUpdateVersions[key] = item.GetConfigVersion()
	var updateErr, publishErr error
	m.AsyncGo("controller.hot_update."+item.GetPipelineId(), func() {
		defer func() {
			if recover() != nil {
				updateErr = fmt.Errorf("controller: runtime update panicked")
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), defaultTargetedPullTimeout)
		defer cancel()
		updateErr = updater.HotUpdate(ctx, item, instance)
	}, func() {
		delete(m.runtimeUpdateVersions, key)
		if m.identityRetired {
			delete(m.pending, key)
			return
		}
		if updateErr == nil {
			prior := m.applied[key]
			prior.PipelineConfigItem = item
			m.applied[key] = prior
			current := m.runtimeInstances[key]
			current.ConfigVersion = item.GetConfigVersion()
			m.runtimeInstances[key] = current
			m.pushPipelinesMirror()
			if err := persistAppliedState(m.opts.Store, m.applied, m.now().UnixMilli()); err != nil {
				slog.Warn("persist hot updated configuration failed", slog.String("pipeline_id", item.GetPipelineId()), slog.Any("error", err))
			}
		} else {
			slog.Warn("pipeline runtime update failed", slog.String("pipeline_id", item.GetPipelineId()), slog.Any("error", updateErr))
		}
		// Persist the actual switch before publishing its receipt. The existing
		// instance remains running when the new plan is rejected.
		m.AsyncGo("controller.hot_update_receipt."+item.GetPipelineId(), func() {
			publishErr = m.publishApplyResult(context.Background(), buildApplyResult(item, kind, updateErr))
		}, func() {
			delete(m.pending, key)
			if publishErr != nil {
				slog.Warn("publish runtime update receipt failed", slog.String("pipeline_id", item.GetPipelineId()), slog.Any("error", publishErr))
			}
			m.reevaluateTrailing(key)
		})
	})
}
