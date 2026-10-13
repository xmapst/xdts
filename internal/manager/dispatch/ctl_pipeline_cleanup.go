package dispatch

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/redact"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

func deletedPipelineConfigs(p model.Pipeline, nodeID uuidx.UUID) []*iproto.PipelineConfigItem {
	var items []*iproto.PipelineConfigItem
	for _, side := range []struct {
		owner uuidx.UUID
		role  iproto.PipelineRole
	}{
		{p.SourceNodeID, iproto.PipelineRole_PIPELINE_ROLE_SOURCE},
		{p.TargetNodeID, iproto.PipelineRole_PIPELINE_ROLE_TARGET},
	} {
		if side.owner == nodeID {
			items = append(items, &iproto.PipelineConfigItem{PipelineId: p.ID.String(), ConfigVersion: p.ConfigVersion, Role: side.role, Deleted: true, CleanupSourceStopped: p.SourceCleanupConfirmedAt != nil})
		}
	}
	return items
}

func (d *Dispatcher) recordPipelineUnload(ctx context.Context, p *model.Pipeline, nodeID uuidx.UUID, side model.Side, result *iproto.ConfigApplyResult) error {
	if result.GetConfigVersion() != p.ConfigVersion || result.GetChangeKind() != iproto.PipelineChangeKind_PIPELINE_CHANGE_KIND_UNLOAD {
		return fmt.Errorf("dispatch: cleanup receipt must confirm current deletion revision and UNLOAD")
	}
	message := ""
	if !result.GetSuccess() {
		message = "node unload failed"
		if e := result.GetError(); e != nil {
			message = redact.PotentialCredentials(e.GetCode() + ": " + e.GetMessage())
		}
	}
	if err := repo.RecordPipelineUnload(ctx, d.db, p.ID, nodeID, side, result.GetConfigVersion(), result.GetSuccess(), message, time.Now()); err != nil {
		return err
	}
	if !result.GetSuccess() {
		slog.Warn("pipeline cleanup unload failed", slog.String("pipeline_id", p.ID.String()), slog.Any("side", side), slog.String("error", message))
	} else {
		slog.Debug("pipeline cleanup unload confirmed", slog.String("pipeline_id", p.ID.String()), slog.Any("side", side), slog.Uint64("config_version", p.ConfigVersion))
	}
	return nil
}
