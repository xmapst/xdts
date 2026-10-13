package dispatch

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/pipelinespec"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

func (r *Reconciler) refreshOriginConfigs(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	changes, err := repo.ListPendingOriginRefreshes(ctx, r.d.db)
	if err != nil {
		slog.Error("origin refresh query failed", slog.Any("error", err))
		return
	}
	for _, change := range changes {
		pipelines, err := repo.ListPipelinesAffectedByOrigin(ctx, r.d.db, change.CenterID)
		if err != nil {
			slog.Error("origin refresh pipeline query failed", slog.String("center_id", change.CenterID.String()), slog.Any("error", err))
			continue
		}
		complete := true
		for _, pipeline := range pipelines {
			applied, err := r.d.refreshPipelineOrigins(ctx, pipeline)
			if err != nil {
				slog.Warn("origin config refresh failed", slog.String("pipeline_id", pipeline.ID.String()), slog.Any("error", err))
			}
			complete = complete && applied && err == nil
		}
		if complete {
			if err := repo.CompleteOriginRefresh(ctx, r.d.db, change.CenterID, change.Revision); err != nil {
				slog.Error("origin refresh completion failed", slog.String("center_id", change.CenterID.String()), slog.Any("error", err))
			}
		}
	}
}

func (d *Dispatcher) recordOriginPull(ctx context.Context, pipeline *model.Pipeline, side model.Side, nodeID uuidx.UUID, digest string) error {
	var count int64
	if err := d.db.WithContext(ctx).Model(&model.PipelineRelease{}).
		Where("pipeline_id=? AND config_version=? AND side=? AND node_id=? AND config_digest=? AND status IN ?", pipeline.ID, pipeline.ConfigVersion, side, nodeID, digest, []model.ReleaseStatus{model.ReleaseStatusPending, model.ReleaseStatusApplied}).
		Count(&count).Error; err != nil {
		return err
	}
	if count != 0 {
		return nil
	}
	return repo.CreatePipelineRelease(ctx, d.db, &model.PipelineRelease{PipelineID: pipeline.ID, ConfigVersion: pipeline.ConfigVersion,
		Side: side, NodeID: nodeID, ConfigDigest: digest, Status: model.ReleaseStatusPending})
}

func (d *Dispatcher) refreshPipelineOrigins(ctx context.Context, pipeline model.Pipeline) (bool, error) {
	// REINIT owns its confirmed config version until both command receipts commit.
	// A topology refresh must not invalidate that identity while it is executing.
	if pipeline.ReinitPending || pipeline.SourceActualState == model.ActualStateDraft || pipeline.TargetActualState == model.ActualStateDraft {
		return false, nil
	}
	current, err := pipelinespec.Assemble(ctx, d.db, pipeline.ID)
	if err != nil {
		return false, err
	}
	ready, recorded := true, true
	for _, side := range []model.Side{model.SideSource, model.SideTarget} {
		digest, nodeID := current.SourceDigest, pipeline.SourceNodeID
		if side == model.SideTarget {
			digest, nodeID = current.TargetDigest, pipeline.TargetNodeID
		}
		var release model.PipelineRelease
		err := d.db.WithContext(ctx).Where("pipeline_id=? AND config_version=? AND side=? AND node_id=? AND config_digest=?", pipeline.ID, pipeline.ConfigVersion, side, nodeID, digest).
			Order("created_at DESC, id DESC").First(&release).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			ready, recorded = false, false
			continue
		}
		if err != nil {
			return false, err
		}
		ready = ready && release.Status == model.ReleaseStatusApplied
	}
	if ready {
		return true, nil
	}
	if recorded {
		// Retry the current version after an interrupted publish or lost reply.
		// Allocating another version here would continually outrun node ACKs.
		d.pushConfigChanged(ctx, pipeline.ID, pipeline.ConfigVersion, false, pipeline.SourceNodeID, pipeline.TargetNodeID)
		return false, nil
	}
	// A committed snapshot handover owns its version before notification has
	// recorded both projections. Complete that publication gap at the committed
	// version; an unrelated changed digest still follows normal SyncPipeline.
	fresh, err := repo.GetPipeline(ctx, d.db, pipeline.ID)
	if err != nil {
		return false, err
	}
	if fresh.ConfigVersion != pipeline.ConfigVersion || fresh.SourceNodeID != pipeline.SourceNodeID || fresh.TargetNodeID != pipeline.TargetNodeID {
		return false, nil
	}
	handover := false
	for _, side := range []model.Side{model.SideSource, model.SideTarget} {
		bundle, err := repo.GetPipelineStateBundle(ctx, d.db, fresh, side)
		if err != nil {
			return false, err
		}
		handover = handover || bundle != nil && bundle.ConfigVersion == fresh.ConfigVersion
	}
	if handover {
		for _, projection := range []struct {
			side   model.Side
			node   uuidx.UUID
			digest string
		}{{model.SideSource, fresh.SourceNodeID, current.SourceDigest}, {model.SideTarget, fresh.TargetNodeID, current.TargetDigest}} {
			var changed int64
			if err := d.db.WithContext(ctx).Model(&model.PipelineRelease{}).Where("pipeline_id=? AND config_version=? AND side=? AND node_id=? AND config_digest<>?", fresh.ID, fresh.ConfigVersion, projection.side, projection.node, projection.digest).Count(&changed).Error; err != nil {
				return false, err
			}
			if changed != 0 {
				return false, d.SyncPipeline(ctx, pipeline.ID)
			}
		}
		for _, side := range []model.Side{model.SideSource, model.SideTarget} {
			digest, node := current.SourceDigest, fresh.SourceNodeID
			if side == model.SideTarget {
				digest, node = current.TargetDigest, fresh.TargetNodeID
			}
			if err := d.recordOriginPull(ctx, fresh, side, node, digest); err != nil {
				return false, err
			}
		}
		d.pushConfigChanged(ctx, fresh.ID, fresh.ConfigVersion, false, fresh.SourceNodeID, fresh.TargetNodeID)
		return false, nil
	}
	return false, d.SyncPipeline(ctx, pipeline.ID)
}
