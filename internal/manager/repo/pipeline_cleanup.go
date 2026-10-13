package repo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

// GetPipelineIncludingDeleted is restricted to cleanup and authenticated control
// paths. Ordinary resource APIs continue to use GetPipeline.
func GetPipelineIncludingDeleted(ctx context.Context, db *gorm.DB, id uuidx.UUID) (*model.Pipeline, error) {
	var p model.Pipeline
	if err := db.WithContext(ctx).Unscoped().First(&p, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrPipelineNotFound
		}
		return nil, err
	}
	return &p, nil
}

func ListPipelinesForNodeCleanupPull(ctx context.Context, db *gorm.DB, nodeID uuidx.UUID) ([]model.Pipeline, error) {
	var ps []model.Pipeline
	err := db.WithContext(ctx).Unscoped().Where("(source_node_id = ? OR target_node_id = ?) AND (deleted_at IS NULL OR cleanup_state <> ?)", nodeID, nodeID, model.CleanupStateDone).Order("created_at ASC").Find(&ps).Error
	return ps, err
}

// RecordPipelineUnload acknowledges only the owner's current deletion revision.
// Successful confirmation is monotonic; delayed failures cannot undo it.
func RecordPipelineUnload(ctx context.Context, db *gorm.DB, id, nodeID uuidx.UUID, side model.Side, version uint64, success bool, message string, now time.Time) error {
	var ownerColumn, confirmedColumn, errorColumn string
	switch side {
	case model.SideSource:
		ownerColumn, confirmedColumn, errorColumn = "source_node_id", "source_cleanup_confirmed_at", "source_cleanup_error"
	case model.SideTarget:
		ownerColumn, confirmedColumn, errorColumn = "target_node_id", "target_cleanup_confirmed_at", "target_cleanup_error"
	default:
		return fmt.Errorf("repo: invalid cleanup side %q", side)
	}
	updates := map[string]any{errorColumn: message}
	if success {
		updates[confirmedColumn], updates[errorColumn] = now, ""
	}
	res := db.WithContext(ctx).Unscoped().Model(&model.Pipeline{}).
		Where("id = ? AND deleted_at IS NOT NULL AND config_version = ? AND "+ownerColumn+" = ? AND "+confirmedColumn+" IS NULL", id, version, nodeID).
		Updates(updates)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected != 0 {
		return nil
	}
	p, err := GetPipelineIncludingDeleted(ctx, db, id)
	if err != nil {
		return err
	}
	owner := p.SourceNodeID
	if side == model.SideTarget {
		owner = p.TargetNodeID
	}
	if !p.DeletedAt.Valid || p.ConfigVersion != version || owner != nodeID {
		return ErrPipelineConfigVersionConflict
	}
	return nil
}

func RequestPipelineCleanupRetry(ctx context.Context, db *gorm.DB, id uuidx.UUID) error {
	p, err := GetPipelineIncludingDeleted(ctx, db, id)
	if err != nil {
		return err
	}
	if !p.DeletedAt.Valid {
		return ErrPipelineConfigVersionConflict
	}
	if p.CleanupState == model.CleanupStateDone {
		return nil
	}
	return db.WithContext(ctx).Unscoped().Model(&model.Pipeline{}).
		Where("id = ? AND deleted_at IS NOT NULL AND cleanup_state <> ?", id, model.CleanupStateDone).
		Updates(map[string]any{"cleanup_state": model.CleanupStatePending, "cleanup_last_attempt_at": nil}).Error
}
