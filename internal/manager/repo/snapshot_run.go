package repo

import (
	"context"
	"fmt"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// EnsurePipelineSnapshotRun allocates the source identity before publishing a
// snapshot configuration. One row lock serializes simultaneous config pulls;
// pipeline, identity registration and progress record commit atomically.
// This never changes an existing run's UUID and is not a reinitialization API.
func EnsurePipelineSnapshotRun(ctx context.Context, db *gorm.DB, id uuidx.UUID) error {
	return db.Transaction(func(tx *gorm.DB) error {
		var pipeline model.Pipeline
		if err := tx.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).First(&pipeline, "id = ?", id).Error; err != nil {
			return err
		}
		if pipeline.InitMode != model.InitModeFullAndIncremental && pipeline.InitMode != model.InitModeFullOnly {
			return nil
		}
		if !pipeline.SnapshotUUID.IsZero() {
			return nil
		}
		endpoint, err := GetEndpoint(ctx, tx, pipeline.SourceEndpointID)
		if err != nil {
			return err
		}
		identity := uuidx.New()
		origin := &model.CenterOriginUUID{CenterID: endpoint.CenterID, UUID: identity, Kind: model.OriginKindSynthetic, RefID: pipeline.ID.String()}
		if err := CreateCenterOriginUUID(ctx, tx, origin); err != nil {
			return err
		}
		if err := CreateSnapshot(ctx, tx, &model.PipelineSnapshot{PipelineID: pipeline.ID, RunID: pipeline.RunID, SnapshotUUID: identity, Status: model.SnapshotStatusPlanning}); err != nil {
			return err
		}
		result := tx.WithContext(ctx).Model(&model.Pipeline{}).Where("id = ? AND config_version = ?", id, pipeline.ConfigVersion).
			Updates(map[string]any{"snapshot_uuid": identity, "config_version": gorm.Expr("config_version + 1")})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("%w", ErrPipelineConfigVersionConflict)
		}
		return nil
	})
}
