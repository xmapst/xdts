package repo

import (
	"context"
	"errors"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// LockPipelineIncludingDeleted must be called in the caller's transaction.
// Lifecycle intents and reinitialization lock the same row before mutation.
func LockPipelineIncludingDeleted(ctx context.Context, tx *gorm.DB, id uuidx.UUID) (*model.Pipeline, error) {
	var p model.Pipeline
	if err := tx.WithContext(ctx).Unscoped().Clauses(clause.Locking{Strength: "UPDATE"}).First(&p, "id=?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrPipelineNotFound
		}
		return nil, err
	}
	return &p, nil
}

func GetPipelineStatusRecord(ctx context.Context, db *gorm.DB, id uuidx.UUID, side model.Side) (*model.PipelineStatusRecord, error) {
	var r model.PipelineStatusRecord
	err := db.WithContext(ctx).Where("pipeline_id=? AND side=?", id, side).First(&r).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &r, err
}

// SavePipelineStatusRecord requires the pipeline row lock; the upsert is part
// of the same transaction as actual state and snapshot state.
func SavePipelineStatusRecord(ctx context.Context, tx *gorm.DB, r *model.PipelineStatusRecord) error {
	return tx.WithContext(ctx).Clauses(clause.OnConflict{UpdateAll: true}).Create(r).Error
}
