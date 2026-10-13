package repo

import (
	"context"
	"errors"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// The caller holds the topology lock and commits the version with its pipeline.
func CreatePipelineRulesVersion(ctx context.Context, tx *gorm.DB, pipelineID uuidx.UUID, rules model.JSON, userID uuidx.UUID) (*model.StrategyVersion, error) {
	var strategy model.Strategy
	err := tx.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).Where("owner_pipeline_id=?", pipelineID).First(&strategy).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		strategy = model.Strategy{Name: "pipeline-" + pipelineID.String(), OwnerPipelineID: &pipelineID}
		if err := CreateStrategy(ctx, tx, &strategy); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	version := model.StrategyVersion{StrategyID: strategy.ID, Version: strategy.CurrentVersion + 1, Rules: rules, CreatedBy: userID}
	if err := tx.WithContext(ctx).Create(&version).Error; err != nil {
		return nil, err
	}
	if err := tx.WithContext(ctx).Model(&model.Strategy{}).Where("id=?", strategy.ID).Update("current_version", version.Version).Error; err != nil {
		return nil, err
	}
	return &version, nil
}

func AssignPipelineRulesOwner(ctx context.Context, tx *gorm.DB, strategyID, pipelineID uuidx.UUID) error {
	return tx.WithContext(ctx).Model(&model.Strategy{}).Where("id=? AND owner_pipeline_id IS NULL", strategyID).Update("owner_pipeline_id", pipelineID).Error
}
