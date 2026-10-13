package repo

import (
	"context"
	"database/sql"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

func RenamePipeline(ctx context.Context, db *gorm.DB, id uuidx.UUID, name string) error {
	if err := normalizeResourceName(&name); err != nil {
		return err
	}
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		current, err := LockPipelineIncludingDeleted(ctx, tx, id)
		if err != nil {
			return err
		}
		if current.DeletedAt.Valid {
			return ErrPipelineNotFound
		}
		if current.Name == name {
			return nil
		}
		if err := tx.Model(&model.Pipeline{}).Where("id=?", id).Update("name", name).Error; err != nil {
			if duplicateIndex(err, "uk_pipelines_name") {
				return ErrPipelineNameConflict
			}
			return err
		}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}
