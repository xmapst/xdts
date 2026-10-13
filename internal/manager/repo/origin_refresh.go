package repo

import (
	"context"
	"time"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

// OriginRefresh retains completed rows so later changes cannot reuse a revision.
type OriginRefresh struct {
	CenterID          uuidx.UUID `gorm:"column:center_id"`
	Revision          uint64     `gorm:"column:revision"`
	CompletedRevision uint64     `gorm:"column:completed_revision"`
	UpdatedAt         time.Time  `gorm:"column:updated_at"`
}

func (OriginRefresh) TableName() string { return "center_origin_refresh" }

func markOriginRefresh(ctx context.Context, tx *gorm.DB, centerID uuidx.UUID) error {
	return tx.WithContext(ctx).Exec(`INSERT INTO center_origin_refresh
 (center_id, revision, completed_revision, updated_at) VALUES (?,1,0,?)
 ON DUPLICATE KEY UPDATE revision=revision+1, updated_at=VALUES(updated_at)`, centerID, time.Now().UTC()).Error
}

func ListPendingOriginRefreshes(ctx context.Context, db *gorm.DB) ([]OriginRefresh, error) {
	var rows []OriginRefresh
	err := db.WithContext(ctx).Where("revision > completed_revision").Order("updated_at ASC, center_id ASC").Find(&rows).Error
	return rows, err
}

// CompleteOriginRefresh uses the revision read before delivery. A concurrent
// change remains pending even when the older delivery subsequently finishes.
func CompleteOriginRefresh(ctx context.Context, db *gorm.DB, centerID uuidx.UUID, revision uint64) error {
	return db.WithContext(ctx).Model(&OriginRefresh{}).Where("center_id=? AND revision=?", centerID, revision).
		Update("completed_revision", revision).Error
}

// ListPipelinesAffectedByOrigin includes every edge of each topology containing
// this center, because CommonConfig carries the topology's complete origin map.
func ListPipelinesAffectedByOrigin(ctx context.Context, db *gorm.DB, centerID uuidx.UUID) ([]model.Pipeline, error) {
	var pipelines []model.Pipeline
	err := db.WithContext(ctx).Where(`topology_id IN (
 SELECT p.topology_id FROM pipelines p JOIN endpoints e
 ON (e.id=p.source_endpoint_id OR e.id=p.target_endpoint_id)
 WHERE p.deleted_at IS NULL AND e.center_id=?)`, centerID).
		Order("id ASC").Find(&pipelines).Error
	return pipelines, err
}

func PipelineHasPendingOriginRefresh(ctx context.Context, db *gorm.DB, topologyID uuidx.UUID) (bool, error) {
	var count int64
	err := db.WithContext(ctx).Model(&OriginRefresh{}).Where(`revision>completed_revision AND center_id IN (
 SELECT e.center_id FROM endpoints e JOIN pipelines p
 ON (e.id=p.source_endpoint_id OR e.id=p.target_endpoint_id)
 WHERE p.topology_id=? AND p.deleted_at IS NULL)`, topologyID).Count(&count).Error
	return count != 0, err
}
