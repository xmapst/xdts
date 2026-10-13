// 位点仓储方法。
package repo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
)

var ErrCheckpointNotFound = errors.New("repo: 位点记录不存在")

// ListPipelineCheckpoints 分页列出某 pipeline 的位点行（按 side 升序）。
//
// 一条 pipeline 的位点行数由 Side 枚举封顶（当前 2 个方向），所以分页在这里
// 近乎恒等；仍然保留分页参数与 total，是因为端点契约是列表形状，改成"一次
// 全返回"以后若 Side 扩展就是一次破坏性变更。
func ListPipelineCheckpoints(ctx context.Context, db *gorm.DB, pipelineID uuidx.UUID, page, pageSize int) ([]model.PipelineCheckpoint, int64, error) {
	limit, offset := normalizePage(page, pageSize)

	base := db.WithContext(ctx).Model(&model.PipelineCheckpoint{}).Where("pipeline_id = ?", pipelineID)

	var total int64
	if err := base.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("repo: 统计位点总数失败: %w", err)
	}

	var rows []model.PipelineCheckpoint
	if err := base.Order("side ASC").Limit(limit).Offset(offset).Find(&rows).Error; err != nil {
		return nil, 0, fmt.Errorf("repo: 查询位点列表失败: %w", err)
	}
	return rows, total, nil
}

// GetLatestCheckpoint 取某 pipeline 事件时间最新的一条位点行（progress 端点用）。
//
// 无记录返回 ErrCheckpointNotFound：零值的 Side 不是合法枚举、EventTime 是
// Go 零时间，调用方拿零值只能靠"ID 是否为 Nil"判断有无——那是个没人会记得
// 检查的判据。
func GetLatestCheckpoint(ctx context.Context, db *gorm.DB, pipelineID uuidx.UUID) (*model.PipelineCheckpoint, error) {
	var c model.PipelineCheckpoint
	err := db.WithContext(ctx).Where("pipeline_id = ?", pipelineID).
		Order("event_time DESC, id DESC").First(&c).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrCheckpointNotFound
		}
		return nil, fmt.Errorf("repo: 查询最新位点失败: %w", err)
	}
	return &c, nil
}

// GetCheckpoint 取某 pipeline 某一端的位点行。
func GetCheckpoint(ctx context.Context, db *gorm.DB, pipelineID uuidx.UUID, side model.Side) (*model.PipelineCheckpoint, error) {
	if !side.Valid() {
		return nil, fmt.Errorf("repo: 未知的 side %q", side)
	}
	var c model.PipelineCheckpoint
	err := db.WithContext(ctx).Where("pipeline_id = ? AND side = ?", pipelineID, side).First(&c).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrCheckpointNotFound
		}
		return nil, fmt.Errorf("repo: 查询位点失败: %w", err)
	}
	return &c, nil
}

// UpsertCheckpoint locks the role record and accepts only a newer observation.
// Repeated and out-of-order reports cannot roll back the stored boundary.
func UpsertCheckpoint(ctx context.Context, db *gorm.DB, in model.PipelineCheckpoint) (applied bool, err error) {
	if err := in.Validate(); err != nil {
		return false, err
	}
	if in.PipelineID.IsZero() {
		return false, fmt.Errorf("repo: 位点上报缺少 pipeline_id")
	}
	if in.EventTime.IsZero() {
		return false, fmt.Errorf("repo: 位点上报缺少 event_time")
	}

	err = db.Transaction(func(tx *gorm.DB) error {
		var cur model.PipelineCheckpoint
		qerr := tx.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("pipeline_id = ? AND side = ?", in.PipelineID, in.Side).First(&cur).Error
		switch {
		case errors.Is(qerr, gorm.ErrRecordNotFound):
			row := in
			row.ID = uuidx.Nil
			if cerr := tx.WithContext(ctx).Create(&row).Error; cerr != nil {
				// 并发首插时唯一键冲突：此时另一条已落地，交给调用方重试，
				// 不在这里静默吞掉。
				if isDuplicateKeyError(cerr) {
					return fmt.Errorf("repo: 位点并发首插冲突，请重试: %w", cerr)
				}
				return fmt.Errorf("repo: 写入位点失败: %w", cerr)
			}
			applied = true
			return nil
		case qerr != nil:
			return fmt.Errorf("repo: 查询位点失败: %w", qerr)
		}

		if !in.EventTime.After(cur.EventTime) {
			applied = false
			return nil
		}
		uerr := tx.WithContext(ctx).Model(&model.PipelineCheckpoint{}).Where("id = ?", cur.ID).
			Updates(map[string]any{
				"read_gtid":      in.ReadGTIDSet,
				"published_gtid": in.PublishedGTIDSet,
				"applied_gtid":   in.AppliedGTIDSet,
				"epoch":          in.Epoch,
				"batch_seq":      in.BatchSeq,
				"event_time":     in.EventTime,
				"updated_at":     time.Now(),
			}).Error
		if uerr != nil {
			return fmt.Errorf("repo: 更新位点失败: %w", uerr)
		}
		applied = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return applied, nil
}
