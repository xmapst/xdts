// 已验证：三个导出函数各自的生产调用方（排除 _test.go 后逐个列出）。
// 未验证：dispatch 之外是否还有别的进程会写这张表。
package repo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// ErrPipelineReleaseNotFound 表示按 id 未查到下发记录。
var ErrPipelineReleaseNotFound = errors.New("repo: 下发记录不存在")

// 反过来看 pipeline_checkpoints（internal/manager/repo/ddl_checkpoint.go）：确实显式声明了
// UNIQUE KEY uk_pipeline_checkpoints_pipeline_side (pipeline_id, side)。
// 两张表结构相似（都按 pipeline+side 归属）、建表迁移紧挨着（v15/v16），
// 作者显然知道"这张表要不要唯一约束"该怎么写——checkpoints 写了，
// releases 没写，是两种表语义不同，不是漏掉了后者。
func CreatePipelineRelease(ctx context.Context, db *gorm.DB, r *model.PipelineRelease) error {
	if r.Status == "" {
		r.Status = model.ReleaseStatusPending
	}
	if err := r.Validate(); err != nil {
		return err
	}
	if err := db.WithContext(ctx).Create(r).Error; err != nil {
		return fmt.Errorf("repo: 记录 pipeline 下发失败: %w", err)
	}
	return nil
}

// status 必须是终态（applied/failed/rejected 之一），不接受 pending——
// pending 只应该在 CreatePipelineRelease 里作为初始值出现，"记录应用
// 结果"这个动作把一行从待定推进到确定，没有"结果是 pending"这回事，
// 传入 pending 视为调用方的编程错误，直接拒绝。
//
// AppliedAt 只在 status == ReleaseStatusApplied 时写入（值为 now）；
// failed/rejected 不写这一列，保持它在 model.PipelineRelease.AppliedAt
// 字段注释里定义的语义（只表示"确认生效的时刻"）。now 由调用方显式传入
// （控制层持有当前时间的既定约定，同 SoftDeletePipeline/
// RecordPipelineCleanupAttempt），本函数不自己取当前时间。
func RecordPipelineReleaseResult(ctx context.Context, db *gorm.DB, id uuidx.UUID, status model.ReleaseStatus, message string, now time.Time) error {
	if status == model.ReleaseStatusPending {
		return fmt.Errorf("model: 下发结果不能记为 pending（pending 只在创建下发记录时由 CreatePipelineRelease 设置）")
	}
	if !status.Valid() {
		return fmt.Errorf("model: 未知的下发状态 %q", status)
	}

	updates := map[string]any{
		"status":  status,
		"message": message,
	}
	if status == model.ReleaseStatusApplied {
		updates["applied_at"] = now
	}

	res := db.WithContext(ctx).Model(&model.PipelineRelease{}).Where("id = ?", id).Updates(updates)
	if res.Error != nil {
		return fmt.Errorf("repo: 写回 pipeline 下发结果失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrPipelineReleaseNotFound
	}
	return nil
}

// Release history is scoped to its pipeline and removed with pipeline cleanup.
func ListPipelineReleases(ctx context.Context, db *gorm.DB, pipelineID uuidx.UUID) ([]model.PipelineRelease, error) {
	var rs []model.PipelineRelease
	err := db.WithContext(ctx).
		Where("pipeline_id = ?", pipelineID).
		Order("created_at DESC, id DESC").
		Find(&rs).Error
	if err != nil {
		return nil, fmt.Errorf("repo: 查询 pipeline 下发历史失败: %w", err)
	}
	return rs, nil
}

// GetLatestAppliedPipelineRelease returns the last confirmed release for one role.
// Unconfirmed or failed releases do not establish an applied digest.
func GetLatestAppliedPipelineRelease(ctx context.Context, db *gorm.DB, pipelineID uuidx.UUID, side model.Side) (*model.PipelineRelease, error) {
	if !side.Valid() {
		return nil, fmt.Errorf("model: 未知的 side %q", side)
	}
	var r model.PipelineRelease
	err := db.WithContext(ctx).
		Where("pipeline_id = ? AND side = ? AND status = ?", pipelineID, side, model.ReleaseStatusApplied).
		Order("config_version DESC, created_at DESC, id DESC").
		First(&r).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrPipelineReleaseNotFound
		}
		return nil, fmt.Errorf("repo: 查询最后一次成功下发记录失败: %w", err)
	}
	return &r, nil
}
