// 写入冲突记录检索仓储方法。
package repo

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
)

const (
	defaultConflictQueryLimit = 50
	maxConflictQueryLimit     = 500
)

// ConflictQuery filters conflict observations with a bounded time window and pagination.
type ConflictQuery struct {
	Since, Until time.Time

	PipelineID uuidx.UUID

	// ConflictType 为空字符串表示不按该维度过滤。有
	// idx_conflicts_type_ts (conflict_type, ts) 承接。
	ConflictType model.ConflictType

	// Resolution 为空字符串表示不按该维度过滤。conflicts 表没有为
	// resolution 单独建索引：它只有 4 个取值（source_wins/target_wins/
	// skipped/stopped），基数很低，且 conflicts 本身只在真实发生写入
	// 冲突时才产生一行，写入频率远低于 logs/events，不是需要靠专门索引
	// 才能撑住的高频表；配合 PipelineID/ConflictType/时间范围任一维度
	// 过滤后剩余行数通常已经很小，属于「索引先筛到小结果集，再对普通列
	// 做一次过滤」，不需要为它单独加索引。
	Resolution model.ConflictResolution

	// Limit 是单页条数，<=0 时取 defaultConflictQueryLimit，超过
	// maxConflictQueryLimit 时截断为 maxConflictQueryLimit。
	Limit int

	// BeforeTs/BeforeID 是游标分页的续页起点，语义与 LogQuery/EventQuery
	// 同名字段一致：传入上一页最后一条记录的 (ts, id)，本次只返回比它
	// 更早的记录（结果按 ts DESC, id DESC 排序）。翻页时必须同时传两者。
	BeforeTs time.Time
	BeforeID uuidx.UUID
}

// applyConflictFilters 把 ConflictQuery 里"这条记录是否满足过滤条件"
// 相关的部分（可选时间范围 + pipeline/冲突类型/处理方式）应用到 tx 上，
// 并做相应的参数校验。ListConflicts 与 CountConflicts 从一开始就共用
// 这一份逻辑，不是先写 List 再补 Count——理由与 internal/manager/repo/log.go 的
// applyLogFilters、event.go 的 applyEventFilters 相同：两套过滤条件
// 拼接代码各自维护迟早会漂移，届时会出现"列表 20 条、总数 35 条"这类从
// 现象上很难联想到"两处 WHERE 子句不一致"的 bug。
//
// 不包含 BeforeTs/BeforeID 游标条件，也不设置 Order/Limit：这些是
// ListConflicts 分页专用的参数，与"总共有多少条满足过滤条件"无关。
func applyConflictFilters(tx *gorm.DB, q ConflictQuery) (*gorm.DB, error) {
	tx = ReadResourceScope(tx.Statement.Context, tx, ReadConflict)
	if !q.Until.IsZero() && q.Until.Before(q.Since) {
		return nil, fmt.Errorf("repo: 冲突记录检索的 Until 不能早于 Since")
	}
	if q.ConflictType != "" && !q.ConflictType.Valid() {
		return nil, fmt.Errorf("model: 未知的冲突类型 %q", q.ConflictType)
	}
	if q.Resolution != "" && !q.Resolution.Valid() {
		return nil, fmt.Errorf("model: 未知的冲突处理方式 %q", q.Resolution)
	}

	if !q.Since.IsZero() {
		tx = tx.Where("ts >= ?", q.Since)
	}
	if !q.Until.IsZero() {
		tx = tx.Where("ts <= ?", q.Until)
	}
	if !q.PipelineID.IsZero() {
		tx = tx.Where("pipeline_id = ?", q.PipelineID)
	}
	if q.ConflictType != "" {
		tx = tx.Where("conflict_type = ?", q.ConflictType)
	}
	if q.Resolution != "" {
		tx = tx.Where("resolution = ?", q.Resolution)
	}
	return tx, nil
}

// ListConflicts 按条件检索冲突记录，按 ts 倒序（最新的在前）分页返回
// （需求 MGR-MON-03）。
func ListConflicts(ctx context.Context, db *gorm.DB, q ConflictQuery) ([]model.Conflict, error) {
	tx, err := applyConflictFilters(db.WithContext(ctx).Model(&model.Conflict{}), q)
	if err != nil {
		return nil, err
	}

	limit := q.Limit
	switch {
	case limit <= 0:
		limit = defaultConflictQueryLimit
	case limit > maxConflictQueryLimit:
		limit = maxConflictQueryLimit
	}

	if !q.BeforeTs.IsZero() {
		tx = tx.Where("(ts < ?) OR (ts = ? AND id < ?)", q.BeforeTs, q.BeforeTs, q.BeforeID)
	}

	var conflicts []model.Conflict
	if err := tx.Order("ts DESC, id DESC").Limit(limit).Find(&conflicts).Error; err != nil {
		return nil, fmt.Errorf("repo: 查询冲突记录失败: %w", err)
	}
	return conflicts, nil
}

func CountConflicts(ctx context.Context, db *gorm.DB, q ConflictQuery) (int64, error) {
	tx, err := applyConflictFilters(db.WithContext(ctx).Model(&model.Conflict{}), q)
	if err != nil {
		return 0, err
	}
	var count int64
	if err := tx.Count(&count).Error; err != nil {
		return 0, fmt.Errorf("repo: 统计冲突记录总数失败: %w", err)
	}
	return count, nil
}
