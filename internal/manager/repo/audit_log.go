// 审计日志仓储方法（需求 MGR-USR-06，P1：登录/登出、注册码生成/作废、
// pipeline 启停与删除、修改位点、重新初始化、跳过错误、确认回环警告、
// 策略变更、端点变更）。
//
// 写入 + 只读检索：审计不像 logs/events 那样有独立的批量入库路径，动作
// 发生的同时就要落一条审计记录，因此这里提供 CreateAuditLog。
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
	defaultAuditLogQueryLimit = 50
	maxAuditLogQueryLimit     = 500
)

// CreateAuditLog 写入一条审计记录。ts 由调用方显式传入（控制层持有当前
// 时间，遵循「只有控制层读取当前时间」的约定），不用 gorm
// 自动时间戳——审计记录的业务时间是动作发生的时刻，不是入库时刻。
//
// 不自动清理（需求 O-10：audit_logs 不设保留期），因此这里
// 没有配套的 DeleteInBatches/DropExpiredPartitions 调用——审计记录要能
// 无限期追溯，清理会破坏这个前提。
func CreateAuditLog(ctx context.Context, db *gorm.DB, a *model.AuditLog, ts time.Time) error {
	a.Ts = ts
	if err := a.Validate(); err != nil {
		return err
	}
	if err := db.WithContext(ctx).Create(a).Error; err != nil {
		return fmt.Errorf("repo: 写入审计日志失败: %w", err)
	}
	return nil
}

// AuditLogQuery 是审计日志检索的过滤条件与分页参数（需求 MGR-USR-06）。
//
// audit_logs 表未分区（见 internal/manager/repo/ddl_audit_log.go），Since/Until 为零值表示不
// 限制起止，只是普通过滤条件。
type AuditLogQuery struct {
	Since, Until time.Time

	// UserID 为 uuidx.Nil 表示不按操作人过滤。
	UserID uuidx.UUID
	// Action 为空字符串表示不按动作类型过滤。
	Action model.AuditAction
	// TargetType 为空字符串表示不按目标类型过滤；TargetID 为 uuidx.Nil
	// 表示不按具体目标过滤（只有 TargetType 非空时按目标类型过滤才有
	// 意义，两者可以独立指定）。
	TargetType model.AuditTargetType
	TargetID   uuidx.UUID

	// Limit 是单页条数，<=0 时取 defaultAuditLogQueryLimit，超过
	// maxAuditLogQueryLimit 时截断为 maxAuditLogQueryLimit。
	Limit int

	// BeforeTs/BeforeID 是游标分页的续页起点，语义与 LogQuery 同名字段
	// 一致：传入上一页最后一条记录的 (ts, id)，本次只返回比它更早的记录
	// （结果按 ts DESC, id DESC 排序）。翻页时必须同时传两者。
	BeforeTs time.Time
	BeforeID uuidx.UUID

	// Offset supports bounded numbered pages; it cannot be combined with a cursor.
	Offset int
	// Actor matches the username snapshot recorded when the action occurred.
	Actor string
}

// ListAuditLogs 按条件检索审计日志，按 ts 倒序（最新的在前）分页返回
// （需求 MGR-USR-06）。
func ListAuditLogs(ctx context.Context, db *gorm.DB, q AuditLogQuery) ([]model.AuditLog, error) {
	tx, err := auditLogScope(ctx, db, q)
	if err != nil {
		return nil, err
	}
	limit := q.Limit
	switch {
	case limit <= 0:
		limit = defaultAuditLogQueryLimit
	case limit > maxAuditLogQueryLimit:
		limit = maxAuditLogQueryLimit
	}
	if !q.BeforeTs.IsZero() {
		tx = tx.Where("(ts < ?) OR (ts = ? AND id < ?)", q.BeforeTs, q.BeforeTs, q.BeforeID)
	}
	var logs []model.AuditLog
	if err := tx.Order("ts DESC, id DESC").Offset(q.Offset).Limit(limit).Find(&logs).Error; err != nil {
		return nil, fmt.Errorf("repo: 查询审计日志失败: %w", err)
	}
	return logs, nil
}

// CountAuditLogs uses the same filters; pagination fields do not narrow the total.
func CountAuditLogs(ctx context.Context, db *gorm.DB, q AuditLogQuery) (int64, error) {
	tx, err := auditLogScope(ctx, db, q)
	if err != nil {
		return 0, err
	}
	var total int64
	if err := tx.Count(&total).Error; err != nil {
		return 0, fmt.Errorf("repo: 统计审计日志失败: %w", err)
	}
	return total, nil
}

func auditLogScope(ctx context.Context, db *gorm.DB, q AuditLogQuery) (*gorm.DB, error) {
	if !q.Until.IsZero() && q.Until.Before(q.Since) {
		return nil, fmt.Errorf("repo: 审计日志检索的 Until 不能早于 Since")
	}
	if q.Action != "" && !q.Action.Valid() {
		return nil, fmt.Errorf("model: 未知的审计动作 %q", q.Action)
	}
	if q.TargetType != "" && !q.TargetType.Valid() {
		return nil, fmt.Errorf("model: 未知的审计目标类型 %q", q.TargetType)
	}

	if q.Offset < 0 || q.Offset > 1000000 || (q.Offset != 0 && !q.BeforeTs.IsZero()) || q.BeforeTs.IsZero() != q.BeforeID.IsZero() {
		return nil, fmt.Errorf("repo: invalid audit pagination")
	}

	tx := db.WithContext(ctx).Model(&model.AuditLog{})
	if !q.Since.IsZero() {
		tx = tx.Where("ts >= ?", q.Since)
	}
	if !q.Until.IsZero() {
		tx = tx.Where("ts <= ?", q.Until)
	}
	if !q.UserID.IsZero() {
		tx = tx.Where("user_id = ?", q.UserID)
	}
	if q.Actor != "" {
		tx = tx.Where("JSON_UNQUOTE(JSON_EXTRACT(detail, '$.username')) = ?", q.Actor)
	}
	if q.Action != "" {
		tx = tx.Where("action = ?", q.Action)
	}
	if q.TargetType != "" {
		tx = tx.Where("target_type = ?", q.TargetType)
	}
	if !q.TargetID.IsZero() {
		tx = tx.Where("target_id = ?", q.TargetID)
	}
	return tx, nil
}
