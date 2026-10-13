// 预检查仓储方法。
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

var (
	// ErrPrecheckNotFound 表示该 pipeline 没有预检查记录。
	ErrPrecheckNotFound = errors.New("repo: 预检查记录不存在")
	// ErrPrecheckAlreadyConfirmed 表示该预检查项已被确认过。
	ErrPrecheckAlreadyConfirmed = errors.New("repo: 该预检查项已被确认")
	// ErrPrecheckNotConfirmable 表示该预检查项不需要（也不允许）确认。
	//
	// 只有 warning 级别的项需要用户显式确认（阻断项不允许启动、
	// info 项无需确认）。对 blocking 项返回"已确认"会让调用方以为可以启动了，
	// 所以这一档必须与 AlreadyConfirmed 分开。
	ErrPrecheckNotConfirmable = errors.New("repo: 该预检查项无需确认或不允许确认")
)

// GetLatestPrecheck 取某 pipeline 最新一条预检查记录（按 checked_at 倒序）。
//
// 无记录时返回 ErrPrecheckNotFound，而不是 (零值, nil)：零值的 Status 是空串、
// Level 是空串，两者都不是合法枚举，调用方拿到它只能靠"ID 是不是 Nil"来判断
// 有没有记录——那是一个没人会记得检查的判据。
func GetLatestPrecheck(ctx context.Context, db *gorm.DB, pipelineID uuidx.UUID) (*model.PipelinePrecheck, error) {
	var p model.PipelinePrecheck
	err := db.WithContext(ctx).Where("pipeline_id = ?", pipelineID).
		Order("checked_at DESC, id DESC").First(&p).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrPrecheckNotFound
		}
		return nil, fmt.Errorf("repo: 查询最新预检查失败: %w", err)
	}
	return &p, nil
}

// ListPrechecks 列出某 pipeline 在指定配置版本下的全部预检查项（按 item 升序）。
func ListPrechecks(ctx context.Context, db *gorm.DB, pipelineID uuidx.UUID, configVersion uint64) ([]model.PipelinePrecheck, error) {
	tx := db.WithContext(ctx).Where("pipeline_id = ?", pipelineID)
	if configVersion > 0 {
		tx = tx.Where("config_version = ?", configVersion)
	}
	var ps []model.PipelinePrecheck
	if err := tx.Order("item ASC, checked_at DESC").Find(&ps).Error; err != nil {
		return nil, fmt.Errorf("repo: 查询预检查列表失败: %w", err)
	}
	return ps, nil
}

// InsertPrecheck 写入一条预检查结果。
//
// checkedAt 由调用方传入而不是用 gorm 自动时间：检查发生的时刻是业务时间，
// 不是入库时刻（同 CreateAuditLog 的理由）。
func InsertPrecheck(ctx context.Context, db *gorm.DB, rec *model.PipelinePrecheck, checkedAt time.Time) error {
	if err := rec.Validate(); err != nil {
		return err
	}
	if rec.PipelineID.IsZero() {
		return fmt.Errorf("repo: 预检查记录缺少 pipeline_id")
	}
	if rec.Item == "" {
		return fmt.Errorf("repo: 预检查记录缺少 item")
	}
	rec.CheckedAt = checkedAt
	if err := db.WithContext(ctx).Create(rec).Error; err != nil {
		return fmt.Errorf("repo: 写入预检查记录失败: %w", err)
	}
	return nil
}

// ConfirmPrecheck 记录用户对某个 warning 级预检查项的显式确认。
//
// # 幂等（红线「有副作用的跨边界流程可重试」）
//
// 重复确认**不重复生效**：函数在事务内先取行并持写锁，已有 ConfirmedBy 时
// 返回 ErrPrecheckAlreadyConfirmed 而不是再写一次。这样"网络超时后重试"不会
// 把确认人改成第二次点的那个人——确认人是审计要追溯的事实，改写它等于伪造。
//
// # 为什么 pipelineID 与 precheckID 都要传
//
// 只用 precheckID 就能定位行，但那样一个越权调用（拿到别的 pipeline 的
// precheck id）会直接生效。红线「服务端是唯一权威：服务端自己校验身份、
// 归属与数值边界」⇒ 归属校验必须在这一层做，不能指望 HTTP 层先查一次。
func ConfirmPrecheck(ctx context.Context, db *gorm.DB, pipelineID, precheckID, userID uuidx.UUID, now time.Time) error {
	var item model.PipelinePrecheck
	if err := db.WithContext(ctx).First(&item, "id=? AND pipeline_id=?", precheckID, pipelineID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrPrecheckNotFound
		}
		return err
	}
	if item.BatchID.IsZero() {
		return ErrPrecheckNotConfirmable
	}
	return ConfirmPrecheckRun(ctx, db, pipelineID, item.BatchID, []uuidx.UUID{precheckID}, userID, now)
}
