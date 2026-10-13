// 告警仓储方法。
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

var (
	// ErrAlertRuleNotFound 表示告警规则不存在。
	ErrAlertRuleNotFound = errors.New("repo: 告警规则不存在")
	// ErrAlertRuleNameConflict 表示告警规则名称与已有记录冲突。
	ErrAlertRuleNameConflict = errors.New("repo: 告警规则名称已存在")
	// ErrAlertDedupeKeyEmpty 表示调用方没有给出去重键。
	//
	// 刻意做成错误而不是"空键就直接插一行"：UpsertAlertByDedupeKey 这个名字
	// 承诺了去重，传空键意味着调用方以为自己在去重而实际没有，而**两者的读数
	// 完全一样**（都成功返回、都能在表里查到行）。要插不去重的行请用 CreateAlert。
	ErrAlertDedupeKeyEmpty = errors.New("repo: 去重键为空，拒绝按去重方式写入告警")
)

const (
	defaultAlertPageSize = 20
	maxAlertPageSize     = 200
)

// normalizePage 把分页参数收敛到合法区间，返回 (limit, offset)。
//
// page 从 1 起。<=0 一律当 1，不报错：分页参数来自 URL query，前端少传或传 0
// 是常态，为此返回错误会让一个纯读接口在正常使用下报 4xx。
func normalizePage(page, pageSize int) (int, int) {
	if page <= 0 {
		page = 1
	}
	switch {
	case pageSize <= 0:
		pageSize = defaultAlertPageSize
	case pageSize > maxAlertPageSize:
		pageSize = maxAlertPageSize
	}
	return pageSize, (page - 1) * pageSize
}

// ListAlertRules 分页列出告警规则，按 created_at 倒序，同时返回总数
// （端点 GET /alert-rules）。
//
// 返回总数而不是只返回一页：前端要显示页码，而"还有没有下一页"用本页条数
// 推不出来（正好整页时推不出）。总数与本页在同一个事务外分别查询——这里
// 允许两者之间存在新插入的行：告警规则的增删是人工低频操作，为一个列表
// 接口开事务的代价高于它防住的那点不一致。
func ListAlertRules(ctx context.Context, db *gorm.DB, page, pageSize int) ([]model.AlertRule, int64, error) {
	limit, offset := normalizePage(page, pageSize)

	var total int64
	if err := db.WithContext(ctx).Model(&model.AlertRule{}).Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("repo: 统计告警规则失败: %w", err)
	}

	var rules []model.AlertRule
	err := db.WithContext(ctx).Order("created_at DESC, id DESC").
		Limit(limit).Offset(offset).Find(&rules).Error
	if err != nil {
		return nil, 0, fmt.Errorf("repo: 查询告警规则列表失败: %w", err)
	}
	return rules, total, nil
}

// GetAlertRule 按主键查询告警规则。
func GetAlertRule(ctx context.Context, db *gorm.DB, id uuidx.UUID) (*model.AlertRule, error) {
	var r model.AlertRule
	if err := db.WithContext(ctx).First(&r, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrAlertRuleNotFound
		}
		return nil, fmt.Errorf("repo: 查询告警规则失败: %w", err)
	}
	return &r, nil
}

// CreateAlertRule 创建告警规则（端点 POST /alert-rules）。
//
// 入参取指针：Create 会回填 gorm 生成的 ID 与时间戳，值传参会让调用方拿不到
// 新建行的主键，而 POST 的响应里要带它。
func CreateAlertRule(ctx context.Context, db *gorm.DB, rule *model.AlertRule) error {
	if err := model.ValidateAlertRuleConfiguration(*rule); err != nil {
		return err
	}
	enabled := rule.Enabled
	if err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(rule).Error; err != nil {
			return err
		}
		if !enabled {
			if err := tx.Model(rule).Update("enabled", false).Error; err != nil {
				return err
			}
			rule.Enabled = false
		}
		return nil
	}); err != nil {
		if isDuplicateKeyError(err) {
			return fmt.Errorf("%w: name=%q", ErrAlertRuleNameConflict, rule.Name)
		}
		return fmt.Errorf("repo: 创建告警规则失败: %w", err)
	}
	return nil
}

// AlertFilter 是告警列表的过滤条件（端点 GET /alerts）。
//
// 指针字段表示"不过滤"与"过滤成零值"的区别：Level 为 nil 是不限级别，
// 指向空串则是一个非法取值（会被拒绝），两者不能用同一个零值表达。
type AlertFilter struct {
	RuleID *uuidx.UUID
	Level  *model.EventLevel
	Status *model.AlertStatus
	Since  *time.Time
	Until  *time.Time
}

// ListAlerts 按条件分页列出告警，按 fired_at 倒序，同时返回总数
// （端点 GET /alerts）。
func ListAlerts(ctx context.Context, db *gorm.DB, f AlertFilter, page, pageSize int) ([]model.Alert, int64, error) {
	if f.Level != nil && !f.Level.Valid() {
		return nil, 0, fmt.Errorf("model: 未知的告警级别 %q", *f.Level)
	}
	if f.Status != nil && !f.Status.Valid() {
		return nil, 0, fmt.Errorf("model: 未知的告警状态 %q", *f.Status)
	}
	if f.Since != nil && f.Until != nil && f.Until.Before(*f.Since) {
		return nil, 0, fmt.Errorf("repo: 告警检索的 Until 不能早于 Since")
	}

	limit, offset := normalizePage(page, pageSize)

	where := func(tx *gorm.DB) *gorm.DB {
		tx = ReadResourceScope(ctx, tx, ReadAlert)
		if f.RuleID != nil {
			tx = tx.Where("rule_id = ?", *f.RuleID)
		}
		if f.Level != nil {
			tx = tx.Where("level = ?", *f.Level)
		}
		if f.Status != nil {
			tx = tx.Where("status = ?", *f.Status)
		}
		if f.Since != nil {
			tx = tx.Where("fired_at >= ?", *f.Since)
		}
		if f.Until != nil {
			tx = tx.Where("fired_at <= ?", *f.Until)
		}
		return tx
	}

	var total int64
	if err := where(db.WithContext(ctx).Model(&model.Alert{})).Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("repo: 统计告警失败: %w", err)
	}

	var alerts []model.Alert
	err := where(db.WithContext(ctx).Model(&model.Alert{})).
		Order("fired_at DESC, id DESC").Limit(limit).Offset(offset).Find(&alerts).Error
	if err != nil {
		return nil, 0, fmt.Errorf("repo: 查询告警列表失败: %w", err)
	}
	return alerts, total, nil
}

// CreateAlert 插入一条告警，不做任何去重（调用方确认每次触发都要独立成行时用）。
func CreateAlert(ctx context.Context, db *gorm.DB, a *model.Alert, now time.Time) error {
	if err := a.Validate(); err != nil {
		return err
	}
	if a.FiredAt.IsZero() {
		a.FiredAt = now
	}
	if a.HitCount <= 0 {
		a.HitCount = 1
	}
	if err := db.WithContext(ctx).Create(a).Error; err != nil {
		return fmt.Errorf("repo: 写入告警失败: %w", err)
	}
	return nil
}

// Lock the deduplication row before incrementing repeated alerts; concurrent
// observations must not create duplicate alert identities.
func UpsertAlertByDedupeKey(ctx context.Context, db *gorm.DB, a model.Alert, dedupeKey string, now time.Time) (*model.Alert, error) {
	if dedupeKey == "" {
		return nil, ErrAlertDedupeKeyEmpty
	}
	if err := a.Validate(); err != nil {
		return nil, err
	}

	var result model.Alert
	err := db.Transaction(func(tx *gorm.DB) error {
		var existing model.Alert
		findErr := tx.WithContext(ctx).
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("dedupe_key = ? AND status = ?", dedupeKey, model.AlertStatusFiring).
			Order("fired_at DESC, id DESC").
			First(&existing).Error

		switch {
		case findErr == nil:
			updates := map[string]any{
				"hit_count":    gorm.Expr("hit_count + 1"),
				"last_seen_at": now,
			}
			if err := tx.WithContext(ctx).Model(&model.Alert{}).
				Where("id = ?", existing.ID).Updates(updates).Error; err != nil {
				return fmt.Errorf("repo: 递增告警命中次数失败: %w", err)
			}
			// 重新读回：hit_count 用的是 SQL 表达式，内存里的 existing 拿不到
			// 递增后的值，而调用方要靠它判断是否首次触发。
			if err := tx.WithContext(ctx).First(&result, "id = ?", existing.ID).Error; err != nil {
				return fmt.Errorf("repo: 回读告警失败: %w", err)
			}
			return nil

		case errors.Is(findErr, gorm.ErrRecordNotFound):
			a.DedupeKey = dedupeKey
			if a.FiredAt.IsZero() {
				a.FiredAt = now
			}
			a.HitCount = 1
			a.LastSeenAt = nil
			if err := tx.WithContext(ctx).Create(&a).Error; err != nil {
				return fmt.Errorf("repo: 写入告警失败: %w", err)
			}
			result = a
			return nil

		default:
			return fmt.Errorf("repo: 查询同去重键告警失败: %w", findErr)
		}
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// ResolveAlertsByDedupeKey 把同键且未恢复的告警标记为已恢复，返回受影响行数。
//
// 与 Upsert 成对：没有它，去重键会永久锁定在第一行上——那行永远是 firing，
// 于是"恢复后再次触发要能插新行"这个前提永远不成立，dedupe_key 不设唯一索引
// 的理由也就落空了。⇒ 这一对必须一起存在，缺一个则另一个的语义是错的。
func ResolveAlertsByDedupeKey(ctx context.Context, db *gorm.DB, dedupeKey string, now time.Time) (int64, error) {
	if dedupeKey == "" {
		return 0, ErrAlertDedupeKeyEmpty
	}
	res := db.WithContext(ctx).Model(&model.Alert{}).
		Where("dedupe_key = ? AND status = ?", dedupeKey, model.AlertStatusFiring).
		Updates(map[string]any{"status": model.AlertStatusResolved, "resolved_at": now})
	if res.Error != nil {
		return 0, fmt.Errorf("repo: 标记告警恢复失败: %w", res.Error)
	}
	return res.RowsAffected, nil
}
