// 系统设置仓储方法。
package repo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
)

var (
	// ErrSettingNotFound 表示该键不存在。
	ErrSettingNotFound = errors.New("repo: 设置项不存在")
	// ErrSettingKeyEmpty 表示传入的键为空串。
	//
	// 空键必须显式拒绝而不是当成一个普通键写进去：settings 的唯一键是 key，
	// 空键会占掉一行并且在任何按键读取里都取不到，属于"非法输入静默写进权威
	// 状态"。
	ErrSettingKeyEmpty = errors.New("repo: 设置项键为空")
	// ErrSettingValueInvalidJSON 表示传入的值不是合法 JSON。
	//
	// value 列是 MySQL JSON 类型，非法值最终也会被数据库拒绝，但那时拿到的是
	// 一个不可判别的驱动错误。在这一层先校验，调用方才能把它映射成 400 而不是
	// 500。
	ErrSettingValueInvalidJSON = errors.New("repo: 设置项值不是合法 JSON")
)

// ListSettings 列出全部设置项（按 key 升序）。
//
// 不分页：设置项是人工维护的运行参数，数量由代码里认识的键封顶，不会随业务
// 数据增长。
func ListSettings(ctx context.Context, db *gorm.DB) ([]model.Setting, error) {
	var rows []model.Setting
	// key 是 MySQL 保留字，ORDER BY 里同样要反引号转义。
	if err := db.WithContext(ctx).Order("`key` ASC").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("repo: 查询设置项列表失败: %w", err)
	}
	return rows, nil
}

// GetSetting 按键取一个设置项，不存在时返回 ErrSettingNotFound。
func GetSetting(ctx context.Context, db *gorm.DB, key string) (*model.Setting, error) {
	if key == "" {
		return nil, ErrSettingKeyEmpty
	}
	var s model.Setting
	err := db.WithContext(ctx).Where("`key` = ?", key).First(&s).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrSettingNotFound
		}
		return nil, fmt.Errorf("repo: 查询设置项失败: %w", err)
	}
	return &s, nil
}

// UpsertSetting 逐键写入一个设置项。
//
// PUT /settings 若实现成"删光再按请求体重建"，两个并发请求各自只带自己关心
// 的那几个键时，后到的那个会把前一个刚写的键连带删掉——而两个请求都会收到
// 200。逐键 upsert 让互不相干的键互不影响，只有同一个键的并发写才需要裁决。
//
// 调用方要把"改设置"和"写审计日志"放在同一个事务里（改了但没留痕，或留痕了
// 但没改，两种都是审计事实与权威状态不一致）。函数因此不自己开事务，接到
// 什么句柄就在什么句柄上执行；传 *gorm.DB 是自动提交，传 tx 就并入调用方的
// 事务。
//
// 重复提交同一个 (key, value) 不会产生新行，也不会改变 value；只有 updated_at
// 与 updated_by 会刷新。这是红线「有副作用的跨边界流程可重试」要的形状。
func UpsertSetting(ctx context.Context, db *gorm.DB, key, value string, updatedBy uuidx.UUID, now time.Time) error {
	if key == "" {
		return ErrSettingKeyEmpty
	}
	if !json.Valid([]byte(value)) {
		// 不把 value 本身放进错误消息：设置项值可能承载运维填入的内容，
		// 错误会一路进日志。这里只报键。
		return fmt.Errorf("%w: key=%s", ErrSettingValueInvalidJSON, key)
	}
	if key == PrecheckValiditySetting || key == "retention.logs_seconds" || key == "retention.metrics_seconds" || key == "retention.events_seconds" || key == "retention.conflicts_seconds" {
		var seconds int64
		maximum := int64(3650 * 86400)
		if key == PrecheckValiditySetting {
			maximum = 86400
		}
		if json.Unmarshal([]byte(value), &seconds) != nil || seconds < 1 || seconds > maximum {
			return ErrSettingValueInvalidJSON
		}
	}

	row := model.Setting{
		Key:       key,
		Value:     model.JSON(value),
		UpdatedBy: updatedBy,

		CreatedAt: now,
		UpdatedAt: now}

	err := db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "key"}},
		DoUpdates: clause.Assignments(map[string]any{
			"value":      row.Value,
			"updated_by": updatedBy,
			"updated_at": now,
		}),
	}).Create(&row).Error
	if err != nil {
		return fmt.Errorf("repo: 写入设置项失败: %w", err)
	}
	return nil
}

// DeleteSetting 按键删除一个设置项，不存在时返回 ErrSettingNotFound。
//
// 返回哨兵而不是静默成功：删除一个不存在的键通常意味着调用方拼错了键名，
// 静默成功会让这个错误一直留在配置面板上。
func DeleteSetting(ctx context.Context, db *gorm.DB, key string) error {
	if key == "" {
		return ErrSettingKeyEmpty
	}
	res := db.WithContext(ctx).Where("`key` = ?", key).Delete(&model.Setting{})
	if res.Error != nil {
		return fmt.Errorf("repo: 删除设置项失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrSettingNotFound
	}
	return nil
}
