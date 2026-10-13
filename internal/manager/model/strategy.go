package model

import (
	"time"

	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

// Strategy 是一组同步规则的容器：过滤、映射、转换、冲突处理等
// （需求 MGR-STR-*）。规则本身的版本化内容在 StrategyVersion。
type Strategy struct {
	Base

	Name        string `gorm:"column:name;type:varchar(128);not null;uniqueIndex:uk_strategies_name" json:"name"`
	Description string `gorm:"column:description;type:text" json:"description"`

	OwnerPipelineID *uuidx.UUID `gorm:"column:owner_pipeline_id;type:binary(16);uniqueIndex:uk_strategies_owner_pipeline" json:"-"`

	CurrentVersion uint32 `gorm:"column:current_version;not null;default:0" json:"current_version"`

	// DeletedAt：策略整体删除需先解除全部 pipeline 引用（MGR-STR-04），
	// 这里仅提供软删除能力。
	DeletedAt gorm.DeletedAt `gorm:"column:deleted_at;index" json:"-"`
}

// TableName 显式指定表名。
func (Strategy) TableName() string { return "strategies" }

// StrategyVersion 是策略的不可变版本快照。创建后不允许修改，变更只能产生新版本。
//
// 不可变数据不设 updated_at 与软删除（本结构体确实没有 DeletedAt 字段——上面
// Strategy 的那个不属于这里），所以删除是真删，不会出现「已删除但仍可见」的歧义。
//
// 已验证：三个哨兵错误在 DeleteStrategyVersion 里的分支、Unscoped 的用法、HTTP 路由
// 接线、本结构体无 DeletedAt。未验证：是否存在绕过该函数直接删表行的路径。
type StrategyVersion struct {
	ID uuidx.UUID `gorm:"column:id;type:binary(16);primaryKey" json:"id"`

	StrategyID uuidx.UUID `gorm:"column:strategy_id;type:binary(16);not null;uniqueIndex:uk_strategy_versions_strategy_version,priority:1" json:"strategy_id"`
	Version    uint32     `gorm:"column:version;not null;uniqueIndex:uk_strategy_versions_strategy_version,priority:2" json:"version"`

	// Rules 是编译前的规则定义（表单或 JSON 编辑产出），落为 JSON 列；
	// 编译为可执行 Plan 是 internal/rule 的职责，不在管理库模型范围内。
	Rules JSON `gorm:"column:rules;type:json;not null" json:"rules"`

	CreatedBy uuidx.UUID `gorm:"column:created_by;type:binary(16);not null" json:"created_by"`
	CreatedAt time.Time  `gorm:"column:created_at;not null;autoCreateTime" json:"created_at"`
}

// TableName 显式指定表名。
func (StrategyVersion) TableName() string { return "strategy_versions" }

// BeforeCreate 在写入前分配 UUIDv7 主键（调用方未显式指定时）。
func (v *StrategyVersion) BeforeCreate(_ *gorm.DB) error {
	if v.ID.IsZero() {
		v.ID = uuidx.New()
	}
	return nil
}
