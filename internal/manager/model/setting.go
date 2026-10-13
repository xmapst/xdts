package model

import "github.com/xmapst/xdts/pkg/uuidx"

// Setting 是系统设置的键值存储，例如保留期天数、预检查有效
// 期、对账周期等可运行期调整的参数。Value 落为 JSON 以承载不同类型
// （数字、字符串、布尔、对象），具体键与 schema 由使用方约定。
type Setting struct {
	Base

	// 设置使用既定数据库列名，查询时按方言转义保留字。
	Key   string `gorm:"column:key;type:varchar(128);not null;uniqueIndex:uk_settings_key" json:"key"`
	Value JSON   `gorm:"column:value;type:json" json:"value,omitempty"`

	// UpdatedBy 是最近一次改动该设置项的用户。设置项是运行期可调的权威参数，
	// "谁把保留期从 30 天改成 1 天"必须能追溯到人；Base.UpdatedAt 只记时刻
	// 不记人，所以这一列不能靠它顶替。
	//
	// 允许为 Nil：v45 之前落地的行没有这个信息，迁移不能凭空编一个用户；
	// 系统自身写入的默认值同样没有"人"。调用方要区分"没人改过"与"某人改过"
	// 用 IsZero 判断，而不是与某个哨兵 UUID 比较。
	UpdatedBy uuidx.UUID `gorm:"column:updated_by;type:binary(16)" json:"updated_by,omitempty"`
}

// TableName 显式指定表名。
func (Setting) TableName() string { return "settings" }
