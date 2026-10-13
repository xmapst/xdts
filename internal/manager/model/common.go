// Package model 定义管理库（独立部署的 MySQL 8，C-18）的 gorm 模型。
package model

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

// Base 是绝大多数管理库表的公共字段：UUIDv7 主键 + 创建 / 更新时间。
type Base struct {
	ID        uuidx.UUID `gorm:"column:id;type:binary(16);primaryKey" json:"id"`
	CreatedAt time.Time  `gorm:"column:created_at;not null;autoCreateTime" json:"created_at"`
	UpdatedAt time.Time  `gorm:"column:updated_at;not null;autoUpdateTime" json:"updated_at"`
}

// BeforeCreate 在写入前分配 UUIDv7 主键（调用方未显式指定时）。
//
// gorm 通过反射在模型（含匿名嵌入字段）上查找钩子方法，因此各表结构体
// 嵌入 Base 后自动获得该行为，无需在每个模型里重复实现。
func (b *Base) BeforeCreate(_ *gorm.DB) error {
	if b.ID.IsZero() {
		b.ID = uuidx.New()
	}
	return nil
}

// LogBase belongs to append-only tables retained by business timestamp Ts.
// It has no update or soft-delete lifecycle. LogID avoids uuidx soft-delete methods
// so composite primary-key records do not acquire an unintended deleted_at scope.
type LogBase struct {
	ID        uuidx.UUID `gorm:"column:id;type:binary(16);primaryKey" json:"id"`
	CreatedAt time.Time  `gorm:"column:created_at;not null;autoCreateTime" json:"created_at"`
}

// BeforeCreate 在写入前分配 UUIDv7 主键（调用方未显式指定时）。
func (b *LogBase) BeforeCreate(_ *gorm.DB) error {
	if b.ID.IsZero() {
		b.ID = uuidx.New()
	}
	return nil
}

// TimePKBase 是按天 RANGE 分区表（logs、pipeline_metrics）的公共字段。
//
// MySQL 要求分区列包含在每个唯一键中，因此主键取 (id, ts) 而非单纯 id
// （）。这两张表写入频率高、且从不更新，因此不提供
// updated_at，也不提供 created_at：Ts 本身就是排序、分区与保留期判定的
// 唯一时间依据，重复记录一次插入时间没有实际用途。
type TimePKBase struct {
	ID uuidx.UUID `gorm:"column:id;type:binary(16);primaryKey" json:"id"`
	Ts time.Time  `gorm:"column:ts;primaryKey;not null" json:"ts"`
}

// BeforeCreate 在写入前分配 UUIDv7 主键（调用方未显式指定时）。
func (b *TimePKBase) BeforeCreate(_ *gorm.DB) error {
	if b.ID.IsZero() {
		b.ID = uuidx.New()
	}
	return nil
}

// JSON 是管理库 JSON 列的落库类型，承担与 gorm.io/datatypes.JSON 相同的职责。
//
// ⇒ 若将来补上依赖并替换，必须逐项比对 Value/Scan 在 nil、空切片、
// 非法 JSON 三种输入下的行为，不要依赖「等价」这个词。
type JSON []byte

// GormDataType 供 gorm 推导列类型。
func (j JSON) GormDataType() string { return "json" }

// Value 实现 driver.Valuer。空值落 NULL，不落字符串 "null"。
func (j JSON) Value() (driver.Value, error) {
	if len(j) == 0 {
		return nil, nil
	}
	return string(j), nil
}

// Scan 实现 sql.Scanner，兼容驱动返回的 []byte、string 与 NULL。
func (j *JSON) Scan(src any) error {
	if src == nil {
		*j = nil
		return nil
	}
	switch v := src.(type) {
	case []byte:
		b := make([]byte, len(v))
		copy(b, v)
		*j = b
	case string:
		*j = JSON(v)
	default:
		return fmt.Errorf("model: JSON 列不支持的扫描类型 %T", src)
	}
	return nil
}

// MarshalJSON 实现 json.Marshaler，空值序列化为 JSON null。
func (j JSON) MarshalJSON() ([]byte, error) {
	if len(j) == 0 {
		return []byte("null"), nil
	}
	return j, nil
}

// UnmarshalJSON 实现 json.Unmarshaler，原样保留原始 JSON 字节。
func (j *JSON) UnmarshalJSON(data []byte) error {
	if j == nil {
		return errors.New("model.JSON: UnmarshalJSON 调用于 nil 指针")
	}
	*j = append((*j)[0:0], data...)
	return nil
}

// IsZero 报告 JSON 列是否为空（未设置）。
func (j JSON) IsZero() bool { return len(j) == 0 }

// redactedString 是脱敏后对外展示的占位值：不回显长度、不回显任何字符，
// 仅表示「该字段存在但已被隐藏」。
const redactedString = ""
