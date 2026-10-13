package model

import (
	"fmt"
	"time"

	"github.com/xmapst/xdts/pkg/uuidx"
)

// PrecheckLevel 定义严重级别：阻断项禁止启动，警告项需要用户确认。
type PrecheckLevel string

const (
	PrecheckLevelInfo     PrecheckLevel = "info"
	PrecheckLevelWarning  PrecheckLevel = "warning"
	PrecheckLevelBlocking PrecheckLevel = "blocking"
)

// Valid 校验级别取值。
func (l PrecheckLevel) Valid() bool {
	switch l {
	case PrecheckLevelInfo, PrecheckLevelWarning, PrecheckLevelBlocking:
		return true
	default:
		return false
	}
}

// PrecheckStatus 是预检查项本身的检查结果。
type PrecheckStatus string

const (
	PrecheckStatusPending PrecheckStatus = "pending"
	PrecheckStatusPass    PrecheckStatus = "pass"
	PrecheckStatusFail    PrecheckStatus = "fail"
)

// Valid 校验检查结果取值。
func (s PrecheckStatus) Valid() bool {
	switch s {
	case PrecheckStatusPending, PrecheckStatusPass, PrecheckStatusFail:
		return true
	default:
		return false
	}
}

// PipelinePrecheck 保存预检查结果及用户确认信息。
type PipelinePrecheck struct {
	Base

	PipelineID    uuidx.UUID `gorm:"column:pipeline_id;type:binary(16);not null;index:idx_pipeline_prechecks_pipeline_id" json:"pipeline_id"`
	ConfigVersion uint64     `gorm:"column:config_version;not null" json:"config_version"`
	// Nil identifies a row without a completed batch; it cannot authorize new starts.
	BatchID uuidx.UUID `gorm:"column:batch_id;type:binary(16);index:idx_pipeline_prechecks_batch" json:"batch_id"`

	Item   string         `gorm:"column:item;type:varchar(64);not null" json:"item"`
	Level  PrecheckLevel  `gorm:"column:level;type:varchar(16);not null" json:"level"`
	Status PrecheckStatus `gorm:"column:status;type:varchar(16);not null" json:"status"`
	// Detail 是检查项的结构化详情（如冲突的 server_id、缺失主键的表名单）。
	Detail JSON `gorm:"column:detail;type:json" json:"detail,omitempty"`

	CheckedAt time.Time `gorm:"column:checked_at;not null" json:"checked_at"`

	ConfirmedBy uuidx.UUID `gorm:"column:confirmed_by;type:binary(16)" json:"confirmed_by"`
	ConfirmedAt *time.Time `gorm:"column:confirmed_at;type:datetime(3)" json:"confirmed_at"`
}

// TableName 显式指定表名。
func (PipelinePrecheck) TableName() string { return "pipeline_prechecks" }

// Validate 校验枚举字段。
func (p *PipelinePrecheck) Validate() error {
	if !p.Level.Valid() {
		return fmt.Errorf("model: 未知的预检查级别 %q", p.Level)
	}
	if !p.Status.Valid() {
		return fmt.Errorf("model: 未知的预检查状态 %q", p.Status)
	}
	return nil
}
