package model

import (
	"fmt"
	"time"

	"github.com/xmapst/xdts/pkg/uuidx"
)

// ReleaseStatus 表示配置下发后的节点应用结果。
type ReleaseStatus string

const (
	ReleaseStatusPending  ReleaseStatus = "pending"  // 已下发，等待节点回执
	ReleaseStatusApplied  ReleaseStatus = "applied"  // 节点应用成功
	ReleaseStatusFailed   ReleaseStatus = "failed"   // 节点应用失败
	ReleaseStatusRejected ReleaseStatus = "rejected" // 节点因版本倒退等原因拒绝（）
)

// Valid 校验下发状态取值。
func (s ReleaseStatus) Valid() bool {
	switch s {
	case ReleaseStatusPending, ReleaseStatusApplied, ReleaseStatusFailed, ReleaseStatusRejected:
		return true
	default:
		return false
	}
}

// PipelineRelease 是一次配置下发记录。
type PipelineRelease struct {
	Base

	PipelineID    uuidx.UUID `gorm:"column:pipeline_id;type:binary(16);not null;index:idx_pipeline_releases_pipeline_id" json:"pipeline_id"`
	ConfigVersion uint64     `gorm:"column:config_version;not null" json:"config_version"`
	Side          Side       `gorm:"column:side;type:varchar(8);not null" json:"side"`
	NodeID        uuidx.UUID `gorm:"column:node_id;type:binary(16);not null" json:"node_id"`

	// ConfigDigest 是下发内容的摘要（如 sha256 十六进制），用于节点回执
	// 比对，避免整份配置往返比较。
	ConfigDigest string `gorm:"column:config_digest;type:varchar(64);not null;default:''" json:"config_digest"`

	Status  ReleaseStatus `gorm:"column:status;type:varchar(16);not null;index:idx_pipeline_releases_status" json:"status"`
	Message string        `gorm:"column:message;type:text" json:"message"`

	// AppliedAt records confirmation of this release; failure does not set it.
	AppliedAt *time.Time `gorm:"column:applied_at" json:"applied_at"`
}

// TableName 显式指定表名。
func (PipelineRelease) TableName() string { return "pipeline_releases" }

// Validate 校验枚举字段。
func (r *PipelineRelease) Validate() error {
	if !r.Side.Valid() {
		return fmt.Errorf("model: 未知的 side %q", r.Side)
	}
	if !r.Status.Valid() {
		return fmt.Errorf("model: 未知的下发状态 %q", r.Status)
	}
	return nil
}
