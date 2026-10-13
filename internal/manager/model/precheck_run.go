package model

import (
	"time"

	"github.com/xmapst/xdts/pkg/uuidx"
)

// PrecheckRun owns one complete precheck result set. Beginning a newer run
// immediately supersedes the previous one, even while probes are still pending.
type PrecheckRun struct {
	Base
	PipelineID    uuidx.UUID `gorm:"column:pipeline_id;type:binary(16);not null;index:idx_precheck_runs_pipeline;uniqueIndex:uk_precheck_runs_sequence,priority:1" json:"pipeline_id"`
	Sequence      uint64     `gorm:"column:sequence;not null;uniqueIndex:uk_precheck_runs_sequence,priority:2" json:"sequence"`
	ConfigVersion uint64     `gorm:"column:config_version;not null" json:"config_version"`
	RunID         uint64     `gorm:"column:run_id;not null" json:"run_id"`
	ConfigDigest  string     `gorm:"column:config_digest;type:char(64);not null" json:"config_digest"`
	Required      JSON       `gorm:"column:required;type:json;not null" json:"required"`
	Complete      bool       `gorm:"column:complete;not null" json:"complete"`
	CheckedAt     time.Time  `gorm:"column:checked_at;type:datetime(3);not null" json:"checked_at"`
	ExpiresAt     time.Time  `gorm:"column:expires_at;type:datetime(3);not null" json:"expires_at"`
	CreatedBy     uuidx.UUID `gorm:"column:created_by;type:binary(16);not null" json:"created_by"`
}

func (PrecheckRun) TableName() string { return "pipeline_precheck_runs" }
