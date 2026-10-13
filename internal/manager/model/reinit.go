package model

import (
	"time"

	"github.com/xmapst/xdts/pkg/uuidx"
)

type ReinitTargetHandling string

const (
	ReinitTargetKeep  ReinitTargetHandling = "keep"
	ReinitTargetClear ReinitTargetHandling = "clear"
)

// PipelineReinit retains a confirmed run's target preparation and command
// identity. Success receipts do not represent the actual pipeline state.
type PipelineReinit struct {
	Base
	PipelineID    uuidx.UUID `gorm:"column:pipeline_id;type:binary(16);not null;uniqueIndex:uk_pipeline_reinit_from_run,priority:1"`
	FromRunID     uint64     `gorm:"column:from_run_id;not null;uniqueIndex:uk_pipeline_reinit_from_run,priority:2"`
	ConfigVersion uint64     `gorm:"column:config_version;not null"`
	Digest        string     `gorm:"column:digest;type:char(64);not null"`
	Document      JSON       `gorm:"column:document;type:json;not null"`
	ConfirmedAt   *time.Time `gorm:"column:confirmed_at;type:datetime(3)"`
	ConfirmedBy   uuidx.UUID `gorm:"column:confirmed_by;type:binary(16)"`
	NewRunID      uint64     `gorm:"column:new_run_id;not null;default:0"`
	SnapshotUUID  uuidx.UUID `gorm:"column:snapshot_uuid;type:binary(16)"`
	SchemaUUID    uuidx.UUID `gorm:"column:schema_uuid;type:binary(16)"`
	PrepareUUID   uuidx.UUID `gorm:"column:prepare_uuid;type:binary(16)"`
	CmdSeq        uint64     `gorm:"column:cmd_seq;not null;default:0"`
	SourceApplied bool       `gorm:"column:source_applied;not null;default:false"`
	TargetApplied bool       `gorm:"column:target_applied;not null;default:false"`
}

func (PipelineReinit) TableName() string { return "pipeline_reinitializations" }
