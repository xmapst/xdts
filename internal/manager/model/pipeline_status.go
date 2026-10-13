package model

import "github.com/xmapst/xdts/pkg/uuidx"

// PipelineStatusRecord belongs to Manager ingestion. Offline state overrides
// the pipeline view without destroying this last authenticated observation.
type PipelineStatusRecord struct {
	PipelineID           uuidx.UUID     `gorm:"column:pipeline_id;type:binary(16);primaryKey"`
	Side                 Side           `gorm:"column:side;primaryKey"`
	NodeID               uuidx.UUID     `gorm:"column:node_id;type:binary(16)"`
	RunID                uint64         `gorm:"column:run_id"`
	ConfigVersion        uint64         `gorm:"column:config_version"`
	State                ActualState    `gorm:"column:state"`
	LastError            string         `gorm:"column:last_error"`
	SnapshotStatus       SnapshotStatus `gorm:"column:snapshot_status"`
	ObservedAtMS         int64          `gorm:"column:observed_at_ms"`
	SessionID            string         `gorm:"column:session_id"`
	SessionStartedAtMS   int64          `gorm:"column:session_started_at_ms"`
	Sequence             uint64         `gorm:"column:sequence"`
	RuntimeInstanceID    string         `gorm:"column:runtime_instance_id"`
	RuntimeInstanceFloor string         `gorm:"column:runtime_instance_floor"`
}

func (PipelineStatusRecord) TableName() string { return "pipeline_status_latest" }
