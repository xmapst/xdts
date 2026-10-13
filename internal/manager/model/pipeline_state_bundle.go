package model

import "github.com/xmapst/xdts/pkg/uuidx"

// One current handover per role. The protobuf binds its run and recipient;
// later configuration refreshes retain it for retry without rewinding progress.
type PipelineStateBundle struct {
	PipelineID uuidx.UUID `gorm:"column:pipeline_id;type:binary(16);primaryKey"`
	Side       Side       `gorm:"column:side;type:varchar(16);primaryKey"`
	Data       []byte     `gorm:"column:bundle;type:mediumblob;not null"`
}

func (PipelineStateBundle) TableName() string { return "pipeline_state_bundles" }
