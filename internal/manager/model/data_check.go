package model

import (
	"time"

	"github.com/xmapst/xdts/pkg/uuidx"
)

type DataCheckRun struct {
	Base
	PipelineID      uuidx.UUID `gorm:"column:pipeline_id;type:binary(16);not null" json:"pipelineId"`
	ConfigVersion   uint64     `gorm:"column:config_version;not null" json:"configVersion"`
	CreatedBy       uuidx.UUID `gorm:"column:created_by;type:binary(16);not null" json:"-"`
	Status          string     `gorm:"column:status;type:varchar(16);not null" json:"status"`
	Config          JSON       `gorm:"column:config;type:json;not null" json:"config"`
	SampleModulo    uint64     `gorm:"column:sample_modulo;not null" json:"sampleModulo"`
	CompletedTables int        `gorm:"column:completed_tables;not null" json:"completedTables"`
	Results         JSON       `gorm:"column:results;type:json;not null" json:"results"`
	Error           string     `gorm:"column:error;type:text" json:"error,omitempty"`
	ExpiresAt       time.Time  `gorm:"column:expires_at;not null" json:"expiresAt"`
	StartedAt       *time.Time `gorm:"column:started_at" json:"startedAt"`
	FinishedAt      *time.Time `gorm:"column:finished_at" json:"finishedAt"`
}

func (DataCheckRun) TableName() string { return "pipeline_data_check_runs" }
