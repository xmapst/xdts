package model

import (
	"time"

	"github.com/xmapst/xdts/pkg/uuidx"
)

type PositionPreview struct {
	Base
	PipelineID    uuidx.UUID `gorm:"column:pipeline_id;type:binary(16);not null;uniqueIndex:uk_position_previews_run,priority:1"`
	RunID         uint64     `gorm:"column:run_id;not null;uniqueIndex:uk_position_previews_run,priority:2"`
	ConfigVersion uint64     `gorm:"column:config_version;not null"`
	Digest        string     `gorm:"column:digest;type:char(64);not null"`
	Document      JSON       `gorm:"column:document;type:json;not null"`
	ConfirmedAt   *time.Time `gorm:"column:confirmed_at;type:datetime(3)"`
	ConfirmedBy   uuidx.UUID `gorm:"column:confirmed_by;type:binary(16)"`
}

func (PositionPreview) TableName() string { return "position_previews" }
