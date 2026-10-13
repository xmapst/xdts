package model

import (
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/pkg/uuidx"
)

type PositionChangeDocument struct {
	Kind                       string   `json:"kind"`
	Position                   JSON     `json:"position"`
	PublishedGTIDSet           *string  `json:"publishedGtidSet"`
	CheckpointEpoch            uint64   `json:"checkpointEpoch"`
	CheckpointPositionRevision uint64   `json:"checkpointPositionRevision"`
	Warnings                   []string `json:"warnings"`
}

type PositionChange struct {
	Base
	PipelineID       uuidx.UUID `gorm:"column:pipeline_id"`
	RunID            uint64     `gorm:"column:run_id"`
	ConfigVersion    uint64     `gorm:"column:config_version"`
	PositionRevision uint64     `gorm:"column:position_revision"`
	Digest           string     `gorm:"column:digest"`
	Document         JSON       `gorm:"column:document;type:json"`
	ExpiresAt        time.Time  `gorm:"column:expires_at"`
	ConfirmedAt      *time.Time `gorm:"column:confirmed_at"`
	ConfirmedBy      uuidx.UUID `gorm:"column:confirmed_by"`
}

func (PositionChange) TableName() string { return "pipeline_position_changes" }

type StoredPositionChange struct {
	Record      PositionChange
	Document    PositionChangeDocument
	Position    *iproto.StartPosition
	ForwardJump bool
}
