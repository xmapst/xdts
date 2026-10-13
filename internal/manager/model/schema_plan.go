package model

import (
	"time"

	"github.com/xmapst/xdts/pkg/uuidx"
)

// SchemaPlan is the persisted authority for one run's preview and confirmation.
// Definition comparisons and executable steps are retained in Document so a
// later export/confirmation never rebuilds a different plan under the same hash.
type SchemaPlan struct {
	Base
	PipelineID    uuidx.UUID `gorm:"column:pipeline_id;type:binary(16);not null;uniqueIndex:uk_schema_plans_run,priority:1"`
	RunID         uint64     `gorm:"column:run_id;not null;uniqueIndex:uk_schema_plans_run,priority:2"`
	ConfigVersion uint64     `gorm:"column:config_version;not null"`
	Digest        string     `gorm:"column:digest;type:char(64);not null"`
	Document      JSON       `gorm:"column:document;type:json;not null"`
	Blocked       bool       `gorm:"column:blocked;not null"`
	ConfirmedAt   *time.Time `gorm:"column:confirmed_at;type:datetime(3)"`
	ConfirmedBy   uuidx.UUID `gorm:"column:confirmed_by;type:binary(16)"`
}

func (SchemaPlan) TableName() string { return "schema_plans" }
