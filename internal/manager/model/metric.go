package model

import "github.com/xmapst/xdts/pkg/uuidx"

// 按天 RANGE 分区，主键取 (id, ts)（TimePKBase）；gorm
// AutoMigrate 管不了分区，本表的建表 DDL（含分区定义与 (pipeline_id, ts)
// 二级索引）由 internal/manager/repo 的迁移用原生 DDL 创建，这里的 struct
// tag 只用于 Scan/Query 时的字段映射，不作为索引的权威定义。
type PipelineMetric struct {
	TimePKBase

	PipelineID uuidx.UUID `gorm:"column:pipeline_id;type:binary(16);not null" json:"pipeline_id"`

	DelayMs    int64   `gorm:"column:delay_ms;not null;default:0" json:"delay_ms"`
	RPS        float64 `gorm:"column:rps;not null;default:0" json:"rps"`
	BPS        float64 `gorm:"column:bps;not null;default:0" json:"bps"`
	QueueDepth int64   `gorm:"column:queue_depth;not null;default:0" json:"queue_depth"`
	Errors     int64   `gorm:"column:errors;not null;default:0" json:"errors"`
	// Added fields are nullable: historical rows from older Managers do not
	// identify their side/run or contain these diagnostics.
	Side      *Side       `gorm:"column:side" json:"side"`
	RunID     *uint64     `gorm:"column:run_id" json:"run_id"`
	NodeID    *uuidx.UUID `gorm:"column:node_id;type:binary(16)" json:"node_id"`
	TPS       *float64    `gorm:"column:tps" json:"tps"`
	Conflicts *int64      `gorm:"column:conflicts" json:"conflicts"`
	Values    JSON        `gorm:"column:values_json;type:json" json:"values"`
}

// TableName 显式指定表名。
func (PipelineMetric) TableName() string { return "pipeline_metrics" }
