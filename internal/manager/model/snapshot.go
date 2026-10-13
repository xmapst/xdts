package model

import (
	"fmt"
	"math"
	"time"

	"github.com/xmapst/xdts/pkg/uuidx"
)

// SnapshotStatus 是全量运行的整体状态。
type SnapshotStatus string

const (
	SnapshotStatusPlanning   SnapshotStatus = "planning"    // 生成分块计划
	SnapshotStatusReading    SnapshotStatus = "reading"     // 分块读取中
	SnapshotStatusChunksDone SnapshotStatus = "chunks_done" // 全部分块已发布，等待应用
	SnapshotStatusConverging SnapshotStatus = "converging"  // 从 L 重放至 H
	SnapshotStatusConverged  SnapshotStatus = "converged"   // 越过 H，全量完成
	SnapshotStatusPaused     SnapshotStatus = "paused"
	SnapshotStatusFailed     SnapshotStatus = "failed"
)

// Valid 校验全量运行状态取值。
func (s SnapshotStatus) Valid() bool {
	switch s {
	case SnapshotStatusPlanning, SnapshotStatusReading, SnapshotStatusChunksDone,
		SnapshotStatusConverging, SnapshotStatusConverged, SnapshotStatusPaused, SnapshotStatusFailed:
		return true
	default:
		return false
	}
}

// PipelineSnapshot 是一次全量运行记录。
//
// low_gtid_set / high_gtid_set 用 text 承载：GTID 集合在长期运行、多来源
// 中心场景下可能很长，不适合定长 varchar。
type PipelineSnapshot struct {
	Base

	PipelineID   uuidx.UUID `gorm:"column:pipeline_id;type:binary(16);not null;uniqueIndex:uk_pipeline_snapshots_pipeline_run,priority:1" json:"pipeline_id"`
	RunID        uint64     `gorm:"column:run_id;not null;uniqueIndex:uk_pipeline_snapshots_pipeline_run,priority:2" json:"run_id"`
	SnapshotUUID uuidx.UUID `gorm:"column:snapshot_uuid;type:binary(16);not null" json:"snapshot_uuid"`

	LowGTIDSet  string `gorm:"column:low_gtid_set;type:text" json:"low_gtid_set"`
	HighGTIDSet string `gorm:"column:high_gtid_set;type:text" json:"high_gtid_set"`

	Status SnapshotStatus `gorm:"column:status;type:varchar(16);not null;index:idx_pipeline_snapshots_status" json:"status"`

	TotalChunks           uint64 `gorm:"column:total_chunks;not null;default:0" json:"total_chunks"`
	DoneChunks            uint64 `gorm:"column:done_chunks;not null;default:0" json:"done_chunks"`
	InitialTotalChunks    uint64 `gorm:"column:initial_total_chunks;not null;default:0" json:"initial_total_chunks"`
	PlanExtensionSequence uint64 `gorm:"column:plan_extension_sequence;not null;default:0" json:"plan_extension_sequence"`
	// Nil means the target has not observed catchup progress for this run.
	CatchupRemainingTxns *uint64 `gorm:"column:catchup_remaining_txns" json:"catchup_remaining_txns"`
	CatchupUpdatedAtMS   int64   `gorm:"column:catchup_updated_at_ms;not null;default:0" json:"-"`

	StartedAt  *time.Time `gorm:"column:started_at" json:"started_at"`
	FinishedAt *time.Time `gorm:"column:finished_at" json:"finished_at"`
}

// TableName 显式指定表名。
func (PipelineSnapshot) TableName() string { return "pipeline_snapshots" }

// Validate 校验枚举字段。
func (s *PipelineSnapshot) Validate() error {
	if !s.Status.Valid() {
		return fmt.Errorf("model: 未知的全量运行状态 %q", s.Status)
	}
	return nil
}

// SnapshotTableStatus 是全量按表进度状态。
type SnapshotTableStatus string

const (
	SnapshotTableStatusPending SnapshotTableStatus = "pending"
	SnapshotTableStatusReading SnapshotTableStatus = "reading" // 分块读取中
	SnapshotTableStatusDone    SnapshotTableStatus = "done"    // 该表全部分块已完成
	SnapshotTableStatusFailed  SnapshotTableStatus = "failed"
)

// Valid 校验按表进度状态取值。
func (s SnapshotTableStatus) Valid() bool {
	switch s {
	case SnapshotTableStatusPending, SnapshotTableStatusReading, SnapshotTableStatusDone, SnapshotTableStatusFailed:
		return true
	default:
		return false
	}
}

// PipelineSnapshotTable 是全量任务按表的进度。
type PipelineSnapshotTable struct {
	Base

	PipelineID uuidx.UUID `gorm:"column:pipeline_id;type:binary(16);not null;uniqueIndex:uk_pipeline_snapshot_tables_scope,priority:1" json:"pipeline_id"`
	RunID      uint64     `gorm:"column:run_id;not null;uniqueIndex:uk_pipeline_snapshot_tables_scope,priority:2" json:"run_id"`
	SchemaName string     `gorm:"column:schema_name;type:varchar(128);not null;uniqueIndex:uk_pipeline_snapshot_tables_scope,priority:3" json:"schema_name"`
	// Table 对应 table_name 列；Go 字段不能叫 TableName，
	// 会与下面的 TableName() 方法（gorm 表名钩子）撞名。
	Table string `gorm:"column:table_name;type:varchar(128);not null;uniqueIndex:uk_pipeline_snapshot_tables_scope,priority:4" json:"table_name"`

	EstRows      int64    `gorm:"column:est_rows;not null;default:0" json:"est_rows"`
	DoneRows     int64    `gorm:"column:done_rows;not null;default:0" json:"done_rows"`
	TotalChunks  *uint64  `gorm:"column:total_chunks" json:"total_chunks"`
	DoneChunks   *uint64  `gorm:"column:done_chunks" json:"done_chunks"`
	RowsPerSec   *float64 `gorm:"column:rows_per_sec" json:"rows_per_sec"`
	ObservedAtMS int64    `gorm:"column:observed_at_ms;not null;default:0" json:"-"`

	Status SnapshotTableStatus `gorm:"column:status;type:varchar(16);not null" json:"status"`
}

// TableName 显式指定表名。
func (PipelineSnapshotTable) TableName() string { return "pipeline_snapshot_tables" }

// Validate 校验枚举字段。
func (t *PipelineSnapshotTable) Validate() error {
	if !t.Status.Valid() {
		return fmt.Errorf("model: 未知的全量按表进度状态 %q", t.Status)
	}
	if t.EstRows < 0 || t.DoneRows < 0 || t.ObservedAtMS < 0 ||
		(t.TotalChunks == nil) != (t.DoneChunks == nil) || t.TotalChunks != nil && (*t.TotalChunks == 0 || *t.DoneChunks > *t.TotalChunks) ||
		t.RowsPerSec != nil && (math.IsNaN(*t.RowsPerSec) || math.IsInf(*t.RowsPerSec, 0) || *t.RowsPerSec < 0) {
		return fmt.Errorf("model: invalid snapshot table counts or rate")
	}
	return nil
}
