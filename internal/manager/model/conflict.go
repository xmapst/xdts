package model

import (
	"fmt"
	"time"

	"github.com/xmapst/xdts/pkg/uuidx"
)

// ConflictType 是写入冲突的类型（写入方式矩阵）。
type ConflictType string

const (
	ConflictTypeLWWRejected     ConflictType = "lww_rejected"
	ConflictTypeInsertDuplicate ConflictType = "insert_duplicate" // INSERT 撞键
	ConflictTypeUpdateMissing   ConflictType = "update_missing"   // UPDATE 目标行不存在
	ConflictTypeUpdateMismatch  ConflictType = "update_mismatch"  // UPDATE 前镜像不符
	ConflictTypeDeleteMismatch  ConflictType = "delete_mismatch"  // DELETE 前镜像不符
	ConflictTypeDeleteMissing   ConflictType = "delete_missing"   // DELETE 目标行不存在
)

// Valid 校验冲突类型取值。
func (t ConflictType) Valid() bool {
	switch t {
	case ConflictTypeInsertDuplicate, ConflictTypeUpdateMissing, ConflictTypeDeleteMissing, ConflictTypeUpdateMismatch, ConflictTypeDeleteMismatch, ConflictTypeLWWRejected:
		return true
	default:
		return false
	}
}

// ConflictResolution 表示冲突的最终处理方式；收敛模式不产生冲突记录。
type ConflictResolution string

const (
	ConflictResolutionSourceWins ConflictResolution = "source_wins" // 源覆盖（默认）：按到达变更覆盖
	ConflictResolutionTargetWins ConflictResolution = "target_wins" // LWW：目标行时间戳更新，保留目标
	ConflictResolutionSkipped    ConflictResolution = "skipped"     // 按策略跳过该行变更
	ConflictResolutionStopped    ConflictResolution = "stopped"     // 停止策略：pipeline 停机等待人工处理
)

// Valid 校验冲突处理方式取值。
func (r ConflictResolution) Valid() bool {
	switch r {
	case ConflictResolutionSourceWins, ConflictResolutionTargetWins, ConflictResolutionSkipped, ConflictResolutionStopped:
		return true
	default:
		return false
	}
}

// Conflict 是行级写入冲突记录，SourceRow 与 TargetRow 使用 JSON 行镜像。
type Conflict struct {
	LogBase

	Ts time.Time `gorm:"column:ts;not null" json:"ts"`

	PipelineID uuidx.UUID `gorm:"column:pipeline_id;type:binary(16);not null" json:"pipeline_id"`
	SchemaName string     `gorm:"column:schema_name;type:varchar(128);not null" json:"schema_name"`
	// Table 对应设计文档的 table_name 列，命名理由同
	// PipelineSnapshotTable.Table。
	Table string `gorm:"column:table_name;type:varchar(128);not null" json:"table_name"`

	// PK 是冲突行的主键 / 唯一键取值（JSON，支持复合键）。
	PK JSON `gorm:"column:pk;type:json;not null" json:"pk"`

	ConflictType ConflictType       `gorm:"column:conflict_type;type:varchar(24);not null" json:"conflict_type"`
	Resolution   ConflictResolution `gorm:"column:resolution;type:varchar(16);not null" json:"resolution"`

	SourceRow JSON `gorm:"column:source_row;type:json" json:"source_row,omitempty"`
	TargetRow JSON `gorm:"column:target_row;type:json" json:"target_row,omitempty"`
}

// TableName 显式指定表名。
func (Conflict) TableName() string { return "conflicts" }

// Validate 校验枚举字段。
func (c *Conflict) Validate() error {
	if !c.ConflictType.Valid() {
		return fmt.Errorf("model: 未知的冲突类型 %q", c.ConflictType)
	}
	if !c.Resolution.Valid() {
		return fmt.Errorf("model: 未知的冲突处理方式 %q", c.Resolution)
	}
	return nil
}
