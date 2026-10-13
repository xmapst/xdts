package model

import (
	"fmt"

	"github.com/xmapst/xdts/pkg/uuidx"
)

// ObjectType 定义结构迁移涉及的数据库对象类型。
type ObjectType string

const (
	ObjectTypeSchema     ObjectType = "schema"
	ObjectTypeTable      ObjectType = "table"
	ObjectTypeView       ObjectType = "view"
	ObjectTypeProcedure  ObjectType = "procedure"
	ObjectTypeFunction   ObjectType = "function"
	ObjectTypeForeignKey ObjectType = "foreign_key"
	ObjectTypeTrigger    ObjectType = "trigger"
	ObjectTypeEvent      ObjectType = "event"
)

// Valid 校验对象类型取值。
func (t ObjectType) Valid() bool {
	switch t {
	case ObjectTypeSchema, ObjectTypeTable, ObjectTypeView, ObjectTypeProcedure,
		ObjectTypeFunction, ObjectTypeForeignKey, ObjectTypeTrigger, ObjectTypeEvent:
		return true
	default:
		return false
	}
}

// MigrationAction 定义计划预览中的新建、跳过、阻断或重建动作。
// 增量 DDL 另有原地改写、重建等执行分类。
type MigrationAction string

const (
	MigrationActionCreate  MigrationAction = "create" // 新建
	MigrationActionAlter   MigrationAction = "alter"  // 原地改写（加列、删列、增删普通索引等）
	MigrationActionDrop    MigrationAction = "drop"
	MigrationActionRebuild MigrationAction = "rebuild" // 需要重建（改列类型、重命名列、加唯一索引等）
	MigrationActionSkip    MigrationAction = "skip"    // 目标已满足期望定义，跳过
	MigrationActionBlock   MigrationAction = "block"   // 无法安全改写，阻断并等待人工处理
)

// Valid 校验迁移动作取值。
func (a MigrationAction) Valid() bool {
	switch a {
	case MigrationActionCreate, MigrationActionAlter, MigrationActionDrop,
		MigrationActionRebuild, MigrationActionSkip, MigrationActionBlock:
		return true
	default:
		return false
	}
}

// MigrationStatus 是该计划项的执行状态。
type MigrationStatus string

const (
	MigrationStatusPending MigrationStatus = "pending"
	MigrationStatusApplied MigrationStatus = "applied"
	MigrationStatusFailed  MigrationStatus = "failed"
)

// Valid 校验执行状态取值。
func (s MigrationStatus) Valid() bool {
	switch s {
	case MigrationStatusPending, MigrationStatusApplied, MigrationStatusFailed:
		return true
	default:
		return false
	}
}

// SchemaMigration 保存结构迁移计划和各步骤的执行结果。
type SchemaMigration struct {
	Base

	PipelineID uuidx.UUID `gorm:"column:pipeline_id;type:binary(16);not null;uniqueIndex:uk_schema_migrations_seq,priority:1" json:"pipeline_id"`
	RunID      uint64     `gorm:"column:run_id;not null;uniqueIndex:uk_schema_migrations_seq,priority:2" json:"run_id"`
	Seq        uint32     `gorm:"column:seq;not null;uniqueIndex:uk_schema_migrations_seq,priority:3" json:"seq"`

	ObjectType ObjectType `gorm:"column:object_type;type:varchar(16);not null" json:"object_type"`
	SchemaName string     `gorm:"column:schema_name;type:varchar(128);not null" json:"schema_name"`
	ObjectName string     `gorm:"column:object_name;type:varchar(128);not null;default:''" json:"object_name"`

	Action MigrationAction `gorm:"column:action;type:varchar(16);not null" json:"action"`

	SourceDDL string `gorm:"column:source_ddl;type:mediumtext" json:"source_ddl"`
	TargetDDL string `gorm:"column:target_ddl;type:mediumtext" json:"target_ddl"`
	// Diff 是改写管线产出的结构化差异（列增删改、索引变化等），供计划预览
	// 界面展示，schema 由 internal/schemamig 定义。
	Diff JSON `gorm:"column:diff;type:json" json:"diff,omitempty"`

	Status MigrationStatus `gorm:"column:status;type:varchar(16);not null;index:idx_schema_migrations_status" json:"status"`
	Error  string          `gorm:"column:error;type:text" json:"error"`
}

// TableName 显式指定表名。
func (SchemaMigration) TableName() string { return "schema_migrations" }

// Validate 校验枚举字段。
func (m *SchemaMigration) Validate() error {
	if !m.ObjectType.Valid() {
		return fmt.Errorf("model: 未知的结构迁移对象类型 %q", m.ObjectType)
	}
	if !m.Action.Valid() {
		return fmt.Errorf("model: 未知的结构迁移动作 %q", m.Action)
	}
	if !m.Status.Valid() {
		return fmt.Errorf("model: 未知的结构迁移状态 %q", m.Status)
	}
	return nil
}
