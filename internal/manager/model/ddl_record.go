package model

import (
	"fmt"
	"time"

	"github.com/xmapst/xdts/pkg/uuidx"
)

// DDLRecordStatus 是一条增量 DDL 应用记录的执行结果。
type DDLRecordStatus string

const (
	DDLRecordStatusApplied DDLRecordStatus = "applied"
	DDLRecordStatusFailed  DDLRecordStatus = "failed"
)

// Valid 校验执行结果取值。
func (s DDLRecordStatus) Valid() bool {
	switch s {
	case DDLRecordStatusApplied, DDLRecordStatusFailed:
		return true
	default:
		return false
	}
}

type DDLRecord struct {
	LogBase

	PipelineID uuidx.UUID `gorm:"column:pipeline_id;type:binary(16);not null;index:idx_ddl_records_pipeline_ts" json:"pipeline_id"`
	Ts         time.Time  `gorm:"column:ts;not null" json:"ts"`
	GTID       string     `gorm:"column:gtid;type:varchar(64);not null;default:''" json:"gtid"`

	SchemaName string `gorm:"column:schema_name;type:varchar(128);not null" json:"schema_name"`
	DDLSQL     string `gorm:"column:ddl_sql;type:mediumtext;not null" json:"ddl_sql"`

	Status DDLRecordStatus `gorm:"column:status;type:varchar(16);not null" json:"status"`
}

// TableName 显式指定表名。
func (DDLRecord) TableName() string { return "ddl_records" }

// Validate 校验枚举字段。
func (r *DDLRecord) Validate() error {
	if !r.Status.Valid() {
		return fmt.Errorf("model: 未知的 DDL 记录状态 %q", r.Status)
	}
	return nil
}
