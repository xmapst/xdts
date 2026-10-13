package model

import (
	"fmt"

	"github.com/xmapst/xdts/pkg/uuidx"
)

// LogLevel 对应 log/slog 的标准级别。
type LogLevel string

const (
	LogLevelDebug LogLevel = "debug"
	LogLevelInfo  LogLevel = "info"
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
)

// Valid 校验日志级别取值。
func (l LogLevel) Valid() bool {
	switch l {
	case LogLevelDebug, LogLevelInfo, LogLevelWarn, LogLevelError:
		return true
	default:
		return false
	}
}

// LogSource 标识日志来源：节点或 Manager。
type LogSource string

const (
	LogSourceNode    LogSource = "node"
	LogSourceManager LogSource = "manager"
)

// Valid 校验日志来源取值。
func (s LogSource) Valid() bool {
	switch s {
	case LogSourceNode, LogSourceManager:
		return true
	default:
		return false
	}
}

// Log 是节点 / Manager 日志的集中存储（需求
// MGR-MON-02/09）。
//
// 按天 RANGE 分区，主键取 (id, ts)（TimePKBase）；分区与二级
// 索引由 internal/manager/repo 的迁移用原生 DDL 创建。
type Log struct {
	TimePKBase

	// NodeID/PipelineID 用 uuidx.Nil 表示「不适用」：Manager 自身日志没有
	// NodeID，节点日志中与具体 pipeline 无关的日志没有 PipelineID。
	NodeID     uuidx.UUID `gorm:"column:node_id;type:binary(16)" json:"node_id"`
	PipelineID uuidx.UUID `gorm:"column:pipeline_id;type:binary(16)" json:"pipeline_id"`

	Level   LogLevel `gorm:"column:level;type:varchar(8);not null" json:"level"`
	Module  string   `gorm:"column:module;type:varchar(64);not null;default:''" json:"module"`
	Message string   `gorm:"column:message;type:text;not null" json:"message"`
	// Attrs 是结构化字段（对应 slog.Attr），承载动态内容；Message 保持稳定
	// 字面量。
	Attrs  JSON      `gorm:"column:attrs;type:json" json:"attrs,omitempty"`
	Source LogSource `gorm:"column:source;type:varchar(8);not null" json:"source"`
}

// TableName 显式指定表名。
func (Log) TableName() string { return "logs" }

// Validate 校验枚举字段。
func (l *Log) Validate() error {
	if !l.Level.Valid() {
		return fmt.Errorf("model: 未知的日志级别 %q", l.Level)
	}
	if !l.Source.Valid() {
		return fmt.Errorf("model: 未知的日志来源 %q", l.Source)
	}
	return nil
}
