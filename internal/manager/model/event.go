package model

import (
	"fmt"
	"time"

	"github.com/xmapst/xdts/pkg/uuidx"
)

// EventType 定义平台事件类型，与上报协议和前端事件词表保持一致。
type EventType string

const (
	EventTypeError             EventType = "error"
	EventTypeDDL               EventType = "ddl"
	EventTypeConflict          EventType = "conflict"
	EventTypeUnknownOrigin     EventType = "unknown_origin"
	EventTypeBackpressure      EventType = "backpressure"
	EventTypeLocalStoreRebuild EventType = "local_store_rebuild"
	EventTypeLinkProbeFailed   EventType = "link_probe_failed"
	EventTypeNodeOffline       EventType = "node_offline"

	// 它描述事件，不描述结果：成功与失败都用这一个取值，成败由 payload
	// 的 success 字段承载。与 error / conflict 那种"这是个坏消息"型取值刻意
	// 区分开——把结果编进类型名（config_apply_failed 之类）会逼出第二个取值，
	// 而两个取值意味着同一件事有两种类型，按类型聚合与检索都会漏掉一半。
	//
	// 为什么不复用已有取值：白名单八个值没有一个能承载"配置应用结果"，而
	// 借用 local_store_rebuild 是对一个已发布取值的就地重新解释（红线
	// 「已发布的契约只增不改语义」）。新增取值是纯增量，旧数据不受影响。
	EventTypeConfigApplied EventType = "config_applied"
)

// Valid 校验事件类型取值。
func (t EventType) Valid() bool {
	switch t {
	case EventTypeError, EventTypeDDL, EventTypeConflict, EventTypeUnknownOrigin,
		EventTypeBackpressure, EventTypeLocalStoreRebuild, EventTypeLinkProbeFailed, EventTypeNodeOffline,
		EventTypeConfigApplied:
		return true
	default:
		return false
	}
}

// EventLevel 是事件级别：info、warning、error。
type EventLevel string

const (
	EventLevelInfo    EventLevel = "info"
	EventLevelWarning EventLevel = "warning"
	EventLevelError   EventLevel = "error"
)

// Valid 校验事件级别取值。
func (l EventLevel) Valid() bool {
	switch l {
	case EventLevelInfo, EventLevelWarning, EventLevelError:
		return true
	default:
		return false
	}
}

// Event 是节点上报的运行事件。
type Event struct {
	LogBase

	Ts time.Time `gorm:"column:ts;not null" json:"ts"`

	NodeID     uuidx.UUID `gorm:"column:node_id;type:binary(16)" json:"node_id"`
	PipelineID uuidx.UUID `gorm:"column:pipeline_id;type:binary(16)" json:"pipeline_id"`

	Type  EventType  `gorm:"column:type;type:varchar(32);not null" json:"type"`
	Level EventLevel `gorm:"column:level;type:varchar(16);not null" json:"level"`

	// Payload 是事件的结构化详情，schema 随事件类型而不同，由上报方与
	// 界面展示逻辑约定。
	Payload JSON `gorm:"column:payload;type:json" json:"payload,omitempty"`

	AckedBy uuidx.UUID `gorm:"column:acked_by;type:binary(16)" json:"acked_by"`

	// 用指针而非零值 `time.Time`，理由同 `Node.LastHeartbeatAt` / `Endpoint.LastProbeAt`：
	// Go 零值（0001-01-01）落库时超出 MySQL DATETIME 取值范围会直接报错。
	//
	// 与 AckedBy 严格同生共死：两列只由 `repo.AckEvent` 在同一条 UPDATE 里
	// 一起写入，没有任何路径能只写其中一个。不要把"确认时刻"理解成可以独立
	// 更新的字段——方案 B（只允许首次确认）下它一旦写入就不再变，
	// 后来者被 `ErrEventAlreadyAcked` 拦在写入之前。
	AckedAt *time.Time `gorm:"column:acked_at" json:"acked_at,omitempty"`
}

// TableName 显式指定表名。
func (Event) TableName() string { return "events" }

// Validate 校验枚举字段。
func (e *Event) Validate() error {
	if !e.Type.Valid() {
		return fmt.Errorf("model: 未知的事件类型 %q", e.Type)
	}
	if !e.Level.Valid() {
		return fmt.Errorf("model: 未知的事件级别 %q", e.Level)
	}
	return nil
}
