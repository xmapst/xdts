package model

import (
	"fmt"
	"time"

	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

// AlertDimension 定义告警规则可观察的指标维度。
type AlertDimension string

const (
	AlertDimensionNodeOffline            AlertDimension = "node_offline"
	AlertDimensionLatencyExceeded        AlertDimension = "latency_exceeded"
	AlertDimensionPipelineError          AlertDimension = "pipeline_error"
	AlertDimensionQueueBacklog           AlertDimension = "queue_backlog"
	AlertDimensionFederationLinkDown     AlertDimension = "federation_link_down"
	AlertDimensionUnregisteredOriginUUID AlertDimension = "unregistered_origin_uuid"
	AlertDimensionLocalStoreAbnormal     AlertDimension = "local_store_abnormal"
)

// Valid 校验告警维度取值。
func (d AlertDimension) Valid() bool {
	switch d {
	case AlertDimensionNodeOffline, AlertDimensionLatencyExceeded, AlertDimensionPipelineError,
		AlertDimensionQueueBacklog, AlertDimensionFederationLinkDown,
		AlertDimensionUnregisteredOriginUUID, AlertDimensionLocalStoreAbnormal:
		return true
	default:
		return false
	}
}

// AlertStatus 是一次告警发生记录的生命周期状态。
type AlertStatus string

const (
	AlertStatusFiring   AlertStatus = "firing"
	AlertStatusResolved AlertStatus = "resolved"
)

// Valid 校验告警状态取值。
func (s AlertStatus) Valid() bool {
	switch s {
	case AlertStatusFiring, AlertStatusResolved:
		return true
	default:
		return false
	}
}

// AlertRule 定义告警维度、阈值、通知目标和抑制窗口。
type AlertRule struct {
	Base

	Name      string         `gorm:"column:name;type:varchar(128);not null;uniqueIndex:uk_alert_rules_name" json:"name"`
	Dimension AlertDimension `gorm:"column:dimension;type:varchar(32);not null;index:idx_alert_rules_dimension" json:"dimension"`
	Enabled   bool           `gorm:"column:enabled;not null;default:true" json:"enabled"`

	// Threshold 是该维度的阈值参数（如延迟毫秒数、积压条数），schema 随
	// 维度而不同。
	Threshold JSON `gorm:"column:threshold;type:json" json:"threshold,omitempty"`

	WebhookURL      string `gorm:"column:webhook_url;type:varchar(255);not null;default:''" json:"webhook_url"`
	SuppressSeconds int64  `gorm:"column:suppress_seconds;not null;default:0" json:"suppress_seconds"`

	CreatedBy uuidx.UUID `gorm:"column:created_by;type:binary(16);not null" json:"created_by"`

	LockVersion int64          `gorm:"column:lock_version;not null;default:1" json:"-"`
	DeletedAt   gorm.DeletedAt `gorm:"column:deleted_at;index" json:"-"`
}

// TableName 显式指定表名。
func (AlertRule) TableName() string { return "alert_rules" }

// Validate 校验枚举字段。
func (r *AlertRule) Validate() error {
	if !r.Dimension.Valid() {
		return fmt.Errorf("model: 未知的告警维度 %q", r.Dimension)
	}
	return nil
}

// Alert 保存一次告警发生记录。
type Alert struct {
	Base

	RuleID    uuidx.UUID     `gorm:"column:rule_id;type:binary(16);not null;index:idx_alerts_rule_id" json:"rule_id"`
	Dimension AlertDimension `gorm:"column:dimension;type:varchar(32);not null;index:idx_alerts_dimension" json:"dimension"`
	Level     EventLevel     `gorm:"column:level;type:varchar(16);not null" json:"level"`
	Status    AlertStatus    `gorm:"column:status;type:varchar(16);not null;index:idx_alerts_status" json:"status"`

	// PipelineID/NodeID 视维度而定，可能同时为 uuidx.Nil（如联邦链路断开
	// 这类中心级告警）。
	PipelineID uuidx.UUID `gorm:"column:pipeline_id;type:binary(16)" json:"pipeline_id"`
	NodeID     uuidx.UUID `gorm:"column:node_id;type:binary(16)" json:"node_id"`

	Detail JSON `gorm:"column:detail;type:json" json:"detail,omitempty"`

	FiredAt time.Time `gorm:"column:fired_at;not null" json:"fired_at"`

	// 为什么需要它，而不能用 (rule_id, dimension, pipeline_id, node_id) 这组
	// 天然键去重：登录锁定这类告警没有对应的 AlertRule 行（RuleID 只能传
	// uuidx.Nil），pipeline_id / node_id 也都是 Nil，于是同一维度下所有主体
	// （不同用户、不同 IP）会塌成同一行，而"锁定针对谁"恰恰是这条告警唯一
	// 有用的信息。⇒ 去重键必须由调用方按自己的主体构造，本层只按字符串比较。
	DedupeKey string `gorm:"column:dedupe_key;type:varchar(191);not null;default:'';index:idx_alerts_dedupe_key" json:"dedupe_key"`

	// HitCount 是本行自 FiredAt 起被重复命中的次数（首次落库为 1）。
	// 去重合并时只递增它，不新增行——撞库场景下每次请求刷一行会把告警表写爆。
	HitCount int64 `gorm:"column:hit_count;not null;default:1" json:"hit_count"`

	// LastSeenAt 是最近一次命中的时刻。不复用 Base.UpdatedAt：UpdatedAt 会被
	// 任何一次更新（包括恢复、通知）改写，而"最后一次触发是什么时候"必须与
	// "这行最后被改是什么时候"分开——否则告警恢复的那一刻会把最后触发时刻
	// 一起改掉，而那正是排查时要看的值。为 nil 表示只命中过一次（等于 FiredAt）。
	LastSeenAt *time.Time `gorm:"column:last_seen_at" json:"last_seen_at"`

	ResolvedAt *time.Time `gorm:"column:resolved_at" json:"resolved_at"`
	NotifiedAt *time.Time `gorm:"column:notified_at" json:"notified_at"`
}

// TableName 显式指定表名。
func (Alert) TableName() string { return "alerts" }

// Validate 校验枚举字段。
func (a *Alert) Validate() error {
	if !a.Dimension.Valid() {
		return fmt.Errorf("model: 未知的告警维度 %q", a.Dimension)
	}
	if !a.Level.Valid() {
		return fmt.Errorf("model: 未知的告警级别 %q", a.Level)
	}
	if !a.Status.Valid() {
		return fmt.Errorf("model: 未知的告警状态 %q", a.Status)
	}
	return nil
}
