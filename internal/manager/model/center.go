package model

import (
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/pkg/uuidx"
)

// Center 是一个业务中心：一套独立部署的 Broker + 若干节点与端点
// （设计 ADR-04、需求 C-*）。
type Center struct {
	Base

	Code   string `gorm:"column:code;type:varchar(64);not null;uniqueIndex:uk_centers_code" json:"code"`
	Name   string `gorm:"column:name;type:varchar(128);not null;uniqueIndex:uk_centers_name" json:"name"`
	Region string `gorm:"column:region;type:varchar(64);not null;default:''" json:"region"`

	// IsManagerCenter 标记该中心是否为 Manager 所在中心（用于巡检节点选址
	// 等特殊逻辑）。
	IsManagerCenter bool `gorm:"column:is_manager_center;not null;default:false" json:"is_manager_center"`

	// MQMgmtURL/User/Password 是该中心 Broker 的 Management API 地址与账号，
	// 只下发给该中心的巡检节点（最小授权）。
	MQMgmtURL  string `gorm:"column:mq_mgmt_url;type:varchar(255);not null;default:''" json:"mq_mgmt_url"`
	MQMgmtUser string `gorm:"column:mq_mgmt_user;type:varchar(128);not null;default:''" json:"mq_mgmt_user"`
	// MQMgmtPassword 明文存储，接口只写不读，查询返回脱敏值；日志 / 审计 /
	// 上报一律脱敏。
	MQMgmtPassword string `gorm:"column:mq_mgmt_password;type:varchar(255);not null;default:''" json:"-"`

	// InspectorNodeID 是承担本中心巡检职责的节点，未指定时为 uuidx.Nil。
	InspectorNodeID uuidx.UUID `gorm:"column:inspector_node_id;type:binary(16)" json:"inspector_node_id"`

	// LastBrokerProbeAt 为 nil 表示从未巡检过；用指针而非零值 time.Time，
	// 避免 Go 零值（0001-01-01）落库时超出 MySQL DATETIME 取值范围报错
	// （与 Endpoint.LastProbeAt 同一原因）。
	LastBrokerProbeAt *time.Time `gorm:"column:last_broker_probe_at" json:"last_broker_probe_at"`
	// LastBrokerProbeResult 落库 BrokerResult 的完整内容（federation_ok /
	// backlogs[] / disk_free_bytes / warnings[]），具体 schema 由探测协议约定，
	// 本层不关心、不做强类型解析，只是落库点（与 Endpoint.LastProbeResult
	// 的职责边界一致）。
	LastBrokerProbeResult JSON `gorm:"column:last_broker_probe_result;type:json" json:"last_broker_probe_result,omitempty"`

	Description string `gorm:"column:description;type:text" json:"description"`

	// LockVersion 是乐观锁字段：巡检任务可能并发更新 InspectorNodeID，
	// 与管理员的编辑请求存在竞争。
	LockVersion int64 `gorm:"column:lock_version;not null;default:1" json:"-"`

	DeletedAt gorm.DeletedAt `gorm:"column:deleted_at;index" json:"-"`
}

// TableName 显式指定表名。
func (Center) TableName() string { return "centers" }

// Redacted 返回可安全对外序列化的副本：mq_mgmt_password 置空。
func (c Center) Redacted() Center {
	c.MQMgmtPassword = redactedString
	return c
}

// OriginKind 是来源身份的登记方式（设计 ADR-01：用 GTID 来源识别替代业务库
// 标记表）。
type OriginKind string

const (
	OriginKindServer    OriginKind = "server"    // 单台 MySQL 实例的 server_uuid
	OriginKindGroup     OriginKind = "group"     // 复制组 / 集群共享的来源标识
	OriginKindManual    OriginKind = "manual"    // 运维手工登记（如断点接管场景）
	OriginKindSynthetic OriginKind = "synthetic" // xdts 自身生成的合成 UUID（全量 / 结构迁移）
)

// Valid 校验来源登记方式取值。
func (k OriginKind) Valid() bool {
	switch k {
	case OriginKindServer, OriginKindGroup, OriginKindManual, OriginKindSynthetic:
		return true
	default:
		return false
	}
}

// CenterOriginUUID 是回环识别所需的来源身份集合（ADR-01）。
//
// uuid 全局唯一：同一个来源 UUID 只能归属一个中心，否则回环判定会产生
// 二义性。
type CenterOriginUUID struct {
	Base

	CenterID uuidx.UUID `gorm:"column:center_id;type:binary(16);not null;index:idx_center_origin_uuids_center_id" json:"center_id"`

	UUID uuidx.UUID `gorm:"column:uuid;type:binary(16);not null;uniqueIndex:uk_center_origin_uuids_uuid" json:"uuid"`
	Kind OriginKind `gorm:"column:kind;type:varchar(16);not null" json:"kind"`

	// RefID 视 Kind 而定：server_uuid 本身、复制组名、pipeline_id（synthetic）
	// 等，留作自由文本以兼容多种来源。
	RefID string `gorm:"column:ref_id;type:varchar(128);not null;default:''" json:"ref_id"`
}

// TableName 显式指定表名。
func (CenterOriginUUID) TableName() string { return "center_origin_uuids" }

// Validate 校验枚举字段。
func (c *CenterOriginUUID) Validate() error {
	if !c.Kind.Valid() {
		return fmt.Errorf("model: 未知的来源登记方式 %q", c.Kind)
	}
	return nil
}
