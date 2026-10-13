package model

import (
	"fmt"
	"time"

	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

// NodeStatus 是节点生命周期状态。
type NodeStatus string

const (
	NodeStatusRegistering NodeStatus = "registering"
	NodeStatusOnline      NodeStatus = "online"   // 心跳正常
	NodeStatusOffline     NodeStatus = "offline"  // 心跳超时
	NodeStatusDisabled    NodeStatus = "disabled" // 管理员禁用
	NodeStatusRevoked     NodeStatus = "revoked"  // token 吊销，需重新注册
)

// Valid 校验节点状态取值。
func (s NodeStatus) Valid() bool {
	switch s {
	case NodeStatusRegistering, NodeStatusOnline, NodeStatusOffline, NodeStatusDisabled, NodeStatusRevoked:
		return true
	default:
		return false
	}
}

// Node 是一个已注册的分布式部署单元（需求 MGR-NODE-*）。
//
// 表的主键沿用全表统一的 UUIDv7 约定；对外 / 对节点自身暴露的 node_id 与
// 该主键是同一个值的字符串形式，不再另设列。
type Node struct {
	Base

	Name     string     `gorm:"column:name;type:varchar(128);not null;uniqueIndex:uk_nodes_name" json:"name"`
	CenterID uuidx.UUID `gorm:"column:center_id;type:binary(16);not null;index:idx_nodes_center_id" json:"center_id"`

	// Labels 是节点标签集合，用于策略执行位置、巡检节点选址等匹配。
	Labels JSON `gorm:"column:labels;type:json" json:"labels,omitempty"`
	// Only authenticated hello updates optional feature support. Heartbeats and
	// administrator edits do not alter this independent configuration guard.
	Capabilities JSON `gorm:"column:capabilities;type:json" json:"-"`
	// MaxPipelines limits assigned distinct pipeline IDs, including drafts and
	// pending cleanup. Zero preserves the existing unlimited allocation policy.
	MaxPipelines uint32 `gorm:"column:max_pipelines;not null;default:0" json:"max_pipelines"`

	Hostname string `gorm:"column:hostname;type:varchar(255);not null;default:''" json:"hostname"`
	// IPs 是节点可路由地址列表，逗号分隔（设计文档未将其标注为 JSON 列，
	// 与 labels/runtime 区别对待）。
	IPs     string `gorm:"column:ips;type:varchar(255);not null;default:''" json:"ips"`
	OSArch  string `gorm:"column:os_arch;type:varchar(64);not null;default:''" json:"os_arch"`
	Version string `gorm:"column:version;type:varchar(32);not null;default:''" json:"version"`

	// TokenHash 是节点身份 token 的哈希，管理库只存哈希、无法反查明文
	// 不得出现在任何查询接口或日志中。
	TokenHash string `gorm:"column:token_hash;type:varchar(64);not null;uniqueIndex:uk_nodes_token_hash" json:"-"`

	// RegisterCodeID 记录签发该节点身份的注册码归属。
	RegisterCodeID uuidx.UUID `gorm:"column:register_code_id;type:binary(16);index:idx_nodes_register_code_id" json:"register_code_id,omitempty"`

	Status NodeStatus `gorm:"column:status;type:varchar(16);not null" json:"status"`

	LastHeartbeatAt *time.Time `gorm:"column:last_heartbeat_at" json:"last_heartbeat_at"`
	RegisteredAt    time.Time  `gorm:"column:registered_at;not null" json:"registered_at"`

	// Runtime 是心跳上报的运行时信息（资源使用、本地状态库大小等），
	// 结构随版本演进，落为 JSON 列。
	Runtime JSON `gorm:"column:runtime;type:json" json:"runtime,omitempty"`

	// LockVersion 是乐观锁字段：心跳消费者与管理员编辑（改名、改标签、
	// 禁用）会并发命中同一行。
	LockVersion int64 `gorm:"column:lock_version;not null;default:1" json:"-"`

	// LogLevel and LogShipLevel are desired local and forwarded logging thresholds.
	// Actual values are reported by Node and remain independently observable.
	LogLevel     LogLevel `gorm:"column:log_level;type:varchar(8);not null;default:'info'" json:"log_level"`
	LogShipLevel LogLevel `gorm:"column:log_ship_level;type:varchar(8);not null;default:'info'" json:"log_ship_level"`

	// 软删除保留行本身就是 node_id 永久退役的保证：注册流程的唯一性校验
	// （token_hash 唯一索引、以及任何按 node_id/主键查重的逻辑）必须能看到
	// 这些已软删除的行，不能因为方便而在查重时过滤 deleted_at IS NOT
	// NULL，否则已退役的 node_id 会被复活并重新签发身份。
	DeletedAt gorm.DeletedAt `gorm:"column:deleted_at;index;index:idx_nodes_deleted_cleanup_state,priority:1" json:"-"`

	// CleanupState 是资源回收整体进度。「待清理」列表 = deleted_at IS NOT
	// NULL AND cleanup_state <> 'done'（与 Pipeline 同口径）。
	// 默认值 'done'：一条从未被注销的节点没有待回收的资源。
	CleanupState CleanupState `gorm:"column:cleanup_state;type:varchar(16);not null;default:'done';index:idx_nodes_deleted_cleanup_state,priority:2" json:"cleanup_state"`

	CleanupDetail JSON `gorm:"column:cleanup_detail;type:json" json:"cleanup_detail,omitempty"`
	// CleanupAttempts 是回收重试次数，用于退避。
	CleanupAttempts int `gorm:"column:cleanup_attempts;not null;default:0" json:"cleanup_attempts"`
	// CleanupLastError 是最近一次回收失败原因。
	CleanupLastError string `gorm:"column:cleanup_last_error;type:text" json:"cleanup_last_error,omitempty"`

	CleanupLastAttemptAt *time.Time `gorm:"column:cleanup_last_attempt_at" json:"cleanup_last_attempt_at,omitempty"`
}

// TableName 显式指定表名。
func (Node) TableName() string { return "nodes" }

// NodeCleanupDetail stores per-pipeline cleanup outcomes for node removal.
type NodeCleanupDetail struct {
	ControlQueue CleanupResourceDetail `json:"control_queue"` // 控制队列 xdts.node.<nodeId>.ctl（需求 MGR-NODE-06a）
}

// Validate 校验枚举字段。
func (n *Node) Validate() error {
	if n.MaxPipelines > 10000 {
		return fmt.Errorf("model: max_pipelines must be between 0 and 10000")
	}
	if !n.Status.Valid() {
		return fmt.Errorf("model: 未知的节点状态 %q", n.Status)
	}
	if !n.CleanupState.Valid() {
		return fmt.Errorf("model: 未知的节点回收状态 %q", n.CleanupState)
	}
	if !n.LogLevel.Valid() {
		return fmt.Errorf("model: 未知的节点日志级别 %q", n.LogLevel)
	}
	if !n.LogShipLevel.Valid() {
		return fmt.Errorf("model: 未知的节点上报日志级别 %q", n.LogShipLevel)
	}
	return nil
}

// Redacted 返回可安全对外序列化的副本：token_hash 置空。
func (n Node) Redacted() Node {
	n.TokenHash = redactedString
	return n
}

// NodeRegisterCode 是节点注册码（需求 O-04：复用为本地状态库口令，
// 因此强制一码一节点，注册后不得清除）。
type NodeRegisterCode struct {
	Base

	CodeHash string `gorm:"column:code_hash;type:varchar(64);not null;uniqueIndex:uk_node_register_codes_code_hash" json:"-"`

	ExpiresAt time.Time `gorm:"column:expires_at;not null" json:"expires_at"`

	// MaxUses/UsedCount：业务规则强制一码一节点（max_uses 应为 1，
	// 需求 O-04）；本层只做类型与非负约束，不在 DB 层写死 1，避免过度
	// 约束未来演进空间。
	MaxUses   int `gorm:"column:max_uses;not null;default:1" json:"max_uses"`
	UsedCount int `gorm:"column:used_count;not null;default:0" json:"used_count"`

	CenterID uuidx.UUID `gorm:"column:center_id;type:binary(16);not null;index:idx_node_register_codes_center_id" json:"center_id"`

	PresetName string `gorm:"column:preset_name;type:varchar(128);not null;default:''" json:"preset_name"`
	// PresetLabels 预置给注册成功节点的标签集合，与 nodes.labels 同构。
	PresetLabels JSON `gorm:"column:preset_labels;type:json" json:"preset_labels,omitempty"`

	CreatedBy uuidx.UUID `gorm:"column:created_by;type:binary(16);not null" json:"created_by"`
	// RevokedAt 非 nil 表示注册码已作废；nil 表示仍然有效。用指针而非零值
	// time.Time，避免 Go 零值落库时超出 MySQL DATETIME 取值范围报错。
	RevokedAt *time.Time `gorm:"column:revoked_at" json:"revoked_at"`
}

// TableName 显式指定表名。
func (NodeRegisterCode) TableName() string { return "node_register_codes" }

// Redacted 返回可安全对外序列化的副本：code_hash 置空。
func (c NodeRegisterCode) Redacted() NodeRegisterCode {
	c.CodeHash = redactedString
	return c
}
