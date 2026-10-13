package model

import (
	"time"

	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

// Endpoint 是一个 MySQL 连接定义：地址、凭据、探测得到的版本与 server_uuid
// 等。
type Endpoint struct {
	Base

	Name     string     `gorm:"column:name;type:varchar(128);not null;uniqueIndex:uk_endpoints_name" json:"name"`
	CenterID uuidx.UUID `gorm:"column:center_id;type:binary(16);not null;index:idx_endpoints_center_id" json:"center_id"`

	Host     string `gorm:"column:host;type:varchar(255);not null" json:"host"`
	Port     uint16 `gorm:"column:port;not null" json:"port"`
	Username string `gorm:"column:username;type:varchar(128);not null" json:"username"`
	// Password 明文存储，接口只写不读，查询返回脱敏值；仅下发给承担对应
	// 角色的节点（最小授权）。
	Password string `gorm:"column:password;type:varchar(255);not null;default:''" json:"-"`

	// TLS/Params 保存连接参数（如 TLS 模式、CA、字符集、超时等），
	// 结构随版本演进，落为 JSON 列，具体 schema 由使用方（节点探测 /
	// 连接组装逻辑）约定并校验，本层不做强类型解析。
	TLS    JSON `gorm:"column:tls;type:json" json:"tls,omitempty"`
	Params JSON `gorm:"column:params;type:json" json:"params,omitempty"`

	// Version is the probed MySQL server version, not the row's concurrency revision.
	Version string `gorm:"column:version;type:varchar(32);not null;default:''" json:"version"`
	// ServerUUID 是探测得到的 @@server_uuid，用于回环识别登记
	// （与 center_origin_uuids 关联）。空字符串表示「未探测」，理由与 Version
	// 一致：@@server_uuid 在一次成功探测里恒非空，不存在探测成功但取值为空的
	// 合法结果。
	//
	// 成功探测由 repo.UpdateEndpointProbeResult 同事务写回实例身份，并自动
	// 登记中心 server/group UUID。手工和合成身份由 repo.CreateCenterOriginUUID
	// 登记；所有身份增删持久化 center_origin_refresh，节点配置经对账刷新。
	//
	// 它比 Version 更需要本层这一道：节点侧对 version 有
	// mysqlx.ParseVersion 校验，对 server_uuid 没有任何校验
	// （internal/node/probe/conntest.go 直接 Scan 出来原样填进回执）。
	ServerUUID string `gorm:"column:server_uuid;type:varchar(36);not null;default:''" json:"server_uuid"`

	// LowerCaseTableNames is nullable probed metadata. Nil means unknown; zero is
	// a valid case-sensitive server policy.
	LowerCaseTableNames string `gorm:"column:lower_case_table_names;type:varchar(32);not null;default:''" json:"lower_case_table_names"`

	ProbeNodeID uuidx.UUID `gorm:"column:probe_node_id;type:binary(16)" json:"probe_node_id"`
	// LastProbeAt 为 nil 表示从未探测过；用指针而非零值 time.Time，避免 Go
	// 零值（0001-01-01）落库时超出 MySQL DATETIME 取值范围报错。
	LastProbeAt *time.Time `gorm:"column:last_probe_at" json:"last_probe_at"`

	LastProbeResult JSON `gorm:"column:last_probe_result;type:json" json:"last_probe_result,omitempty"`

	// LockVersion 是乐观锁字段：探测结果写回与管理员编辑连接信息可能并发
	// 命中同一行。
	LockVersion int64 `gorm:"column:lock_version;not null;default:1" json:"-"`

	DeletedAt gorm.DeletedAt `gorm:"column:deleted_at;index" json:"-"`
}

// TableName 显式指定表名。
func (Endpoint) TableName() string { return "endpoints" }

func (e *Endpoint) Validate() error {
	return nil
}

// Redacted 返回可安全对外序列化的副本：password 置空。
func (e Endpoint) Redacted() Endpoint {
	e.Password = redactedString
	return e
}
