package model

import "gorm.io/gorm"

// TopologyType 描述拓扑的连接形态，用于拓扑视图展示与来源策略推荐。
type TopologyType string

const (
	TopologyTypeOneWay        TopologyType = "one_way"       // 单向：A → B（UC-01）
	TopologyTypeBidirectional TopologyType = "bidirectional" // 双向：A ⇄ B（UC-02）
	TopologyTypeCascade       TopologyType = "cascade"       // 级联：A → B → C（UC-03）
	TopologyTypeCustom        TopologyType = "custom"        // 其他自定义拓扑
)

// Valid 校验拓扑类型取值。
func (t TopologyType) Valid() bool {
	switch t {
	case TopologyTypeOneWay, TopologyTypeBidirectional, TopologyTypeCascade, TopologyTypeCustom:
		return true
	default:
		return false
	}
}

// Topology 是中心为点、pipeline 为边的逻辑分组，用于回环校验与拓扑视图
// （需求 MGR-TOPO-*）。
type Topology struct {
	Base

	Name        string       `gorm:"column:name;type:varchar(128);not null;uniqueIndex:uk_topologies_name" json:"name"`
	Type        TopologyType `gorm:"column:type;type:varchar(16);not null" json:"type"`
	Description string       `gorm:"column:description;type:text" json:"description"`

	DeletedAt gorm.DeletedAt `gorm:"column:deleted_at;index" json:"-"`
}

// TableName 显式指定表名。
func (Topology) TableName() string { return "topologies" }
