package model

import (
	"time"

	"github.com/xmapst/xdts/pkg/uuidx"
)

type TopologyConfirmation struct {
	Base
	TopologyID   uuidx.UUID `gorm:"column:topology_id;type:binary(16);not null;uniqueIndex:uk_topology_confirmation"`
	PolicyDigest string     `gorm:"column:policy_digest;type:char(64);not null"`
	ConfirmedBy  uuidx.UUID `gorm:"column:confirmed_by;type:binary(16);not null"`
	ConfirmedAt  time.Time  `gorm:"column:confirmed_at;type:datetime(3);not null"`
}

func (TopologyConfirmation) TableName() string { return "topology_confirmations" }
