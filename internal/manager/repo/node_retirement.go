package repo

import (
	"context"
	"errors"
	"time"

	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

// NodeRetiredCredential is only used for authenticated node.hello rejection.
// It never participates in normal RPC/report authentication or public views.
type NodeRetiredCredential struct {
	NodeID    uuidx.UUID `gorm:"column:node_id;primaryKey" json:"-"`
	TokenHash string     `gorm:"column:token_hash" json:"-"`
	RetiredAt time.Time  `gorm:"column:retired_at"`
}

func (NodeRetiredCredential) TableName() string { return "node_retired_credentials" }

func GetNodeRetiredCredential(ctx context.Context, db *gorm.DB, id uuidx.UUID) (*NodeRetiredCredential, error) {
	var record NodeRetiredCredential
	if err := db.WithContext(ctx).First(&record, "node_id=?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNodeNotFound
		}
		return nil, err
	}
	return &record, nil
}
