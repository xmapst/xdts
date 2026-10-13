package repo

import (
	"context"
	"encoding/json"
	"errors"
	"slices"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func SetNodeCapabilities(ctx context.Context, db *gorm.DB, nodeID uuidx.UUID, capabilities []string) error {
	if err := iproto.ValidateNodeCapabilities(capabilities); err != nil {
		return err
	}
	data, err := json.Marshal(append([]string{}, capabilities...))
	if err != nil {
		return err
	}
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var node model.Node
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND status<>?", nodeID, model.NodeStatusRevoked).First(&node).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrNodeNotFound
			}
			return err
		}
		var prior []string
		_ = json.Unmarshal(node.Capabilities, &prior)
		changed := slices.Contains(prior, iproto.CapabilityPersistentLineage) != slices.Contains(capabilities, iproto.CapabilityPersistentLineage)
		if err := tx.Model(&model.Node{}).Where("id=?", nodeID).Updates(map[string]any{"capabilities": model.JSON(data), "lock_version": gorm.Expr("lock_version+1")}).Error; err != nil {
			return err
		}
		// Reuse the durable configuration refresh outbox. Upgrading either
		// assigned role must produce a new revision before changing route flags.
		if changed {
			return markOriginRefresh(ctx, tx, node.CenterID)
		}
		return nil
	})
}
