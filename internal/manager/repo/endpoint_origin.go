package repo

import (
	"context"
	"errors"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrEndpointOriginInvalid = errors.New("repo: endpoint origin UUID is invalid")
var ErrEndpointProbeNodeInvalid = errors.New("repo: endpoint probe node is unavailable or belongs to another center")

func parseEndpointOrigin(raw string) (uuidx.UUID, error) {
	id, err := uuidx.Parse(raw)
	if err != nil || id.IsZero() || len(raw) != 36 {
		return uuidx.UUID{}, ErrEndpointOriginInvalid
	}
	return id, nil
}

func registerEndpointOrigin(ctx context.Context, tx *gorm.DB, endpoint *model.Endpoint, identity uuidx.UUID, kind model.OriginKind) error {
	// Insert first and let the global unique index serialize concurrent discoveries.
	// ON DUPLICATE KEY does not change existing ownership or attribution.
	row := &model.CenterOriginUUID{CenterID: endpoint.CenterID, UUID: identity, Kind: kind, RefID: endpoint.ID.String()}
	res := tx.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(row)
	if res.Error != nil {
		return res.Error
	}
	var owner model.CenterOriginUUID
	if err := tx.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).Where("uuid=?", identity).First(&owner).Error; err != nil {
		return err
	}
	if owner.CenterID != endpoint.CenterID || (owner.Kind != kind && owner.Kind != model.OriginKindManual) {
		return ErrOriginUUIDConflict
	}
	if kind == model.OriginKindServer {
		var duplicates int64
		if err := tx.WithContext(ctx).Model(&model.Endpoint{}).
			Where("id<>? AND server_uuid=? AND (center_id<>? OR host<>? OR port<>?)", endpoint.ID, identity.String(), endpoint.CenterID, endpoint.Host, endpoint.Port).
			Count(&duplicates).Error; err != nil {
			return err
		}
		if duplicates != 0 {
			return ErrOriginUUIDConflict
		}
	}
	// MySQL may report a no-op duplicate as one affected row depending on client
	// flags. The generated ID proves whether this transaction actually inserted it.
	if owner.ID == row.ID {
		return markOriginRefresh(ctx, tx, endpoint.CenterID)
	}
	return nil
}
