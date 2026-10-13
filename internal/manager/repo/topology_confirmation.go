package repo

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrTopologyConfirmationRequired = errors.New("topology warning confirmation required")
var ErrTopologyAnalysisChanged = errors.New("topology analysis changed or incomplete")

// LockTopology serializes graph edits, confirmation and new start intents.
// Graph reads still validate existence; stop and cleanup can use persisted rows.
func LockTopology(ctx context.Context, db *gorm.DB, id uuidx.UUID) error {
	var t model.Topology
	if id.String() != AutomaticTopologyID {
		if err := db.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=?", uuidx.MustParse(AutomaticTopologyID)).Find(&t).Error; err != nil {
			return err
		}
	}
	return db.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=?", id).Find(&t).Error
}

func TopologyConfirmed(ctx context.Context, db *gorm.DB, analysis *TopologyAnalysis) (bool, error) {
	if analysis.Incomplete {
		return false, nil
	}
	if len(analysis.Warnings) == 0 {
		return true, nil
	}
	var confirmation model.TopologyConfirmation
	id, err := uuidx.Parse(analysis.TopologyID)
	if err != nil {
		return false, err
	}
	err = db.WithContext(ctx).Where("topology_id=?", id).First(&confirmation).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return confirmation.PolicyDigest == analysis.PolicyDigest, nil
}

func CheckTopologyStart(ctx context.Context, db *gorm.DB, id uuidx.UUID) error {
	id = uuidx.MustParse(AutomaticTopologyID)
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		analysis, err := GetTopologyAnalysis(ctx, tx, id)
		if err != nil {
			return err
		}
		if err := CheckTopologyInitialization(ctx, tx, id); err != nil {
			return err
		}
		confirmed, err := TopologyConfirmed(ctx, tx, analysis)
		if err != nil {
			return err
		}
		if !confirmed {
			return ErrTopologyConfirmationRequired
		}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
}

// Confirmation and its permanent audit row commit together. Preview digest is
// exact; ongoing authorization is keyed to semantic policyDigest.
func ConfirmTopology(ctx context.Context, db *gorm.DB, id uuidx.UUID, digest string, userID uuidx.UUID, username string, now time.Time) error {
	if userID.IsZero() || len(digest) != 64 {
		return ErrTopologyAnalysisChanged
	}
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := LockTopology(ctx, tx, id); err != nil {
			return err
		}
		analysis, err := GetTopologyAnalysis(ctx, tx, id)
		if err != nil {
			return err
		}
		if analysis.Digest != digest || analysis.Incomplete {
			return ErrTopologyAnalysisChanged
		}
		if len(analysis.Warnings) == 0 {
			return nil
		}
		already, err := TopologyConfirmed(ctx, tx, analysis)
		if err != nil {
			return err
		}
		if already {
			return nil
		}
		record := model.TopologyConfirmation{TopologyID: id, PolicyDigest: analysis.PolicyDigest, ConfirmedBy: userID, ConfirmedAt: now}
		if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "topology_id"}}, DoUpdates: clause.AssignmentColumns([]string{"policy_digest", "confirmed_by", "confirmed_at", "updated_at"})}).Create(&record).Error; err != nil {
			return err
		}
		detail, err := json.Marshal(map[string]any{"username": username, "success": true, "changes": map[string]any{"digest": digest, "policyDigest": analysis.PolicyDigest, "warnings": analysis.Warnings}})
		if err != nil {
			return err
		}
		return CreateAuditLog(ctx, tx, &model.AuditLog{UserID: userID, Action: model.AuditActionConfirmLoopWarning, TargetType: model.AuditTargetTypeTopology, TargetID: id, Detail: model.JSON(detail)}, now)
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}
