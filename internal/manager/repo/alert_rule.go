package repo

import (
	"context"
	"errors"
	"time"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrAlertRuleVersionConflict = errors.New("repo: alert rule version conflict")

func UpdateAlertRule(ctx context.Context, db *gorm.DB, rule model.AlertRule) error {
	if err := model.ValidateAlertRuleConfiguration(rule); err != nil {
		return err
	}
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var previous model.AlertRule
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&previous, "id=?", rule.ID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrAlertRuleNotFound
			}
			return err
		}
		if previous.LockVersion != rule.LockVersion {
			return ErrAlertRuleVersionConflict
		}
		res := tx.Model(&model.AlertRule{}).Where("id=? AND lock_version=?", rule.ID, rule.LockVersion).Updates(map[string]any{
			"name": rule.Name, "dimension": rule.Dimension, "enabled": rule.Enabled, "threshold": rule.Threshold, "webhook_url": rule.WebhookURL, "suppress_seconds": rule.SuppressSeconds, "lock_version": gorm.Expr("lock_version + 1"),
		})
		if isDuplicateKeyError(res.Error) {
			return ErrAlertRuleNameConflict
		}
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return ErrAlertRuleVersionConflict
		}
		if !rule.Enabled || previous.Dimension != rule.Dimension {
			return tx.Model(&model.Alert{}).Where("rule_id=? AND status=?", rule.ID, model.AlertStatusFiring).Updates(map[string]any{"status": model.AlertStatusResolved, "resolved_at": time.Now()}).Error
		}
		return nil
	})
}

func DeleteAlertRule(ctx context.Context, db *gorm.DB, id uuidx.UUID, version int64, now time.Time) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res := tx.Model(&model.AlertRule{}).Where("id=? AND lock_version=?", id, version).Updates(map[string]any{"deleted_at": now, "enabled": false, "lock_version": gorm.Expr("lock_version + 1")})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return ErrAlertRuleVersionConflict
		}
		return tx.Model(&model.Alert{}).Where("rule_id=? AND status=?", id, model.AlertStatusFiring).Updates(map[string]any{"status": model.AlertStatusResolved, "resolved_at": now}).Error
	})
}
