package task

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (l *AlertLoop) applyRule(ctx context.Context, db *gorm.DB, r model.AlertRule, s alertSnapshot, now time.Time) error {
	if err := model.ValidateAlertRuleConfiguration(r); err != nil {
		return fmt.Errorf("invalid stored alert rule %s", r.ID)
	}
	observations, err := s.evaluate(r, now)
	if err != nil {
		return err
	}
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current model.AlertRule
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&current, "id=?", r.ID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		if current.LockVersion != r.LockVersion {
			return nil
		}
		if !current.Enabled {
			return tx.Model(&model.Alert{}).Where("rule_id=? AND status=?", r.ID, model.AlertStatusFiring).Updates(map[string]any{"status": model.AlertStatusResolved, "resolved_at": now}).Error
		}
		var active []model.Alert
		if err := tx.Where("rule_id=? AND status=?", r.ID, model.AlertStatusFiring).Limit(10001).Find(&active).Error; err != nil {
			return err
		}
		if len(active) > 10000 {
			return errors.New("active alert evaluation limit exceeded")
		}
		prefix := "rule/" + r.ID.String() + "/"
		for subject, o := range observations {
			if !o.known || !o.firing {
				continue
			}
			_, err := repo.UpsertAlertByDedupeKey(ctx, tx, model.Alert{RuleID: r.ID, Dimension: r.Dimension, Level: model.EventLevelError, Status: model.AlertStatusFiring, PipelineID: o.pipeline, NodeID: o.node, Detail: o.detail}, prefix+subject, now)
			if err != nil {
				return err
			}
			if err := tx.Model(&model.Alert{}).Where("dedupe_key=? AND status=?", prefix+subject, model.AlertStatusFiring).Update("detail", o.detail).Error; err != nil {
				return err
			}
		}
		for _, a := range active {
			if !strings.HasPrefix(a.DedupeKey, prefix) {
				continue
			}
			o, present := observations[strings.TrimPrefix(a.DedupeKey, prefix)]
			if !present && (r.Dimension == model.AlertDimensionQueueBacklog || r.Dimension == model.AlertDimensionLatencyExceeded) {
				for _, p := range s.pipelines {
					if p.ID == a.PipelineID {
						present = true
						break
					}
				}
			}
			if present && (!o.known || o.firing) {
				continue
			}
			if _, err := repo.ResolveAlertsByDedupeKey(ctx, tx, a.DedupeKey, now); err != nil {
				return err
			}
		}
		return nil
	})
}

// A rule row remains locked while sending, so disabling/deleting a rule cannot
// succeed and subsequently race an old notification. Requests are bounded.
func (l *AlertLoop) notifyRule(ctx context.Context, db *gorm.DB, id uuidx.UUID, now time.Time) error {
	var candidates []model.Alert
	if err := db.WithContext(ctx).Where("rule_id=? AND status=?", id, model.AlertStatusFiring).Order("notified_at ASC, fired_at ASC, id ASC").Limit(100).Find(&candidates).Error; err != nil {
		return err
	}
	var failures []error
	for _, candidate := range candidates {
		if ctx.Err() != nil {
			return errors.Join(append(failures, ctx.Err())...)
		}
		err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var r model.AlertRule
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&r, "id=?", id).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil
				}
				return err
			}
			if !r.Enabled || r.WebhookURL == "" {
				return nil
			}
			if err := model.ValidateAlertRuleConfiguration(r); err != nil {
				return errors.New("invalid alert notification configuration")
			}
			var a model.Alert
			if err := tx.First(&a, "id=? AND status=?", candidate.ID, model.AlertStatusFiring).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil
				}
				return err
			}
			if a.NotifiedAt != nil && r.SuppressSeconds > 0 && a.NotifiedAt.After(now.Add(-time.Duration(r.SuppressSeconds)*time.Second)) {
				return nil
			}
			if err := l.sendWebhook(ctx, r, a); err != nil {
				return err
			}
			return tx.Model(&model.Alert{}).Where("id=? AND status=?", a.ID, model.AlertStatusFiring).Update("notified_at", now).Error
		})
		if err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (l *AlertLoop) sendWebhook(ctx context.Context, r model.AlertRule, a model.Alert) error {
	body, err := json.Marshal(struct {
		ID         string               `json:"id"`
		RuleID     string               `json:"ruleId"`
		RuleName   string               `json:"ruleName"`
		Dimension  model.AlertDimension `json:"dimension"`
		Level      model.EventLevel     `json:"level"`
		Status     model.AlertStatus    `json:"status"`
		PipelineID *string              `json:"pipelineId"`
		NodeID     *string              `json:"nodeId"`
		FiredAt    time.Time            `json:"firedAt"`
		HitCount   int64                `json:"hitCount"`
		Detail     json.RawMessage      `json:"detail"`
	}{a.ID.String(), r.ID.String(), r.Name, a.Dimension, a.Level, a.Status, alertWebhookID(a.PipelineID), alertWebhookID(a.NodeID), a.FiredAt, a.HitCount, json.RawMessage(a.Detail)})
	if err != nil {
		return errors.New("alert notification encoding failed")
	}
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, r.WebhookURL, bytes.NewReader(body))
	if err != nil {
		return errors.New("alert webhook request invalid")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-XDTS-Alert-ID", a.ID.String())
	notificationKey := a.ID.String() + ":first"
	if a.NotifiedAt != nil {
		notificationKey = fmt.Sprintf("%s:%d", a.ID, a.NotifiedAt.UnixMilli())
	}
	req.Header.Set("X-XDTS-Notification-Key", notificationKey)
	response, err := l.client.Do(req)
	if err != nil {
		return fmt.Errorf("alert webhook delivery failed for alert %s", a.ID)
	}
	defer func(cleanup func() error) { _ = cleanup() }(response.Body.Close)
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("alert webhook rejected alert %s with status %d", a.ID, response.StatusCode)
	}
	return nil
}

func alertWebhookID(id uuidx.UUID) *string {
	if id.IsZero() {
		return nil
	}
	v := id.String()
	return &v
}
