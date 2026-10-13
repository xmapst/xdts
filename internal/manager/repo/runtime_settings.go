package repo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrRuntimeSettingsInvalid = errors.New("invalid runtime settings")
var ErrRuntimeSettingsVersion = errors.New("runtime settings changed")

type RuntimeSettings struct {
	LogRetentionSeconds      int64 `json:"logRetentionSeconds"`
	MetricsRetentionSeconds  int64 `json:"metricsRetentionSeconds"`
	EventRetentionSeconds    int64 `json:"eventRetentionSeconds"`
	ConflictRetentionSeconds int64 `json:"conflictRetentionSeconds"`
	PrecheckValiditySeconds  int64 `json:"precheckValiditySeconds"`
}

type RuntimeSettingsSnapshot struct {
	RuntimeSettings
	Version string `json:"version"`
}

func (s RuntimeSettings) Validate() error {
	for _, value := range []int64{s.LogRetentionSeconds, s.MetricsRetentionSeconds, s.EventRetentionSeconds, s.ConflictRetentionSeconds} {
		if value < 1 || value > 3650*86400 {
			return ErrRuntimeSettingsInvalid
		}
	}
	if s.PrecheckValiditySeconds < 1 || s.PrecheckValiditySeconds > 86400 {
		return ErrRuntimeSettingsInvalid
	}
	return nil
}

// Entries always follow key order, shared by initialization and updates.
func (s RuntimeSettings) entries() []struct {
	key   string
	value int64
} {
	return []struct {
		key   string
		value int64
	}{{PrecheckValiditySetting, s.PrecheckValiditySeconds}, {"retention.conflicts_seconds", s.ConflictRetentionSeconds}, {"retention.events_seconds", s.EventRetentionSeconds}, {"retention.logs_seconds", s.LogRetentionSeconds}, {"retention.metrics_seconds", s.MetricsRetentionSeconds}}
}

func (s RuntimeSettings) version() string {
	data, _ := json.Marshal(s)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

// InitializeRuntimeSettings only inserts missing keys. Existing values survive
// restarts and must validate before destructive background tasks start.
func InitializeRuntimeSettings(ctx context.Context, db *gorm.DB, defaults RuntimeSettings, now time.Time) error {
	if defaults.Validate() != nil || now.IsZero() {
		return ErrRuntimeSettingsInvalid
	}
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, entry := range defaults.entries() {
			value, _ := json.Marshal(entry.value)
			row := model.Setting{CreatedAt: now, UpdatedAt: now, Key: entry.key, Value: model.JSON(value)}
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&row).Error; err != nil {
				return err
			}
		}
		_, err := ReadRuntimeSettings(ctx, tx)
		return err
	})
}

func ReadRuntimeSettings(ctx context.Context, db *gorm.DB) (RuntimeSettingsSnapshot, error) {
	return readRuntimeSettings(ctx, db, false)
}

func readRuntimeSettings(ctx context.Context, db *gorm.DB, lock bool) (RuntimeSettingsSnapshot, error) {
	keys := make([]string, 0, 5)
	for _, entry := range (RuntimeSettings{}).entries() {
		keys = append(keys, entry.key)
	}
	query := db.WithContext(ctx).Where("`key` IN ?", keys).Order("`key` ASC")
	if lock {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var rows []model.Setting
	if err := query.Find(&rows).Error; err != nil {
		return RuntimeSettingsSnapshot{}, err
	}
	if len(rows) != len(keys) {
		return RuntimeSettingsSnapshot{}, ErrRuntimeSettingsInvalid
	}
	values := make(map[string]int64, len(rows))
	for _, row := range rows {
		var value int64
		if json.Unmarshal(row.Value, &value) != nil {
			return RuntimeSettingsSnapshot{}, ErrRuntimeSettingsInvalid
		}
		values[row.Key] = value
	}
	s := RuntimeSettings{LogRetentionSeconds: values["retention.logs_seconds"], MetricsRetentionSeconds: values["retention.metrics_seconds"], EventRetentionSeconds: values["retention.events_seconds"], ConflictRetentionSeconds: values["retention.conflicts_seconds"], PrecheckValiditySeconds: values[PrecheckValiditySetting]}
	if err := s.Validate(); err != nil {
		return RuntimeSettingsSnapshot{}, err
	}
	return RuntimeSettingsSnapshot{RuntimeSettings: s, Version: s.version()}, nil
}

// UpdateRuntimeSettings joins the caller's transaction, including its audit.
// A version conflict returns before any value is written.
func UpdateRuntimeSettings(ctx context.Context, tx *gorm.DB, settings RuntimeSettings, expected string, user uuidx.UUID, now time.Time) (RuntimeSettingsSnapshot, bool, error) {
	if settings.Validate() != nil || len(expected) != 64 || user.IsZero() || now.IsZero() {
		return RuntimeSettingsSnapshot{}, false, ErrRuntimeSettingsInvalid
	}
	if _, err := hex.DecodeString(expected); err != nil {
		return RuntimeSettingsSnapshot{}, false, ErrRuntimeSettingsInvalid
	}
	current, err := readRuntimeSettings(ctx, tx, true)
	if err != nil {
		return RuntimeSettingsSnapshot{}, false, err
	}
	if current.RuntimeSettings == settings {
		return current, false, nil
	}
	if current.Version != expected {
		return RuntimeSettingsSnapshot{}, false, ErrRuntimeSettingsVersion
	}
	for _, entry := range settings.entries() {
		value, _ := json.Marshal(entry.value)
		if err := UpsertSetting(ctx, tx, entry.key, string(value), user, now); err != nil {
			return RuntimeSettingsSnapshot{}, false, fmt.Errorf("update runtime setting %s: %w", entry.key, err)
		}
	}
	return RuntimeSettingsSnapshot{RuntimeSettings: settings, Version: settings.version()}, true, nil
}
