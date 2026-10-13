package repo

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrPrecheckConfigChanged        = errors.New("precheck configuration changed")
	ErrPrecheckIncomplete           = errors.New("precheck results incomplete")
	ErrPrecheckExpired              = errors.New("precheck expired")
	ErrPrecheckBlocked              = errors.New("precheck blocking item failed")
	ErrPrecheckConfirmationRequired = errors.New("precheck warning confirmation required")
	ErrPrecheckInvalid              = errors.New("invalid precheck results")
)

const PrecheckValiditySetting = "precheck.validity_seconds"
const DefaultPrecheckValiditySeconds = 1800

type PrecheckRequirement struct {
	Item  string              `json:"item"`
	Level model.PrecheckLevel `json:"level"`
}

type StoredPrecheck struct {
	Run   model.PrecheckRun
	Items []model.PipelinePrecheck
}

// PrecheckState combines one consistent batch snapshot with the current start
// decision. GateError is a domain rejection, distinct from a database failure.
type PrecheckState struct {
	Stored    StoredPrecheck
	GateError error
}

func PrecheckValidity(ctx context.Context, db *gorm.DB) (time.Duration, error) {
	setting, err := GetSetting(ctx, db, PrecheckValiditySetting)
	if errors.Is(err, ErrSettingNotFound) {
		return DefaultPrecheckValiditySeconds * time.Second, nil
	}
	if err != nil {
		return 0, err
	}
	var seconds int64
	if json.Unmarshal(setting.Value, &seconds) != nil || seconds < 1 || seconds > 86400 {
		return 0, ErrSettingValueInvalidJSON
	}
	return time.Duration(seconds) * time.Second, nil
}

func precheckRequirements(required []PrecheckRequirement) ([]byte, error) {
	if len(required) == 0 || len(required) > 100 {
		return nil, ErrPrecheckInvalid
	}
	seen := map[string]bool{}
	for _, item := range required {
		if len(item.Item) == 0 || len(item.Item) > 64 || !utf8.ValidString(item.Item) || strings.ContainsRune(item.Item, 0) || strings.TrimSpace(item.Item) != item.Item || seen[item.Item] || !item.Level.Valid() {
			return nil, ErrPrecheckInvalid
		}
		seen[item.Item] = true
	}
	return json.Marshal(required)
}

// BeginPrecheckRun commits the pending batch before any external probe starts.
// Starting a new batch makes the previous batch ineligible for authorization.
func BeginPrecheckRun(ctx context.Context, db *gorm.DB, id uuidx.UUID, digest string, required []PrecheckRequirement, user uuidx.UUID, now time.Time) (*model.PrecheckRun, error) {
	document, err := precheckRequirements(required)
	if err != nil || user.IsZero() || now.IsZero() || len(digest) != 64 {
		return nil, ErrPrecheckInvalid
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return nil, ErrPrecheckInvalid
	}
	var run model.PrecheckRun
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		config, err := readPrecheckConfig(ctx, tx, id, true)
		if err != nil {
			return err
		}
		if config.Digest != digest {
			return ErrPrecheckConfigChanged
		}
		validity, err := PrecheckValidity(ctx, tx)
		if err != nil {
			return err
		}
		// The pipeline lock serializes allocation. Wall clock corrections cannot
		// make an older authorized batch supersede a newly pending batch.
		var sequence uint64
		if err := tx.Model(&model.PrecheckRun{}).Where("pipeline_id=?", id).Select("COALESCE(MAX(sequence),0)").Scan(&sequence).Error; err != nil {
			return err
		}
		if sequence == math.MaxUint64 {
			return ErrPrecheckInvalid
		}
		run = model.PrecheckRun{PipelineID: id, Sequence: sequence + 1, ConfigVersion: config.Pipeline.ConfigVersion, RunID: config.Pipeline.RunID, ConfigDigest: digest, Required: model.JSON(document), CheckedAt: now.UTC().Truncate(time.Millisecond), ExpiresAt: now.Add(validity).UTC().Truncate(time.Millisecond), CreatedBy: user}
		return tx.Create(&run).Error
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	return &run, err
}

func latestPrecheckRun(ctx context.Context, db *gorm.DB, id uuidx.UUID, lock bool) (*model.PrecheckRun, error) {
	query := db.WithContext(ctx).Where("pipeline_id=?", id).Order("sequence DESC")
	if lock {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var run model.PrecheckRun
	if err := query.First(&run).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrPrecheckNotFound
		}
		return nil, err
	}
	return &run, nil
}

func checkPrecheckCurrent(ctx context.Context, db *gorm.DB, config *PrecheckConfig, run *model.PrecheckRun, now time.Time) error {
	if run.PipelineID != config.Pipeline.ID || run.ConfigDigest != config.Digest || run.ConfigVersion != config.Pipeline.ConfigVersion || run.RunID != config.Pipeline.RunID {
		return ErrPrecheckConfigChanged
	}
	validity, err := PrecheckValidity(ctx, db)
	if err != nil {
		return err
	}
	if !now.Before(run.ExpiresAt) || !now.Before(run.CheckedAt.Add(validity)) || now.Before(run.CheckedAt) {
		return ErrPrecheckExpired
	}
	return nil
}

func verifyPrecheckItems(run *model.PrecheckRun, items []model.PipelinePrecheck) error {
	var required []PrecheckRequirement
	if json.Unmarshal(run.Required, &required) != nil {
		return ErrPrecheckInvalid
	}
	if _, err := precheckRequirements(required); err != nil {
		return err
	}
	if len(required) != len(items) {
		return ErrPrecheckIncomplete
	}
	expected := map[string]model.PrecheckLevel{}
	for _, item := range required {
		expected[item.Item] = item.Level
	}
	totalBytes := 0
	for _, item := range items {
		level, ok := expected[item.Item]
		if !ok || level != item.Level || item.Validate() != nil || item.Status == model.PrecheckStatusPending || item.PipelineID != run.PipelineID || item.ConfigVersion != run.ConfigVersion || item.BatchID != run.ID || (item.ConfirmedBy.IsZero() && item.ConfirmedAt != nil) || (!item.ConfirmedBy.IsZero() && (item.Level != model.PrecheckLevelWarning || item.Status != model.PrecheckStatusFail || item.ConfirmedAt == nil)) {
			return ErrPrecheckInvalid
		}
		totalBytes += len(item.Detail)
		if totalBytes > 4<<20 || (len(item.Detail) > 0 && !json.Valid(item.Detail)) {
			return ErrPrecheckInvalid
		}
		delete(expected, item.Item)
	}
	return nil
}

func CompletePrecheckRun(ctx context.Context, db *gorm.DB, id, batchID uuidx.UUID, items []model.PipelinePrecheck, now time.Time) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		config, err := readPrecheckConfig(ctx, tx, id, true)
		if err != nil {
			return err
		}
		run, err := latestPrecheckRun(ctx, tx, id, true)
		if err != nil {
			return err
		}
		if run.ID != batchID || run.Complete {
			return ErrPrecheckConfigChanged
		}
		if err := checkPrecheckCurrent(ctx, tx, config, run, now); err != nil {
			return err
		}
		// Create independent copies; do not mutate the caller's slice or JSON.
		results := make([]model.PipelinePrecheck, len(items))
		for i, item := range items {
			if !item.ConfirmedBy.IsZero() || item.ConfirmedAt != nil || !item.ID.IsZero() || (!item.PipelineID.IsZero() && item.PipelineID != id) || (!item.BatchID.IsZero() && item.BatchID != run.ID) || (item.ConfigVersion != 0 && item.ConfigVersion != run.ConfigVersion) {
				return ErrPrecheckInvalid
			}
			item.ID = uuidx.New()
			item.PipelineID = id
			item.ConfigVersion = run.ConfigVersion
			item.BatchID = run.ID
			item.CheckedAt = run.CheckedAt
			item.Detail = append(model.JSON(nil), item.Detail...)
			results[i] = item
		}
		if err := verifyPrecheckItems(run, results); err != nil {
			return err
		}
		if err := tx.Create(&results).Error; err != nil {
			return err
		}
		return tx.Model(run).Updates(map[string]any{"complete": true, "updated_at": now}).Error
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}

func GetLatestPrecheckRun(ctx context.Context, db *gorm.DB, id uuidx.UUID) (*StoredPrecheck, error) {
	var stored *StoredPrecheck
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if _, err := GetPipeline(ctx, tx, id); err != nil {
			return err
		}
		run, err := latestPrecheckRun(ctx, tx, id, false)
		if err != nil {
			return err
		}
		var items []model.PipelinePrecheck
		if err := tx.Where("batch_id=? AND pipeline_id=?", run.ID, id).Order("item ASC").Find(&items).Error; err != nil {
			return err
		}
		stored = &StoredPrecheck{Run: *run, Items: items}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	return stored, err
}

// CheckPrecheckStartTx must share the transaction that writes cmd_seq and
// desired_state. The caller owns the transaction and retains all config locks.
func CheckPrecheckStartTx(ctx context.Context, tx *gorm.DB, id uuidx.UUID, now time.Time) error {
	config, err := readPrecheckConfig(ctx, tx, id, true)
	if err != nil {
		return err
	}
	run, err := latestPrecheckRun(ctx, tx, id, true)
	if err != nil {
		return err
	}
	var items []model.PipelinePrecheck
	if err := tx.WithContext(ctx).Where("pipeline_id=? AND batch_id=?", id, run.ID).Find(&items).Error; err != nil {
		return err
	}
	return checkStoredPrecheck(ctx, tx, config, run, items, now)
}

func checkStoredPrecheck(ctx context.Context, db *gorm.DB, config *PrecheckConfig, run *model.PrecheckRun, items []model.PipelinePrecheck, now time.Time) error {
	if err := checkPrecheckCurrent(ctx, db, config, run, now); err != nil {
		return err
	}
	if !run.Complete {
		return ErrPrecheckIncomplete
	}
	if err := verifyPrecheckItems(run, items); err != nil {
		return err
	}
	for _, item := range items {
		if item.Status == model.PrecheckStatusFail && item.Level == model.PrecheckLevelBlocking {
			return ErrPrecheckBlocked
		}
	}
	for _, item := range items {
		if item.Status == model.PrecheckStatusFail && item.Level == model.PrecheckLevelWarning && item.ConfirmedBy.IsZero() {
			return ErrPrecheckConfirmationRequired
		}
	}
	return nil
}

func GetPrecheckState(ctx context.Context, db *gorm.DB, id uuidx.UUID, now time.Time) (*PrecheckState, error) {
	var state *PrecheckState
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		config, err := readPrecheckConfig(ctx, tx, id, false)
		if err != nil {
			return err
		}
		run, err := latestPrecheckRun(ctx, tx, id, false)
		if err != nil {
			return err
		}
		var items []model.PipelinePrecheck
		if err := tx.WithContext(ctx).Where("pipeline_id=? AND batch_id=?", id, run.ID).Order("item ASC").Find(&items).Error; err != nil {
			return err
		}
		gate := checkStoredPrecheck(ctx, tx, config, run, items, now)
		if gate != nil && !errors.Is(gate, ErrPrecheckConfigChanged) && !errors.Is(gate, ErrPrecheckExpired) && !errors.Is(gate, ErrPrecheckIncomplete) && !errors.Is(gate, ErrPrecheckInvalid) && !errors.Is(gate, ErrPrecheckBlocked) && !errors.Is(gate, ErrPrecheckConfirmationRequired) {
			return gate
		}
		state = &PrecheckState{Stored: StoredPrecheck{Run: *run, Items: items}, GateError: gate}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	return state, err
}

func ConfirmPrecheckRun(ctx context.Context, db *gorm.DB, id, batchID uuidx.UUID, itemIDs []uuidx.UUID, user uuidx.UUID, now time.Time) error {
	if user.IsZero() || len(itemIDs) == 0 || len(itemIDs) > 100 {
		return ErrPrecheckInvalid
	}
	seen := map[uuidx.UUID]bool{}
	for _, item := range itemIDs {
		if item.IsZero() || seen[item] {
			return ErrPrecheckInvalid
		}
		seen[item] = true
	}
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		config, err := readPrecheckConfig(ctx, tx, id, true)
		if err != nil {
			return err
		}
		run, err := latestPrecheckRun(ctx, tx, id, true)
		if err != nil {
			return err
		}
		if run.ID != batchID {
			return ErrPrecheckConfigChanged
		}
		if err := checkPrecheckCurrent(ctx, tx, config, run, now); err != nil {
			return err
		}
		if !run.Complete {
			return ErrPrecheckIncomplete
		}
		var items []model.PipelinePrecheck
		if err := tx.Where("pipeline_id=? AND batch_id=?", id, run.ID).Order("item ASC").Find(&items).Error; err != nil {
			return err
		}
		if err := verifyPrecheckItems(run, items); err != nil {
			return err
		}
		for _, item := range items {
			if item.Status == model.PrecheckStatusFail && item.Level == model.PrecheckLevelBlocking {
				return ErrPrecheckBlocked
			}
		}
		confirmed := []uuidx.UUID{}
		for _, item := range items {
			if !seen[item.ID] {
				continue
			}
			delete(seen, item.ID)
			if item.Level != model.PrecheckLevelWarning || item.Status != model.PrecheckStatusFail {
				return ErrPrecheckNotConfirmable
			}
			if !item.ConfirmedBy.IsZero() {
				return ErrPrecheckAlreadyConfirmed
			}
			confirmed = append(confirmed, item.ID)
		}
		if len(seen) > 0 {
			return ErrPrecheckNotFound
		}
		if err := tx.Model(&model.PipelinePrecheck{}).Where("id IN ? AND batch_id=?", confirmed, run.ID).Updates(map[string]any{"confirmed_by": user, "confirmed_at": now, "updated_at": now}).Error; err != nil {
			return err
		}
		detail, err := json.Marshal(map[string]any{"batch_id": run.ID.String(), "config_digest": run.ConfigDigest, "items": confirmed})
		if err != nil {
			return err
		}
		return CreateAuditLog(ctx, tx, &model.AuditLog{UserID: user, Action: model.AuditActionPrecheckConfirm, TargetType: model.AuditTargetTypePipeline, TargetID: id, Detail: model.JSON(detail)}, now)
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}
