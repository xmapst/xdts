package repo

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/schemaplan"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrSchemaPlanNotFound = errors.New("schema migration plan not found")
var ErrSchemaPlanConflict = errors.New("schema migration plan changed or pipeline no longer accepts a preview")
var ErrSchemaPlanBlocked = errors.New("schema migration plan blocked")

// StoredSchemaPlan returns an independently decoded plan. The SQL document is
// immutable after confirmation, and the digest includes its configuration/run.
type StoredSchemaPlan struct {
	Record model.SchemaPlan
	Plan   schemaplan.Plan
}

func schemaPlanDocument(id uuidx.UUID, run, version uint64, plan schemaplan.Plan) ([]byte, string, error) {
	for i, step := range plan.Steps {
		if step.Seq != uint32(i+1) || !step.Kind.Valid() || (step.Action != model.MigrationActionCreate && step.Action != model.MigrationActionSkip && step.Action != model.MigrationActionBlock) || step.Schema == "" || step.Name == "" || (step.Action == model.MigrationActionCreate && step.DDL == "") {
			return nil, "", errors.New("invalid migration plan step")
		}
		if step.Action == model.MigrationActionBlock && !plan.Blocked {
			return nil, "", errors.New("inconsistent plan blocked state")
		}
	}
	if len(plan.Steps) == 0 || len(plan.Steps) > 4000 {
		return nil, "", errors.New("invalid migration plan count")
	}
	document, err := json.Marshal(plan)
	if err != nil {
		return nil, "", err
	}
	if len(document) > 16<<20 {
		return nil, "", errors.New("migration plan exceeds size limit")
	}
	hash := sha256.New()
	_, _ = fmt.Fprintf(hash, "%s:%d:%d:", id, run, version)
	hash.Write(document)
	return document, hex.EncodeToString(hash.Sum(nil)), nil
}

func GetSchemaPlan(ctx context.Context, db *gorm.DB, id uuidx.UUID, run uint64) (*StoredSchemaPlan, error) {
	var record model.SchemaPlan
	if err := db.WithContext(ctx).Where("pipeline_id = ? AND run_id = ?", id, run).First(&record).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrSchemaPlanNotFound
		}
		return nil, err
	}
	var plan schemaplan.Plan
	if err := json.Unmarshal(record.Document, &plan); err != nil {
		return nil, fmt.Errorf("decode stored migration plan: %w", err)
	}
	_, digest, err := schemaPlanDocument(id, run, record.ConfigVersion, plan)
	if err != nil || digest != record.Digest || record.Blocked != plan.Blocked {
		return nil, ErrSchemaPlanConflict
	}
	return &StoredSchemaPlan{Record: record, Plan: plan}, nil
}

func schemaPreviewAllowed(p *model.Pipeline) bool {
	return !p.ReinitPending && p.DesiredState == model.DesiredStateStopped && (p.SourceActualState == model.ActualStateDraft || p.SourceActualState == model.ActualStateStopped) && (p.TargetActualState == model.ActualStateDraft || p.TargetActualState == model.ActualStateStopped)
}

// SaveSchemaPlan serializes with configuration edits and confirmation by locking
// the owning pipeline first. No plan may replace statements that already have
// assigned execution identities.
func SaveSchemaPlan(ctx context.Context, db *gorm.DB, id uuidx.UUID, run, version uint64, plan schemaplan.Plan) (*StoredSchemaPlan, error) {
	document, digest, err := schemaPlanDocument(id, run, version, plan)
	if err != nil {
		return nil, err
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		tx = tx.WithContext(ctx)
		var pipeline model.Pipeline
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&pipeline, "id = ?", id).Error; err != nil {
			return err
		}
		if pipeline.RunID != run || pipeline.ConfigVersion != version || !schemaPreviewAllowed(&pipeline) || !pipeline.SchemaUUID.IsZero() {
			return ErrSchemaPlanConflict
		}
		var prior model.SchemaPlan
		err := tx.Where("pipeline_id = ? AND run_id = ?", id, run).First(&prior).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if prior.ConfirmedAt != nil {
			return ErrSchemaPlanConflict
		}
		record := model.SchemaPlan{PipelineID: id, RunID: run, ConfigVersion: version, Digest: digest, Document: model.JSON(document), Blocked: plan.Blocked}
		if prior.ID.IsZero() {
			if err := tx.Create(&record).Error; err != nil {
				return err
			}
		} else {
			if err := tx.Model(&prior).Updates(map[string]any{"config_version": version, "digest": digest, "document": model.JSON(document), "blocked": plan.Blocked}).Error; err != nil {
				return err
			}
		}
		if err := tx.Where("pipeline_id = ? AND run_id = ?", id, run).Delete(&model.SchemaMigration{}).Error; err != nil {
			return err
		}
		rows := make([]model.SchemaMigration, 0, len(plan.Steps))
		for _, step := range plan.Steps {
			diff, err := json.Marshal(step.Differences)
			if err != nil {
				return err
			}
			rows = append(rows, model.SchemaMigration{PipelineID: id, RunID: run, Seq: step.Seq, ObjectType: step.Kind, SchemaName: step.Schema, ObjectName: step.Name, Action: step.Action, SourceDDL: step.SourceDDL, TargetDDL: step.DDL, Diff: model.JSON(diff), Status: model.MigrationStatusPending, Error: step.BlockReason})
		}
		return tx.CreateInBatches(rows, 100).Error
	})
	if err != nil {
		return nil, err
	}
	return GetSchemaPlan(ctx, db, id, run)
}

// ConfirmSchemaPlan compares the exact preview digest and allocates one unique
// structure identity, separate from the snapshot identity. Duplicate confirms
// by any user return the same already confirmed plan without reallocating UUIDs.
func ConfirmSchemaPlan(ctx context.Context, db *gorm.DB, id uuidx.UUID, run uint64, digest string, userID uuidx.UUID) (*StoredSchemaPlan, error) {
	if userID.IsZero() || len(digest) != 64 {
		return nil, ErrSchemaPlanConflict
	}
	err := db.Transaction(func(tx *gorm.DB) error {
		tx = tx.WithContext(ctx)
		current, err := GetPipeline(ctx, tx, id)
		if err != nil {
			return err
		}
		if err := LockTopology(ctx, tx, current.TopologyID); err != nil {
			return err
		}
		var pipeline model.Pipeline
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&pipeline, "id = ?", id).Error; err != nil {
			return err
		}
		var record model.SchemaPlan
		if err := tx.Where("pipeline_id = ? AND run_id = ?", id, run).First(&record).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrSchemaPlanNotFound
			}
			return err
		}
		if pipeline.RunID != run || record.Digest != digest {
			return ErrSchemaPlanConflict
		}
		if record.ConfirmedAt != nil {
			if pipeline.SchemaUUID.IsZero() || record.ConfigVersion+1 > pipeline.ConfigVersion {
				return ErrSchemaPlanConflict
			}
			return nil
		}
		if record.ConfigVersion != pipeline.ConfigVersion || !schemaPreviewAllowed(&pipeline) || !pipeline.SchemaUUID.IsZero() {
			return ErrSchemaPlanConflict
		}
		if record.Blocked {
			return ErrSchemaPlanBlocked
		}
		var plan schemaplan.Plan
		if err := json.Unmarshal(record.Document, &plan); err != nil {
			return err
		}
		_, storedDigest, err := schemaPlanDocument(id, run, record.ConfigVersion, plan)
		if err != nil || storedDigest != record.Digest {
			return ErrSchemaPlanConflict
		}
		if _, err := plan.ExportDDL(); err != nil {
			return ErrSchemaPlanBlocked
		}
		if err := CheckBidirectionalInitialization(ctx, tx, &pipeline, true); err != nil {
			return err
		}
		source, err := GetEndpoint(ctx, tx, pipeline.SourceEndpointID)
		if err != nil {
			return err
		}
		identity := uuidx.New()
		if err := CreateCenterOriginUUID(ctx, tx, &model.CenterOriginUUID{CenterID: source.CenterID, UUID: identity, Kind: model.OriginKindSynthetic, RefID: id.String()}); err != nil {
			return err
		}
		if err := tx.Model(&pipeline).Updates(map[string]any{"schema_uuid": identity, "config_version": gorm.Expr("config_version + 1")}).Error; err != nil {
			return err
		}
		now := time.Now().UTC()
		return tx.Model(&record).Updates(map[string]any{"confirmed_at": now, "confirmed_by": userID}).Error
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	return GetSchemaPlan(ctx, db, id, run)
}

// RecordSchemaMigrationResult accepts only the assigned target node and the
// exact current run/identity/sequence. Applied results never regress on a late
// failure delivery; each step is a retryable idempotent update.
func RecordSchemaMigrationResult(ctx context.Context, db *gorm.DB, id, nodeID uuidx.UUID, run uint64, identity string, seq uint64, success bool, message string) error {
	if seq == 0 || seq > 4000 {
		return ErrSchemaPlanConflict
	}
	return db.Transaction(func(tx *gorm.DB) error {
		tx = tx.WithContext(ctx)
		var pipeline model.Pipeline
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&pipeline, "id = ?", id).Error; err != nil {
			return err
		}
		if pipeline.TargetNodeID != nodeID || pipeline.RunID != run || pipeline.SchemaUUID.String() != identity {
			return ErrSchemaPlanConflict
		}
		var step model.SchemaMigration
		if err := tx.Where("pipeline_id = ? AND run_id = ? AND seq = ?", id, run, seq).First(&step).Error; err != nil {
			return err
		}
		if step.Status == model.MigrationStatusApplied {
			return nil
		}
		status := model.MigrationStatusFailed
		if success {
			status = model.MigrationStatusApplied
			message = ""
		}
		return tx.Model(&step).Updates(map[string]any{"status": status, "error": message}).Error
	})
}
