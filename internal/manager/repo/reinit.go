package repo

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/schemaplan"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Reinitializations struct{ db *gorm.DB }

func NewReinitializations(db *gorm.DB) *Reinitializations { return &Reinitializations{db: db} }

type StoredReinit struct {
	Record   model.PipelineReinit
	Document ReinitDocument
}

func reinitAllowed(p *model.Pipeline) bool {
	return schemaPreviewAllowed(p) && !p.ReinitPending && p.RunID < math.MaxInt64 && p.ConfigVersion < math.MaxInt64 && p.LastCmdSeq < math.MaxInt64 && p.DeletedAt.Time.IsZero()
}

func (r *Reinitializations) GetPreview(ctx context.Context, id uuidx.UUID, fromRun uint64) (*StoredReinit, error) {
	var record model.PipelineReinit
	if err := r.db.WithContext(ctx).Where("pipeline_id=? AND from_run_id=?", id, fromRun).First(&record).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.Join(ErrReinitConflict, ErrReinitNotFound)
		}
		return nil, err
	}
	var doc ReinitDocument
	if json.Unmarshal(record.Document, &doc) != nil {
		return nil, ErrReinitConflict
	}
	_, digest, err := reinitDocument(id, fromRun, record.ConfigVersion, doc)
	if err != nil || digest != record.Digest {
		return nil, ErrReinitConflict
	}
	if record.ConfirmedAt != nil && (record.NewRunID != fromRun+1 || record.SnapshotUUID.IsZero() || record.ConfirmedBy.IsZero() || record.CmdSeq == 0 || (doc.SchemaPlan != nil) != (!record.SchemaUUID.IsZero()) || (doc.TargetHandling == model.ReinitTargetClear) != (!record.PrepareUUID.IsZero())) {
		return nil, ErrReinitConflict
	}
	if record.ConfirmedAt != nil && ((!record.SchemaUUID.IsZero() && record.SchemaUUID == record.SnapshotUUID) || (!record.PrepareUUID.IsZero() && (record.PrepareUUID == record.SnapshotUUID || record.PrepareUUID == record.SchemaUUID))) {
		return nil, ErrReinitConflict
	}
	return &StoredReinit{Record: record, Document: doc}, nil
}

func (r *Reinitializations) SavePreview(ctx context.Context, id uuidx.UUID, run, version uint64, doc ReinitDocument) (*StoredReinit, error) {
	blob, digest, err := reinitDocument(id, run, version, doc)
	if err != nil {
		return nil, err
	}
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		p, err := GetPipeline(ctx, tx, id)
		if err != nil {
			return err
		}
		if err := LockTopology(ctx, tx, p.TopologyID); err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(p, "id=?", id).Error; err != nil {
			return err
		}
		if p.RunID != run || p.ConfigVersion != version || !reinitAllowed(p) || (!p.SchemaUUID.IsZero() && doc.SchemaPlan == nil) {
			return ErrReinitConflict
		}
		if err := NewReinitializations(tx).validateInputs(ctx, p, doc.InputsDigest); err != nil {
			return err
		}
		candidate := *p
		candidate.InitMode = model.InitModeFullAndIncremental
		if err := CheckBidirectionalInitialization(ctx, tx, &candidate, doc.SchemaPlan != nil); err != nil {
			return err
		}
		var prior model.PipelineReinit
		err = tx.Where("pipeline_id=? AND from_run_id=?", id, run).First(&prior).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if prior.ConfirmedAt != nil {
			return ErrReinitConflict
		}
		if prior.ID.IsZero() {
			return tx.Create(&model.PipelineReinit{PipelineID: id, FromRunID: run, ConfigVersion: version, Digest: digest, Document: model.JSON(blob)}).Error
		}
		return tx.Model(&prior).Updates(map[string]any{"config_version": version, "digest": digest, "document": model.JSON(blob)}).Error
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	return r.GetPreview(ctx, id, run)
}

func (r *Reinitializations) Confirm(ctx context.Context, id uuidx.UUID, fromRun uint64, digest string, user uuidx.UUID, now time.Time) (*StoredReinit, error) {
	if user.IsZero() || len(digest) != 64 || now.IsZero() {
		return nil, ErrReinitConflict
	}
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		p, err := GetPipeline(ctx, tx, id)
		if err != nil {
			return err
		}
		if err := LockTopology(ctx, tx, p.TopologyID); err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(p, "id=?", id).Error; err != nil {
			return err
		}
		stored, err := NewReinitializations(tx).GetPreview(ctx, id, fromRun)
		if err != nil {
			return err
		}
		record, doc := &stored.Record, stored.Document
		if record.Digest != digest {
			return ErrReinitConflict
		}
		if record.ConfirmedAt != nil {
			if p.RunID != record.NewRunID || p.SnapshotUUID != record.SnapshotUUID || p.SchemaUUID != record.SchemaUUID {
				return ErrReinitConflict
			}
			return nil
		}
		if p.RunID != fromRun || p.ConfigVersion != record.ConfigVersion || !reinitAllowed(p) || (!p.SchemaUUID.IsZero() && doc.SchemaPlan == nil) {
			return ErrReinitConflict
		}
		if err := NewReinitializations(tx).validateInputs(ctx, p, doc.InputsDigest); err != nil {
			return err
		}
		candidate := *p
		candidate.InitMode = model.InitModeFullAndIncremental
		if err := CheckBidirectionalInitialization(ctx, tx, &candidate, doc.SchemaPlan != nil); err != nil {
			return err
		}
		if doc.SchemaPlan != nil {
			if _, err := doc.SchemaPlan.ExportDDL(); err != nil {
				return ErrReinitBlocked
			}
		}
		source, err := GetEndpoint(ctx, tx, p.SourceEndpointID)
		if err != nil {
			return err
		}
		snapshotID := uuidx.New()
		schemaID, prepareID := uuidx.Nil, uuidx.Nil
		if doc.SchemaPlan != nil {
			schemaID = uuidx.New()
		}
		if doc.TargetHandling == model.ReinitTargetClear {
			prepareID = uuidx.New()
		}
		for _, identity := range []uuidx.UUID{snapshotID, schemaID, prepareID} {
			if identity.IsZero() {
				continue
			}
			if err := CreateCenterOriginUUID(ctx, tx, &model.CenterOriginUUID{CenterID: source.CenterID, UUID: identity, Kind: model.OriginKindSynthetic, RefID: id.String()}); err != nil {
				return err
			}
		}
		newRun, newVersion, newSeq := fromRun+1, p.ConfigVersion+1, p.LastCmdSeq+1
		if err := CreateSnapshot(ctx, tx, &model.PipelineSnapshot{PipelineID: id, RunID: newRun, SnapshotUUID: snapshotID, Status: model.SnapshotStatusPlanning}); err != nil {
			return err
		}
		if doc.SchemaPlan != nil {
			if err := NewReinitializations(tx).createSchemaPlan(ctx, id, newRun, p.ConfigVersion, *doc.SchemaPlan, user, now); err != nil {
				return err
			}
		}
		if err := tx.Model(p).Updates(map[string]any{"run_id": newRun, "config_version": newVersion, "last_cmd_seq": newSeq, "snapshot_uuid": snapshotID, "schema_uuid": schemaID, "init_mode": model.InitModeFullAndIncremental, "start_position": nil, "position_revision": 0, "desired_state": model.DesiredStateRunning, "reinit_pending": true, "recovery_exhausted_cmd_seq": nil}).Error; err != nil {
			return err
		}
		if err := tx.Model(record).Updates(map[string]any{"confirmed_at": now, "confirmed_by": user, "new_run_id": newRun, "snapshot_uuid": snapshotID, "schema_uuid": schemaID, "prepare_uuid": prepareID, "cmd_seq": newSeq}).Error; err != nil {
			return err
		}
		detail, err := json.Marshal(map[string]any{"digest": digest, "fromRunId": fromRun, "runId": newRun, "configVersion": newVersion, "cmdSeq": newSeq, "targetHandling": doc.TargetHandling, "tables": doc.Tables, "snapshotUuid": snapshotID, "schemaUuid": schemaID, "prepareUuid": prepareID})
		if err != nil {
			return err
		}
		return CreateAuditLog(ctx, tx, &model.AuditLog{UserID: user, Action: model.AuditActionPipelineReinit, TargetType: model.AuditTargetTypePipeline, TargetID: id, Detail: model.JSON(detail)}, now)
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	return r.GetPreview(ctx, id, fromRun)
}

func (r *Reinitializations) createSchemaPlan(ctx context.Context, id uuidx.UUID, run, version uint64, plan schemaplan.Plan, user uuidx.UUID, now time.Time) error {
	blob, digest, err := schemaPlanDocument(id, run, version, plan)
	if err != nil {
		return err
	}
	if err := r.db.WithContext(ctx).Create(&model.SchemaPlan{PipelineID: id, RunID: run, ConfigVersion: version, Digest: digest, Document: model.JSON(blob), Blocked: plan.Blocked, ConfirmedAt: &now, ConfirmedBy: user}).Error; err != nil {
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
	return r.db.WithContext(ctx).CreateInBatches(rows, 100).Error
}

func (r *Reinitializations) RecordApplied(ctx context.Context, id, node uuidx.UUID, version, cmdSeq uint64, sides []model.Side) error {
	if len(sides) == 0 || len(sides) > 2 || (len(sides) == 2 && sides[0] == sides[1]) {
		return ErrReinitConflict
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var p model.Pipeline
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&p, "id=?", id).Error; err != nil {
			return err
		}
		for _, side := range sides {
			if (side == model.SideSource && node != p.SourceNodeID) || (side == model.SideTarget && node != p.TargetNodeID) || (side != model.SideSource && side != model.SideTarget) {
				return ErrReinitConflict
			}
		}
		if !p.ReinitPending || cmdSeq == 0 || p.LastCmdSeq != cmdSeq || p.ConfigVersion != version {
			return nil
		}
		if p.RunID == 0 {
			return ErrReinitConflict
		}
		stored, err := NewReinitializations(tx).GetPreview(ctx, id, p.RunID-1)
		if err != nil {
			return err
		}
		record := &stored.Record
		if record.ConfirmedAt == nil || record.CmdSeq != cmdSeq || record.NewRunID != p.RunID || record.SnapshotUUID != p.SnapshotUUID || record.SchemaUUID != p.SchemaUUID {
			return ErrReinitConflict
		}
		for _, side := range sides {
			if side == model.SideSource {
				record.SourceApplied = true
			} else {
				record.TargetApplied = true
			}
		}
		if err := tx.Model(record).Updates(map[string]any{"source_applied": record.SourceApplied, "target_applied": record.TargetApplied}).Error; err != nil {
			return err
		}
		if record.SourceApplied && record.TargetApplied {
			return tx.Model(&p).Update("reinit_pending", false).Error
		}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}
