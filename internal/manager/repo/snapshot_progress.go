package repo

import (
	"context"
	"fmt"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func RecordSnapshotProgress(ctx context.Context, db *gorm.DB, pipelineID uuidx.UUID, runID, total, done uint64, tables []model.PipelineSnapshotTable) error {
	return RecordSnapshotProgressWithExtension(ctx, db, pipelineID, runID, total, done, tables, nil)
}

func RecordSnapshotProgressWithExtension(ctx context.Context, db *gorm.DB, pipelineID uuidx.UUID, runID, total, done uint64, tables []model.PipelineSnapshotTable, extension *model.SnapshotPlanExtensionProgress) error {
	if total == 0 || done > total {
		return fmt.Errorf("repo: invalid snapshot progress counts")
	}
	for _, table := range tables {
		if table.PipelineID != pipelineID || table.RunID != runID || table.SchemaName == "" || table.Table == "" || table.EstRows < 0 || table.DoneRows < 0 {
			return fmt.Errorf("repo: invalid snapshot table progress")
		}
		if err := table.Validate(); err != nil {
			return err
		}
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var run model.PipelineSnapshot
		if err := tx.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).Where("pipeline_id=? AND run_id=?", pipelineID, runID).First(&run).Error; err != nil {
			return err
		}
		observedTotal, observedDone, sequence, stale, err := snapshotPlanObservation(run, total, done, extension, false)
		if err != nil {
			return err
		}
		if stale || observedDone < run.DoneChunks {
			return nil
		}
		base := total
		total, done = observedTotal, observedDone
		status := model.SnapshotStatusReading
		if done == total {
			status = model.SnapshotStatusChunksDone
		}
		if run.Status == model.SnapshotStatusConverged || run.Status == model.SnapshotStatusConverging {
			status = run.Status
		}
		if err := tx.WithContext(ctx).Model(&model.PipelineSnapshot{}).Where("id=?", run.ID).Updates(map[string]any{"total_chunks": total, "done_chunks": done, "initial_total_chunks": base, "plan_extension_sequence": sequence, "status": status}).Error; err != nil {
			return err
		}
		for _, table := range tables {
			if _, err := UpsertSnapshotTable(ctx, tx, table); err != nil {
				return err
			}
		}
		return nil
	})
}

// Target catchup is distinct from source read progress. Timestamps reject older
// deliveries; a restart may legitimately increase pending work by replaying L.
func RecordSnapshotCatchupProgress(ctx context.Context, db *gorm.DB, pipelineID uuidx.UUID, runID, total, remaining uint64, observedMS int64) error {
	return RecordSnapshotCatchupProgressWithExtension(ctx, db, pipelineID, runID, total, remaining, observedMS, nil)
}

func RecordSnapshotCatchupProgressWithExtension(ctx context.Context, db *gorm.DB, pipelineID uuidx.UUID, runID, total, remaining uint64, observedMS int64, extension *model.SnapshotPlanExtensionProgress) error {
	if total == 0 || observedMS <= 0 {
		return fmt.Errorf("repo: invalid snapshot catchup progress")
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var run model.PipelineSnapshot
		if err := tx.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).Where("pipeline_id=? AND run_id=?", pipelineID, runID).First(&run).Error; err != nil {
			return err
		}
		observedTotal, observedDone, sequence, stale, err := snapshotPlanObservation(run, total, total, extension, true)
		if err != nil {
			return err
		}
		if observedTotal != observedDone {
			return fmt.Errorf("repo: catchup extension is incomplete")
		}
		if stale {
			return nil
		}
		if run.Status == model.SnapshotStatusConverged && run.CatchupUpdatedAtMS > 0 || observedMS <= run.CatchupUpdatedAtMS {
			return nil
		}
		status := run.Status
		if status == model.SnapshotStatusConverged {
			remaining = 0
		}
		if status == model.SnapshotStatusPlanning || status == model.SnapshotStatusReading || status == model.SnapshotStatusChunksDone {
			status = model.SnapshotStatusConverging
		}
		return tx.WithContext(ctx).Model(&model.PipelineSnapshot{}).Where("id=?", run.ID).Updates(map[string]any{
			"total_chunks": observedTotal, "done_chunks": observedTotal, "initial_total_chunks": total, "plan_extension_sequence": sequence, "catchup_remaining_txns": remaining, "catchup_updated_at_ms": observedMS, "status": status,
		}).Error
	})
}
