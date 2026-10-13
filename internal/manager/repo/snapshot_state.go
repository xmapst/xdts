package repo

import (
	"context"
	"fmt"
	"time"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func RecordSnapshotState(ctx context.Context, db *gorm.DB, pipelineID uuidx.UUID, runID uint64, status model.SnapshotStatus) error {
	if !status.Valid() {
		return fmt.Errorf("repo: invalid snapshot status")
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var run model.PipelineSnapshot
		if err := tx.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).Where("pipeline_id=? AND run_id=?", pipelineID, runID).First(&run).Error; err != nil {
			return err
		}
		// Source progress and target convergence travel on independent channels.
		// Delayed source reports must not revert target-confirmed completion.
		if run.Status == model.SnapshotStatusConverged && status != model.SnapshotStatusFailed {
			return nil
		}
		if run.Status == model.SnapshotStatusConverging && (status == model.SnapshotStatusPlanning || status == model.SnapshotStatusReading || status == model.SnapshotStatusChunksDone) {
			return nil
		}
		changes := map[string]any{"status": status}
		if status == model.SnapshotStatusConverged {
			changes["finished_at"] = time.Now().UTC()
			changes["catchup_remaining_txns"] = uint64(0)
		}
		return tx.WithContext(ctx).Model(&model.PipelineSnapshot{}).Where("id=?", run.ID).Updates(changes).Error
	})
}
