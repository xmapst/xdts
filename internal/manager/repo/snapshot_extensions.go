package repo

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

func ValidateSnapshotExtensionSupport(ctx context.Context, db *gorm.DB, pipelineID uuidx.UUID) error {
	pipeline, err := GetPipeline(ctx, db, pipelineID)
	if err != nil {
		return err
	}
	if pipeline.InitMode != model.InitModeFullAndIncremental && pipeline.InitMode != model.InitModeFullOnly {
		return fmt.Errorf("repo: snapshot extensions require a full initialization run")
	}
	for _, id := range [2]uuidx.UUID{pipeline.SourceNodeID, pipeline.TargetNodeID} {
		node, err := GetNode(ctx, db, id)
		if err != nil {
			return err
		}
		var capabilities []string
		if json.Unmarshal(node.Capabilities, &capabilities) != nil || iproto.ValidateNodeCapabilities(capabilities) != nil || !slices.Contains(capabilities, iproto.CapabilitySnapshotPlanExtensions) {
			return fmt.Errorf("repo: snapshot extension support was not negotiated by both nodes")
		}
	}
	return nil
}

// Called under the run row lock. A delayed old report cannot overwrite a
// durable extension, while a conflicting proposal at the same sequence fails.
func snapshotPlanObservation(run model.PipelineSnapshot, base, baseDone uint64, ext *model.SnapshotPlanExtensionProgress, targetFinal bool) (total, done, sequence uint64, stale bool, err error) {
	if base == 0 || base > 1<<63-2 || baseDone > base {
		return 0, 0, 0, false, fmt.Errorf("repo: invalid base snapshot counts")
	}
	initial := run.InitialTotalChunks
	if initial == 0 {
		initial = run.TotalChunks
	}
	if initial != 0 && initial != base {
		return 0, 0, 0, false, fmt.Errorf("repo: snapshot total changed during run")
	}
	if ext == nil {
		return base, baseDone, 0, run.PlanExtensionSequence > 0, nil
	}
	if ext.Sequence == 0 || ext.TotalChunks <= base || ext.TotalChunks > 1<<63-2 || ext.Sequence > ext.TotalChunks-base || ext.DoneChunks > ext.TotalChunks || baseDone != min(ext.DoneChunks, base) {
		return 0, 0, 0, false, fmt.Errorf("repo: invalid snapshot extension counts")
	}
	if ext.Sequence < run.PlanExtensionSequence {
		return ext.TotalChunks, ext.DoneChunks, ext.Sequence, true, nil
	}
	if ext.Sequence == run.PlanExtensionSequence && ext.TotalChunks != run.TotalChunks {
		return 0, 0, 0, false, fmt.Errorf("repo: snapshot extension total conflicts at current sequence")
	}
	if ext.Sequence > run.PlanExtensionSequence {
		if ext.TotalChunks <= run.TotalChunks || ext.Sequence-run.PlanExtensionSequence > ext.TotalChunks-run.TotalChunks || run.CatchupUpdatedAtMS > 0 || !targetFinal && (run.Status == model.SnapshotStatusConverging || run.Status == model.SnapshotStatusConverged) {
			return 0, 0, 0, false, fmt.Errorf("repo: snapshot extension cannot grow after finalization or replace a prefix")
		}
	}
	return ext.TotalChunks, ext.DoneChunks, ext.Sequence, false, nil
}
