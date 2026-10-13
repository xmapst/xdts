package ingest

import (
	"fmt"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// validateStatusObservation runs after authenticated ownership is established.
// It returns false for an out-of-order observation and never changes persistent state.
func validateStatusObservation(p *model.Pipeline, previous *model.PipelineStatusRecord, r *model.PipelineStatusRecord) (bool, error) {
	if r.ObservedAtMS < 0 {
		return false, fmt.Errorf("ingest: invalid status observation time")
	}
	if _, err := uuidx.Parse(r.SessionID); err != nil || r.SessionStartedAtMS <= 0 || r.Sequence == 0 || r.ObservedAtMS <= 0 {
		return false, fmt.Errorf("ingest: incomplete status session")
	}
	if r.State == model.ActualStateUnknown {
		return false, fmt.Errorf("ingest: only Manager can mark a side unknown")
	}
	if r.RuntimeInstanceID != "" {
		if _, err := uuidx.Parse(r.RuntimeInstanceID); err != nil {
			return false, fmt.Errorf("ingest: invalid runtime instance")
		}
	}
	if r.RunID > p.RunID || r.ConfigVersion > p.ConfigVersion {
		return false, fmt.Errorf("ingest: pipeline status belongs to a future run or configuration")
	}
	if r.RunID < p.RunID {
		return false, nil
	}
	from := p.SourceActualState
	if r.Side == model.SideTarget {
		from = p.TargetActualState
	}
	newInstance := false
	if previous != nil && previous.RunID == r.RunID && previous.NodeID == r.NodeID {
		r.RuntimeInstanceFloor = previous.RuntimeInstanceFloor
	}
	if r.RuntimeInstanceID != "" {
		if r.RuntimeInstanceFloor != "" && r.RuntimeInstanceID < r.RuntimeInstanceFloor {
			return false, nil
		}
		if previous != nil && previous.RuntimeInstanceID == "" && r.RuntimeInstanceID == r.RuntimeInstanceFloor &&
			(previous.State == model.ActualStatePaused || previous.State == model.ActualStateStopped) {
			return false, nil
		}
		r.RuntimeInstanceFloor = r.RuntimeInstanceID
	}
	if previous == nil && r.RuntimeInstanceID != "" {
		// The first observation can belong to a worker restored before Manager connected.
		newInstance = true
	}
	if previous != nil && previous.NodeID == r.NodeID {
		if r.RunID < previous.RunID || r.RunID == previous.RunID && r.ConfigVersion < previous.ConfigVersion {
			return false, nil
		}
		if previous.SessionID != "" {
			if r.SessionID == previous.SessionID {
				if r.SessionStartedAtMS != previous.SessionStartedAtMS {
					return false, fmt.Errorf("ingest: status session start changed")
				}
				if r.Sequence < previous.Sequence {
					return false, nil
				}
				if r.Sequence == previous.Sequence {
					if *r != *previous {
						return false, fmt.Errorf("ingest: conflicting status sequence")
					}
					// Periodic reports restore an offline Unknown overlay as well.
					return true, nil
				}
			} else {
				if r.SessionStartedAtMS < previous.SessionStartedAtMS || r.SessionStartedAtMS == previous.SessionStartedAtMS && r.SessionID < previous.SessionID {
					return false, nil
				}
				newInstance = true
			}
		}
		newInstance = newInstance || r.RunID != previous.RunID ||
			r.RuntimeInstanceID != "" && r.RuntimeInstanceID != previous.RuntimeInstanceID
	}
	if r.State == model.ActualStateDraft || r.State == model.ActualStatePrechecking {
		// These are Manager-controlled lifecycle stages, never a Node reset.
		if from != r.State {
			return false, fmt.Errorf("%w: node cannot reset pipeline to %s", model.ErrPipelineTransition, r.State)
		}
	}
	if newInstance && r.State != model.ActualStateDraft && r.State != model.ActualStatePrechecking {
		return true, nil
	}
	if !model.CanObservePipelineTransition(from, r.State) {
		return false, fmt.Errorf("%w: %s -> %s", model.ErrPipelineTransition, from, r.State)
	}
	return true, nil
}

func snapshotStatusFromProto(v iproto.SnapshotRunState) (model.SnapshotStatus, error) {
	switch v {
	case iproto.SnapshotRunState_SNAPSHOT_RUN_STATE_UNSPECIFIED:
		return "", nil
	case iproto.SnapshotRunState_SNAPSHOT_RUN_STATE_PLANNING:
		return model.SnapshotStatusPlanning, nil
	case iproto.SnapshotRunState_SNAPSHOT_RUN_STATE_READING:
		return model.SnapshotStatusReading, nil
	case iproto.SnapshotRunState_SNAPSHOT_RUN_STATE_CHUNKS_DONE:
		return model.SnapshotStatusChunksDone, nil
	case iproto.SnapshotRunState_SNAPSHOT_RUN_STATE_CONVERGING:
		return model.SnapshotStatusConverging, nil
	case iproto.SnapshotRunState_SNAPSHOT_RUN_STATE_CONVERGED:
		return model.SnapshotStatusConverged, nil
	case iproto.SnapshotRunState_SNAPSHOT_RUN_STATE_FAILED:
		return model.SnapshotStatusFailed, nil
	case iproto.SnapshotRunState_SNAPSHOT_RUN_STATE_PAUSED:
		return model.SnapshotStatusPaused, nil
	default:
		return "", fmt.Errorf("ingest: unknown snapshot state")
	}
}
