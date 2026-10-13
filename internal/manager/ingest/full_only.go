package ingest

import "github.com/xmapst/xdts/internal/manager/model"

// Called only after authenticated role ownership, ordered observation validation
// and the snapshot-state write, under the same pipeline row lock. The target has
// already latched input at H; this intent stops both production roles durably.
func fullOnlyStopObservation(p *model.Pipeline, r *model.PipelineStatusRecord) bool {
	return p.InitMode == model.InitModeFullOnly && p.DesiredState == model.DesiredStateRunning &&
		!p.ReinitPending && r.Side == model.SideTarget && r.NodeID == p.TargetNodeID &&
		r.RunID == p.RunID && r.ConfigVersion == p.ConfigVersion && r.ConfigVersion > 0 &&
		r.State == model.ActualStateRunning && r.SnapshotStatus == model.SnapshotStatusConverged
}
