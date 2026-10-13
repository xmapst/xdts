package binlogin

import (
	"math"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/reporter"
)

// Called only in the binlogin actor. The session supplies position mirrors;
// atomic counters are the existing cross-goroutine diagnostic snapshots.
func (m *Module) reportTelemetry() {
	if m.opts.InstanceID != "" && iproto.PipelineState(m.pipelineState.Load()) == iproto.PipelineState_PIPELINE_STATE_ERROR {
		m.Cast("controller", iproto.PipelineRuntimeStatus{PipelineID: m.opts.PipelineID, Role: iproto.PipelineRole_PIPELINE_ROLE_SOURCE, RunID: m.opts.RunID, ConfigVersion: m.runtimeVersion.Load(), InstanceID: m.opts.InstanceID, State: iproto.PipelineState_PIPELINE_STATE_ERROR})
	}
	now := m.now().UnixMilli()
	lag := float64(0)
	if m.lastEventTsMs > 0 {
		lag = math.Max(0, float64(now-m.lastEventTsMs))
	}
	q := m.ChanRPC()
	m.Cast(reporter.ModuleName, iproto.PipelineTelemetry{
		PipelineID: m.opts.PipelineID, RunID: m.opts.RunID, Role: iproto.PipelineRole_PIPELINE_ROLE_SOURCE, Component: iproto.TelemetrySourceReader, InstanceID: m.telemetryInstanceID, ObservedTSMS: now,
		ReadGTIDSet: m.readGTIDSet, PublishedGTIDSet: m.publishedGTIDSet, SourceEventTSMS: m.lastEventTsMs, LagMS: lag,
		SourceNodeID: m.opts.NodeID, SourceEpoch: m.epoch, SourceBatchSeq: m.batchSeq.Load(), SourcePositionRevision: m.opts.PositionRevision, HasSourceCheckpoint: m.opts.RequireEpochRange,
		Rows: uint64(m.stats.rowsEmitted.Load()), Txns: uint64(m.stats.txnsEmitted.Load()), Errors: uint64(m.stats.telemetryErrors.Load()),
		QueueModule: ModuleName(m.opts.PipelineID), QueueLen: uint32(q.Len()), QueueCap: uint32(q.Cap()),
	})
}
