package amqpin

import (
	"math"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/reporter"
)

// captureTelemetry belongs to startup, then the sole consumer handler. Its
// immutable value is transferred through an atomic mirror to the actor timer.
func (m *Module) captureTelemetry() {
	v := iproto.PipelineTelemetry{
		PipelineID: m.opts.PipelineID, RunID: m.opts.RunID, Role: iproto.PipelineRole_PIPELINE_ROLE_TARGET, Component: iproto.TelemetryTargetConsumer, InstanceID: m.telemetryInstanceID, ObservedTSMS: m.now().UnixMilli(),
		AppliedGTIDSet: m.appliedGTIDSetString(), SourceEventTSMS: m.telemetrySourceTS, LagMS: m.telemetryLagMS,
		Rows: m.telemetryRows, Txns: m.telemetryTxns, Conflicts: m.stats.conflicts.Load(),
	}
	m.telemetryMirror.Store(&v)
}

// recordTelemetryResult counts actual MySQL commits, including partial batches;
// policy-discarded transactions do not count as applied rows or transactions.
func (m *Module) recordTelemetryResult(batch *iproto.DataBatch, result iproto.BatchApplyResult) {
	applied := make(map[string]bool, len(result.AppliedGTIDs))
	for _, gtid := range result.AppliedGTIDs {
		applied[gtid] = true
	}
	for _, txn := range batch.GetTxns() {
		if !applied[txn.GetGtid()] {
			continue
		}
		m.telemetryRows += uint64(len(txn.GetRows()))
		m.telemetryTxns++
		m.telemetrySourceTS = txn.GetCommitTsMs()
		if m.telemetrySourceTS > 0 {
			m.telemetryLagMS = math.Max(0, float64(m.now().UnixMilli()-m.telemetrySourceTS))
		}
	}
	m.captureTelemetry()
}

// Actor timer never reads the handler's mutable GTID/catchup/counter fields.
func (m *Module) reportTelemetry() {
	if m.opts.InstanceID != "" && m.isFrozen() {
		m.Cast("controller", iproto.PipelineRuntimeStatus{PipelineID: m.opts.PipelineID, Role: iproto.PipelineRole_PIPELINE_ROLE_TARGET, RunID: m.opts.RunID, ConfigVersion: m.runtimeVersion.Load(), InstanceID: m.opts.InstanceID, State: iproto.PipelineState_PIPELINE_STATE_ERROR})
	}
	mirror := m.telemetryMirror.Load()
	if mirror == nil {
		return
	}
	v := *mirror
	v.ObservedTSMS = m.now().UnixMilli()
	v.Bytes = m.stats.receivedBytes.Load()
	v.Errors = uint64(m.stats.permanentFailures.Load() + m.stats.applyFailures.Load() + m.stats.ackFailures.Load())
	v.Conflicts = m.stats.conflicts.Load()
	q := m.ChanRPC()
	v.QueueModule = ModuleName(m.opts.PipelineID)
	v.QueueLen = uint32(q.Len())
	v.QueueCap = uint32(q.Cap())
	m.Cast(reporter.ModuleName, v)
}
