package reporter

import (
	"log/slog"
	"math"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xhive/chanrpc"
)

type telemetryComponent struct {
	current  iproto.PipelineTelemetry
	previous iproto.PipelineTelemetry
}

type telemetryWindow struct {
	components map[string]*telemetryComponent
	lastTSMS   int64
}

func (m *Module) onTelemetry(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
	v, ok := ci.Request.(iproto.PipelineTelemetry)
	if !ok || v.PipelineID == "" || v.InstanceID == "" || v.ObservedTSMS <= 0 || v.SourceEventTSMS < 0 || v.QueueLen > v.QueueCap || math.IsNaN(v.LagMS) || math.IsInf(v.LagMS, 0) || v.LagMS < 0 {
		return nil
	}
	valid := v.Role == iproto.PipelineRole_PIPELINE_ROLE_SOURCE && (v.Component == iproto.TelemetrySourceReader || v.Component == iproto.TelemetrySourcePublisher || v.Component == iproto.TelemetrySourceSnapshot) || v.Role == iproto.PipelineRole_PIPELINE_ROLE_TARGET && (v.Component == iproto.TelemetryTargetConsumer || v.Component == iproto.TelemetryTargetSnapshot)
	if !valid {
		return nil
	}
	key := statusKeyOf(v.PipelineID, v.Role)
	if st := m.statusByPipeline[key]; st != nil && v.RunID < st.GetRunId() {
		return nil
	}
	pg := m.progressByPipeline[key]
	if pg != nil && v.RunID < pg.GetRunId() {
		return nil
	}
	if pg == nil || v.RunID > pg.GetRunId() {
		m.progressByPipeline[key] = &iproto.PipelineProgress{PipelineId: v.PipelineID, Role: v.Role, RunId: v.RunID, TsMs: v.ObservedTSMS}
		delete(m.telemetryByPipeline, key)
	}
	if v.HasSourceCheckpoint {
		cp := &iproto.SourceCheckpoint{PipelineId: v.PipelineID, NodeId: v.SourceNodeID, RunId: v.RunID, Epoch: v.SourceEpoch, BatchSeq: v.SourceBatchSeq, PublishedGtidSet: v.PublishedGTIDSet, ObservedTsMs: v.ObservedTSMS, PositionRevision: v.SourcePositionRevision}
		if v.Component != iproto.TelemetrySourceReader || v.Role != iproto.PipelineRole_PIPELINE_ROLE_SOURCE || cp.NodeId != m.opts.NodeID || iproto.ValidateSourceCheckpoint(cp) != nil {
			return nil
		}
		progress := m.progressByPipeline[key]
		prior := progress.SourceCheckpoint
		if prior == nil || cp.Epoch > prior.Epoch || cp.Epoch == prior.Epoch && cp.ObservedTsMs > prior.ObservedTsMs {
			progress.SourceCheckpoint = cp
			progress.TsMs = max(progress.TsMs, cp.ObservedTsMs)
		}
	}
	if m.telemetryByPipeline == nil {
		m.telemetryByPipeline = make(map[statusKey]*telemetryWindow)
	}
	w := m.telemetryByPipeline[key]
	if w == nil {
		w = &telemetryWindow{components: make(map[string]*telemetryComponent)}
		m.telemetryByPipeline[key] = w
	}
	c := w.components[v.Component]
	if c != nil {
		if v.ObservedTSMS <= c.current.ObservedTSMS {
			return nil
		}
		if v.InstanceID == c.current.InstanceID && (v.Rows < c.current.Rows || v.Txns < c.current.Txns || v.Bytes < c.current.Bytes || v.Errors < c.current.Errors || v.Conflicts < c.current.Conflicts) {
			slog.Warn("telemetry counter regressed", slog.String("pipeline_id", v.PipelineID), slog.String("component", v.Component))
			return nil
		}
		if v.InstanceID != c.current.InstanceID {
			c.previous = iproto.PipelineTelemetry{InstanceID: v.InstanceID}
		}
		c.current = v
	} else {
		w.components[v.Component] = &telemetryComponent{current: v}
	}
	return nil
}

// sampleTelemetry runs only in the reporter actor, before producing an owned
// publish snapshot. Missing or stale components leave their observation old.
func (m *Module) sampleTelemetry(pg *iproto.PipelineProgress, nowMS int64) {
	w := m.telemetryByPipeline[statusKeyOf(pg.GetPipelineId(), pg.GetRole())]
	if w == nil {
		return
	}
	if w.lastTSMS >= nowMS {
		return
	}
	names := []string{iproto.TelemetryTargetConsumer}
	if pg.GetRole() == iproto.PipelineRole_PIPELINE_ROLE_SOURCE {
		names = []string{iproto.TelemetrySourceReader, iproto.TelemetrySourcePublisher}
	}
	observed := nowMS
	for _, name := range names {
		c := w.components[name]
		if c == nil || c.current.RunID != pg.GetRunId() || c.current.ObservedTSMS > nowMS || nowMS-c.current.ObservedTSMS > 15000 {
			return
		}
		if c.current.ObservedTSMS < observed {
			observed = c.current.ObservedTSMS
		}
	}
	// Snapshot modules accompany incremental modules only during a full run.
	// Their final counters remain observable while the module is alive; an
	// unloaded module must not prevent healthy incremental observations.
	optional := iproto.TelemetryTargetSnapshot
	if pg.GetRole() == iproto.PipelineRole_PIPELINE_ROLE_SOURCE {
		optional = iproto.TelemetrySourceSnapshot
	}
	if c := w.components[optional]; c != nil && c.current.RunID == pg.GetRunId() && c.current.ObservedTSMS <= nowMS && nowMS-c.current.ObservedTSMS <= 15000 {
		names = append(names, optional)
		if c.current.ObservedTSMS < observed {
			observed = c.current.ObservedTSMS
		}
	}
	if pg.MetricsObservedTsMs != nil && observed <= *pg.MetricsObservedTsMs {
		return
	}
	var rows, txns, bytes float64
	pg.ErrorCount = 0
	pg.ConflictCount = 0
	pg.ModuleQueues = nil
	for _, name := range names {
		c := w.components[name]
		v := c.current
		rows += float64(v.Rows - c.previous.Rows)
		txns += float64(v.Txns - c.previous.Txns)
		bytes += float64(v.Bytes - c.previous.Bytes)
		pg.ErrorCount += v.Errors
		pg.ConflictCount += v.Conflicts
		if v.QueueModule != "" {
			pg.ModuleQueues = append(pg.ModuleQueues, &iproto.QueueWatermark{Module: v.QueueModule, Len: v.QueueLen, Cap: v.QueueCap})
		}
		if name == iproto.TelemetrySourcePublisher {
			pg.InflightBatches = v.InflightBatches
			pg.InflightBytes = v.InflightBytes
		}
		if name == iproto.TelemetrySourceReader || name == iproto.TelemetryTargetConsumer {
			pg.ReadGtidSet = v.ReadGTIDSet
			pg.PublishedGtidSet = v.PublishedGTIDSet
			pg.AppliedGtidSet = v.AppliedGTIDSet
			pg.SourceEventTsMs = v.SourceEventTSMS
			pg.LagMs = v.LagMS
		}
		c.previous = v
	}
	pg.RowsPerSec = 0
	pg.TxnsPerSec = 0
	pg.BytesPerSec = 0
	if w.lastTSMS > 0 && nowMS > w.lastTSMS {
		seconds := float64(nowMS-w.lastTSMS) / 1000
		pg.RowsPerSec = rows / seconds
		pg.TxnsPerSec = txns / seconds
		pg.BytesPerSec = bytes / seconds
	}
	w.lastTSMS = nowMS
	pg.MetricsObservedTsMs = &observed
	pg.TsMs = nowMS
}
