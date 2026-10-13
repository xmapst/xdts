package snapwriter

import (
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/reporter"
)

// Only the coordinator updates counters. Each transfer owns its plain value.
func (m *Module) captureTelemetry(v iproto.PipelineTelemetry) {
	m.telemetryMirror.Store(&v)
}

func (m *Module) reportTelemetry() {
	if m.opts.InstanceID != "" && m.runtimeFailed.Load() {
		m.Cast("controller", iproto.PipelineRuntimeStatus{PipelineID: m.opts.Run.PipelineID, Role: iproto.PipelineRole_PIPELINE_ROLE_TARGET, RunID: m.opts.Run.RunID, ConfigVersion: m.runtimeVersion.Load(), InstanceID: m.opts.InstanceID, State: iproto.PipelineState_PIPELINE_STATE_ERROR})
	}
	v := *m.telemetryMirror.Load()
	v.PipelineID, v.RunID = m.opts.Run.PipelineID, m.opts.Run.RunID
	v.Role, v.Component = iproto.PipelineRole_PIPELINE_ROLE_TARGET, iproto.TelemetryTargetSnapshot
	v.InstanceID, v.ObservedTSMS = m.telemetryInstanceID, time.Now().UnixMilli()
	q := m.ChanRPC()
	v.QueueModule, v.QueueLen, v.QueueCap = ModuleName(m.opts.Run.PipelineID), uint32(q.Len()), uint32(q.Cap())
	m.Cast(reporter.ModuleName, v)
}
