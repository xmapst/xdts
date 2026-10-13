package snapreader

import (
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/reporter"
)

// The timer reads an immutable counter mirror, never the Runner's state.
func (m *Module) reportTelemetry() {
	if m.opts.InstanceID != "" && m.runtimeFailed.Load() {
		m.Cast("controller", iproto.PipelineRuntimeStatus{PipelineID: m.opts.PipelineID, Role: iproto.PipelineRole_PIPELINE_ROLE_SOURCE, RunID: m.opts.RunID, ConfigVersion: m.runtimeVersion.Load(), InstanceID: m.opts.InstanceID, State: iproto.PipelineState_PIPELINE_STATE_ERROR})
	}
	v := *m.telemetryMirror.Load()
	v.PipelineID, v.RunID = m.opts.PipelineID, m.opts.RunID
	v.Role, v.Component = iproto.PipelineRole_PIPELINE_ROLE_SOURCE, iproto.TelemetrySourceSnapshot
	v.InstanceID, v.ObservedTSMS = m.telemetryInstanceID, time.Now().UnixMilli()
	q := m.ChanRPC()
	v.QueueModule, v.QueueLen, v.QueueCap = ModuleName(m.opts.PipelineID), uint32(q.Len()), uint32(q.Cap())
	m.Cast(reporter.ModuleName, v)
}
