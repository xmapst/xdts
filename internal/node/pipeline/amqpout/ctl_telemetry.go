package amqpout

import (
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/reporter"
)

// Only called on this actor. Send* workers update independent atomic counters.
func (m *Module) reportTelemetry() {
	q := m.ChanRPC()
	m.Cast(reporter.ModuleName, iproto.PipelineTelemetry{
		PipelineID: m.opts.PipelineID, RunID: m.opts.RunID, Role: iproto.PipelineRole_PIPELINE_ROLE_SOURCE, Component: iproto.TelemetrySourcePublisher, InstanceID: m.telemetryInstanceID, ObservedTSMS: m.now().UnixMilli(),
		Bytes: m.stats.confirmedBytes.Load(), Errors: m.stats.publishErrors.Load(), InflightBatches: uint32(m.stats.inflightBatches.Load()), InflightBytes: m.stats.inflightBytes.Load(),
		QueueModule: ModuleName(m.opts.PipelineID), QueueLen: uint32(q.Len()), QueueCap: uint32(q.Cap()),
	})
}
