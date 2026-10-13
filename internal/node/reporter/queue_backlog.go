package reporter

import (
	"context"
	"log/slog"

	"google.golang.org/protobuf/proto"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/transport"
)

// progressSnapshot only runs in the reporter event loop. Each result owns its
// complete message, including nested snapshot/table progress. A stopped or idle
// target still needs queue observation even when amqpin emits no new checkpoint.
func (m *Module) progressSnapshot(nowMS int64) []*iproto.PipelineProgress {
	active := make(map[statusKey]bool, len(m.pipelinesMirror))
	for _, entry := range m.pipelinesMirror {
		active[statusKeyOf(entry.GetPipelineId(), entry.GetRole())] = true
	}
	snapshots := make(map[statusKey]*iproto.PipelineProgress, len(m.progressByPipeline))
	for key, pg := range m.progressByPipeline {
		if m.pipelinesMirrorReceived && !active[key] {
			continue
		}
		owned := proto.Clone(pg).(*iproto.PipelineProgress)
		m.sampleTelemetry(owned, nowMS)
		m.progressByPipeline[key] = proto.Clone(owned).(*iproto.PipelineProgress)
		snapshots[key] = owned
	}
	if m.opts.QueueInspector != nil {
		for key := range active {
			if key.role == iproto.PipelineRole_PIPELINE_ROLE_TARGET && snapshots[key] == nil {
				snapshots[key] = &iproto.PipelineProgress{PipelineId: key.pipelineID, Role: key.role, TsMs: nowMS}
			}
		}
	}
	out := make([]*iproto.PipelineProgress, 0, len(snapshots))
	for _, pg := range snapshots {
		out = append(out, pg)
	}
	return out
}

// inspectProgressQueue runs outside the event loop on an independently owned
// snapshot. Failed observations are not published as zero or as fresh old values.
func (m *Module) inspectProgressQueue(ctx context.Context, pg *iproto.PipelineProgress) bool {
	if pg.GetRole() != iproto.PipelineRole_PIPELINE_ROLE_TARGET || m.opts.QueueInspector == nil {
		return true
	}
	queryCtx, cancel := context.WithTimeout(ctx, m.opts.QueueInspectTimeout)
	defer cancel()
	stats, err := m.opts.QueueInspector.InspectQueue(queryCtx, transport.PipelineDataQueue(pg.GetPipelineId()))
	if err != nil {
		slog.Warn("data queue inspection failed", slog.String("pipeline_id", pg.GetPipelineId()), slog.String("reason", "queue observation unavailable"))
		m.stats.failed.Add(1)
		return false
	}
	pg.DataQueueBacklog = stats.Messages
	pg.TsMs = m.now().UnixMilli()
	observed := pg.TsMs
	pg.QueueObservedTsMs = &observed
	return true
}
