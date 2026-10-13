package amqpin

import (
	"fmt"
	"log/slog"
)

// acceptRun runs before continuity, reassembly or checkpoint mutation. A
// confirmed new run supersedes queued messages from every earlier run.
func (m *Module) acceptRun(pipelineID string, run uint64, ack iAckHandle) bool {
	if pipelineID != m.opts.PipelineID || run > m.opts.RunID {
		m.reject(ack, fmt.Errorf("消息不属于当前 pipeline/run: pipeline=%s run=%d expected_run=%d", pipelineID, run, m.opts.RunID))
		return false
	}
	if run == m.opts.RunID {
		return true
	}
	if err := ack.Ack(); err != nil {
		m.stats.ackFailures.Add(1)
		slog.Error("ack superseded run message failed", slog.String("pipeline_id", m.opts.PipelineID), slog.Uint64("run_id", run), slog.String("message_id", ack.MessageID()), slog.Any("error", err))
	} else {
		m.stats.supersededAcked.Add(1)
	}
	return false
}
