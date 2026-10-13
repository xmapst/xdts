package amqpin

import (
	"context"
	"fmt"
	"math"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdts/internal/iproto"
)

type iCatchupApplier interface {
	BeginCatchup(string) error
	EndCatchup()
}

type iCompletedRecoveryApplier interface {
	FlushCompletedRecoveryTransactions(context.Context) (iproto.BatchApplyResult, error)
}

// beginCatchup runs before creating the consumer. After it returns, all catchup
// state is transferred to that consumer's single handler goroutine.
func (m *Module) beginCatchup(end *iproto.SnapshotEnd) error {
	m.recoveryPending = len(end.GetRecoveryTables()) > 0
	m.recoverySequence = end.GetRecovery().GetSequence()
	m.recoveryTables = make([]*iproto.SnapshotRecoveryTable, 0, len(end.GetRecoveryTables()))
	for _, table := range end.GetRecoveryTables() {
		m.recoveryTables = append(m.recoveryTables, &iproto.SnapshotRecoveryTable{Schema: table.GetSchema(), Table: table.GetTable()})
	}
	if len(end.GetRecoveryTables()) > 0 || end.GetRecovery() != nil {
		applier, ok := m.opts.Applier.(iTableRecoveryApplier)
		if !ok {
			return fmt.Errorf("amqpin: recovery applier unavailable")
		}
		if err := applier.SetRecoveryTables(m.shutdownCtx, end.RecoveryTables); err != nil {
			return err
		}
	}
	if err := iproto.ValidateSnapshotEndPlan(end); err != nil {
		return err
	}
	if end.GetPlanExtensionSequence() > 0 && !m.opts.AllowPlanExtensions {
		return fmt.Errorf("amqpin: snapshot extensions were not negotiated")
	}
	if end == nil || end.GetPipelineId() != m.opts.PipelineID || end.GetRunId() != m.opts.RunID || end.GetTotalChunks() == 0 {
		return fmt.Errorf("amqpin: invalid snapshot completion identity")
	}
	low, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, end.GetLowGtidSet())
	if err != nil {
		return err
	}
	high, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, end.GetHighGtidSet())
	if err != nil || !high.Contain(low) {
		return fmt.Errorf("amqpin: invalid catchup watermarks")
	}
	applier, ok := m.opts.Applier.(iCatchupApplier)
	if !ok {
		return fmt.Errorf("amqpin: catchup applier required")
	}
	if err := applier.BeginCatchup(end.GetHighGtidSet()); err != nil {
		return err
	}
	if end.GetRecovery() != nil {
		recovery, ok := m.opts.Applier.(iCompletedRecoveryApplier)
		if !ok {
			return fmt.Errorf("amqpin: stored recovery requires support")
		}
		result, err := recovery.FlushCompletedRecoveryTransactions(m.shutdownCtx)
		if err != nil {
			return err
		}
		// Merged after resetting the inherited low below.
		defer m.recordApplyResult(result)
	}
	// The source stream always restarts at L. Local checkpoints are observation
	// only and may contain positions past a currently re-delivered gap.
	m.appliedSet = withoutRejected(low, m.rejectedSet)
	m.consumedSet = low.Clone()
	m.catchupHigh = high
	if m.opts.FullOnly {
		m.fullOnlyHigh = high.Clone()
		m.fullOnlyDone = false
	}
	m.catchupTotalChunks = end.GetTotalChunks()
	m.catchupInitialTotal = end.GetInitialTotalChunks()
	m.catchupExtensionSequence = end.GetPlanExtensionSequence()
	m.publishAppliedSnapshot()
	m.publishConsumedSnapshot()
	m.reportProgress()
	return nil
}

func (m *Module) maybeFinishCatchup() {
	if m.catchupHigh == nil || !m.consumedSet.Contain(m.catchupHigh) {
		return
	}
	if m.recoveryPending {
		return
	}
	m.opts.Applier.(iCatchupApplier).EndCatchup()
	if m.opts.AfterCatchup != nil {
		ctx, cancel := context.WithTimeout(m.shutdownCtx, m.opts.ApplyAttemptTimeout)
		err := m.opts.AfterCatchup(ctx)
		cancel()
		if err != nil {
			if m.shutdownCtx.Err() != nil {
				return
			}
			m.freeze("全量追平后结构迁移失败，请检查节点日志后重试")
			return
		}
	}
	m.catchupHigh = nil
	m.fullOnlyDone = m.opts.FullOnly
	if m.fullOnlyDone {
		m.discardPublicationBuffers()
	}
	// The final batch can arrive inside the checkpoint throttle interval. No
	// later DML is required, so completion must publish and persist immediately.
	m.persistCheckpoint()
	m.reportProgress()
	m.reportStatus(iproto.PipelineState_PIPELINE_STATE_RUNNING, "")
}

// Watermarks are published strictly after all preceding batches are confirmed.
// A filtered/empty source transaction emits no DataBatch; its GTID is carried
// by this ordered watermark. Never advance past an unfinished reassembly.
func (m *Module) handleWatermark(env *iproto.Envelope, h iAckHandle) {
	var watermark iproto.Watermark
	if err := iproto.Decode(env.Body, env.Properties.ContentEncoding, &watermark); err != nil {
		m.reject(h, fmt.Errorf("watermark 解码失败: %w", err))
		return
	}
	if header, ok := env.HeaderString(iproto.HeaderPipelineID); ok && header != "" && header != watermark.GetPipelineId() {
		m.reject(h, fmt.Errorf("watermark pipeline header mismatch"))
		return
	}
	if _, exists := env.Headers[iproto.HeaderRunID]; exists {
		if run, ok := iproto.HeaderUint64FromString(env.Headers, iproto.HeaderRunID); !ok || run != watermark.GetRunId() {
			m.reject(h, fmt.Errorf("watermark run header mismatch"))
			return
		}
	}
	set, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, watermark.GetGtidSet())
	if err != nil {
		m.reject(h, fmt.Errorf("watermark GTID 水位无效: %w", err))
		return
	}
	if !m.acceptRun(watermark.GetPipelineId(), watermark.GetRunId(), h) {
		return
	}
	if m.fullOnlyHigh != nil {
		if !m.recoveryPending {
			set = withoutRejected(set, withoutRejected(set, m.fullOnlyHigh))
		}
	}
	m.telemetrySourceTS = watermark.GetSourceTsMs()
	if m.telemetrySourceTS > 0 {
		m.telemetryLagMS = math.Max(0, float64(m.now().UnixMilli()-m.telemetrySourceTS))
	}
	if m.reassembly != nil {
		m.freeze("收敛期间水位消息无效或处于未完成事务中")
		return
	}
	appliedWatermark := withoutRejected(set, m.rejectedSet)
	if m.recoveryPending {
		appliedWatermark = withoutRejected(appliedWatermark, withoutRejected(m.consumedSet, m.appliedSet))
	}
	if err := m.appliedSet.Update(appliedWatermark.String()); err != nil {
		m.freeze("收敛期间 GTID 水位合并失败")
		return
	}
	if err := m.consumedSet.Update(set.String()); err != nil {
		m.freeze("收敛期间消费水位合并失败")
		return
	}
	m.publishAppliedSnapshot()
	m.publishConsumedSnapshot()
	m.captureTelemetry()
	m.maybePersistCheckpoint()
	m.maybeFinishCatchup()
	if m.isFrozen() {
		return
	}
	m.handleAckOnly(h, "watermark 消息", &m.stats.watermarksAcked)
}
