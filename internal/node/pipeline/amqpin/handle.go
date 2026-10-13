package amqpin

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync/atomic"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/resourcequota"
	"github.com/xmapst/xdts/internal/transport"
	"google.golang.org/protobuf/proto"
)

func (m *Module) handlePreparedMessage(msg *transport.Message, prepared *preparedBatch) {
	tracked := &trackedAck{inner: msg.Ack}
	defer func() {
		if recover() != nil {
			m.freeze("增量处理发生异常，保留消息并停止流水线")
		}
		if !tracked.done {
			m.heldHandles = append(m.heldHandles, tracked)
		}
		kept := m.heldHandles[:0]
		for _, h := range m.heldHandles {
			if a, ok := h.(*trackedAck); !ok || !a.done {
				kept = append(kept, h)
			}
		}
		clear(m.heldHandles[len(kept):])
		m.heldHandles = kept
		if m.isFrozen() {
			m.discardReassembly()
		}
	}()
	if m.isFrozen() || m.fullOnlyDone {

		return
	}

	env, err := msg.Envelope()
	if err != nil {
		m.reject(tracked, fmt.Errorf("信封解析失败: %w", err))
		return
	}
	m.dispatchPrepared(env, tracked, prepared)
}

type trackedAck struct {
	inner iAckHandle
	done  bool
}

func (a *trackedAck) Ack() error {
	err := a.inner.Ack()
	if err == nil {
		a.done = true
	}
	return err
}
func (a *trackedAck) MessageID() string { return a.inner.MessageID() }

// Called only by the serial callback, or Close after preventing new callbacks
// and observing that the previous callback has returned.
func (m *Module) discardReassembly() {
	if m.reassembly == nil {
		return
	}
	r := m.reassembly
	m.reassembly = nil
	r.release()
}

// dispatch 按 Properties.Type 分派到具体处理路径。
func (m *Module) dispatch(env *iproto.Envelope, h iAckHandle) {
	m.dispatchPrepared(env, h, nil)
}

func (m *Module) dispatchPrepared(env *iproto.Envelope, h iAckHandle, prepared *preparedBatch) {
	m.stats.receivedBytes.Add(uint64(len(env.Body)))
	if env.Properties.Type != iproto.MessageTypeDataBatch {
		m.flushReplay()
		if m.isFrozen() {
			return
		}
	}
	switch env.Properties.Type {
	case iproto.MessageTypeWatermark:
		m.handleWatermark(env, h)
	case iproto.MessageTypeDataProbe:
		// Stopped-pipeline LINK is proved by probe's temporary consumer and
		// signed reply. Runtime consumers only drain leftover probes; this
		// acknowledgement cannot authorize a Manager precheck batch.
		m.handleAckOnly(h, "Probe 消息", &m.stats.probesAcked)
	case iproto.MessageTypeDataBatch:
		m.handlePreparedDataBatch(env, h, prepared)
	default:

		m.reject(h, fmt.Errorf("未知消息类型 %q（增量队列上只应出现 %s/%s/%s；协议另有 %s/%s 走全量队列，出现在这里说明路由有误）",
			env.Properties.Type, iproto.MessageTypeDataBatch, iproto.MessageTypeWatermark, iproto.MessageTypeDataProbe,
			iproto.MessageTypeSnapshotChunk, iproto.MessageTypeSnapshotEnd))
	}
}

// handleAckOnly 处理"收到即 Ack、不做其余业务处理"的消息类型（Watermark/
// Probe）。如果当前有分片重组正在进行，说明 binlogin 在分片流中间插入了别的
// 消息类型——正常路径下不会发生（分片属于同一次 emitTxn 调用，同步发完才可能
// 轮到别的消息），出现即结构性异常，交给 reject 统一冻结。
func (m *Module) handleAckOnly(h iAckHandle, kind string, counter *atomic.Int64) {
	if m.reassembly != nil {
		m.reject(h, fmt.Errorf("重组进行中收到了 %s，违反预期的严格顺序（分片不应与其它类型消息交错）", kind))
		return
	}
	if err := h.Ack(); err != nil {
		slog.Error("ack failed", slog.String("pipeline_id", m.opts.PipelineID),
			slog.String("kind", kind), slog.String("message_id", h.MessageID()), slog.Any("error", err))
		return
	}
	counter.Add(1)
}

// handleDataBatch 处理 DataBatch 类型消息：解码 → 交叉校验 → 结构校验 →
// 按是否分片分派到直接应用或重组路径。
func (m *Module) handleDataBatch(env *iproto.Envelope, h iAckHandle) {
	m.handlePreparedDataBatch(env, h, nil)
}

func (m *Module) handlePreparedDataBatch(env *iproto.Envelope, h iAckHandle, prepared *preparedBatch) {
	defer func() {
		if recover() != nil {
			m.freeze("增量批次处理异常，保留消息并停止流水线")
		}
		if m.isFrozen() {
			m.discardReassembly()
		}
	}()
	batch := new(iproto.DataBatch)
	var buffer *resourcequota.Buffer
	var err error
	if prepared == nil {
		buffer, err = m.opts.Budget.Decode(env.Body, env.Properties.ContentEncoding, batch)
	} else {
		buffer, err = prepared.buffer, prepared.err
		if prepared.batch != nil {
			batch = prepared.batch
		}
		// Transfer the decoded buffer before any retention or rejection path.
		prepared.buffer = nil
		prepared.batch = nil
	}
	if err != nil {
		if errors.Is(err, resourcequota.ErrExceeded) {
			m.freeze(err.Error())
			return
		}
		m.reject(h, fmt.Errorf("DataBatch 解码失败: %w", err))
		return
	}
	retained := false
	defer func() {
		if !retained {
			buffer.Release()
		}
	}()

	if hdrPipelineID, ok := env.HeaderString(iproto.HeaderPipelineID); ok && hdrPipelineID != "" &&
		hdrPipelineID != batch.GetPipelineId() {
		m.reject(h, fmt.Errorf("x-pipeline-id header(%s) 与解码后 DataBatch.PipelineId(%s) 不一致，"+
			"疑似类型混淆或消息损坏", hdrPipelineID, batch.GetPipelineId()))
		return
	}

	if err := iproto.ValidateDataBatch(batch); err != nil {
		m.reject(h, fmt.Errorf("DataBatch 结构校验失败: %w", err))
		return
	}
	if _, exists := env.Headers[iproto.HeaderRunID]; exists {
		if run, ok := iproto.HeaderUint64FromString(env.Headers, iproto.HeaderRunID); !ok || run != batch.GetRunId() {
			m.reject(h, fmt.Errorf("DataBatch run header mismatch"))
			return
		}
	}
	if m.pendingReplay != nil && (batch.GetPipelineId() != m.opts.PipelineID || batch.GetRunId() != m.opts.RunID || batch.GetEpoch() != m.pendingReplay.batch.Epoch || batch.GetChunk() != nil || batchHasDDL(batch)) {
		m.flushReplay()
		if m.isFrozen() {
			return
		}
	}
	if !m.acceptRun(batch.GetPipelineId(), batch.GetRunId(), h) {
		return
	}

	if batch.GetPublicationWindowFirstSeq() != 0 {
		for header, expected := range map[string]uint64{iproto.HeaderEpoch: batch.GetEpoch(), iproto.HeaderBatchSeq: batch.GetBatchSeq()} {
			if actual, ok := iproto.HeaderUint64FromString(env.Headers, header); !ok || actual != expected {
				m.freeze("发布窗口消息头与批次身份不一致")
				return
			}
		}
		retained = m.acceptPublicationBatch(batch, h, buffer)
		return
	}

	// ok=false（header 缺失或格式不对）不当作永久性坏拒绝这条 DataBatch，也不
	// 送进 checkContinuity——这两个 header 只是 NODE-MQI-02 的判断输入，不是
	// 数据正确性的输入（正确性由目标库 gtid_executed 兜底，NODE-CKP-04），该
	// 批次仍然正常往下走应用流程，只是跳过这一次的连续性判断并记一条 WARN，
	// 避免因为一次可观测性数据的缺失就把整条 pipeline 卡住（那会是比"检测器
	// 暂时不准"更糟的后果）。
	if epoch, ok := iproto.HeaderUint64FromString(env.Headers, iproto.HeaderEpoch); ok {
		if batchSeq, ok := iproto.HeaderUint64FromString(env.Headers, iproto.HeaderBatchSeq); ok {
			switch verdict, reason := m.checkContinuity(epoch, batchSeq); verdict {
			case continuityGap:
				// 跳号发生时如果恰好有分片重组正在进行，把重组上下文一并写进
				// 冻结原因，排查时能一眼看出"丢的是哪个事务重组期间的哪一片"
				// ——与 reject() 拼重组上下文是同一个理由，这里不复用 reject()
				// 本身，因为 reject() 的语义是"结构性异常"，跳号不是结构性
				// 异常（消息本身没有任何问题），是连续性异常，需要一条独立的
				// 冻结原因文案。
				if m.reassembly != nil {
					r := m.reassembly
					reason = fmt.Sprintf("%s（此时有分片重组正在进行：gtid=%s，已收 %d/%d 片）",
						reason, r.gtid(), len(r.handles), r.total)
				}
				m.freeze(reason)
				return
			case continuityDuplicate:
				m.flushReplay()
				if m.isFrozen() {
					return
				}
				// 同一 epoch 内已经见过的 batch_seq（recordSeen 记录过、可证
				// 内容重复）：直接 Ack，不进入分片/应用流程——这是
				// internal/node/pipeline/amqpin/continuity.go 顶部说的"丢弃"两种含义里的 Ack 那一种，不是
				// NODE-MQI-04 禁止的 NackDiscard。放在分派到 handleChunk 之前
				// 拦截，重复分片不会碰到重组状态机、不会被误判成"分片顺序
				// 异常"（internal/node/pipeline/amqpin/continuity.go 同一段注释的完整理由）。**不包括**
				// epoch < lastEpoch 的情形，那是 continuityStale，见下。
				m.ackDuplicate(h)
				return
			}
			// continuityNormal：checkContinuity 内部已经调用 recordSeen 记录
			// 新的 lastEpoch/lastBatchSeq，这里不需要再做任何事，直接往下走
			// 既有的分派逻辑。
		} else {
			slog.Warn("x-batch-seq header missing or malformed, skipping continuity check",
				slog.String("pipeline_id", m.opts.PipelineID), slog.String("message_id", h.MessageID()))
		}
	} else {
		slog.Warn("x-epoch header missing or malformed, skipping continuity check",
			slog.String("pipeline_id", m.opts.PipelineID), slog.String("message_id", h.MessageID()))
	}

	if batch.GetChunk() == nil {
		if m.reassembly != nil {
			m.reject(h, fmt.Errorf("收到一条非分片 DataBatch(gtid=%s)，但重组尚未收全", firstGtid(batch)))
			return
		}
		if m.coalescing && !batchHasDDL(batch) {
			retained = m.appendReplay(batch, h, buffer)
		} else {
			m.applyAndFinish(batch, []iAckHandle{h})
		}
		return
	}

	// proto/data.proto Chunk 的注释："此时 DataBatch.txns 必须恰好包含一个
	// Txn"——ValidateDataBatch 不检查这条（它只校验行内容，见 internal/iproto/validate.go），
	// 这里自己补上，不假设上游一定按协议来（NODE-OUT-07 doc 注释同一条原则）。
	if len(batch.GetTxns()) != 1 {
		m.reject(h, fmt.Errorf("分片 DataBatch 携带 %d 个事务，协议要求恰好 1 个", len(batch.GetTxns())))
		return
	}

	retained = m.handleChunkBudget(batch, h, buffer)
}

func (m *Module) handleChunkBudget(batch *iproto.DataBatch, h iAckHandle, buffer *resourcequota.Buffer) bool {
	chunk := batch.GetChunk()
	txnGtid := batch.GetTxns()[0].GetGtid()
	if m.opts.FullOnly {
		included, err := m.fullOnlyTransaction(txnGtid)
		if err != nil || chunk.GetTotal() == 0 || chunk.GetIndex() >= chunk.GetTotal() || chunk.GetTotal() > uint32(m.opts.MaxReassemblyChunks) {
			m.freeze("仅全量事务分片边界无效，保留消息")
			return false
		}
		if !included {
			// No SQL or GTID is meaningful beyond H. An interrupted post-H
			// transaction cannot leave a reassembly that blocks a later proof or
			// replay of the original H. Each valid fragment is a policy no-op.
			if m.reassembly != nil {
				m.freeze("仅全量忽略分片前仍有未完成的 H 内事务")
				return false
			}
			m.applyAndFinish(batch, []iAckHandle{h})
			return false
		}
	}

	if m.reassembly == nil {
		if chunk.GetIndex() != 0 {
			m.freeze(fmt.Sprintf("收到分片 index=%d 但当前没有进行中的重组（期望从 index=0 开始，gtid=%s）",
				chunk.GetIndex(), txnGtid))
			return false
		}
		if chunk.GetTotal() > uint32(m.opts.MaxReassemblyChunks) {
			m.freeze(fmt.Sprintf("事务(gtid=%s)分片数 %d 超过本节点重组上限 %d（Options.MaxReassemblyChunks）",
				txnGtid, chunk.GetTotal(), m.opts.MaxReassemblyChunks))
			return false
		}
		if chunk.GetTotal() == 0 {
			m.freeze(fmt.Sprintf("事务(gtid=%s)携带 Chunk 但 Total=0，协议要求分片总数至少为 1", txnGtid))
			return false
		}

		m.reassembly = newReassemblyState(batch, h)
		m.reassembly.buffers = append(m.reassembly.buffers, buffer)
		m.maybeFinishReassembly()
		return true
	}

	r := m.reassembly
	if txnGtid == r.gtid() && batch.GetEpoch() > r.shell.GetEpoch() && chunk.GetIndex() == 0 {
		// The source checkpoint only covers whole transactions. A new epoch can
		// restart a previously partial transaction; old deliveries are retained
		// until the fresh complete transaction supplies actual completion proof.
		if chunk.GetTotal() == 0 || uint64(len(r.handles)+len(r.superseded))+uint64(chunk.GetTotal()) > uint64(m.opts.MaxReassemblyChunks) {
			m.freeze("重启分片与旧消息超过消费者容量")
			return false
		}
		previous := append(r.superseded, r.handles...)
		r.release()
		m.reassembly = newReassemblyState(batch, h)
		m.reassembly.superseded = previous
		m.reassembly.buffers = append(m.reassembly.buffers, buffer)
		m.maybeFinishReassembly()
		return true
	}
	if txnGtid == r.gtid() && batch.GetEpoch() < r.shell.GetEpoch() {
		if len(r.handles)+len(r.superseded)+int(r.total-r.nextExpectedIndex()) >= m.opts.MaxReassemblyChunks {
			m.freeze("旧分片持有消息超过消费者容量")
			return false
		}
		r.superseded = append(r.superseded, h)
		return false
	}
	switch {
	case batch.GetEpoch() != r.shell.GetEpoch():
		m.freeze("事务分片的epoch发生变化且没有从首片重新开始")
		return false
	case txnGtid != r.gtid():
		m.freeze(fmt.Sprintf("重组进行中（gtid=%s，已收 %d/%d 片）收到了另一个事务的分片(gtid=%s)",
			r.gtid(), len(r.handles), r.total, txnGtid))
		return false
	case chunk.GetIndex() != r.nextExpectedIndex():
		m.freeze(fmt.Sprintf("重组进行中（gtid=%s）分片顺序异常：期望 index=%d，实际收到 index=%d",
			r.gtid(), r.nextExpectedIndex(), chunk.GetIndex()))
		return false
	case chunk.GetTotal() != r.total:
		m.freeze(fmt.Sprintf("重组进行中（gtid=%s）分片总数前后不一致：首片声明 total=%d，第 %d 片声明 total=%d",
			r.gtid(), r.total, chunk.GetIndex(), chunk.GetTotal()))
		return false
	}
	first, next := r.shell.GetTxns()[0], batch.GetTxns()[0]
	if first.GetOriginCenter() != next.GetOriginCenter() || first.GetHops() != next.GetHops() || first.GetLineageVersion() != next.GetLineageVersion() ||
		!slices.Equal(first.GetPath(), next.GetPath()) || !slices.Equal(first.GetEndpointPath(), next.GetEndpointPath()) {
		m.freeze("transaction fragment lineage changed during reassembly")
		return false
	}
	if !proto.Equal(first.GetShadowTablePolicy(), next.GetShadowTablePolicy()) {
		m.freeze("transaction fragment shadow policy changed during reassembly")
		return false
	}

	r.buffers = append(r.buffers, buffer)
	if r.append(batch, h) {
		m.maybeFinishReassembly()
	}
	return true
}

// maybeFinishReassembly 在重组已收全（r.handles 数量达到 r.total）时拼出完整
// DataBatch 并交给 applyAndFinish；未收全则什么都不做，继续等待下一片。
func (m *Module) maybeFinishReassembly() {
	r := m.reassembly
	if uint32(len(r.handles)) < r.total {
		return
	}
	full, handles := r.finish()
	defer r.release()
	m.reassembly = nil // 允许下一个事务开始
	m.stats.chunksReassembled.Add(int64(len(handles)))
	m.applyAndFinish(full, handles)
}

// Permanent failures and exhausted retry budgets both freeze consumption and
// report ERROR. Explicitly authorized, durably recorded SQL skips return a
// rejected result instead; their GTIDs never become applied.
func (m *Module) applyAndFinish(batch *iproto.DataBatch, handles []iAckHandle) {
	if m.fullOnlyDone {
		return
	}
	bounded, err := m.fullOnlyBatch(batch)
	if err != nil {
		m.freeze("仅全量事务边界无效，保留消息")
		return
	}
	filtered := m.filterOriginBatch(bounded)
	var applyErr error
	var result iproto.BatchApplyResult
	if len(filtered.GetTxns()) != 0 {
		result, applyErr = m.applyWithRetry(filtered)
	}
	// Each source transaction commits independently. A later failure cannot
	// undo earlier commits in this batch, even though its delivery stays held.
	m.recordApplyResult(result)
	m.recordTelemetryResult(filtered, result)
	if err := applyErr; err != nil {
		if len(result.AppliedGTIDs)+len(result.RejectedGTIDs)+len(result.DeferredGTIDs) > 0 {
			completed := &iproto.DataBatch{}
			for _, gtid := range result.AppliedGTIDs {
				completed.Txns = append(completed.Txns, &iproto.Txn{Gtid: gtid})
			}
			for _, gtid := range result.RejectedGTIDs {
				completed.Txns = append(completed.Txns, &iproto.Txn{Gtid: gtid})
			}
			for _, gtid := range result.DeferredGTIDs {
				completed.Txns = append(completed.Txns, &iproto.Txn{Gtid: gtid})
			}
			m.recordConsumed(completed)
			// The frozen consumer may receive no subsequent successful batch.
			// Publish completed transactions immediately instead of throttling.
			m.persistCheckpoint()
			m.reportProgress()
		}
		m.stats.applyFailures.Add(1)
		if m.catchupHigh != nil {
			m.freeze("收敛期间事务写入失败，停止后续消费以保持顺序")
			return
		}
		for _, txn := range batch.GetTxns() {
			if txn.GetDdl() != nil {
				m.freeze(fmt.Sprintf("DDL 应用失败，停止后续消费: %v", err))
				return
			}
		}
		if errors.Is(err, errApplyNotRetryable) {
			// 原因字符串带上 delivery_count：freeze 的日志与上报只接一个 reason，
			// 这个数落在 reason 里才不会随着原有那条 slog.Error 一起消失。
			m.freeze(fmt.Sprintf("应用失败且不可重试（%d 条 delivery 持有不确认）: %v",
				len(handles), err))
			return
		}
		slog.Error("apply databatch failed after exhausting retries, holding for manual handling",
			slog.String("pipeline_id", m.opts.PipelineID),
			slog.Int("delivery_count", len(handles)),
			slog.Any("error", err))
		m.freeze("事务重试预算耗尽，保留未确认消息并停止后续消费")
		return
	}
	// A completed batch also consumes lineage-filtered transactions. They do
	// not enter the MySQL applied position.
	m.recordConsumed(bounded)
	m.maybePersistCheckpoint()
	if err := m.persistPublicationAcknowledgements(handles); err != nil {
		m.freeze("发布窗口确认游标关键提交失败，保留消息")
		return
	}
	m.maybeFinishCatchup()
	if m.isFrozen() {
		return
	}
	for _, h := range handles {
		if ackErr := h.Ack(); ackErr != nil {
			// ApplyBatch 已经成功提交：Ack 本身失败是基础设施瞬时问题，不是
			// 数据问题。GTID 幂等保证重投安全（broker 重投后再跑一次
			// ApplyBatch 会被 MySQL 自动跳过），只记日志，不做额外处理。
			slog.Error("applied to target db but ack failed",
				slog.String("pipeline_id", m.opts.PipelineID),
				slog.String("message_id", h.MessageID()), slog.Any("error", ackErr))
			m.stats.ackFailures.Add(1)
		}
	}
	m.stats.batchesApplied.Add(1)
}

func (m *Module) reject(h iAckHandle, reason error) {
	if m.reassembly != nil {
		r := m.reassembly
		m.freeze(fmt.Sprintf("重组进行中（gtid=%s，已收 %d/%d 片）收到异常消息: %v",
			r.gtid(), len(r.handles), r.total, reason))
		return
	}
	slog.Error("message permanently invalid, holding for manual handling",
		slog.String("pipeline_id", m.opts.PipelineID),
		slog.String("message_id", h.MessageID()),
		slog.Any("error", reason))
	m.stats.permanentFailures.Add(1)
}

// ackDuplicate 处理 checkContinuity 判定为"重复"的消息：直接 Ack，不进入
// 分片/应用流程（internal/node/pipeline/amqpin/continuity.go 顶部"两种丢弃"一段：这是 Ack，不是 NODE-MQI-04
// 禁止的 NackDiscard）。Ack 本身失败按现有 applyAndFinish 同一条原则处理——
// 记 WARN 不中断，消息保持未 Ack、可能被重投，下次仍会被 checkContinuity 判定
// 为重复，不会造成数据问题，只是多一次判断。
func (m *Module) ackDuplicate(h iAckHandle) {
	if err := h.Ack(); err != nil {
		slog.Warn("ack duplicate batch failed",
			slog.String("pipeline_id", m.opts.PipelineID), slog.String("message_id", h.MessageID()), slog.Any("error", err))
		m.stats.ackFailures.Add(1)
		return
	}
	m.stats.duplicatesAcked.Add(1)
}

// logFreeze 是 freeze() 实际打印的 ERROR 日志，单独抽出来只是让 internal/node/pipeline/amqpin/amqpin.go 里
// freeze 方法本身不需要 import log/slog。
func logFreeze(pipelineID, reason string) {
	slog.Error("consumption frozen, awaiting manual intervention",
		slog.String("pipeline_id", pipelineID), slog.String("reason", reason))
}

// firstGtid 是一个小助手：从一个非分片 DataBatch 里取第一个 Txn 的 GTID 用于
// 日志，取不到（空 Txns）时返回空字符串。空 Txns 现在会被 handleDataBatch 里的
// iproto.ValidateDataBatch 挡在调用本函数之前（iproto.ErrDataBatchEmpty），
// 这里仍然保留判断只是防御性的，不依赖调用方一定先过了那道检查。
func firstGtid(batch *iproto.DataBatch) string {
	if len(batch.GetTxns()) == 0 {
		return ""
	}
	return batch.GetTxns()[0].GetGtid()
}
