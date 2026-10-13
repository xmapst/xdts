package amqpin

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/store"
)

const checkpointCollection = "checkpoints"

func checkpointID(pipelineID string) string {
	return pipelineID + ".target"
}

func emptyGTIDSet() gmysql.GTIDSet {
	set, _ := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, "")
	return set
}

// 与 binlogin.loadSchemaHistory 同一定性：Store 未注入、文档不存在（首次运行）、
// 读取失败、或 applied_gtid_set 字段本身解析失败，都不是致命错误——appliedSet
// 保持 New() 里已经给好的空集合，lastEpoch/lastBatchSeq 保持零值。本包的检查点
// 不是任何恢复/去重逻辑的输入（目标端去重权威始终是目标库 gtid_executed，
// NODE-CKP-04），恢复失败只影响重启后第一次上报的起点是否连续，不是正确性
// 依赖，因此可以在 OnInit 里放心地"尽力而为、失败不中断启动"。
func (m *Module) loadCheckpoint() {
	if m.opts.Store == nil {
		return
	}
	var doc map[string]any
	if err := m.opts.Store.Get(context.Background(), checkpointCollection, checkpointID(m.opts.PipelineID), &doc); err != nil {

		return
	}

	if !store.MatchesCheckpointRun(doc, m.opts.RunID) {
		return
	}
	if gset, ok := doc["applied_gtid_set"].(string); ok && gset != "" {
		if parsed, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, gset); err == nil {
			m.appliedSet = parsed
			m.publishAppliedSnapshot()
		} else {
			slog.Warn("restored applied_gtid_set could not be parsed",
				slog.String("pipeline_id", m.opts.PipelineID), slog.Any("error", err))
		}
	}
	m.consumedSet = m.appliedSet.Clone()
	if gset, ok := doc["consumed_gtid_set"].(string); ok {
		if parsed, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, gset); err == nil && parsed.Contain(m.appliedSet) {
			m.consumedSet = parsed
		}
	}
	m.publishConsumedSnapshot()
	m.lastEpoch = parseStoredUint64(doc["epoch"])
	if gset, ok := doc["rejected_gtid_set"].(string); ok {
		if parsed, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, gset); err == nil {
			m.rejectedSet = parsed
			rejected := parsed.String()
			m.stats.rejectedGTIDSet.Store(&rejected)
		}
	}
	m.lastBatchSeq = parseStoredUint64(doc["batch_seq"])
	m.stats.lastEpoch.Store(m.lastEpoch)
	m.stats.lastBatchSeq.Store(m.lastBatchSeq)
}

func parseStoredUint64(v any) uint64 {
	switch n := v.(type) {
	case int64:
		if n < 0 {
			return 0
		}
		return uint64(n)
	case int:
		if n < 0 {
			return 0
		}
		return uint64(n)
	case uint64:
		return n
	case float64:
		if n < 0 {
			return 0
		}
		return uint64(n)
	case json.Number:
		i, err := n.Int64()
		if err != nil || i < 0 {
			return 0
		}
		return uint64(i)
	default:
		return 0
	}
}

// recordSeen 记录"最近一次从源端看到的" epoch/batch_seq（不管这条 DataBatch 是否
// 已经完整重组/应用），在 handleDataBatch 对每一条被接纳的有效 DataBatch（含分片
// 重组中的每一片）上调用。
//
// 为什么不能只在 applyAndFinish（重组收全）之后取值：分片场景下每一片是独立的
// AMQP 消息，各自携带自己的 x-batch-seq（协议注释"epoch 内连续递增"，按消息
// 递增，不是按事务），但 reassemblyState.shell 复用的是第一片解出的结构（见
// internal/node/pipeline/amqpin/reassembly.go 的文档），只在 Epoch 上下过"同一事务各片相同"的功夫，没有对
// BatchSeq 做同样的保证——如果这里从 finish() 之后的 shell 取 BatchSeq，一次 N
// 片的重组会系统性地少算 N-1，把下一条正常消息误判成跳号。因此本函数在
// handleDataBatch/handleChunk 处理每一条消息（不管是不是分片、是不是重组中）时
// 都调用一次，覆盖到每一片各自的 batch_seq。
func (m *Module) recordSeen(epoch, batchSeq uint64) {
	m.lastEpoch = epoch
	m.lastBatchSeq = batchSeq
	m.stats.lastEpoch.Store(epoch)
	m.stats.lastBatchSeq.Store(batchSeq)
}

// publishAppliedSnapshot 把 appliedSet 当前的字符串形式发布到 stats.appliedGTIDSet
// （供 Stats() 跨 goroutine 安全读取，见 internal/node/pipeline/amqpin/stats.go 的文档），在 loadCheckpoint 恢复
// 出非空集合、以及每次 recordApplied 之后调用。
func (m *Module) publishAppliedSnapshot() {
	s := m.appliedSet.String()
	m.stats.appliedGTIDSet.Store(&s)
}

// consumedSet includes complete transactions intentionally discarded by the
// target lineage policy. It never claims those GTIDs were executed by MySQL.
func (m *Module) recordConsumed(batch *iproto.DataBatch) {
	for _, txn := range batch.GetTxns() {
		if err := m.consumedSet.Update(txn.GetGtid()); err != nil {
			slog.Warn("merge gtid into consumed checkpoint failed", slog.String("pipeline_id", m.opts.PipelineID), slog.Any("error", err))
		}
	}
	m.publishConsumedSnapshot()
}

func (m *Module) publishConsumedSnapshot() {
	s := m.consumedSet.String()
	m.stats.consumedGTIDSet.Store(&s)
}

// checkpointPersistInterval 是 maybePersistCheckpoint 的节流间隔，对应
//
//	checkpoints 集合"写入时机：周期 1s"。
const checkpointPersistInterval = time.Second

// reportProgress 和 persistCheckpoint 共用节流判断，避免进度和检查点采样节奏分离。
func (m *Module) maybePersistCheckpoint() {
	now := m.now()
	if !m.lastPersistedAt.IsZero() && now.Sub(m.lastPersistedAt) < checkpointPersistInterval {
		return
	}
	m.lastPersistedAt = now
	m.persistCheckpoint()
	m.reportProgress()
}

// 只读 m.appliedGTIDSetString()/m.stats.lastEpoch/m.stats.lastBatchSeq 这几个
// 已经发布好的 atomic 快照，不重新调用 m.appliedSet.String()（那是
// publishAppliedSnapshot 已经做过一次的工作，见该函数文档"为什么需要节流"一段
// 提到的重复编码问题——两次序列化改成一次，是同一条修复的另一半）、也不直接读
// m.lastEpoch/m.lastBatchSeq 这两个非原子字段。这个选择顺带带来一个好处：
// persistCheckpoint 因此可以安全地从任何 goroutine 调用，不要求调用方一定是
// handler goroutine——Close() 正是这样一个例外调用方，见其文档。
//
// 前提：必须在 publishAppliedSnapshot 至少跑过一次之后调用（recordApplied 或
// loadCheckpoint 的恢复分支都会跑它），否则 appliedGTIDSetString() 返回空
// 字符串（New() 里的初始 appliedSet 恰好也是空集合，两者巧合地一致，但这是
// New() 的初始值，不是本函数的保证）。
//
// 触发时机：不新引入 xhive Timer/事件循环自投递管道，由 maybePersistCheckpoint
// 节流后调用，复用 binlogin persistSchemaHistory/reportDDLEvent 已经确立的
// "后台 goroutine 直接调外部模块 Put"先例（那两处同样是从会话 goroutine 而不是
// 事件循环里调用）。
func (m *Module) persistCheckpoint() {
	if m.opts.Store == nil {
		return
	}
	doc := map[string]any{
		"run_id":           m.opts.RunID,
		"applied_gtid_set": m.appliedGTIDSetString(),
		"epoch":            int64(m.stats.lastEpoch.Load()),
		"batch_seq":        int64(m.stats.lastBatchSeq.Load()),
		"updated_at_ms":    m.now().UnixMilli(),
	}
	if consumed := m.stats.consumedGTIDSet.Load(); consumed != nil {
		doc["consumed_gtid_set"] = *consumed
	}
	if rejected := m.stats.rejectedGTIDSet.Load(); rejected != nil {
		doc["rejected_gtid_set"] = *rejected
	}
	if err := m.opts.Store.Put(store.Op{
		Collection: checkpointCollection,
		ID:         checkpointID(m.opts.PipelineID),
		Doc:        doc,
	}); err != nil {
		slog.Warn("write target checkpoint failed", slog.String("pipeline_id", m.opts.PipelineID), slog.Any("error", err))
	}
}

// recordApplyResult keeps policy rollbacks out of the MySQL applied position.
func (m *Module) recordApplyResult(result iproto.BatchApplyResult) {
	for _, gtid := range result.AppliedGTIDs {
		if err := m.appliedSet.Update(gtid); err != nil {
			slog.Warn("merge applied transaction failed", slog.String("pipeline_id", m.opts.PipelineID), slog.Any("error", err))
		}
	}
	for _, gtid := range result.RejectedGTIDs {
		if err := m.rejectedSet.Update(gtid); err != nil {
			slog.Warn("merge rejected transaction failed", slog.String("pipeline_id", m.opts.PipelineID), slog.Any("error", err))
		}
	}
	m.publishAppliedSnapshot()
	rejected := m.rejectedSet.String()
	m.stats.rejectedGTIDSet.Store(&rejected)
}
