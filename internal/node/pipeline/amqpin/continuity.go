package amqpin

import (
	"fmt"
	"log/slog"
)

// 本文件的"重复批次直接丢弃"是 Ack（确认消费）：消息内容已经在更早一次投递
// 里成功处理过，这次投递不含任何新信息，确认掉即可，没有数据丢失。
//
// 后来人看到"丢弃"两个字时，先确认是本文件说的 Ack 分支，还是 NODE-MQI-04 禁止
// 的 NackDiscard 分支——中文用词一样，方向相反。
//
// 具体会出问题的路径：amqp_in 持着若干未 Ack 的 epoch N 消息崩溃 → binlogin
// 重启为 epoch N+1 → amqp_in 重启，continuityBaselineSet=false，它看到的第一条
// 是 epoch N+1，基线定为 N+1 → 被 broker 重投的那条 epoch N 消息随后到达 →
// 如果判"重复"直接 Ack 丢弃，这条消息的数据回不来了：它当初 published 成功，
// binlogin 的 published 位点已经越过它，下一次会话只从 published 位点恢复
// （不是从 read），epoch N+1 不会重发它；它自己也从未被 apply 过——这正是
// C-22 禁止的那种静默丢弃。这条路径在真实 RabbitMQ 的 requeue 顺序下是否可达
// 没有定论（requeue 通常回到队头，epoch N 消息可能先于 epoch N+1 到达、直接
// 当基线，分支根本不触发），但可达性只决定这是不是紧急，不决定该不该处理——
// 一个没有论证支撑的丢弃分支，在 C-22 这条线上本身就是问题。
type continuityVerdict int

const (
	// continuityNormal：建立基线 / epoch 前进 / 同一 epoch 内连续，均正常往下
	// 处理，checkContinuity 内部已经用 recordSeen 记录了新的 lastEpoch/
	// lastBatchSeq。
	continuityNormal continuityVerdict = iota
	// continuityDuplicate：同一 epoch 内 batch_seq 已经见过（或更旧），已经被
	// recordSeen 记录过、可证内容重复，直接 Ack，不进入分片/应用流程。**不包括**
	// epoch < lastEpoch 的情形——那是 continuityStale，见本文件顶部"为什么
	// epoch 后退不能判重复"。
	continuityDuplicate
	// continuityGap：同一 epoch 内 batch_seq 跳号，freeze。
	continuityGap
	// continuityStale：epoch < lastEpoch，消息属于一个已经被更新 epoch 取代的
	// 旧一轮，但这条具体消息从未被 recordSeen 记录过、不可证"内容重复"——照常
	// 往下处理（与 continuityNormal 在 internal/node/pipeline/amqpin/handle.go 里是同一条分派路径，不提前
	// return），交给 ApplyBatch + 目标库 gtid_executed 去重，只是不推进
	// lastEpoch/lastBatchSeq。
	continuityStale
)

// 调用时机：必须在 internal/node/pipeline/amqpin/handle.go handleDataBatch 分派到分片/非分片处理之前调用（见
// internal/node/pipeline/amqpin/handle.go 的调用点注释）。放在分派之后会有一个隐患：重组进行中收到一条已经
// 处理过的分片重投，它的 Chunk.Index 必然小于当前重组期待的下一个 index，会被
// handleChunk 既有的"分片顺序异常"检测误判成结构性异常而 freeze，而不是被本函数
// 正确识别成"重复"直接 Ack——放在分派之前，重复分片在进入 handleChunk 之前就被
// 拦截掉，两套逻辑不会互相踩。
func (m *Module) checkContinuity(epoch, batchSeq uint64) (continuityVerdict, string) {
	if !m.continuityBaselineSet {
		m.continuityBaselineSet = true
		m.recordSeen(epoch, batchSeq)
		return continuityNormal, ""
	}

	if epoch > m.lastEpoch {
		// 源端重启，epoch 前进：新一轮的起点，无条件接受为新基线（见本文件
		// 顶部"为什么 epoch 前进不算跳号"）。
		m.recordSeen(epoch, batchSeq)
		return continuityNormal, ""
	}
	if epoch < m.lastEpoch {
		// 不判"重复"（见本文件顶部"为什么 epoch 后退不能判重复"）：这条具体
		// 消息从未被 recordSeen 记录过，"已经处理过"这句论证在这里不成立。按
		// ADR 照常往下处理，交给目标库 gtid_executed 去重，只是不推进
		// lastEpoch/lastBatchSeq（这条信息比已知的更旧）。
		slog.Warn("received stale epoch message",
			slog.String("pipeline_id", m.opts.PipelineID),
			slog.Uint64("epoch", epoch), slog.Uint64("last_epoch", m.lastEpoch), slog.Uint64("batch_seq", batchSeq))
		return continuityStale, ""
	}

	// epoch == m.lastEpoch：同一 epoch 内比较 batch_seq。
	switch {
	case batchSeq == m.lastBatchSeq+1:
		m.recordSeen(epoch, batchSeq)
		return continuityNormal, ""
	case batchSeq <= m.lastBatchSeq:
		return continuityDuplicate, ""
	default:
		reason := fmt.Sprintf("批次跳号：epoch=%d 上次记录 batch_seq=%d，本次收到 batch_seq=%d（跳过 %d 条）",
			epoch, m.lastBatchSeq, batchSeq, batchSeq-m.lastBatchSeq-1)
		return continuityGap, reason
	}
}
