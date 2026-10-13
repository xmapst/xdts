package amqpin

import "sync/atomic"

// statsCounters 全部使用 atomic：handle 运行在 xamqp 自己的后台 goroutine 里，
// 不是本模块事件循环私有状态，Stats() 又可能被任意其它 goroutine 调用，天然
// 满足"确实被多个 goroutine 访问的数据才加锁"——与
// amqpout.statsCounters 同一模式。
type statsCounters struct {
	batchesApplied    atomic.Int64
	permanentFailures atomic.Int64
	applyFailures     atomic.Int64
	// ackFailures 统计 Ack 调用本身失败的次数（不影响正确性）：既覆盖
	// applyAndFinish 里"ApplyBatch 成功后 Ack 失败"的既有场景，也覆盖
	// ackDuplicate 里"NODE-MQI-02 判定为重复、Ack 失败"的场景（internal/node/pipeline/amqpin/handle.go）——
	// 两种场景失败原因同一类（基础设施瞬时问题），复用同一个计数器，不为后者
	// 单独开一个。
	ackFailures       atomic.Int64
	watermarksAcked   atomic.Int64
	probesAcked       atomic.Int64
	chunksReassembled atomic.Int64
	// duplicatesAcked 统计 NODE-MQI-02 连续性检查判定为"重复"、直接 Ack 掉的
	// 消息数（internal/node/pipeline/amqpin/continuity.go），与 watermarksAcked/probesAcked 同一模式："确认
	// 了但没有走完整应用流程"这类事件需要单独可观测，不能被悄悄并进
	// batchesApplied。
	duplicatesAcked atomic.Int64
	// Messages explicitly superseded by a confirmed newer initialization run.
	supersededAcked atomic.Int64

	appliedGTIDSet  atomic.Pointer[string]
	consumedGTIDSet atomic.Pointer[string]
	rejectedGTIDSet atomic.Pointer[string]
	lastEpoch       atomic.Uint64
	lastBatchSeq    atomic.Uint64
	receivedBytes   atomic.Uint64
	conflicts       atomic.Uint64
}
