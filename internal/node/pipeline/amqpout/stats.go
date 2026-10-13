package amqpout

import "sync/atomic"

// statsCounters 全部使用 atomic：Send* 方法运行在调用方的 goroutine 里（可能是
// binlogin 的会话 goroutine，也可能是全量读取阶段的多个并发 goroutine，见包文档
// NODE-MQO-04），不是本模块事件循环私有状态，满足"确实被多个
// goroutine 访问的数据才加锁"——这里用无锁的 atomic 而不是 mutex，与 binlogin.batchSeq
// 的场景和理由完全一致。
type statsCounters struct {
	batchesSent        atomic.Int64
	watermarksSent     atomic.Int64
	snapshotChunksSent atomic.Int64
	snapshotEndsSent   atomic.Int64
	retriedCalls       atomic.Int64
	failedCalls        atomic.Int64
	oversizeRejected   atomic.Int64
	backpressureEvents atomic.Int64
	confirmedBytes     atomic.Uint64
	publishErrors      atomic.Uint64
	inflightBatches    atomic.Uint64
	inflightBytes      atomic.Uint64
}
