package binlogin

import "sync/atomic"

// statsCounters 是 binlog_in 的原子计数器，可从任意 goroutine 安全读取/写入
// （Stats() 的实现依据）——与 reporter/logship 的同名类型同一模式。之所以需要原子量
// 而不是事件循环私有字段：写入方是会话 goroutine（AsyncGo 的 f），读取方可能是任意
// 调用 Stats() 的外部 goroutine（如测试），两者都不在本模块事件循环上，不满足
// "只在事件循环里读写不需要加锁"的前提（"只有确实被多个 goroutine
// 访问的数据才加锁"，这正是那类数据）。
type statsCounters struct {
	txnsEmitted    atomic.Int64
	rowsEmitted    atomic.Int64
	ddlEmitted     atomic.Int64
	batchesSent    atomic.Int64
	watermarksSent atomic.Int64
	reconnects     atomic.Int64
	purgeDetected  atomic.Int64
	tablesFiltered atomic.Int64
	// rateLimitWaitNs 累计增量限流的等待时长（纳秒）。存成 int64 纳秒而不是
	// time.Duration：atomic 没有 Duration 特化，Stats() 转换一次即可。
	rateLimitWaitNs atomic.Int64
	telemetryErrors atomic.Uint64
}
