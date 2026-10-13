package ingest

import "sync/atomic"

// stats 是 Service 内部的计数器集合。C-22 要求"数据消息不得静默丢弃"，本包
// 对 report/log 两个队列采用"处理完（无论成功失败）都 Ack + 失败时结构化
// 错误日志"的策略（见 internal/manager/ingest/handler.go process 的详细说明），这里的计数器是那句
// "不得静默"里"能被看见、能被告警"的另一半——错误日志给排障，计数器给
// 监控面板/告警规则（MGR-MON-01）。
type stats struct {
	reportProcessed atomic.Int64
	reportFailed    atomic.Int64
	logProcessed    atomic.Int64
	logFailed       atomic.Int64
	offlineChecks   atomic.Int64
	offlineMarked   atomic.Int64

	// 这是量表（gauge）不是累加计数器：进 process() 加一，process() 返回
	// 减一，稳态下应当回到 0。本包唯一一个会减的计数器。
	//
	// 为什么必须有它：reportProcessed/logProcessed 的语义是"收到了几条"
	// （在 process() 开头就加一，失败路径同样计入，见下），reportFailed/
	// logFailed 是"没能落库几条"。于是一条卡住不返回的消息在这两个量上
	// 是 processed+1、failed+0 —— 与"处理成功了"逐位相同。而 internal/manager/ingest/ctl_stats.go
	// 的告警链读的正是这两个量的增量：数据源本身分不出卡死和成功。
	// inflight 是唯一能把它们分开的量：卡住 ⇒ 它不回 0。
	reportInflight atomic.Int64
	logInflight    atomic.Int64

	// 必须与 reportFailed/logFailed 分开，不能合并进去：那两个计数器的语义
	// 是"这条消息没能落库"（decode/verify/dispatch 三处，见 internal/manager/ingest/handler.go），
	// 数据丢了但库里没有错误数据。下面这两个说的是完全不同的事，而且互相之间
	// 也不能合并：
	//
	// *AckDuplicate   —— 同一个 AckHandle 被 ack 两次（transport.ErrAckHandleAlreadyResolved）。
	// 	                   这是编程错误，对数据无害：投递已经被确认过了。
	// 	*AckUnconfirmed —— ack 没有送达 broker（信道/连接断开，amqp091 的 ErrClosed）。
	// 	                   投递未被确认 ⇒ broker 恢复后会重投 ⇒ 已落库的那部分会被重复写入。
	//
	// 后者是本包唯一会产生重复数据的路径，前者对数据完全无害。混进一个计数器
	// 就再也分不开"某处代码 ack 了两次"（可以慢慢修）和"这批数据正在被重复
	// 写入"（要立刻处理）。
	reportAckDuplicate   atomic.Int64
	reportAckUnconfirmed atomic.Int64
	logAckDuplicate      atomic.Int64
	logAckUnconfirmed    atomic.Int64
}

// Stats 是消费与离线判定的统计快照。
type Stats struct {
	ReportProcessed int64
	ReportFailed    int64
	LogProcessed    int64
	LogFailed       int64
	OfflineChecks   int64
	OfflineMarked   int64

	// 见 stats 里同名字段的文档：这两个是量表，稳态回 0；不回 0 表示有投递
	// 卡在 process() 里没返回，而这种卡死在 *Processed/*Failed 上与成功同形。
	ReportInflight int64
	LogInflight    int64

	// 见 stats 里同名字段的文档：AckUnconfirmed 是本包唯一会产生重复数据的
	// 路径，AckDuplicate 对数据无害，两者不可合并读。
	ReportAckDuplicate   int64
	ReportAckUnconfirmed int64
	LogAckDuplicate      int64
	LogAckUnconfirmed    int64
}

// Stats 返回当前计数器快照，供健康检查/监控面板轮询（MGR-MON-01）。
func (s *Service) Stats() Stats {
	return Stats{
		ReportProcessed: s.stats.reportProcessed.Load(),
		ReportFailed:    s.stats.reportFailed.Load(),
		LogProcessed:    s.stats.logProcessed.Load(),
		LogFailed:       s.stats.logFailed.Load(),
		OfflineChecks:   s.stats.offlineChecks.Load(),
		OfflineMarked:   s.stats.offlineMarked.Load(),

		ReportInflight: s.stats.reportInflight.Load(),
		LogInflight:    s.stats.logInflight.Load(),

		ReportAckDuplicate:   s.stats.reportAckDuplicate.Load(),
		ReportAckUnconfirmed: s.stats.reportAckUnconfirmed.Load(),
		LogAckDuplicate:      s.stats.logAckDuplicate.Load(),
		LogAckUnconfirmed:    s.stats.logAckUnconfirmed.Load(),
	}
}
