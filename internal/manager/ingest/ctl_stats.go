package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/xmapst/xdts/internal/manager/model"
)

// 1. 完全在本包目录内可以做完，不需要跨包接线、不被排期挡住；
//
//  2. events 表不分区，所以在
//     logs/pipeline_metrics 因分区耗尽、InsertLogs/InsertPipelineMetrics
//     开始报 MySQL 1526 的那一刻，events 表仍然写得进去。选一个会跟着
//     失败通道一起坏掉的告警手段等于没告警，这正是选 events 而不是选别的
//     的实质理由。
//
// 用一个 ticker 同时驱动两件事（失败增量检测 + 周期汇总心跳），不是两个
// 独立 goroutine：两者本质上是同一个"巡检"关注点的两种输出频率，没必要为
// 这点差异多开一个 goroutine 和一对 stop/done channel。
func (s *Service) runStatsLoop() {
	defer close(s.statsDone)

	ticker := time.NewTicker(s.opts.StatsCheckInterval)
	defer ticker.Stop()

	lastSummary := s.now()

	for {
		select {
		case <-s.stopStats:
			return
		case <-ticker.C:
			now := s.now()

			func() {
				ctx, cancel := context.WithTimeout(context.Background(), checkDBTimeout)
				defer cancel()
				s.checkStatsFailures(ctx, now)
			}()
			if now.Sub(lastSummary) >= s.opts.StatsSummaryInterval {
				s.logStatsSummary()
				lastSummary = now
			}
		}
	}
}

// `*AckDuplicate` 刻意不在这里发事件，只进 logStatsSummary（见下方说明）。
func (s *Service) checkStatsFailures(ctx context.Context, now time.Time) {
	curReportFailed := s.stats.reportFailed.Load()
	curLogFailed := s.stats.logFailed.Load()

	deltaReport := curReportFailed - s.lastReportFailed
	deltaLog := curLogFailed - s.lastLogFailed
	s.lastReportFailed = curReportFailed
	s.lastLogFailed = curLogFailed

	if deltaReport > 0 {
		s.emitStatsFailureEvent(ctx, now, "report", deltaReport, curReportFailed)
	}
	if deltaLog > 0 {
		s.emitStatsFailureEvent(ctx, now, "log", deltaLog, curLogFailed)
	}

	// 为什么它比 report_failed/log_failed 更值得告警，而不是更不值得：
	// 那两个说的是"这条消息没落库"——数据丢了，但库里没有错误数据。
	// 这一个说的是"投递没被确认，broker 会重投"，而 repo 层不去重
	// ⇒ 库里即将出现重复数据。丢数据可以靠节点重发补，重复数据不能靠
	// 任何自动手段消掉。
	//
	// 用与上面完全相同的增量机制（不是累计值），理由见本函数顶部注释。
	// `*AckDuplicate` 刻意不在这里发事件：它是编程错误、对数据无害，
	// 进 logStatsSummary 的周期汇总就够。
	curReportAckUnconfirmed := s.stats.reportAckUnconfirmed.Load()
	curLogAckUnconfirmed := s.stats.logAckUnconfirmed.Load()
	deltaReportAck := curReportAckUnconfirmed - s.lastReportAckUnconfirmed
	deltaLogAck := curLogAckUnconfirmed - s.lastLogAckUnconfirmed
	s.lastReportAckUnconfirmed = curReportAckUnconfirmed
	s.lastLogAckUnconfirmed = curLogAckUnconfirmed

	if deltaReportAck > 0 {
		s.emitStatsFailureEvent(ctx, now, "report_ack_unconfirmed", deltaReportAck, curReportAckUnconfirmed)
	}
	if deltaLogAck > 0 {
		s.emitStatsFailureEvent(ctx, now, "log_ack_unconfirmed", deltaLogAck, curLogAckUnconfirmed)
	}
}

// statsFailureEventPayload 是 emitStatsFailureEvent 写入 Event.Payload 的
// 结构：具体是哪一类失败（report/log）、本次巡检周期的增量、以及供人工核对
// 用的累计值。
type statsFailureEventPayload struct {
	Category   string `json:"category"`
	Delta      int64  `json:"delta"`
	Cumulative int64  `json:"cumulative"`
}

// emitStatsFailureEvent 写一条 error 级别的事件。
//
// 防自噬：如果 InsertEvents 本身失败，不再为"发事件失败"这件事本身发另一条
// 事件——那会形成"事件写不进去 → 发一条报告事件写不进去的事件 → 那条也写
// 不进去 → ……"的递归。这种情况下只记结构化错误日志，退回本文件的周期汇总
// 日志兜底（logStatsSummary 不依赖 events 表可写）。
func (s *Service) emitStatsFailureEvent(ctx context.Context, now time.Time, category string, delta, cumulative int64) {
	payload, err := json.Marshal(statsFailureEventPayload{
		Category:   category,
		Delta:      delta,
		Cumulative: cumulative,
	})
	if err != nil {
		slog.Error("marshal alert event payload failed",
			slog.String("category", category), slog.Any("error", err))
		return
	}

	row := model.Event{
		Ts:      now,
		Type:    model.EventTypeError,
		Level:   model.EventLevelError,
		Payload: model.JSON(payload),
	}
	if err := s.opts.InsertEvents(ctx, s.opts.DB, []model.Event{row}); err != nil {
		// 防自噬（见函数注释）：这里只记日志，不再发事件报告"发事件失败"。
		slog.Error("insert alert event failed",
			slog.String("category", category), slog.Int64("delta", delta), slog.Any("error", err))
		return
	}
	slog.Warn("consume failure counter increased",
		slog.String("category", category), slog.Int64("delta", delta), slog.Int64("cumulative", cumulative))
}

// logStatsSummary 打一条周期性汇总日志（心跳），无论计数器是否变化都打。
// 没有它，ingest 完全没收到任何消息、和它收到并成功处理了一万条，在日志里
// 是同一个样子（都没有输出）——这条日志同时是 events 也写不进去时的最后
// 兜底出口。
func (s *Service) logStatsSummary() {
	st := s.Stats()
	slog.Info("consume status summary",
		slog.Int64("report_processed", st.ReportProcessed),
		slog.Int64("report_failed", st.ReportFailed),
		slog.Int64("log_processed", st.LogProcessed),
		slog.Int64("log_failed", st.LogFailed),
		slog.Int64("offline_checks", st.OfflineChecks),
		slog.Int64("offline_marked", st.OfflineMarked),
		slog.Int64("report_inflight", st.ReportInflight),
		slog.Int64("log_inflight", st.LogInflight),
		slog.Int64("report_ack_duplicate", st.ReportAckDuplicate),
		slog.Int64("report_ack_unconfirmed", st.ReportAckUnconfirmed),
		slog.Int64("log_ack_duplicate", st.LogAckDuplicate),
		slog.Int64("log_ack_unconfirmed", st.LogAckUnconfirmed),
	)
}
