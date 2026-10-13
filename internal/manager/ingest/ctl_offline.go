package ingest

import (
	"context"
	"log/slog"
	"time"

	"github.com/xmapst/xdts/internal/manager/model"
)

// runOfflineLoop 每 OfflineCheckInterval 跑一次心跳超时判定（MGR-NODE-05），
// 直到 stopOffline 被关闭。用 time.Ticker 而不是 xhive.Timer——
// internal/manager 当前没有引入 xhive（只有 internal/node 侧使用，见包
// 文档 internal/manager/ingest/ingest.go），本包按 Manager 现有约定走最朴素的 goroutine + ticker，
// 与 cmd/manager/main.go 里 HUP 信号处理、signal.NotifyContext 那套朴素
// 并发风格一致。
func (s *Service) runOfflineLoop() {
	defer close(s.offlineDone)

	ticker := time.NewTicker(s.opts.OfflineCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopOffline:
			return
		case <-ticker.C:

			func() {
				ctx, cancel := context.WithTimeout(context.Background(), checkDBTimeout)
				defer cancel()
				s.checkOffline(ctx)
			}()
		}
	}
}

// checkOffline 调用既有的 MarkOfflineNodes（不自己写 SQL），并把
// MarkOfflineNodes 自己文档里点名的两件调用方职责接完："据此产生事件/
// 告警"（补一条 node_offline 事件）与"把关联 pipeline 的实际状态标记为
// 未知"（逐个调用 SetPipelineActualState 标记为未知，lastError 取值理由见
// markNodePipelinesUnknown 注释——不是传空字符串）——本包就是
// MarkOfflineNodes 文档说的那个"service 层编排"调用方。
func (s *Service) checkOffline(ctx context.Context) {
	s.stats.offlineChecks.Add(1)
	now := s.now()

	offline, err := s.opts.MarkOfflineNodes(ctx, s.opts.DB, now, s.opts.OfflineTimeout)
	if err != nil {
		slog.Error("offline detection failed", slog.Any("error", err))
		return
	}
	if len(offline) == 0 {
		return
	}
	s.stats.offlineMarked.Add(int64(len(offline)))

	rows := make([]model.Event, 0, len(offline))
	for _, n := range offline {
		rows = append(rows, model.Event{
			Ts:     now,
			NodeID: n.ID,
			Type:   model.EventTypeNodeOffline,
			Level:  model.EventLevelWarning,
		})
	}
	if err := s.opts.InsertEvents(ctx, s.opts.DB, rows); err != nil {
		// 节点已经被标记离线（MarkOfflineNodes 已经落库成功），这里只是补
		// 事件失败，不回滚离线标记——回滚需要反向写一次状态，收益不明确，
		// 且 node_offline 事件缺失时用户仍能从节点列表本身的 Offline 状态
		// 看到问题，不是唯一信息来源。如实报错，不静默，与
		// InsertLogs/InsertEvents"部分成功不回滚"的既定取舍保持一致的
		// 处理风格。
		slog.Error("node_offline event insert failed",
			slog.Int("offline_count", len(offline)), slog.Any("error", err))
	}

	for _, n := range offline {
		slog.Warn("node marked offline",
			slog.String("node_id", n.ID.String()),
			slog.String("node_name", n.Name),
			slog.Duration("offline_timeout", s.opts.OfflineTimeout))
		s.markNodePipelinesUnknown(ctx, n)
	}
}

// markNodePipelinesUnknown clears observations only for roles owned by the offline node.
// It preserves last errors and checks current assignment before updating.
func (s *Service) markNodePipelinesUnknown(ctx context.Context, n model.Node) {
	pipelines, err := s.opts.ListPipelinesByNode(ctx, s.opts.DB, n.ID)
	if err != nil {
		slog.Error("list node pipelines failed",
			slog.String("node_id", n.ID.String()), slog.Any("error", err))
		return
	}
	for _, p := range pipelines {
		if p.SourceNodeID == n.ID {
			if err := s.opts.MarkPipelineRoleUnknown(ctx, s.opts.DB, p.ID, model.SideSource); err != nil {
				slog.Error("mark pipeline role unknown failed",
					slog.String("node_id", n.ID.String()),
					slog.String("pipeline_id", p.ID.String()),
					slog.String("side", string(model.SideSource)),
					slog.Any("error", err))
			}
		}
		if p.TargetNodeID == n.ID {
			if err := s.opts.MarkPipelineRoleUnknown(ctx, s.opts.DB, p.ID, model.SideTarget); err != nil {
				slog.Error("mark pipeline role unknown failed",
					slog.String("node_id", n.ID.String()),
					slog.String("pipeline_id", p.ID.String()),
					slog.String("side", string(model.SideTarget)),
					slog.Any("error", err))
			}
		}
	}
}
