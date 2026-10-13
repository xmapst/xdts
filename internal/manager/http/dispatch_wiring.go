package http

import (
	"context"
	"log/slog"

	"github.com/xmapst/xdts/internal/manager/dispatch"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// noopPipelineDispatcher 是 Options.Dispatcher 未提供时的兜底实现——与
// internal/manager/http/audit.go 的 SlogAuditRecorder{} 同一种"骨架/测试期没有真实依赖时仍然
// 可用，但不完全沉默"的做法。真正的生产实现由 cmd/manager/main.go 在
// AMQP 接线装配步骤里构造 dispatch.New(...) 并通过 Options.Dispatcher
// 传入；本类型只应该出现在没有配置它的路径（多数是不关心 dispatch 行为
// 的测试，以及数据库未配置的场景）。
//
// 两个方法都返回 nil（不让调用方的写请求因为"没配置 dispatcher"这种
// 环境问题失败），但各记一条 Warn——完全静默会让"忘记在 cmd/manager/main.go 里接线
// dispatch"这类装配错误在生产环境里毫无痕迹，红线第 3 条"绝不静默写进
// 权威状态"反过来也要求"绝不静默跳过本该有的下游动作"。
type noopPipelineDispatcher struct{}

var _ dispatch.IPipelineDispatcher = noopPipelineDispatcher{}

func (noopPipelineDispatcher) SyncPipeline(ctx context.Context, pipelineID uuidx.UUID) error {
	slog.Warn("pipeline dispatcher not configured, config.changed sync skipped",
		slog.String("pipeline_id", pipelineID.String()))
	return nil
}

func (noopPipelineDispatcher) NotifyDeleted(ctx context.Context, pipelineID uuidx.UUID, configVersion uint64, sourceNodeID, targetNodeID uuidx.UUID) error {
	slog.Warn("pipeline dispatcher not configured, config.changed delete notice skipped",
		slog.String("pipeline_id", pipelineID.String()))
	return nil
}

// syncPipelineBestEffort 是 internal/manager/http/pipelines.go 与 internal/manager/http/centers.go 写 handler 调用
// dispatch.SyncPipeline 的统一入口：失败只记日志，不让整个 HTTP 请求
// 因此返回 500。
func syncPipelineBestEffort(ctx context.Context, d dispatch.IPipelineDispatcher, pipelineID uuidx.UUID) {
	if err := d.SyncPipeline(ctx, pipelineID); err != nil {
		slog.Error("pipeline dispatch sync failed",
			slog.String("pipeline_id", pipelineID.String()), slog.Any("error", err))
	}
}

// notifyPipelineDeletedBestEffort 是 internal/manager/http/pipelines.go 的 delete handler 调用
// dispatch.NotifyDeleted 的统一入口，失败处理策略与
// syncPipelineBestEffort 相同。
func notifyPipelineDeletedBestEffort(ctx context.Context, d dispatch.IPipelineDispatcher, pipelineID uuidx.UUID, configVersion uint64, sourceNodeID, targetNodeID uuidx.UUID) {
	if err := d.NotifyDeleted(ctx, pipelineID, configVersion, sourceNodeID, targetNodeID); err != nil {
		slog.Error("deleted pipeline dispatch notification failed",
			slog.String("pipeline_id", pipelineID.String()), slog.Any("error", err))
	}
}
