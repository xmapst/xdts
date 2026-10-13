// Package reclaim 统一处理流水线和节点的资源回收。
// 各资源独立记录 pending/in_progress/done/failed，完成项在重试时跳过。
package reclaim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/dispatch"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/redact"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/transport"
)

// DefaultInterval 是回收循环未显式配置时的轮询间隔。资源回收不是热路径
// ，30 秒足够及时（界面的
// "待清理"列表不会长期显示明显滞后的状态），也不会对管理库造成有意义的
// 轮询压力。
const DefaultInterval = 30 * time.Second

var errQueueDeleterUnavailable = errors.New("reclaim: 队列删除能力未配置（AMQP 未接线）")

// IQueueDeleter 是回收循环需要的队列删除能力。定义成本包自己的接口，而不是
// 直接依赖 internal/transport.Declarator 的具体类型：
//  1. 回收循环的核心逻辑（扫描、按资源幂等跳过、退避重试、写回
//     cleanup_detail）不关心底层怎么删队列，用一个假实现就能把这些分支
//     测全，不需要单测也连一个真实 AMQP 实例；
//  2. *transport.Declarator 已经有签名完全匹配的 DeleteDataQueue/
//     DeleteControlQueue 方法（Go 的结构化接口，不需要 transport 那边为
//     了适配这里新增任何代码）。
type IQueueDeleter interface {
	// DeleteDataQueue 幂等删除一个数据队列（pipeline 的增量/全量分块
	// 队列）；队列非空时应返回错误而不是丢弃未消费的消息
	// （transport.Declarator.DeleteDataQueue 就是这个语义）。
	DeleteDataQueue(name string) error
	// DeleteControlQueue 幂等删除一个节点控制队列；控制指令可丢弃，不检查
	// 是否为空。
	DeleteControlQueue(name string) error
}

// Options 配置回收循环。
type Options struct {
	DB *gorm.DB
	// Queues 为 nil 时队列回收失败并重试；复制 ID 保留到两侧卸载和队列回收
	// 都已确认。合成 UUID 登记永久保留，只记录不再产生新 GTID 的完成标记。
	Queues   IQueueDeleter
	Interval time.Duration
}

// Loop 是节点与 pipeline 两步删除共用的资源回收后台任务。
type Loop struct {
	db       *gorm.DB
	interval time.Duration

	// queuesMu 保护 queues：cmd/manager/main.go 在 AMQP 连接建立后台重试成功时会调用
	// SetQueues 热插入队列删除能力（见 SetQueues 注释），这与 tick 所在的
	// 常驻 goroutine 并发访问同一个字段，需要加锁（「只有
	// 确实被多个 goroutine 访问的数据才加锁」——这里确实是）。
	queuesMu   sync.RWMutex
	queues     IQueueDeleter
	dispatcher dispatch.IPipelineDispatcher
	wake       chan struct{}
}

func New(opts Options) *Loop {
	interval := opts.Interval
	if interval <= 0 {
		interval = DefaultInterval
	}
	return &Loop{db: opts.DB, queues: opts.Queues, interval: interval, wake: make(chan struct{}, 1)}
}

func (l *Loop) SetDispatcher(d dispatch.IPipelineDispatcher) {
	l.queuesMu.Lock()
	l.dispatcher = d
	l.queuesMu.Unlock()
}

// Wake schedules work on the sole cleanup execution loop; HTTP never runs a
// concurrent reclaim scan or shares mutable progress with the background loop.
func (l *Loop) Wake() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// SetQueues 在回收循环已经在运行之后热插入（或替换）队列删除能力——典型
// 场景是 cmd/manager/main.go 里 AMQP 连接一开始未接通、循环先以 nil Queues 启动
// 持续重试，等后台的连接重试 goroutine 真正连上 AMQP 之后再调用它，循环
// 不需要重启、下一次 tick 就会用上新的实现。
func (l *Loop) SetQueues(q IQueueDeleter) {
	l.queuesMu.Lock()
	l.queues = q
	l.queuesMu.Unlock()
}

func (l *Loop) queuesSnapshot() IQueueDeleter {
	l.queuesMu.RLock()
	defer l.queuesMu.RUnlock()
	return l.queues
}

// Run 阻塞运行回收循环直到 ctx 被取消；调用方应该在自己的 goroutine 里
// 启动它。启动时先跑一轮（不等第一个 interval），避免刚启动时"待清理"
// 列表里已经有记录却要空等一个轮询周期才开始处理。
func (l *Loop) Run(ctx context.Context) {
	l.tick(ctx)
	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.tick(ctx)
		case <-l.wake:
			l.tick(ctx)
		}
	}
}

func (l *Loop) tick(ctx context.Context) {
	now := time.Now()
	if err := l.reclaimPipelines(ctx, now); err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "pipeline cleanup batch query failed", slog.Any("error", err))
	}
	if err := l.reclaimNodes(ctx, now); err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "node cleanup batch query failed", slog.Any("error", err))
	}
}

// RunOnce 同步执行一轮回收扫描（pipeline + node 各一遍），把两边的错误用
// errors.Join 合并后返回，而不是像 tick 那样只记日志。给测试（不必启动
// 常驻 goroutine、也不必靠 ticker 时序等待第一轮跑完）和一次性运维触发
// 使用；Run 内部的常驻循环继续用 tick，行为不变。
func (l *Loop) RunOnce(ctx context.Context) error {
	now := time.Now()
	pErr := l.reclaimPipelines(ctx, now)
	nErr := l.reclaimNodes(ctx, now)
	return errors.Join(pErr, nErr)
}

// tryResource 跳过已经完成的资源，其余资源执行 action 并记录成功或失败。
// 幂等依据是各资源的状态，不能只依赖聚合状态。
func tryResource(res *model.CleanupResourceDetail, action func() error) {
	if res.State == model.CleanupResourceStateDone {
		return
	}
	if err := action(); err != nil {
		res.State = model.CleanupResourceStateFailed
		res.LastError = redact.PotentialCredentials(err.Error())
		return
	}
	res.State = model.CleanupResourceStateDone
	res.LastError = ""
}

// Cleanup failures remain persisted and observable while retries are pending.
func overallState(resources ...model.CleanupResourceDetail) (model.CleanupState, string) {
	allDone := true
	lastError := ""
	for _, r := range resources {
		if r.State != model.CleanupResourceStateDone {
			allDone = false
		}
		if r.State == model.CleanupResourceStateFailed && r.LastError != "" {
			lastError = r.LastError
		}
	}
	if allDone {
		return model.CleanupStateDone, ""
	}
	if lastError != "" {
		return model.CleanupStateFailed, lastError
	}
	return model.CleanupStateInProgress, ""
}

// ---- pipeline ----

func (l *Loop) reclaimPipelines(ctx context.Context, now time.Time) error {
	pipelines, err := repo.ListPendingCleanupPipelines(ctx, l.db)
	if err != nil {
		return fmt.Errorf("reclaim: 查询待清理 pipeline 列表失败: %w", err)
	}
	for _, p := range pipelines {
		l.reclaimPipeline(ctx, p, now)
	}
	return nil
}

func decodePipelineCleanupDetail(raw model.JSON) model.PipelineCleanupDetail {
	var detail model.PipelineCleanupDetail
	if len(raw) == 0 {
		return detail
	}
	if err := json.Unmarshal(raw, &detail); err != nil {
		// 解析失败按「从未记录过任何进度」处理，重新尝试全部资源——这比
		// 直接放弃这一轮回收更安全：所有回收动作本身都是幂等的（队列删除
		// 幂等、binlog_server_id 释放只在整体 done 时原子发生一次），
		// 重新尝试不会造成重复副作用。
		return model.PipelineCleanupDetail{}
	}
	return detail
}

// deletePipelineQueues 删除一条 pipeline 的全部数据面队列：增量数据队列
// 与全量分块队列（PipelineCleanupDetail.Queue 是一个字段，但对应两个物理
// 队列，见 的队列清单）——两个都成功才算这一项资源完成。两个
// 删除动作都要尝试、不能因为前一个失败就跳过后一个：否则一次数据队列的
// 瞬时失败会连累明明可以删掉的全量分块队列也一直不去尝试，白白拖慢下一轮
// 重试时本该只剩一个失败项的进度（两个删除都是幂等的，都尝试不会有副
// 作用）。
func (l *Loop) deletePipelineQueues(pipelineID string) error {
	queues := l.queuesSnapshot()
	if queues == nil {
		return errQueueDeleterUnavailable
	}
	dataErr := queues.DeleteDataQueue(transport.PipelineDataQueue(pipelineID))
	snapErr := queues.DeleteDataQueue(transport.PipelineSnapshotQueue(pipelineID))
	return errors.Join(dataErr, snapErr)
}

func (l *Loop) reclaimPipeline(ctx context.Context, p model.Pipeline, now time.Time) {
	detail := decodePipelineCleanupDetail(p.CleanupDetail)
	if p.SourceCleanupConfirmedAt == nil || p.TargetCleanupConfirmedAt == nil {
		l.queuesMu.RLock()
		d := l.dispatcher
		l.queuesMu.RUnlock()
		if d != nil {
			if err := d.NotifyDeleted(ctx, p.ID, p.ConfigVersion, p.SourceNodeID, p.TargetNodeID); err != nil {
				slog.Warn("pipeline cleanup notification failed", slog.String("pipeline_id", p.ID.String()), slog.Any("error", err))
			}
		}
		message := "等待源端和目标端确认停止"
		if p.SourceCleanupError != "" {
			message = redact.PotentialCredentials(p.SourceCleanupError)
		}
		if p.TargetCleanupError != "" {
			message = redact.PotentialCredentials(p.TargetCleanupError)
		}
		state := model.CleanupStateInProgress
		if p.SourceCleanupError != "" || p.TargetCleanupError != "" {
			state = model.CleanupStateFailed
		}
		if err := repo.RecordPipelineCleanupAttempt(ctx, l.db, p.ID, now, p.CleanupDetail, state, message); err != nil {
			slog.Error("pipeline cleanup progress record failed", slog.String("pipeline_id", p.ID.String()), slog.Any("error", err))
		}
		return
	}

	tryResource(&detail.Queue, func() error {
		return l.deletePipelineQueues(p.ID.String())
	})
	// ServerID：binlog_server_id 的释放已经由
	// repo.RecordPipelineCleanupAttempt 在 newState=Done 那一刻原子完成
	// （唯一索引在 done 之前不释放的约束由那一层保证，不是这里），这里
	// 只是把它标记为完成、没有独立的物理动作。
	if detail.Queue.State == model.CleanupResourceStateDone {
		tryResource(&detail.ServerID, func() error { return nil })
	}

	// Synthetic GTIDs remain in target history after pipeline deletion. Retain their
	// origin identity so later pipelines cannot mistake replayed synthetic data for
	// a new business source.
	tryResource(&detail.SnapshotUUID, func() error { return nil })
	tryResource(&detail.SchemaUUID, func() error { return nil })

	newState, lastError := overallState(detail.Queue, detail.ServerID, detail.SnapshotUUID, detail.SchemaUUID)

	encoded, err := json.Marshal(detail)
	if err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "pipeline cleanup detail encode failed",
			slog.String("pipeline_id", p.ID.String()), slog.Any("error", err))
		return
	}
	if err := repo.RecordPipelineCleanupAttempt(ctx, l.db, p.ID, now, model.JSON(encoded), newState, lastError); err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "pipeline cleanup progress record failed",
			slog.String("pipeline_id", p.ID.String()), slog.Any("error", err))
	}
}

// ---- node ----

func (l *Loop) reclaimNodes(ctx context.Context, now time.Time) error {
	nodes, err := repo.ListPendingCleanupNodes(ctx, l.db)
	if err != nil {
		return fmt.Errorf("reclaim: 查询待清理节点列表失败: %w", err)
	}
	for _, n := range nodes {
		l.reclaimNode(ctx, n, now)
	}
	return nil
}

func decodeNodeCleanupDetail(raw model.JSON) model.NodeCleanupDetail {
	var detail model.NodeCleanupDetail
	if len(raw) == 0 {
		return detail
	}
	if err := json.Unmarshal(raw, &detail); err != nil {
		return model.NodeCleanupDetail{}
	}
	return detail
}

func (l *Loop) reclaimNode(ctx context.Context, n model.Node, now time.Time) {
	detail := decodeNodeCleanupDetail(n.CleanupDetail)

	tryResource(&detail.ControlQueue, func() error {
		queues := l.queuesSnapshot()
		if queues == nil {
			return errQueueDeleterUnavailable
		}
		return queues.DeleteControlQueue(transport.NodeControlQueue(n.ID.String()))
	})

	newState, lastError := overallState(detail.ControlQueue)

	encoded, err := json.Marshal(detail)
	if err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "node cleanup detail encode failed",
			slog.String("node_id", n.ID.String()), slog.Any("error", err))
		return
	}
	if err := repo.RecordNodeCleanupAttempt(ctx, l.db, n.ID, now, model.JSON(encoded), newState, lastError); err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "node cleanup progress record failed",
			slog.String("node_id", n.ID.String()), slog.Any("error", err))
	}
}
