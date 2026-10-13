package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/bootstrap"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// pullConfig 通过 RPC 拉取配置。pipelineIDs 为空时拉取本节点完整集合，
// 非空时按指定流水线集合拉取。
func (m *Module) pullConfig(ctx context.Context, pipelineIDs []string) (*iproto.ConfigPullReply, error) {
	req := &iproto.ConfigPullRequest{
		NodeId:      m.opts.NodeID,
		PipelineIds: pipelineIDs,
	}
	body, encoding, err := iproto.Encode(req, iproto.EncodeOptions{Compress: false})
	if err != nil {
		return nil, fmt.Errorf("controller: 编码 config.pull 请求失败: %w", err)
	}
	env := iproto.NewEnvelope(body, iproto.NewEnvelopeParams{
		MessageID:       uuidx.New().String(),
		Type:            iproto.MessageTypeConfigPull,
		TimestampMS:     m.now().UnixMilli(),
		ProtocolVersion: m.opts.ProtocolVersion,
		Center:          m.opts.Center,
		NodeID:          m.opts.NodeID,
		ContentEncoding: encoding,
	})

	msg, err := m.rpcClient.Call(ctx, transport.ExchangeRPC, transport.RoutingKeyRPC(), env,
		transport.WithEnvelopeDecorator(func(e *iproto.Envelope) error {
			sig := iproto.Sign(m.signKey, e.SignParamsFor())
			e.WithSignature(sig)
			return nil
		}))
	if err != nil {
		return nil, fmt.Errorf("controller: config.pull 调用失败: %w", err)
	}

	var reply iproto.ConfigPullReply
	if _, err := transport.ParseAndDecode(msg, &reply, nil); err != nil {
		return nil, fmt.Errorf("controller: 解析 config.pull.reply 失败: %w", err)
	}
	if reply.GetError() != nil {
		return nil, fmt.Errorf("controller: config.pull 被 Manager 拒绝: %s: %s",
			reply.GetError().GetCode(), reply.GetError().GetMessage())
	}
	return &reply, nil
}

// onStartupTimer 是 OnInit 里注册的零延迟一次性 Timer 的处理函数：把依赖网络 RPC 与
// Store 读取的启动期工作挪到事件循环已经在跑之后才执行（完整理由见 internal/node/controller/controller.go
// OnInit 的注释）。
func (m *Module) onStartupTimer(_ int64, _ map[string]string) {
	m.resolveStartupConfig()
}

// resolveStartupConfig 是 NODE-CFG-01"启动...全量拉取"与 NODE-CFG-04"Manager 不可达
// 时按本地配置恢复运行"的共同入口：优先尝试全量拉取，成功则以 Manager 的回复为
// 权威；RPCClient 重试耗尽判定 Manager 不可达后，退回本地持久化快照，按空
// m.applied 与快照内容做 diff（全部落在 changeLoad 分支）逐条恢复。
//
// 网络调用（pullConfig）与 Store 读取（loadAppliedState）都是阻塞 IO，通过单次
// AsyncGo 顺序执行。
func (m *Module) resolveStartupConfig() {
	var reply *iproto.ConfigPullReply
	var pullErr error
	var fallback map[appliedKey]appliedPipeline
	var priorApplied map[appliedKey]appliedPipeline
	var priorDedup map[cmdDedupSlot]cmdPipelineKey
	var priorDeleted map[appliedKey]uint64
	var deletionReadErr error
	var identityErr error

	m.AsyncGo("controller.startup", func() {
		ctx, cancel := context.WithTimeout(context.Background(), defaultStartupPullTimeout)
		defer cancel()
		helloCtx, cancelHello := context.WithTimeout(ctx, 6*time.Second)
		identityErr = m.confirmIdentity(helloCtx)
		cancelHello()
		if errors.Is(identityErr, bootstrap.ErrIdentityRetired) {
			return
		}
		reply, pullErr = m.pullConfig(ctx, nil)
		priorApplied = loadAppliedState(context.Background(), m.opts.Store)
		// An unreachable Manager can consume the entire network pull budget.
		// Local deletion authority must still be read before fallback assembly.
		localCtx, cancelLocal := context.WithTimeout(context.Background(), DefaultPublishTimeout)
		priorDeleted, deletionReadErr = restorePipelineDeletions(localCtx, m.opts.Store, priorApplied)
		cancelLocal()
		if deletionReadErr == nil {
			for key := range priorDeleted {
				delete(priorApplied, key)
			}
		}

		priorDedup = loadCmdDedup(context.Background(), m.opts.Store)
		if pullErr != nil {
			fallback = priorApplied
		}
	}, func() {
		if errors.Is(identityErr, bootstrap.ErrIdentityRetired) {
			m.markMirrorReady()
			m.beginIdentityRetirement()
			return
		}
		if m.identityRetired {
			return
		}
		m.priorRunApplied = priorApplied
		if deletionReadErr != nil {
			slog.Error("pipeline deletion state read failed", slog.Any("error", deletionReadErr))
			return
		}
		m.deletedRoleVersions = priorDeleted

		m.cmdPipelineResolved = priorDedup
		m.applyStartupResult(reply, pullErr, fallback)
	})
}

// applyStartupResult 回到事件循环执行，按 resolveStartupConfig 的结果触发装配。
func (m *Module) applyStartupResult(reply *iproto.ConfigPullReply, pullErr error, fallback map[appliedKey]appliedPipeline) {
	// 用 defer 而不是在三个 return 分支各写一行：三条分支都是"解析出来的结果怎么用"，
	// 而"解析已完成"是同一件事，defer 让将来加第四个分支也不需要记得补这一行。
	defer m.markMirrorReady()

	if pullErr == nil {
		slog.Info("startup full pull succeeded", slog.Int("pipeline_count", len(reply.GetConfigs())))
		m.restoreStartupPlans(diffConfigs(m.applied, reply.GetConfigs(), true))
		return
	}

	slog.Warn("startup configuration pull failed, restoring local applied state",
		slog.Any("error", pullErr), slog.Int("local_pipeline_count", len(fallback)))
	if len(fallback) == 0 {
		slog.Warn("local applied state unavailable, awaiting manager configuration")
		return
	}
	items := make([]*iproto.PipelineConfigItem, 0, len(fallback))
	for _, ap := range fallback {
		items = append(items, ap.PipelineConfigItem)
	}
	m.restoreStartupPlans(diffConfigs(m.applied, items, true))
}

// changeStop（internal/node/controller/diff.go 文档）专指"曾经装配、现在要卸载但保留记录"——本函数处理的
// 恰恰是相反的起点：进程刚启动，m.applied 是空 map，这些 pipeline 在本进程生命
// 周期里从未被装配过，没有旧实例可"卸载"。这里的场景是纯粹的本地记账、不涉及任何
// 需要独立 goroutine 承担的阻塞 IO（persistAppliedState 的 Put 是非阻塞的批量提交，
// finishApply/finishUnload/finishStop 都已经在事件循环里直接调用，见
// internal/node/controller/applied_state.go persistAppliedState 文档），套用 changeStop 的 AsyncGo 外壳纯属
// 多余。
//
// 不发布 ConfigApplyResult：这些 pipeline 的这次"恢复"不是响应 Manager 的任何请求
// ——它们的配置内容要么是这次全量拉取回复本身的一部分，要么来自本地快照，Manager
// 无法、也不需要靠一条回执来确认"节点收到了自己已经知道的东西"。
func (m *Module) restoreStartupPlans(plans []pipelinePlan) {
	plans = rejectStaleStartupPlans(plans, m.priorRunApplied)
	var normal []pipelinePlan
	var rejected []pipelinePlan
	stopped := make(map[appliedKey]appliedPipeline)
	for _, p := range plans {
		if p.kind == changeReject {
			rejected = append(rejected, p)
			continue
		}

		key := keyOf(p.item)
		if p.kind == changeUnload {
			normal = append(normal, p)
			continue
		}
		if prior, existed := m.priorRunApplied[key]; existed && !prior.running {

			if p.item.GetCommon().GetReinitIdentity() != nil &&
				(prior.GetCommon().GetRunId() != p.item.GetCommon().GetRunId() ||
					!sameReinitIdentity(prior.GetCommon().GetReinitIdentity(), p.item.GetCommon().GetReinitIdentity())) {
				stopped[key] = prior
				continue
			}

			stopped[key] = appliedPipeline{PipelineConfigItem: p.item, running: false, paused: prior.paused}
			continue
		}
		normal = append(normal, p)
	}

	if len(stopped) > 0 {
		for key, ap := range stopped {
			m.applied[key] = ap
			// Re-report the restored stopped or paused intent so Manager observes this process's state.
			target := iproto.PipelineState_PIPELINE_STATE_STOPPED
			if ap.paused {
				target = iproto.PipelineState_PIPELINE_STATE_PAUSED
			}
			m.reportPipelineStatus(ap.PipelineConfigItem, target, "")
		}
		if err := persistAppliedState(m.opts.Store, m.applied, m.now().UnixMilli()); err != nil {
			slog.Warn("persist applied state failed while restoring stopped pipelines at startup",
				slog.Any("error", err))
		}
		slog.Info("restored pipelines as configured-but-not-running at startup",
			slog.Int("count", len(stopped)))
	}

	m.applyPlans(rejected)
	m.applyPlans(normal)
}

// rejectStaleStartupPlans compares the Manager reply with the persisted state,
// which has not yet been copied into m.applied during startup.
func rejectStaleStartupPlans(plans []pipelinePlan, prior map[appliedKey]appliedPipeline) []pipelinePlan {
	latest := make(map[string]uint64)
	for key, ap := range prior {
		if ap.GetConfigVersion() > latest[key.pipelineID] {
			latest[key.pipelineID] = ap.GetConfigVersion()
		}
	}
	stale := make(map[string]bool)
	for _, plan := range plans {
		if plan.item.GetConfigVersion() < latest[plan.item.GetPipelineId()] {
			stale[plan.item.GetPipelineId()] = true
		}
	}
	if len(stale) == 0 {
		return plans
	}

	adjusted := make([]pipelinePlan, 0, len(plans)+len(prior))
	for _, plan := range plans {
		id := plan.item.GetPipelineId()
		if stale[id] {
			adjusted = append(adjusted, pipelinePlan{item: plan.item, kind: changeReject, previousConfigVersion: latest[id]})
			continue
		}
		adjusted = append(adjusted, plan)
	}
	for key, ap := range prior {
		if stale[key.pipelineID] {
			adjusted = append(adjusted, pipelinePlan{item: ap.PipelineConfigItem, kind: changeLoad})
		}
	}
	return adjusted
}
