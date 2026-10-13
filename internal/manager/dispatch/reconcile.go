// Reconciliation compares each role's observed state with Manager's desired intent.
// Retries retain command identities; a new sequence resets the in-memory budget.
// Exhaustion is persisted so restarts cannot silently resume abandoned retries.
package dispatch

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/redact"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// DefaultReconcileInterval 是对账循环未显式配置时的轮询间隔。
const DefaultReconcileInterval = 15 * time.Second

// backoffBase/backoffMax 是指数退避的下限与上限（"指数
// 退避避免风暴"，未给出具体数值，这里选定）。下限取跟对账循环默认轮询
// 间隔一致：新意图之后的第一次重发排到下一轮自然轮询就够，不需要额外
// 拉长等待；上限 5 分钟封顶，避免一条长期离线的 pipeline 恢复之后，
// 退避已经涨到小时级、迟迟等不到下一次重发。
const (
	backoffBase = 15 * time.Second
	backoffMax  = 5 * time.Minute
)

func backoffDuration(attempts int) time.Duration {
	if attempts <= 0 {
		return 0
	}
	d := backoffBase
	for i := 1; i < attempts; i++ {
		if d >= backoffMax {
			return backoffMax
		}
		d *= 2
	}
	if d > backoffMax {
		d = backoffMax
	}
	return d
}

// 20 这个数字不是精确推导出来的，可调（诚实标注，保留）：
// backoffDuration 第 6 次起封顶在 backoffMax=5 分钟，20 次累计约合
// 75 分钟以上的持续无效重试。
//
// 一次瞬时但很长的故障（目标库停机两小时）会撞上上限、需要人工点一次
// 重试，尽管它本来会自愈。这个取舍是有意选的：停下来加告警是人
// 能发现、能恢复的；无限静默重试人发现不了。第一次遇到它的人不要当
// bug 改——要改就连同"怎么和真正的永久故障区分开"一起改。
const maxRecoveryAttempts = 20

const (
	recoveryTriggerExplicitNonRetryable = "explicit_non_retryable"
	recoveryTriggerAttemptsExhausted    = "attempts_exhausted_no_resolution"
)

// RecoveryExhausted reports exhaustion for the current command identity.
// A nil record means no exhaustion; zero is a valid command sequence.
// A new sequence invalidates both the exhaustion record and the retry budget.
func RecoveryExhausted(p model.Pipeline) bool {
	return p.RecoveryExhaustedCmdSeq != nil && *p.RecoveryExhaustedCmdSeq == p.LastCmdSeq
}

// backoffState 是单条 pipeline 的退避状态，只存在于进程内存（见本文件
// 顶部包注释）。lastSeenCmdSeq 不是用来决定"要不要 bump"（那是 http 层
// setDesiredState 的职责，本类型永远不 bump），只是用来**识别**"这一轮
// 看到的候选，是不是我之前已经在退避等待的同一个意图"——不同就说明
// cmd_seq 已经被 setDesiredState 推进过，出现了新意图，退避应该清零、
// 立即重发。
type backoffState struct {
	lastSeenCmdSeq uint64
	attempts       int
	nextAttemptAt  time.Time
}

// ReconcileOptions 配置 Reconciler。
type ReconcileOptions struct {
	// Interval 可选，未提供时使用 DefaultReconcileInterval。
	Interval time.Duration
}

type Reconciler struct {
	d        *Dispatcher
	interval time.Duration

	// backoff 记录每条当前不一致的 pipeline 的退避状态。只在 tick 所在的
	// 单一 goroutine 里读写——Run 是本类型唯一预期的驱动方式，不会有第二
	// 个 goroutine 并发调用 tick，因此不需要加锁（"只有
	// 确实被多个 goroutine 访问的数据才加锁"）。测试里直接、顺序调用
	// tick/reconcileOne 也遵守这个前提，不并发调用。
	backoff map[uuidx.UUID]*backoffState
}

// NewReconciler 构造 Reconciler。d 不能为 nil——本类型不独立持有
// db/发布者/签名 key 解析，全部复用 d 已经具备的能力（与 SyncPipeline/
// NotifyDeleted 共用同一个 Dispatcher 实例，不是各建一份）。
func NewReconciler(d *Dispatcher, opts ReconcileOptions) *Reconciler {
	interval := opts.Interval
	if interval <= 0 {
		interval = DefaultReconcileInterval
	}
	return &Reconciler{d: d, interval: interval, backoff: make(map[uuidx.UUID]*backoffState)}
}

// Run 阻塞运行对账循环直到 ctx 被取消；调用方应该在自己的 goroutine 里
// 启动它。启动时先跑一轮（不等第一个 interval），与 reclaim.Loop.Run 同一
// 个理由：刚启动时如果已经存在 desired != actual 的 pipeline，不应该空等
// 一个轮询周期才开始处理。
func (r *Reconciler) Run(ctx context.Context) {
	r.tick(ctx)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.tick(ctx)
		}
	}
}

// tick 跑一轮完整扫描。
func (r *Reconciler) tick(ctx context.Context) {
	r.refreshOriginConfigs(ctx)
	pipelines, err := repo.ListPipelinesNeedingReconciliation(ctx, r.d.db)
	if err != nil {
		slog.Error("dispatch: reconcile list candidates failed", slog.Any("error", err))
		return
	}
	seen := make(map[uuidx.UUID]struct{}, len(pipelines))
	for _, p := range pipelines {
		seen[p.ID] = struct{}{}
		r.reconcileOne(ctx, p)
	}
	// 清理这一轮没有再出现的候选（已收敛，或已被两步删除）的退避状态，
	// 避免长期运行的进程里无限堆积——参与过对账、后来收敛的 pipeline
	// 不需要继续占着这份状态；它将来又变得不一致时，会被当成全新的一次
	// 遭遇重新开始（attempts 从 0 起立即下发），这正是正确的语义："距
	// 上次收敛之后的这次不一致"不是同一次持续未收敛，是一次新的不一致，
	// 不应该继承旧的退避进度。
	for id := range r.backoff {
		if _, ok := seen[id]; !ok {
			delete(r.backoff, id)
		}
	}
}

// 本方法不调用 model.Pipeline.DeriveDisplayState()，也不读旧的
// p.ActualState——前者是展示专用的聚合，在 desired=Paused、
// source=Paused、target=Stopped 这一格上会给出 Stopped，使
// nextPipelineCommand 判定无匹配指令而永久跳过（该方法文档自己点名了
// 这个后果）；后者是"两侧共用一列、后到者覆盖先到者"这个缺陷本身的
// 落地位置。对账决策只看两列按侧的真实状态。
func (r *Reconciler) reconcileOne(ctx context.Context, p model.Pipeline) {
	if p.DesiredState == model.DesiredStateRunning {
		if err := repo.CheckTopologyStart(ctx, r.d.db, p.TopologyID); err != nil {
			slog.Warn("pipeline start blocked by topology analysis", slog.String("pipeline_id", p.ID.String()), slog.Any("error", err))
			return
		}
		if !p.ReinitPending && (p.SourceActualState == model.ActualStateDraft || p.TargetActualState == model.ActualStateDraft) {
			// A queued first START may outlive the check that authorized its
			// HTTP request. Revalidate before dispatch when still unassembled;
			// configuration changes or expiry cannot authorize a fresh load.
			state, err := repo.GetPrecheckState(ctx, r.d.db, p.ID, r.d.now())
			if err != nil || state.GateError != nil {
				slog.Debug("initial pipeline start awaits current precheck", slog.String("pipeline_id", p.ID.String()))
				return
			}
		}
	}
	if RecoveryExhausted(p) {
		// 已经在这个 cmd_seq 上放弃自动恢复，等新意图或人工重试
		// （POST /pipelines/:id/retry 会无条件 bump cmd_seq，本判据随即
		// 转假）。这里不记 Warn：告警在放弃的那一刻已经落进 events 了，
		// 每 15s 再喊一次只会训练所有人忽略它。
		slog.Debug("dispatch: reconcile skipped pipeline, recovery exhausted on current cmd_seq",
			slog.String("pipeline_id", p.ID.String()),
			slog.Uint64("cmd_seq", p.LastCmdSeq))
		return
	}

	plans := planPipelineSides(p)
	if p.ReinitPending && p.DesiredState == model.DesiredStateRunning {
		stored, err := repo.NewReinitializations(r.d.db).GetPreview(ctx, p.ID, p.RunID-1)
		if err != nil || stored.Record.ConfirmedAt == nil || stored.Record.CmdSeq != p.LastCmdSeq || stored.Record.NewRunID != p.RunID {
			slog.Error("reinit reconciliation identity invalid", slog.String("pipeline_id", p.ID.String()), slog.Any("error", err))
			return
		}
		plans = nil
		for _, side := range []struct {
			side    model.Side
			node    uuidx.UUID
			state   model.ActualState
			applied bool
		}{
			{model.SideSource, p.SourceNodeID, p.SourceActualState, stored.Record.SourceApplied},
			{model.SideTarget, p.TargetNodeID, p.TargetActualState, stored.Record.TargetApplied},
		} {
			if !side.applied && side.state != model.ActualStateUnknown {
				plans = append(plans, sidePlan{side: side.side, nodeID: side.node, cmdType: iproto.PipelineCmdType_PIPELINE_CMD_TYPE_REINIT})
			}
		}
	}
	if len(plans) == 0 {
		slog.Debug("dispatch: reconcile skipped pipeline, no unambiguous command mapping on either side",
			slog.String("pipeline_id", p.ID.String()),
			slog.String("desired_state", string(p.DesiredState)),
			slog.String("source_actual_state", string(p.SourceActualState)),
			slog.String("target_actual_state", string(p.TargetActualState)))
		return
	}

	nodePlans := groupPlansByNode(plans)

	now := r.d.now()
	st, tracked := r.backoff[p.ID]
	switch {
	case !tracked || st.lastSeenCmdSeq != p.LastCmdSeq:

		st = &backoffState{lastSeenCmdSeq: p.LastCmdSeq}
		r.backoff[p.ID] = st
	case now.Before(st.nextAttemptAt):

		return
	}

	r.dispatchPlans(ctx, p, nodePlans)
	st.attempts++
	st.nextAttemptAt = now.Add(backoffDuration(st.attempts))

	if st.attempts >= maxRecoveryAttempts {
		r.markRecoveryExhausted(ctx, p, recoveryTriggerAttemptsExhausted, st.attempts)
	}
}

// markRecoveryExhausted 把 pipeline 标记为"在当前 cmd_seq 上已放弃自动
// 恢复"，并落一条 events 告警。
//
// 两件事都做、且都不因为对方失败而跳过：标记决定 Reconciler 以后还发
// 不发（正确性），事件决定人能不能知道（可见性）。标记失败时仍然落
// 事件——否则一次数据库抖动会让这条 pipeline 既继续无效重试、又没有
// 任何人知道它在重试。
func (r *Reconciler) markRecoveryExhausted(ctx context.Context, p model.Pipeline, trigger string, attempts int) {
	logFields := []any{
		slog.String("pipeline_id", p.ID.String()),
		slog.Uint64("cmd_seq", p.LastCmdSeq),
		slog.String("trigger", trigger),
		slog.Int("attempts", attempts),
	}
	slog.Warn("dispatch: giving up automatic recovery for pipeline", logFields...)

	if err := repo.SetPipelineRecoveryExhausted(ctx, r.d.db, p.ID, p.LastCmdSeq); err != nil {
		slog.Error("dispatch: mark pipeline recovery exhausted failed",
			append(logFields, slog.Any("error", err))...)
	}

	payload, err := json.Marshal(map[string]any{
		"kind":                eventKindRecoveryExhausted,
		"trigger":             trigger,
		"attempts":            attempts,
		"cmd_seq":             p.LastCmdSeq,
		"desired_state":       string(p.DesiredState),
		"source_actual_state": string(p.SourceActualState),
		"target_actual_state": string(p.TargetActualState),

		"source_last_error": redact.PotentialCredentials(p.SourceLastError),
		"target_last_error": redact.PotentialCredentials(p.TargetLastError),
	})
	if err != nil {
		slog.Error("dispatch: encode recovery exhausted event payload failed",
			append(logFields, slog.Any("error", err))...)
		return
	}

	event := model.Event{
		Ts:         r.d.now(),
		PipelineID: p.ID,
		Type:       model.EventTypeError,
		Level:      model.EventLevelError,
		Payload:    model.JSON(payload),
	}
	if err := repo.InsertEvents(ctx, r.d.db, []model.Event{event}); err != nil {
		slog.Error("dispatch: insert recovery exhausted event failed",
			append(logFields, slog.Any("error", err))...)
	}
}

// sidePlan 是对账循环为 pipeline 的某一侧算出的下发计划：这一侧的实际
// 状态翻译出了 cmdType 这条指令，该发给 nodeID 这个节点。
type sidePlan struct {
	side    model.Side
	nodeID  uuidx.UUID
	cmdType iproto.PipelineCmdType
}

// nodeID 为零值的一侧跳过：没有节点可发，与 dispatchCommandToNode 的
// 既有前置检查同一个理由，提前在这里挡掉可以顺带避免把一条不可能发出
// 的指令算进"这一侧有动作"。
func planPipelineSides(p model.Pipeline) []sidePlan {
	sides := [2]struct {
		side   model.Side
		actual model.ActualState
		nodeID uuidx.UUID
	}{
		{model.SideSource, p.SourceActualState, p.SourceNodeID},
		{model.SideTarget, p.TargetActualState, p.TargetNodeID},
	}
	plans := make([]sidePlan, 0, len(sides))
	for _, s := range sides {
		if s.nodeID.IsZero() {
			continue
		}
		cmdType, ok := nextPipelineCommand(p.DesiredState, s.actual)
		if !ok {
			continue
		}
		plans = append(plans, sidePlan{side: s.side, nodeID: s.nodeID, cmdType: cmdType})
	}
	return plans
}

type nodeCmdPlan struct {
	nodeID  uuidx.UUID
	cmdType iproto.PipelineCmdType
	sides   []model.Side
}

// groupPlansByNode combines independently planned source and target roles.
// Same-node transfers must retain both roles.
func groupPlansByNode(plans []sidePlan) []nodeCmdPlan {
	out := make([]nodeCmdPlan, 0, len(plans))
	for _, p := range plans {
		merged := false
		for i := range out {
			if out[i].nodeID == p.nodeID && out[i].cmdType == p.cmdType {
				out[i].sides = append(out[i].sides, p.side)
				merged = true
				break
			}
		}
		if !merged {
			out = append(out, nodeCmdPlan{nodeID: p.nodeID, cmdType: p.cmdType, sides: []model.Side{p.side}})
		}
	}
	return out
}

// pipelineCmdKey 是规则表 pipelineCmdRules 的键：一个 (desired, 某一侧的
// actual) 组合。**actual 是某一侧的实际状态，不是聚合值**，见
// planPipelineSides。
type pipelineCmdKey struct {
	desired model.DesiredState
	actual  model.ActualState
}

// pipelineCmdRule 是规则表的一格。
//
// cmdType 为 PIPELINE_CMD_TYPE_UNSPECIFIED 表示"这一格不下发任何指令"；
// reason 每一格都必须非空，下发的格子写下发依据，不下发的格子写
// 为什么不下发。
type pipelineCmdRule struct {
	cmdType iproto.PipelineCmdType
	reason  string
}

// skip 构造一个"不下发"的格子，让表里"发什么"与"为什么不发"两类格子
// 在视觉上一眼可分。
func skip(reason string) pipelineCmdRule {
	return pipelineCmdRule{cmdType: iproto.PipelineCmdType_PIPELINE_CMD_TYPE_UNSPECIFIED, reason: reason}
}

// send 构造一个"下发 cmdType"的格子。
func send(cmdType iproto.PipelineCmdType, reason string) pipelineCmdRule {
	return pipelineCmdRule{cmdType: cmdType, reason: reason}
}

// pipelineCmdRules 根据期望状态与单侧实际状态选择控制指令。
// Unknown 表示该侧离线，不向其重发命令。
var pipelineCmdRules = map[pipelineCmdKey]pipelineCmdRule{
	// ---------- desired = Running ----------
	{model.DesiredStateRunning, model.ActualStateDraft}: send(
		iproto.PipelineCmdType_PIPELINE_CMD_TYPE_START,
		"HTTP 已在写入 running 意图与命令序号的事务内验证当前预检查及警告确认；Draft 尚未装配，需要 START 触发定向拉取和首次装配，对账仅重试已授权意图"),
	{model.DesiredStateRunning, model.ActualStatePrechecking}: skip(
		"预检查等待用户确认，对账循环不自动启动流水线"),
	{model.DesiredStateRunning, model.ActualStateDeploying}: skip(
		"装配推进中，状态图上没有一条由 Manager 指令触发的出边；装配成功或失败都由节点自行上报，届时会落到 (Running, Running) 或 (Running, Error) 两格"),
	{model.DesiredStateRunning, model.ActualStateMigrating}: skip(
		"结构迁移正在执行，完成前不插入控制指令"),
	{model.DesiredStateRunning, model.ActualStateSnapshotting}: skip(
		"全量快照推进中，而期望本来就是 Running：快照完成后节点自行进入 CatchingUp 直至 Running，方向一致，没有任何需要 Manager 纠正的偏差"),
	{model.DesiredStateRunning, model.ActualStateCatchingUp}: skip(
		"增量追赶中，而期望本来就是 Running：追上之后节点自行上报 Running，方向一致，不需要指令"),
	{model.DesiredStateRunning, model.ActualStateRunning}: skip(
		"这一侧已经收敛（desired == 该侧 actual）。按侧拆分后这一格是可达的：另一侧尚未收敛时整行仍是候选，本侧必须明确表态不发"),
	{model.DesiredStateRunning, model.ActualStatePaused}: send(
		iproto.PipelineCmdType_PIPELINE_CMD_TYPE_RESUME,
		"状态图 Paused --> Running「恢复」，一条直达边，指令类型与边的标签一一对应"),
	{model.DesiredStateRunning, model.ActualStateError}: send(
		iproto.PipelineCmdType_PIPELINE_CMD_TYPE_RESUME,
		"运行意图下通过 RESUME 重试装配，退避与最大恢复次数限制由 Manager 维护"),
	{model.DesiredStateRunning, model.ActualStateStopped}: send(
		iproto.PipelineCmdType_PIPELINE_CMD_TYPE_START,
		"状态图 Stopped --> Deploying「启动」，一条直达边；START 之后经 Deploying（开启结构迁移时再经 Migrating）到达 Running"),
	{model.DesiredStateRunning, model.ActualStateUnknown}: skip(
		"该侧节点心跳超时离线，repo.MarkPipelineRoleUnknown 把这一侧标成 Unknown；MGR-PL-16「节点离线不重发指令」——对着一个收不到消息的节点重发，只会在它重新上线时一次性砸下一批过期指令"),

	// ---------- desired = Paused ----------
	{model.DesiredStatePaused, model.ActualStateDraft}: skip(
		"从未启动过的 pipeline 既没在跑、也就无从暂停；要让它变成 Paused 必须先经预检查与启动，而那是用户表达「现在要启动」时才该发生的事，不能由一个「请少做一点」的请求触发"),
	{model.DesiredStatePaused, model.ActualStatePrechecking}: skip(
		"预检查正在进行，它的出边「通过并启动」是用户在向导里点击的人工闸门；设了 desired=Paused 的用户不会去点启动，因此这一格会停在原地而不是继续推进——安全的原因是这道人工闸门，不是「瞬态会自己走完」"),
	{model.DesiredStatePaused, model.ActualStateDeploying}: skip(
		"装配推进中，状态图上没有 Deploying --> Paused 的边，也没有任何指令能叫停装配；装配完成后进入 Snapshotting 或 Running，届时 (Paused, Snapshotting) 与 (Paused, Running) 两格都有 PAUSE 可发"),
	{model.DesiredStatePaused, model.ActualStateMigrating}: skip(
		"结构迁移推进中，状态图上没有 Migrating --> Paused 的边；迁移是目标库上的真实 DDL 执行，半途叫停会留下一个既不是迁移前也不是迁移后的结构，而 Manager 没有任何指令能表达「回滚这次迁移」"),
	{model.DesiredStatePaused, model.ActualStateSnapshotting}: send(
		iproto.PipelineCmdType_PIPELINE_CMD_TYPE_PAUSE,
		"状态图 Snapshotting --> Paused「暂停」，一条直达边"),
	{model.DesiredStatePaused, model.ActualStateCatchingUp}: send(
		iproto.PipelineCmdType_PIPELINE_CMD_TYPE_PAUSE,
		"追平期间仍须响应暂停；节点有序卸载并保存本次运行的全量计划、发布记录及检查点，恢复沿用相同身份，不要求等待目标追平"),
	{model.DesiredStatePaused, model.ActualStateRunning}: send(
		iproto.PipelineCmdType_PIPELINE_CMD_TYPE_PAUSE,
		"状态图 Running --> Paused「暂停」，一条直达边"),
	{model.DesiredStatePaused, model.ActualStatePaused}: skip(
		"这一侧已经收敛（desired == 该侧 actual）。按侧拆分后这一格是可达的：另一侧尚未收敛时整行仍是候选"),
	{model.DesiredStatePaused, model.ActualStateError}: send(
		iproto.PipelineCmdType_PIPELINE_CMD_TYPE_PAUSE,
		"暂停意图下直接发送 PAUSE；已停止的模块无需恢复运行即可进入 Paused"),
	{model.DesiredStatePaused, model.ActualStateStopped}: skip(
		"Stopped 已满足不运行的暂停意图，不触发 START"),
	{model.DesiredStatePaused, model.ActualStateUnknown}: skip(
		"该侧节点心跳超时离线，这一侧的状态本身就是不可知的；MGR-PL-16「节点离线不重发指令」——此时发 PAUSE 既无法送达，也无法判断送达后该不该发"),

	// ---------- desired = Stopped ----------
	{model.DesiredStateStopped, model.ActualStateDraft}: skip(
		"repo.ListPipelinesNeedingReconciliation 文档自己举的例子：刚创建、从未启动过，desired=stopped 与 actual=draft 字符串不相等但语义已经收敛，它已经满足「没有在跑」这个真实意图"),
	{model.DesiredStateStopped, model.ActualStatePrechecking}: skip(
		"预检查完成后仍需用户确认启动；停止意图下不自动进入装配阶段"),
	{model.DesiredStateStopped, model.ActualStateDeploying}: skip(
		"装配推进中，状态图上没有 Deploying --> Stopped 的直达边；装配完成后进入 Snapshotting 或 Running，(Stopped, Running) 有 STOP 可发，届时自然收敛，无害"),
	{model.DesiredStateStopped, model.ActualStateMigrating}: skip(
		"结构迁移推进中，STOP 意图延后至 DDL 完成再执行；Migrating --> Stopped 仅表示 schema_only 自然完成，不表示可以中断迁移。其它模式完成后进入全量或增量阶段，再发 STOP"),
	{model.DesiredStateStopped, model.ActualStateSnapshotting}: send(
		iproto.PipelineCmdType_PIPELINE_CMD_TYPE_STOP,
		"全量期间停止直接有序卸载两端；持久化计划、已发布块及检查点保留，后续普通 START 延续同一运行，不重新清表"),
	{model.DesiredStateStopped, model.ActualStateCatchingUp}: send(
		iproto.PipelineCmdType_PIPELINE_CMD_TYPE_STOP,
		"追平期间停止不能等待 RUNNING：源端全量结束只报告 CATCHING_UP，目标停下后追平也无法继续；直接 STOP 按既有卸载和检查点流程收敛"),
	{model.DesiredStateStopped, model.ActualStateRunning}: send(
		iproto.PipelineCmdType_PIPELINE_CMD_TYPE_STOP,
		"状态图 Running --> Stopped「停止」，一条直达边"),
	{model.DesiredStateStopped, model.ActualStatePaused}: send(
		iproto.PipelineCmdType_PIPELINE_CMD_TYPE_STOP,
		"状态图 Paused --> Stopped「停止」，一条直达边"),
	{model.DesiredStateStopped, model.ActualStateError}: send(
		iproto.PipelineCmdType_PIPELINE_CMD_TYPE_STOP,
		"状态图 Error --> Stopped「停止」，一条直达边；这也是一条失败的 pipeline 的正常收尾路径，与 (Running, Error) 的自动恢复是两个方向，由 desired 区分"),
	{model.DesiredStateStopped, model.ActualStateStopped}: skip(
		"这一侧已经收敛（desired == 该侧 actual）。按侧拆分后这一格是可达的：另一侧尚未收敛时整行仍是候选"),
	{model.DesiredStateStopped, model.ActualStateUnknown}: skip(
		"该侧节点心跳超时离线，MGR-PL-16「节点离线不重发指令」；一个离线的节点上跑的东西是不是已经停了无从得知，重发 STOP 不会让这件事变得更确定"),
}

// nextPipelineCommand 根据期望状态和单侧实际状态查表决定指令。
// 未登记的组合记录错误并跳过；各侧分别对账。
func nextPipelineCommand(desired model.DesiredState, actual model.ActualState) (iproto.PipelineCmdType, bool) {
	rule, ok := pipelineCmdRules[pipelineCmdKey{desired: desired, actual: actual}]
	if !ok {
		slog.Error("dispatch: pipeline command rule table has no entry for this combination",
			slog.String("desired_state", string(desired)),
			slog.String("actual_state", string(actual)))
		return iproto.PipelineCmdType_PIPELINE_CMD_TYPE_UNSPECIFIED, false
	}
	if rule.cmdType == iproto.PipelineCmdType_PIPELINE_CMD_TYPE_UNSPECIFIED {
		return iproto.PipelineCmdType_PIPELINE_CMD_TYPE_UNSPECIFIED, false
	}
	return rule.cmdType, true
}

// dispatchPlans 按 groupPlansByNode 归并后的计划逐条发出 cmd.pipeline。
func (r *Reconciler) dispatchPlans(ctx context.Context, p model.Pipeline, nodePlans []nodeCmdPlan) {
	pub := r.d.publisher()
	if pub == nil {
		slog.Warn("dispatch: cmd.pipeline push skipped, amqp publisher not wired yet",
			slog.String("pipeline_id", p.ID.String()))
		return
	}
	for _, np := range nodePlans {
		slog.Debug("dispatch: sending pipeline command",
			slog.String("pipeline_id", p.ID.String()),
			slog.String("node_id", np.nodeID.String()),
			slog.Any("cmd_type", np.cmdType),
			slog.Uint64("cmd_seq", p.LastCmdSeq),
			// 命令面的按侧归属：这个节点为什么收到这条指令，是哪一侧
			// （可能两侧）的实际状态算出来的。
			slog.Any("sides", np.sides))
		r.dispatchCommandToNode(ctx, pub, np.nodeID, p, np.cmdType)
	}
}

func (r *Reconciler) dispatchCommandToNode(ctx context.Context, pub iPublisher, nodeID uuidx.UUID, p model.Pipeline, cmdType iproto.PipelineCmdType) {
	logFields := []any{
		slog.String("node_id", nodeID.String()),
		slog.String("pipeline_id", p.ID.String()),
		slog.Any("cmd_type", cmdType),
		slog.Uint64("cmd_seq", p.LastCmdSeq),
	}

	node, err := repo.GetNode(ctx, r.d.db, nodeID)
	if err != nil {
		slog.Error("dispatch: cmd.pipeline push skipped, node lookup failed",
			append(logFields, slog.Any("error", err))...)
		return
	}

	msg := &iproto.PipelineCmd{
		PipelineId:    p.ID.String(),
		ConfigVersion: p.ConfigVersion,
		CmdSeq:        p.LastCmdSeq,
		Type:          cmdType,
	}
	if cmdType == iproto.PipelineCmdType_PIPELINE_CMD_TYPE_REINIT {
		if !p.ReinitPending || p.RunID == 0 {
			return
		}
		stored, err := repo.NewReinitializations(r.d.db).GetPreview(ctx, p.ID, p.RunID-1)
		if err != nil || stored.Record.ConfirmedAt == nil || stored.Record.CmdSeq != p.LastCmdSeq || stored.Record.NewRunID != p.RunID || stored.Record.SnapshotUUID != p.SnapshotUUID || stored.Record.SchemaUUID != p.SchemaUUID {
			slog.Error("reinit command identity invalid", append(logFields, slog.Any("error", err))...)
			return
		}
		msg.Reinit = &iproto.ReinitArgs{NewRunId: p.RunID, SnapshotUuid: p.SnapshotUUID.String(), SchemaUuid: optionalReinitUUID(p.SchemaUUID), Digest: stored.Record.Digest, PrepareUuid: optionalReinitUUID(stored.Record.PrepareUUID)}
	}
	// 控制面消息体积很小，不压缩——与 pushConfigChangedToNode/
	// probe.publishResult 的既有选择一致。
	body, encoding, err := iproto.Encode(msg, iproto.EncodeOptions{Compress: false})
	if err != nil {
		slog.Error("dispatch: encode cmd.pipeline failed",
			append(logFields, slog.Any("error", err))...)
		return
	}
	env := iproto.NewEnvelope(body, iproto.NewEnvelopeParams{

		MessageID:       uuidx.New().String(),
		Type:            iproto.MessageTypeCmdPipeline,
		TimestampMS:     r.d.now().UnixMilli(),
		ProtocolVersion: iproto.CurrentProtocolVersion(),
		ContentEncoding: encoding,
		// Center/NodeID 留空：Manager 发出的消息没有 x-node-id，与
		// pushConfigChangedToNode 同一先例。
	})
	signKey := iproto.DeriveSignKeyFromTokenHash(node.TokenHash)
	env.WithSignature(iproto.Sign(signKey, env.SignParamsFor()))

	outcome, err := pub.PublishEnvelope(ctx, transport.ExchangeControl, transport.RoutingKeyNode(nodeID.String()), env)
	if err != nil {
		slog.Error("dispatch: publish cmd.pipeline failed",
			append(logFields, slog.Any("error", err))...)
		return
	}
	if outcome != transport.OutcomeConfirmed {
		slog.Error("dispatch: cmd.pipeline not confirmed delivered",
			append(logFields, slog.Any("outcome", outcome))...)
	}
}
