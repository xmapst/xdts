package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strings"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/safeerror"
	"github.com/xmapst/xdts/internal/transport"
)

// cmdPipelineKey binds command type, configuration version and command sequence.
// The pipeline and role provide the deduplication slot; retries retain this identity.
type cmdPipelineKey struct {
	ConfigVersion uint64
	CmdSeq        uint64

	Type iproto.PipelineCmdType
}

// cmdDedupSlot 为同一 pipeline 的每种指令类型分别保留去重槽位。
type cmdDedupSlot struct {
	pipelineID string
	cmdType    iproto.PipelineCmdType
}

// 合并的理由：Type 进入去重键之后，独立的 cmdType 字段与 key.Type 是同一个值的
// 两个存放处，四个构造点都得记得同时填对——这正是 AGENTS.md 红线"契约改全"关心
// 的那类"只改一端不会报错"的形状，而这里连编译器都帮不上忙（两个字段类型相同）。
// 合并后"只要 cmdContext 存在，key.Type 就是触发它的真实指令类型"这条不变量由
// 类型本身保证，不依赖每个构造点的自觉。
type cmdContext struct {
	// fanout 是这条指令**整体**的收尾状态（ack、幂等键、还有几侧没回来、各侧结果）。
	// 一条指令在自环拓扑下会派生出多个 cmdContext（每侧一个），它们**共享同一个
	// fanout 指针**——ack 与回执都只有一份，见 cmdFanout 文档。
	fanout *cmdFanout
	// role 是这个 cmdContext 负责的那一侧。
	role iproto.PipelineRole
}

// cmdSideOutcome 是一条指令在**某一侧**的执行结果。
type cmdSideOutcome struct {
	role  iproto.PipelineRole
	state iproto.PipelineState
	err   error
}

// - 全部侧成功 → 一条 success=true 的回执 + ack + markCmdResolved；
//   - 任一侧失败 → 一条 success=false 的回执（Error.Message 点名失败的那一侧）+
//     回执确认后 ack；未分类或可重试失败不 markCmdResolved，由 Manager 退避重试；
//     明确永久失败记收敛键。（见 cmdPipelineKey"去重表记的是已收敛"一节）。
//     重试粒度因此是 pipeline 而不是侧：再次下发后已经成功的
//     那一侧会再执行一次，安全——每一侧的 exec 分支都有按侧的效果幂等短路（已处
//     于目标状态则 no-op），但这一点必须写明，它是"不给去重槽位加 role 维"这个
//     决定的直接代价（见 cmdDedupSlot 文档同一节）。
//   - resulting_state：两侧一致则报该状态；不一致报 UNSPECIFIED，不报 ERROR
//     ——见 aggregateCmdState 文档。
type cmdFanout struct {
	ack        iAckHandle
	pipelineID string
	key        cmdPipelineKey
	// Initialize pendingSides before dispatching either role; synchronous failure
	// callbacks must see the complete group size.
	pendingSides int
	outcomes     []cmdSideOutcome
}

// aggregateCmdState 把各侧的 resulting_state 聚合成回执里那一个值。
func aggregateCmdState(outcomes []cmdSideOutcome) iproto.PipelineState {
	if len(outcomes) == 0 {
		return iproto.PipelineState_PIPELINE_STATE_UNSPECIFIED
	}
	first := outcomes[0].state
	for _, o := range outcomes[1:] {
		if o.state != first {
			return iproto.PipelineState_PIPELINE_STATE_UNSPECIFIED
		}
	}
	return first
}

// aggregateCmdErr 把各侧失败合成诊断文本，保留失败侧名与状态。
// 结构化侧结果同时写入 PipelineCmdResult.sides。全部成功时返回 nil。
func aggregateCmdErr(pipelineID string, outcomes []cmdSideOutcome) error {
	var failed []error
	for _, o := range outcomes {
		if o.err != nil {
			failed = append(failed, fmt.Errorf("%s 侧: %w", o.role, o.err))
		}
	}
	if len(failed) == 0 {
		return nil
	}
	states := make([]string, 0, len(outcomes))
	for _, o := range outcomes {
		states = append(states, o.role.String()+"="+o.state.String())
	}
	return fmt.Errorf("controller: pipeline %q 指令执行失败（各侧状态 %s）: %w",
		pipelineID, strings.Join(states, " "), errors.Join(failed...))
}

// completeCmdSide 聚合 finishApply/finishStop 的各侧结果。cmdCtx 为 nil 时
// 不处理命令回执；全部侧完成后由 finishCmdFanout 一次发布结果并确认投递。
// 执行失败的后续重试由 Manager 调度，回执确认与关键落盘失败仍重新入队。
//
// 调用方用 defer 在函数入口处安装（见 finishApply/finishStop），execErr 作为参数
// 在 defer 语句本身求值时就已经固定，与 defer 的实际执行时机（函数返回前）无关
// ——Go"defer 的参数在 defer 语句执行时求值"这条规则保证这里读到的是
// finishApply/finishStop 函数体内最终确定的错误值，不是过时的快照。
func (m *Module) completeCmdSide(cmdCtx *cmdContext, execErr error, state iproto.PipelineState) {
	if cmdCtx == nil {
		return
	}
	f := cmdCtx.fanout
	f.outcomes = append(f.outcomes, cmdSideOutcome{role: cmdCtx.role, state: state, err: execErr})
	f.pendingSides--
	if f.pendingSides > 0 {
		// 还有别的侧在跑：这一侧的结果先记下，ack 与回执等最后一侧回来再一次性
		// 处理（cmdFanout 文档"一条消息，两侧执行，一个 ack"）。
		return
	}
	m.finishCmdFanout(f)
}

// finishCmdFanout aggregates every executed side into one receipt. Manager
// schedules execution retries; only confirmed success/permanent failure is deduped.
//
// 复用 publishCmdResultThenAck 而不是自己写一遍 AsyncGo 外壳：回执发布是阻塞 IO
// （要等 Broker confirm），必须在事件循环之外做；而 ack/requeue 与 markCmdResolved
// 又必须回到事件循环——由该方法保持回执确认、持久化与投递确认的顺序。
func (m *Module) finishCmdFanout(f *cmdFanout) {
	execErr := aggregateCmdErr(f.pipelineID, f.outcomes)
	state := aggregateCmdState(f.outcomes)

	result := buildCmdResult(f.pipelineID, f.key, state, execErr, f.outcomes)
	// Manager owns execution retries and their backoff. A confirmed failure
	// receipt ends this delivery without marking an unresolved command deduped.
	// Requeuing it here would let Broker bypass Manager's bounded recovery.
	m.publishCmdResultThenAck("controller.cmdresult."+f.pipelineID, f.pipelineID, f.key, result, f.ack, false)
}

// markCmdResolved 把 (pipelineID, key) 记为"已收敛"——调用方必须只在真正决定
// ack（不 requeue）这条 PipelineCmd 时调用，绝不能在 requeue 分支调用，否则
// 下次同键重发会被误判成"已经处理过"而短路掉本该发生的重新执行（见
// Module.cmdPipelineResolved 文档"三类结局与是否写入的对应关系"一节）。
//
// 只做一件事、单独抽出来而不是让每个调用点各自写 m.cmdPipelineResolved[id] =
// key：调用点分散在 resolveCmdAck 与 publishCmdResultThenAck 两处，抽成命名
// 函数让"这一行在做什么、为什么只能在 ack 分支调用"有唯一的文档落点，不需要在
// 每个调用点重复解释。
func (m *Module) markCmdResolved(pipelineID string, key cmdPipelineKey) {
	if (key.Type == iproto.PipelineCmdType_PIPELINE_CMD_TYPE_START || key.Type == iproto.PipelineCmdType_PIPELINE_CMD_TYPE_RESUME) && m.pipelineRuntimeFailed(pipelineID) {
		return
	}
	m.cmdPipelineResolved[cmdDedupSlot{pipelineID: pipelineID, cmdType: key.Type}] = key
}

// resultingStateOnStop 是 changeStop 场景下上报状态（cmd.result.resulting_state
// 与 finishStop 成功分支调用 reportPipelineStatus 时的 state 参数，两处共用同一套
// 判断，见各自调用点）的取值：卸载失败 → ERROR（Unload 报错时旧模块是否还活着不
// 确定，internal/node/controller/loader.go Unload 文档"两种 false 原因无法区分"，报 STOPPED/PAUSED 都会是
// 一句谎言，不需要区分是哪一句）；卸载成功 → 按 cmdType 是 STOP 还是 PAUSE 分开
// 取值，PAUSE 报 PAUSED、STOP（以及任何防御性的默认值）报 STOPPED。
func resultingStateOnStop(cmdType iproto.PipelineCmdType, unloadErr error) iproto.PipelineState {
	if unloadErr != nil {
		return iproto.PipelineState_PIPELINE_STATE_ERROR
	}
	if cmdType == iproto.PipelineCmdType_PIPELINE_CMD_TYPE_PAUSE {
		return iproto.PipelineState_PIPELINE_STATE_PAUSED
	}
	return iproto.PipelineState_PIPELINE_STATE_STOPPED
}

// resultingStateOnStart 是 changeLoad/changeRebuild 命令触发场景下
// cmd.result.resulting_state 的取值：装配成功 → RUNNING（xhive.AddDynamicModules
// 文档"模块在 OnInit 成功后立即启动"，此刻底层模块确实已经进入自己的运行生命
// 周期——更精细的子状态，如 CATCHING_UP/PAUSED，由它们自己后续的 PipelineStatus
// 上报体现，这里只承诺"没有卡在装配阶段"这一个粗粒度事实，与 internal/node/controller/report.go
// reportPipelineStatus 文档"不需要单独覆盖的情况"一节对成功路径的同一个精度
// 承诺）；装配失败 → ERROR。
func resultingStateOnStart(applyErr error) iproto.PipelineState {
	if applyErr != nil {
		return iproto.PipelineState_PIPELINE_STATE_ERROR
	}
	return iproto.PipelineState_PIPELINE_STATE_RUNNING
}

// bestEffortState 返回 m.applied[id] 能提供的最佳猜测状态：存在则按 running/
// paused 位映射到 RUNNING/PAUSED/STOPPED，不存在时如实报告 UNSPECIFIED（连
// "装配与否"都不知道，不能编造一个更具体的状态）——proto/control.proto
// PipelineCmdResult.resulting_state 字段注释要求"success=false 时同样应当据实
// 填充当前实际状态，不因为执行失败就退回 PIPELINE_STATE_UNSPECIFIED"，这是本包
// 在没有走到真正装配/卸载那一步之前（例如 config.pull 本身就失败了）能诚实回答
// 到的精度——不冒充自己知道更细的子状态（CATCHING_UP 等只有数据面模块自己
// 知道）。
func (m *Module) bestEffortState(pipelineID string) iproto.PipelineState {
	sides := m.sidesOf(pipelineID)
	outcomes := make([]cmdSideOutcome, 0, len(sides))
	for _, side := range sides {
		outcomes = append(outcomes, cmdSideOutcome{role: side.role, state: m.sideState(m.applied[side])})
	}
	return aggregateCmdState(outcomes)
}

// sideState 是**单侧**的最佳猜测状态，bestEffortState 的按侧部分。
func (m *Module) sideState(ap appliedPipeline) iproto.PipelineState {
	if m.runtimeFaults[keyOf(ap.PipelineConfigItem)] {
		return iproto.PipelineState_PIPELINE_STATE_ERROR
	}
	if ap.unloadUncertain {
		return iproto.PipelineState_PIPELINE_STATE_UNSPECIFIED
	}
	if ap.running {
		return iproto.PipelineState_PIPELINE_STATE_RUNNING
	}
	if ap.paused {
		return iproto.PipelineState_PIPELINE_STATE_PAUSED
	}
	return iproto.PipelineState_PIPELINE_STATE_STOPPED
}

// logCmdExecuted records completed lifecycle execution with command identity.
func logCmdExecuted(pipelineID string, key cmdPipelineKey, execKind string, execErr error, extra ...slog.Attr) {
	attrs := []any{
		slog.String("pipeline_id", pipelineID), slog.Uint64("cmd_seq", key.CmdSeq),
		slog.String("cmd_type", key.Type.String()), slog.String("exec_kind", execKind),
	}
	for _, a := range extra {
		attrs = append(attrs, a)
	}
	if execErr != nil {
		slog.Warn("cmd.pipeline execution failed", append(attrs, slog.Any("error", execErr))...)
		return
	}
	slog.Info("cmd.pipeline executed", attrs...)
}

// onCmdPipeline deduplicates authenticated lifecycle commands and dispatches
// role-scoped execution. Pending work retains delivery ownership until completion.
func (m *Module) onCmdPipeline(cmd *iproto.PipelineCmd, ack iAckHandle) {
	if m.identityRetired {
		m.discard(ack)
		return
	}
	id := cmd.GetPipelineId()
	if id == "" {
		slog.Warn("received cmd.pipeline with empty pipeline_id, ignoring")
		m.ack(ack)
		return
	}

	key := cmdPipelineKey{ConfigVersion: cmd.GetConfigVersion(), CmdSeq: cmd.GetCmdSeq(), Type: cmd.GetType()}
	for deletedKey := range m.deletedRoleVersions {
		if deletedKey.pipelineID == id {
			m.ack(ack)
			return
		}
	}
	for deletingKey := range m.deletingRoles {
		if deletingKey.pipelineID == id {
			m.ack(ack)
			return
		}
	}
	if prev, ok := m.cmdPipelineResolved[cmdDedupSlot{pipelineID: id, cmdType: key.Type}]; ok && prev == key && cmd.GetType() != iproto.PipelineCmdType_PIPELINE_CMD_TYPE_REINIT && (key.Type != iproto.PipelineCmdType_PIPELINE_CMD_TYPE_START && key.Type != iproto.PipelineCmdType_PIPELINE_CMD_TYPE_RESUME || !m.pipelineRuntimeFailed(id)) {
		slog.Debug("resolved pipeline command acknowledged", slog.String("pipeline_id", id), slog.Uint64("config_version", key.ConfigVersion), slog.Uint64("cmd_seq", key.CmdSeq), slog.Any("cmd_type", key.Type))
		// 与这个 (pipeline_id, cmd_type) 槽位上一次收敛的键完全相同：上一次要么
		// 成功、要么判定为
		// 不可重试的失败（见 cmdPipelineKey"去重表记的是已收敛"一节），这次不
		// 重复执行，记录 Debug 确认后直接 ack——不是需要重试的失败。如果上一次是
		// 可重试的失败，prev 根本不会等于 key（那种情况从未写入过这张表），会
		// 落到下面正常走执行分支，不会停在这里。
		m.ack(ack)
		return
	}

	if m.pipelineBusy(id) {
		// Serialize new lifecycle work behind pending assembly or unload for this pipeline.
		slog.Warn("pipeline command conflicts with an in-flight operation, requeueing",
			slog.String("pipeline_id", id), slog.Uint64("cmd_seq", cmd.GetCmdSeq()))
		if err := ack.NackRequeue(); err != nil {
			slog.Error("requeue cmd.pipeline failed", slog.String("pipeline_id", id), slog.Any("error", err))
		}
		return
	}

	slog.Info("executing cmd.pipeline",
		slog.String("pipeline_id", id), slog.Uint64("config_version", cmd.GetConfigVersion()),
		slog.Uint64("cmd_seq", cmd.GetCmdSeq()), slog.String("cmd_type", cmd.GetType().String()))

	switch cmd.GetType() {
	case iproto.PipelineCmdType_PIPELINE_CMD_TYPE_STOP, iproto.PipelineCmdType_PIPELINE_CMD_TYPE_PAUSE:
		m.execStopPause(cmd, ack)
	case iproto.PipelineCmdType_PIPELINE_CMD_TYPE_START, iproto.PipelineCmdType_PIPELINE_CMD_TYPE_RESUME:
		m.execStartResume(cmd, ack)
	case iproto.PipelineCmdType_PIPELINE_CMD_TYPE_REINIT:
		m.execReinit(cmd, ack)
	case iproto.PipelineCmdType_PIPELINE_CMD_TYPE_RESTART,
		iproto.PipelineCmdType_PIPELINE_CMD_TYPE_SET_POSITION,
		iproto.PipelineCmdType_PIPELINE_CMD_TYPE_SKIP_ERROR,
		iproto.PipelineCmdType_PIPELINE_CMD_TYPE_DELETE_QUEUE:

		m.execUnsupported(id, key, "CMD_TYPE_NOT_IMPLEMENTED",
			fmt.Sprintf("%s 本轮未实现", cmd.GetType()), ack)
	default:
		// PIPELINE_CMD_TYPE_UNSPECIFIED 或协议未来新增、本包还不认识的取值——按
		// "未识别，重试也不会变好"处理，与上面显式排除的四种同样 ack、不 requeue，
		// 区别只在错误文案，方便排障时分辨"知道这个类型但没做"与"根本不认识这个
		// 取值"。
		m.execUnsupported(id, key, "CMD_TYPE_UNRECOGNIZED",
			fmt.Sprintf("未识别的 PipelineCmdType: %s", cmd.GetType()), ack)
	}
}

// publishCmdResultThenAck confirms the receipt before resolving its delivery.
// Execution failures are acknowledged after their receipt is confirmed; Manager
// schedules their retries with bounded backoff. Only successful or explicitly
// permanent outcomes enter persistent dedup. Unknown/retryable failures keep the
// same command eligible for the next Manager dispatch.
//
// requeueOnFailure is reserved for work not yet executed (busy or failed config
// pull). Receipt publication and critical dedup failures also requeue, because
// delivery has not completed. They never mark an unresolved command as done.
//
// Persistent dedup still precedes Ack. Applied configuration is enqueued by the
// finishApply/finishStop deferred completion path before this method is called;
// both documents share the store file and its transactional flush. Publication
// runs first so a failed receipt cannot leave a dedup key that swallows redelivery.
// All mutable controller state is captured and updated in the event loop; only
// immutable snapshots and network/store work cross into the AsyncGo worker.
func (m *Module) publishCmdResultThenAck(taskKey, pipelineID string, key cmdPipelineKey, result *iproto.CmdResult, ack iAckHandle, requeueOnFailure bool) {
	var publishErr error
	var persistErr error
	// 落盘用的快照在事件循环里取（此刻就是 cb 之外的调用方上下文，与
	// markCmdResolved 同一个执行上下文），不在 f 里读 m.cmdPipelineResolved——
	// 那张 map 只在事件循环里读写、不加锁（Module.cmdPipelineResolved 文档），
	// 在 f 的 goroutine 里读它就是一次真正的数据竞争。
	var pending map[cmdDedupSlot]cmdPipelineKey
	resolved := result.GetSuccess() || result.GetPipeline().Retryable != nil && !result.GetPipeline().GetRetryable()
	if !requeueOnFailure && resolved {
		pending = make(map[cmdDedupSlot]cmdPipelineKey, len(m.cmdPipelineResolved)+1)
		maps.Copy(pending, m.cmdPipelineResolved)
		pending[cmdDedupSlot{pipelineID: pipelineID, cmdType: key.Type}] = key
	}
	nowMS := m.now().UnixMilli()
	m.AsyncGo(taskKey, func() {
		publishErr = m.publishCmdResult(context.Background(), result)
		if publishErr != nil {
			return
		}

		if pending != nil {
			persistErr = persistCmdDedup(context.Background(), m.opts.Store, pending, nowMS)
		}
	}, func() {
		if publishErr != nil {
			slog.Warn("publish cmd.result failed",
				slog.Uint64("cmd_seq", result.GetCmdSeq()), slog.Any("error", publishErr))
			if err := ack.NackRequeue(); err != nil {
				slog.Error("requeue cmd.pipeline failed", slog.Uint64("cmd_seq", result.GetCmdSeq()), slog.Any("error", err))
			}
			return
		}
		// 不变量 2：落盘失败 ⇒ requeue，且不 ack、不 markCmdResolved。
		// 放在 requeueOnFailure 判断之前：本来要 requeue 的照旧 requeue，
		// 本来要 ack 的因为落盘失败改为 requeue。
		if persistErr != nil {
			slog.Error("persist cmd dedup failed, requeueing instead of acking",
				slog.String("pipeline_id", pipelineID),
				slog.Uint64("cmd_seq", result.GetCmdSeq()), slog.Any("error", persistErr))
			if err := ack.NackRequeue(); err != nil {
				slog.Error("requeue cmd.pipeline failed",
					slog.Uint64("cmd_seq", result.GetCmdSeq()), slog.Any("error", err))
			}
			return
		}
		if requeueOnFailure {
			if err := ack.NackRequeue(); err != nil {
				slog.Error("requeue cmd.pipeline failed",
					slog.Uint64("cmd_seq", result.GetCmdSeq()), slog.Any("error", err))
			}
			return
		}
		if resolved {
			m.markCmdResolved(pipelineID, key)
		}
		m.ack(ack)
	})
}

// execUnsupported returns an explicit non-retryable command failure.
func (m *Module) execUnsupported(pipelineID string, key cmdPipelineKey, code, message string, ack iAckHandle) {
	slog.Warn("cmd.pipeline type not supported this round",
		slog.String("pipeline_id", pipelineID), slog.Uint64("cmd_seq", key.CmdSeq),
		slog.String("code", code), slog.String("message", message))
	result := &iproto.CmdResult{
		CmdSeq:  key.CmdSeq,
		Success: false,
		Error:   &iproto.Error{Code: code, Message: message},
		Target: &iproto.CmdResult_Pipeline{Pipeline: &iproto.PipelineCmdResult{
			PipelineId:     pipelineID,
			ConfigVersion:  key.ConfigVersion,
			Retryable:      new(false),
			ResultingState: m.bestEffortState(pipelineID),

			CmdType: key.Type,
		}},
	}
	m.publishCmdResultThenAck("controller.cmdunsupported."+pipelineID, pipelineID, key, result, ack, false)
}

// execStopPause 处理 STOP/PAUSE（changeKind 文档"推论一"：两者节点侧动作收敛为
// 同一个 changeStop 机制；但上报给 Manager 的目标状态不收敛，见
// resultingStateOnStop 文档——本函数三个分支的 ResultingState/reportPipelineStatus
// 取值全部经它统一判断，不在这里各自重复 STOP/PAUSE 的分支逻辑）。
func (m *Module) execStopPause(cmd *iproto.PipelineCmd, ack iAckHandle) {
	id := cmd.GetPipelineId()
	key := cmdPipelineKey{ConfigVersion: cmd.GetConfigVersion(), CmdSeq: cmd.GetCmdSeq(), Type: cmd.GetType()}
	cmdType := key.Type

	sides := m.sidesOf(id)
	if len(sides) == 0 {
		// When no local role has configuration, pull it explicitly before executing intent.
		result := buildCmdResult(id, key, resultingStateOnStop(cmdType, nil), nil, nil)
		m.publishCmdResultThenAck("controller.cmdnoop."+id, id, key, result, ack, false)
		return
	}

	f := &cmdFanout{ack: ack, pipelineID: id, key: key, pendingSides: len(sides)}
	for _, side := range sides {
		ap := m.applied[side]
		cmdCtx := &cmdContext{fanout: f, role: side.role}
		if ap.running {
			m.startApply(pipelinePlan{item: ap.PipelineConfigItem, kind: changeStop, cmd: cmdCtx})
			continue
		}
		// 与 len(sides)==0 分支的关键差别：这里 ap.PipelineConfigItem 是有的，
		// 因此必须调用 reportPipelineStatus——否则 Manager 的 actual_state
		// 不会更新，会持续认为 (desired, actual) 未收敛而不断重发同一条指令。
		// 目标状态按这次收到的指令类型取（不是按上一次已经是什么），与
		// resultingStateOnStop 处理其它分支同一个判断标准：重复收到的 STOP 确认
		// STOPPED、重复收到的 PAUSE 确认 PAUSED，即使这一侧上一次进入"不在跑"
		// 状态时走的是另一种指令。
		//
		// 只在取值真的变化时才重新持久化：重复收到同一种指令（最常见的情形）不该
		// 每次都产生一次 store 写入。unloadUncertain 在本分支恒为 false（那个状态
		// 下 running 保持 true，走不到这里），因此原样复制 ap 的其余字段不会把一个
		// 不确定态写进快照。
		if paused := cmdType == iproto.PipelineCmdType_PIPELINE_CMD_TYPE_PAUSE; paused != ap.paused {
			ap.paused = paused
			m.applied[side] = ap
			if err := persistAppliedState(m.opts.Store, m.applied, m.now().UnixMilli()); err != nil {
				slog.Warn("persist applied state failed",
					slog.String("pipeline_id", id), slog.Any("role", side.role), slog.Any("error", err))
			}
		}
		target := resultingStateOnStop(cmdType, nil)
		m.reportPipelineStatus(ap.PipelineConfigItem, target, "")
		// 这一侧没有任何模块级动作，直接回报结果；全部侧都回报之后由
		// finishCmdFanout 统一发一条回执并 ack（不在这里各发一条）。
		m.completeCmdSide(cmdCtx, nil, target)
	}
}

// execStartResume 处理 START/RESUME（changeKind 文档"推论一"：两者节点侧收敛为
// 同一机制）。三种情形：已经在跑（幂等短路）、曾经装配过但当前已停止（恢复，不
// 需要拉取配置）、本地完全没有这条 pipeline 的配置（"节点在收到
// cmd.pipeline START 时，若尚无该 pipeline 的配置，必须先拉取"——节点只能连业务库
// 与 AMQP，够不到管理库，config.pull 是唯一取配置途径）。
func (m *Module) execStartResume(cmd *iproto.PipelineCmd, ack iAckHandle) {
	id := cmd.GetPipelineId()
	key := cmdPipelineKey{ConfigVersion: cmd.GetConfigVersion(), CmdSeq: cmd.GetCmdSeq(), Type: cmd.GetType()}

	if sides := m.sidesOf(id); len(sides) > 0 {
		f := &cmdFanout{ack: ack, pipelineID: id, key: key, pendingSides: len(sides)}
		for _, side := range sides {
			ap := m.applied[side]
			cmdCtx := &cmdContext{fanout: f, role: side.role}
			if ap.running && !ap.unloadUncertain && !m.runtimeFaults[side] {
				// 这一侧已经在跑（且确认，不是不确定）：START/RESUME 的意图对它
				// 已经成立，幂等短路。报 RUNNING 是"controller 认为的应然状态"，
				// 不是从数据面模块实时读到的精确子状态——与 resultingStateOnStart
				// 同一个精度承诺；这一侧没有发生任何装配动作，同样不重复上报
				// PipelineStatus（理由同 execStopPause 对称分支）。
				m.completeCmdSide(cmdCtx, nil, iproto.PipelineState_PIPELINE_STATE_RUNNING)
				continue
			}
			// Previously stopped or uncertain roles require lifecycle cleanup before restart.
			m.startApply(pipelinePlan{item: ap.PipelineConfigItem, kind: changeRebuild, cmd: cmdCtx})
		}
		return
	}

	// 本节点这条 pipeline 的任何一侧都没有配置：先发起一次定向 config.pull。
	// m.pendingPull[id] 在这次拉取期间保持为真——与 startApply 自己的 pending
	// 语义一致（onCmdPipeline 顶部的忙碌检查正是为了保护这个窗口），拉取完成后
	// 如果成功则用 changeLoad 走一遍正常装配（协议起点，符合"真正意义上从未跑过"
	// 的既有判断，见 resolveStartGTIDSet 文档"changeLoad 不等于从未跑过"一节
	// ——那里的判断针对的是"m.applied 是否有记录"，这里恰恰是真的没有）。
	m.pendingPull[id] = true
	var reply *iproto.ConfigPullReply
	var pullErr error
	m.AsyncGo("controller.cmdpull."+id, func() {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTargetedPullTimeout)
		defer cancel()
		reply, pullErr = m.pullConfig(ctx, []string{id})
	}, func() {
		m.applyCmdPullResult(id, key, reply, pullErr, ack)
	})
}

// 为什么要定序：一条指令在两侧的派发顺序会决定日志顺序、以及两侧都走
// startApply 时谁先拿到 AsyncGo——随机序会让同一份输入产生不同的可观察行为，
// 测试要么变得不可断言、要么只能断言成集合。这与 Manager 侧
// groupPlansByNode "保持 source→target 的发现顺序，不用 map 遍历"是同一条理由。
func (m *Module) sidesOf(pipelineID string) []appliedKey {
	var out []appliedKey
	for _, role := range []iproto.PipelineRole{
		iproto.PipelineRole_PIPELINE_ROLE_SOURCE,
		iproto.PipelineRole_PIPELINE_ROLE_TARGET,
	} {
		k := appliedKey{pipelineID: pipelineID, role: role}
		if _, existed := m.applied[k]; existed {
			out = append(out, k)
		}
	}
	return out
}

// applyCmdPullResult resumes a lifecycle command after its directed configuration pull.
func (m *Module) applyCmdPullResult(id string, key cmdPipelineKey, reply *iproto.ConfigPullReply, pullErr error, ack iAckHandle) {
	// 必须先清除，再决定要不要调用 startApply——它内部会自己按侧重新设置
	// m.pending[appliedKey]=true 承接真正的装配阶段；不清除的话 onCmdPipeline
	// 的忙碌检查（pipelineBusy）会一直看到这条 pipeline 在忙。
	delete(m.pendingPull, id)
	if m.identityRetired {
		m.discard(ack)
		return
	}

	if pullErr != nil {
		slog.Error("cmd.pipeline start: targeted config pull failed",
			slog.String("pipeline_id", id), slog.Uint64("cmd_seq", key.CmdSeq), slog.Any("error", pullErr))
		result := &iproto.CmdResult{
			CmdSeq:  key.CmdSeq,
			Success: false,
			Error:   &iproto.Error{Code: "CONFIG_PULL_FAILED", Message: safeerror.Public(pullErr)},
			Target: &iproto.CmdResult_Pipeline{Pipeline: &iproto.PipelineCmdResult{
				ConfigVersion: key.ConfigVersion, Retryable: new(true),
				PipelineId: id, ResultingState: m.bestEffortState(id), CmdType: key.Type,
			}},
		}
		// 拉取失败通常是网络/Manager 暂时不可达一类瞬时问题，requeue 让它有
		// 机会更快被重试，不必等到 Manager 对账循环的下一轮（最多 15s）——且不
		// markCmdResolved：判定为可重试，下次同键到达必须重新落到这条分支。
		m.publishCmdResultThenAck("controller.cmdpullreply."+id, id, key, result, ack, true)
		return
	}

	var items []*iproto.PipelineConfigItem
	for _, it := range reply.GetConfigs() {
		if it.GetPipelineId() == id && !it.GetDeleted() && !it.GetOwnershipReleased() {
			items = append(items, it)
		}
	}
	if len(items) == 0 {
		slog.Error("cmd.pipeline start: pipeline unknown to manager after targeted pull",
			slog.String("pipeline_id", id), slog.Uint64("cmd_seq", key.CmdSeq))
		result := &iproto.CmdResult{
			CmdSeq:  key.CmdSeq,
			Success: false,
			Error:   &iproto.Error{Code: "PIPELINE_UNKNOWN", Message: "config.pull 回复未包含这条 pipeline_id"},
			Target: &iproto.CmdResult_Pipeline{Pipeline: &iproto.PipelineCmdResult{
				ConfigVersion: key.ConfigVersion, Retryable: new(false),
				PipelineId: id, ResultingState: iproto.PipelineState_PIPELINE_STATE_UNSPECIFIED,
				CmdType: key.Type,
			}},
		}
		// 不 requeue、markCmdResolved：Manager 自己的 config.pull 回复都不认识
		// 这条 pipeline_id 了，重试这条 START 不会有不同结果，判定为不可重试。
		m.publishCmdResultThenAck("controller.cmdpullreply."+id, id, key, result, ack, false)
		return
	}
	f := &cmdFanout{ack: ack, pipelineID: id, key: key, pendingSides: len(items)}
	for _, item := range items {
		m.startApply(pipelinePlan{
			item: item,
			kind: changeLoad,
			cmd:  &cmdContext{fanout: f, role: item.GetRole()},
		})
	}
}

// 两张表都要查：pending 按 appliedKey（已知角色的装配/卸载），pendingPull 按
// pipeline_id（还不知道角色的拉取窗口，见 internal/node/controller/controller.go 该字段文档）。
func (m *Module) pipelineBusy(pipelineID string) bool {
	if m.pendingPull[pipelineID] {
		return true
	}
	for _, role := range []iproto.PipelineRole{
		iproto.PipelineRole_PIPELINE_ROLE_SOURCE,
		iproto.PipelineRole_PIPELINE_ROLE_TARGET,
	} {
		if m.pending[appliedKey{pipelineID: pipelineID, role: role}] {
			return true
		}
	}
	return false
}

// 不校验签名：与 onConfigChangedMessage 同一理由，
// 节点侧目前没有可用于校验 Manager 下行消息的公钥材料。
//
// 解决手法与 internal/node/controller/dispatch.go onConfigChanged 处理 config.changed 完全一致（f 在独立
// goroutine 执行、cb 回到事件循环，"cb 本身就在事件循环里执行"是 AsyncGo 自己的
// 契约，与调用 AsyncGo 本身所在的 goroutine 无关，见 xhive Skeleton.AsyncGo 文档）
// ——区别只在 f 留空：onConfigChanged 的 f 里有真正需要独立 goroutine 的阻塞 IO
// （config.pull），而这里解码已经在本方法同步完成，没有别的阻塞工作要做，AsyncGo
// 在这里纯粹当"跳到事件循环"的手段用，cb 里才是 onCmdPipeline 的全部真正逻辑。
// taskKey 按 pipeline_id 区分只是为了 DumpStat 统计更好读，不是唯一性约束
// （AsyncGo name 参数本身就只用于统计分组）。
func (m *Module) onCmdPipelineMessage(msg *transport.Message, ack iAckHandle) error {
	var cmd iproto.PipelineCmd
	if _, err := transport.ParseAndDecode(msg, &cmd, nil); err != nil {
		return fmt.Errorf("controller: 解析 cmd.pipeline 失败: %w", err)
	}
	m.AsyncGo("controller.cmdpipeline."+cmd.GetPipelineId(), func() {}, func() {
		m.dispatchPipelineCommand(&cmd, ack)
	})
	return nil
}

// The control consumer starts before startup RPCs and disk reads finish. A
// queued delivery must retain its retry eligibility until the dedup table and
// applied configuration have been restored in the controller event loop.
func (m *Module) dispatchPipelineCommand(cmd *iproto.PipelineCmd, ack iAckHandle) {
	if m.identityRetired {
		m.discard(ack)
		return
	}
	if !m.mirrorReady {
		slog.Debug("pipeline command awaits startup state", slog.String("pipeline_id", cmd.GetPipelineId()), slog.Uint64("cmd_seq", cmd.GetCmdSeq()))
		if err := ack.NackRequeue(); err != nil {
			slog.Error("requeue pipeline command before startup failed", slog.String("pipeline_id", cmd.GetPipelineId()), slog.Any("error", err))
		}
		return
	}
	m.onCmdPipeline(cmd, ack)
}

// cmdCtxType 取 cmdCtx 携带的指令类型；cmdCtx 为 nil（配置面触发，或直接构造裸
// pipelinePlan{kind: changeStop} 的既有测试）时返回零值
// PIPELINE_CMD_TYPE_UNSPECIFIED，消费方按"非 PAUSE"处理。
func cmdCtxType(cmdCtx *cmdContext) iproto.PipelineCmdType {
	if cmdCtx == nil {
		return iproto.PipelineCmdType_PIPELINE_CMD_TYPE_UNSPECIFIED
	}
	return cmdCtx.fanout.key.Type
}
