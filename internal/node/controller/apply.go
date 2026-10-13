package controller

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"math"

	_ "github.com/go-sql-driver/mysql" // 注册 "mysql" driver，供 resolveLatestStartGTID 的 sql.Open 使用

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/position"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// applyPlans 依次触发 diffConfigs 产出的每一条动作（NODE-CFG-02 的执行阶段）。
func (m *Module) applyPlans(plans []pipelinePlan) {
	for _, p := range plans {
		m.startApply(p)
	}
}

// authorizedToAssemble requires lifecycle intent or verified prior applied state.
// Receiving configuration alone does not authorize starting data movement.
func (m *Module) authorizedToAssemble(p pipelinePlan) bool {
	if p.kind == changeLoad || p.kind == changeRebuild || p.kind == changeHotUpdate || p.kind == changeNone {
		if identity := p.item.GetCommon().GetReinitIdentity(); identity != nil {
			if !validReinitIdentity(identity, p.item.GetCommon().GetRunId()) {
				return false
			}
			if cmdCtxType(p.cmd) == iproto.PipelineCmdType_PIPELINE_CMD_TYPE_REINIT {
				return true
			}
			if bundle := p.item.GetCommon().GetStateBundle(); p.cmd != nil && bundle != nil && iproto.ValidateStateBundle(bundle) == nil && bundle.PipelineId == p.item.PipelineId && bundle.RunId == p.item.GetCommon().GetRunId() && bundle.Role == p.item.Role && bundle.DestinationNodeId == m.opts.NodeID && bundle.ConfigVersion <= p.item.ConfigVersion && bundle.SnapshotUuid == identity.GetSnapshotUuid() {
				return true
			}
			for _, records := range []map[appliedKey]appliedPipeline{m.applied, m.priorRunApplied} {
				if prior, exists := records[keyOf(p.item)]; exists && prior.GetCommon().GetRunId() == p.item.GetCommon().GetRunId() && (prior.running || p.cmd != nil || p.kind == changeNone) && sameReinitIdentity(prior.GetCommon().GetReinitIdentity(), identity) {
					return true
				}
			}
			return false
		}
	}
	if p.kind != changeLoad {
		return true
	}
	if p.cmd != nil {
		return true
	}
	prior, existed := m.priorRunApplied[keyOf(p.item)]
	return existed && prior.running
}

// startApply runs in Controller's actor. Blocking assembly runs in a worker;
// completion returns to the actor before updating authoritative state.
func (m *Module) startApply(p pipelinePlan) {
	if m.identityRetired {
		return
	}
	id := p.item.GetPipelineId()

	key := keyOf(p.item)
	_, deleted := m.deletedRoleVersions[key]
	if (deleted || m.deletingRoles[key]) && p.kind != changeUnload && p.kind != changeReject {
		if p.cmd != nil {
			m.completeCmdSide(p.cmd, fmt.Errorf("pipeline was deleted"), iproto.PipelineState_PIPELINE_STATE_STOPPED)
		}
		return
	}
	if m.pending[key] {
		m.trailingItem[key] = p.item
		slog.Warn("pipeline apply already in flight, recorded latest config for re-evaluation after it completes",
			slog.String("pipeline_id", id), slog.Any("role", p.item.GetRole()),
			slog.String("kind", p.kind.String()))
		return
	}
	// A stopped module has no live instance to rebuild. Preserve that state when
	// refreshing configuration; the next explicit START loads the new revision.
	if prior, exists := m.applied[key]; exists && !prior.running && !prior.unloadUncertain && p.cmd == nil && prior.GetCommon().GetRunId() == p.item.GetCommon().GetRunId() && sameReinitIdentity(prior.GetCommon().GetReinitIdentity(), p.item.GetCommon().GetReinitIdentity()) && (p.kind == changeRebuild || p.kind == changeHotUpdate) {
		p.kind = changeNone
	}
	if !m.authorizedToAssemble(p) {

		slog.Info("pipeline config received but not assembled, no start authorization yet",
			slog.String("pipeline_id", id), slog.Any("role", p.item.GetRole()),
			slog.Uint64("config_version", p.item.GetConfigVersion()))
		if p.cmd != nil {
			m.completeCmdSide(p.cmd, fmt.Errorf("new reinit run requires matching REINIT command"), iproto.PipelineState_PIPELINE_STATE_ERROR)
		}
		return
	}
	m.pending[key] = true

	if p.kind == changeReject {
		item := p.item
		var publishErr error
		m.AsyncGo("controller.reject."+id, func() {
			publishErr = m.publishApplyResult(context.Background(), buildRejectedApplyResult(item, p.previousConfigVersion))
		}, func() {

			delete(m.pending, key)
			if publishErr != nil {
				slog.Warn("publish stale config rejection failed",
					slog.String("pipeline_id", id), slog.Any("role", item.GetRole()), slog.Any("error", publishErr))
			}
			m.reevaluateTrailing(key)
		})
		return
	}

	if p.kind == changeNone {
		// The other role may own the changed fields, but this live role must
		// still observe the confirmed version. Use its existing transaction
		// barrier to advance the version without reconstructing any modules.
		if prior := m.applied[key]; prior.running && p.item.GetConfigVersion() > prior.GetConfigVersion() {
			if updater, ok := m.loader.(iPipelineHotUpdater); ok && updater.CanHotUpdate(p.item) {
				m.startHotUpdate(p.item, updater, changeNone)
				return
			}
		}
		// 没有任何模块级动作，但仍然要按"应用成功"回执并刷新持久化快照的
		// config_version（见 internal/node/controller/diff.go changeNone 文档、apply_state.go"不单独持久化
		// 期望状态"一节）。回执发布同样是阻塞 IO，走同一条 AsyncGo 路径，不在事件
		// 循环里直接调用。
		item := p.item
		var publishErr error
		m.AsyncGo("controller.noop."+id, func() {
			result := buildApplyResult(item, changeNone, nil)
			publishErr = m.publishApplyResult(context.Background(), result)
		}, func() {
			// A configuration-only refresh cannot start a stopped pipeline.
			// Keep its actual stopped/paused state while refreshing the item.
			if prior, exists := m.applied[key]; exists && !prior.running {
				delete(m.pending, key)
				prior.PipelineConfigItem = item
				m.applied[key] = prior
				if publishErr != nil {
					slog.Warn("publish apply result failed", slog.String("pipeline_id", id), slog.Any("error", publishErr))
				}
				if err := persistAppliedState(m.opts.Store, m.applied, m.now().UnixMilli()); err != nil {
					slog.Warn("persist applied state failed", slog.String("pipeline_id", id), slog.Any("error", err))
				}
				m.reevaluateTrailing(key)
				return
			}

			m.finishApply(item, nil, publishErr, nil, false)
		})
		return
	}

	if p.kind == changeUnload {
		m.startPipelineDeletion(p.item)
		return
	}

	// 幂等短路：m.applied[id] 已经存在且 running 已经是 false（重复收到 STOP，或者
	// STOP 与 PAUSE 各自触发同一个动作时先后到达）时不重复调用 m.loader.Unload——
	// "同一个意图会在每次配置版本变化后被重放，必须对已处于目标状态的 pipeline
	// 表现为 no-op"这条硬约束（F 项工单原话）的节点侧落地；重复调用 Unload 对一个
	// 已经不存在的动态模块只会产生一次可以避免的失败（internal/node/controller/loader.go Unload 文档：对
	// SOURCE 角色，模块不存在与真正卸载失败在公开 API 层面无法区分，一律报错）。
	if p.kind == changeStop {
		item := p.item
		role := item.GetRole()
		old, existed := m.applied[key]
		alreadyStopped := existed && !old.running
		cmdCtx := p.cmd
		var unloadErr error
		var publishErr error
		m.AsyncGo("controller.stop."+id, func() {
			if !alreadyStopped {
				unloadErr = m.loader.Unload(id, role)
			}
			if cmdCtx != nil {

				logCmdExecuted(id, cmdCtx.fanout.key, "stop_or_pause", unloadErr,
					slog.Any("role", role))
			} else {
				result := buildApplyResult(item, changeStop, unloadErr)
				publishErr = m.publishApplyResult(context.Background(), result)
			}
		}, func() {
			m.finishStop(item, unloadErr, publishErr, cmdCtx)
		})
		return
	}

	if p.kind == changeHotUpdate && p.cmd == nil && !m.runtimeFaults[key] && !m.applied[key].unloadUncertain {
		if updater, ok := m.loader.(iPipelineHotUpdater); ok && updater.CanHotUpdate(p.item) {
			m.startHotUpdate(p.item, updater, changeHotUpdate)
			return
		}
	}

	// changeLoad/changeHotUpdate/changeRebuild：统一先（如果已有旧实例）卸载、
	// 再装配新的。支持原地更新的增量两侧已在上方分流；全量热更路径尚需接入。
	startPos := m.resolveStartGTIDSet(p)
	item := p.item
	role := item.GetRole()
	kind := p.kind
	cmdCtx := p.cmd
	// A lifecycle command owns its unload decision; configuration application must
	// not unload the same role again.
	needsUnloadFirst := (kind == changeHotUpdate || kind == changeRebuild) && cmdCtx == nil
	if prior, existed := m.applied[key]; existed && (prior.unloadUncertain || m.runtimeFaults[key] || cmdCtxType(cmdCtx) == iproto.PipelineCmdType_PIPELINE_CMD_TYPE_REINIT && prior.running) {
		needsUnloadFirst = true
	}
	instance := iproto.PipelineRuntimeStatus{PipelineID: id, Role: role, RunID: item.GetCommon().GetRunId(), ConfigVersion: item.GetConfigVersion(), InstanceID: uuidx.New().String()}
	m.runtimeInstances[key] = instance
	delete(m.runtimeFaults, key)
	loadCtx := iproto.WithPipelineInstance(context.Background(), instance.InstanceID)

	var applyErr error
	var publishErr error
	var unloadPending bool

	var preUnloadFailed bool
	// 此刻已接受本侧装配；先排入状态上报，再启动可能发出 RUNNING 的数据面模块。
	m.reportPipelineStatus(item, iproto.PipelineState_PIPELINE_STATE_DEPLOYING, "")
	m.AsyncGo("controller.apply."+id, func() {
		defer func() {
			if recover() != nil {
				applyErr = fmt.Errorf("controller: pipeline assembly panicked")
				preUnloadFailed = unloadPending
			}
		}()
		if needsUnloadFirst {
			unloadPending = true
			// 这个前提绝大多数情况下成立：xhive.RemoveDynamicModule 文档承诺"同步
			// 阻塞，返回时已经走完 OnDestroy→Close"（已读 xmapst/xhive vendored
			// 源码 github.com/xmapst/xhive/module.go shutdownModule 确认），binlogin 自己的 runSession 用
			// ctx-aware 的 streamer.GetEvent(ctx) 作为循环体（internal/node/pipeline/binlogin/session.go），模块
			// cancel 后立即以 context.Canceled 退出、defer syncer.Close() 随即
			// 执行——正常路径下 Unload 返回即代表连接确已关闭。
			//
			// 但有一种框架自己承认、不是假设的例外：Serve 的 goroutine 没有在
			// shutdown 超时内退出时，shutdownModule 直接放弃、跳过 OnDestroy/
			// Close，返回 false 且"再也不会有人补做这次销毁"（github.com/xmapst/xhive/module.go 同一处
			// 注释）。prodLoader.Unload（internal/node/controller/loader.go）已经改为把这种情况当错误往上
			// 传（见该方法文档），此处必须真的检查这个错误、不能像以前一样丢弃——
			// 一旦发生，旧连接很可能仍然存活，此时如果照常往下走、用同一个
			// server_id 开一条新的探测连接，源库会踢掉其中一条，现象是一次看起来
			// 后续网络错误，且没有任何线索指向"这其实是 server_id
			// 冲突"——宁可在这里提前失败并如实报告原因，也不要制造一个更难排查的
			// 二次故障。
			if unloadErr := m.loader.Unload(id, role); unloadErr != nil {
				applyErr = fmt.Errorf("controller: 重建/热更前卸载旧实例失败，已放弃本次装配: %w", unloadErr)
				preUnloadFailed = true
				// Return lifecycle-command failures through cmd.result rather than only config.apply.result.
				if cmdCtx != nil {

					logCmdExecuted(id, cmdCtx.fanout.key, "start_or_resume", applyErr,
						slog.Any("role", role))
				} else {
					result := buildApplyResult(item, kind, applyErr)
					publishErr = m.publishApplyResult(context.Background(), result)
				}
				return
			}
			unloadPending = false
		}
		switch role {
		case iproto.PipelineRole_PIPELINE_ROLE_SOURCE:
			// convertStartPosition 可能发起真实的 MySQL 连接（Kind=LATEST/
			// BINLOG_POS），必须留在这个 AsyncGo 的 f 里、事件循环之外执行
			// （"热路径禁止同步等待式调用"）——与本函数其余阻塞
			// IO（LoadSource 本身、下面的回执发布）同一条纪律，不因为它是新加的
			// 一步就破例挪到同步部分。
			//
			// item.GetSource().GetBinlogServerId() 一并传入：只有 Kind=BINLOG_POS
			// 分支会用到它（探测连接复用这个已经全局唯一分配的 server_id，见
			// resolveBinlogPosStartGTID 文档），其余分支忽略这个参数，与
			// convertStartPosition 内部按 Kind 分派、各分支只读自己需要的参数是
			// 同一个既有约定（ep 对 UNSPECIFIED/GTID_SET 分支同样是"传了但不用"）。
			// Snapshot modes obtain their start from the durable L inside the
			// loader; an unrelated start_position must not trigger a second probe.
			var startGTID string
			var convErr error
			if !needsSnapshot(item) {
				startGTID, convErr = m.convertStartPosition(context.Background(),
					item.GetSource().GetEndpoint(), item.GetSource().GetBinlogServerId(), startPos)
			}
			if convErr != nil {
				applyErr = convErr
			} else {
				var provided *string
				if startPos != nil && !needsSnapshot(item) {
					provided = &startGTID
				}
				applyErr = m.loader.LoadSource(loadCtx, item, provided)
			}
		case iproto.PipelineRole_PIPELINE_ROLE_TARGET:
			applyErr = m.loader.LoadTarget(loadCtx, item)
		default:
			applyErr = fmt.Errorf("controller: pipeline %q 收到未知 role %s，无法装配", id, role)
		}
		if cmdCtx != nil {

			logCmdExecuted(id, cmdCtx.fanout.key, "start_or_resume", applyErr,
				slog.Any("role", role))
		} else {
			result := buildApplyResult(item, kind, applyErr)
			publishErr = m.publishApplyResult(context.Background(), result)
		}
	}, func() {
		m.finishApply(item, applyErr, publishErr, cmdCtx, preUnloadFailed)
	})
}

// resolveStartGTIDSet 决定装配这条 pipeline（仅 role=SOURCE 时有意义，target 侧
// 忽略返回值）应该使用协议里的起点、还是留给 binlogin 自己从检查点恢复。
//
// 返回 nil 表示"留空、从检查点恢复"；返回非 nil 表示"必须使用这个协议起点"，具体
// 换算成 binlogin.Options.StartGTIDSet 需要的字符串由 convertStartPosition 按
// Kind 完成（换算本身可能发起真实 MySQL 连接，不能留在本函数——本函数运行在事件
// 循环里，见 startApply 调用处）。
//
// 后果：不区分这两种 changeLoad，会让任何一次普通进程重启都让所有正在跑的
// pipeline 从协议 start_position 重新订阅 binlog，静默绕开真实进度——不是低概率
// 边缘情形，是最常见的重启路径本身。
//
// 下面 changeLoad 分支命中 priorRunApplied 时返回 nil，依赖一条跨
// controller/binlogin 两个模块的不变量（"priorRunApplied 命中 ⟹ binlogin 那边的
// 检查点一定已经落盘"）——完整链路、每一步对应的代码位置、以及钉住它的两条回归
// 测试，见 internal/node/controller/controller.go priorRunApplied 字段文档"依赖的跨模块不变量"一节，这里
// 不重复，只留指针：不管读代码的人是从这个函数进来还是从字段声明进来，都应该能
// 找到同一份说明。
func (m *Module) resolveStartGTIDSet(p pipelinePlan) *iproto.StartPosition {
	if p.item.GetRole() != iproto.PipelineRole_PIPELINE_ROLE_SOURCE {
		return nil
	}
	if p.item.GetSource().GetPositionRevision() > 0 {
		old, existed := m.applied[keyOf(p.item)]
		if !existed {
			old, existed = m.priorRunApplied[keyOf(p.item)]
		}
		if !existed || old.GetCommon().GetRunId() != p.item.GetCommon().GetRunId() || old.GetSource().GetPositionRevision() != p.item.GetSource().GetPositionRevision() {
			return p.item.GetSource().GetStartPosition()
		}
	}
	if p.kind == changeLoad {
		if prior, existed := m.priorRunApplied[keyOf(p.item)]; existed &&
			prior.GetCommon().GetRunId() == p.item.GetCommon().GetRunId() {
			return nil
		}
		return p.item.GetSource().GetStartPosition()
	}
	old, existed := m.applied[keyOf(p.item)]
	if !existed || old.GetCommon().GetRunId() != p.item.GetCommon().GetRunId() {
		return p.item.GetSource().GetStartPosition()
	}
	return nil
}

// convertStartPosition 把 resolveStartGTIDSet 给出的协议起点换算成
// binlogin.Options.StartGTIDSet 需要的 GTID 集合字符串（NODE-POS-04"节点内部一律
// 使用换算后的 GTID 集合"，proto/common.proto StartPosition 文档同一句）。
//
// pos 为 nil：resolveStartGTIDSet 已经判定"留空、让 binlogin 自己从检查点恢复"，
// 直接返回空字符串、不做任何 IO——checkpoint 是否优先于协议起点的判断完全由
// resolveStartGTIDSet 决定，本函数只负责"pos 非 nil 时，按它的 Kind 换算成
// 什么"，两者职责不重叠，也不在本函数里重新判断一遍（完整的检查点优先级推理见
// resolveStartGTIDSet 文档，不在这里重复）。
//
// 未识别的 Kind（default 分支）：协议只增不改语义，未来新增的
// InitPositionKind 值落到这里会被拒绝而不是被静默当成 UNSPECIFIED/GTID_SET 透传
// ——透传一个本函数根本不认识的新 Kind 对应的 GtidSet 字段，语义上可能完全文不对
// 题（新 Kind 可能要求另一套字段组合），错误地把它当成"已经是最终值"直接使用，
// 后果比报错更隐蔽、更危险。新增 Kind 时必须回到这个函数显式加分支，不能指望
// default 兜底出正确行为。
func (m *Module) convertStartPosition(ctx context.Context, ep *iproto.EndpointConn, serverID uint32, pos *iproto.StartPosition) (string, error) {
	if pos == nil {
		return "", nil
	}
	if pos.GetResolved() {
		return m.validateResolvedPosition(ctx, ep, pos.GetGtidSet(), true)
	}
	switch pos.GetKind() {
	case iproto.InitPositionKind_INIT_POSITION_KIND_UNSPECIFIED, iproto.InitPositionKind_INIT_POSITION_KIND_GTID_SET:
		return pos.GetGtidSet(), nil
	case iproto.InitPositionKind_INIT_POSITION_KIND_LATEST:
		return m.resolveLatestStartGTID(ctx, ep)
	case iproto.InitPositionKind_INIT_POSITION_KIND_BINLOG_POS:
		return m.resolveBinlogPosStartGTID(ctx, ep, serverID, pos.GetSourceFile(), pos.GetSourceOffset())
	case iproto.InitPositionKind_INIT_POSITION_KIND_TIMESTAMP:
		return m.resolveConfirmedTimestampGTID(ctx, ep, pos)
	default:
		return "", fmt.Errorf("controller: 未识别的 InitPositionKind(%d)，拒绝执行（协议只增不改语义，新增 kind 需要同步更新 convertStartPosition）", pos.GetKind())
	}
}

// resolveLatestStartGTID 实现 Kind=LATEST（NODE-POS-01"当前最新"）：连接源库、
// 读取 @@GLOBAL.gtid_executed 作为起始 GTID 集合。底层查询已经在
// internal/node/position.ResolveLatest 实现（含只读会话包装），本函数只负责按
// 本包既定的一次性连接生命周期打开/关闭 *sql.DB——用完即关，不放进任何连接池或
// 模块状态，与 internal/node/probe/conntest.go probeConnTest 对"一次性探测连接"的处理方式一致
// （一次装配只查一次，不是需要长期复用的连接）。
//
// DSN 构造复用 internal/node/probe/conntest.go 与 internal/node/pipeline/mysqlout/writer.go 共同的既定模式
// （user:password@tcp(host:port)/，不消费 EndpointConn.Tls/Params）——同一个已经
// 记录过的全仓已知边界，不在本函数里单独解决；真正打通 TLS/params 需要
// mysqlout/binlogin/probe/本函数四个消费方一起接，不属于本任务范围。
//
// # 错误文本在出站边界重新映射
//
// 只用 hostPort 拼错误信息，dsn（含明文密码）只用于 sql.Open，不出现在任何返回
// 错误或日志里。即使底层驱动未来改变错误文案，buildApplyResult 也只使用
// safeerror.Public 映射后的固定文本或数字错误码，不依赖驱动当前恰好不回显凭据。
func (m *Module) resolveLatestStartGTID(ctx context.Context, ep *iproto.EndpointConn) (string, error) {
	hostPort := fmt.Sprintf("%s:%d", ep.GetHost(), ep.GetPort())
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/", ep.GetUsername(), ep.GetPassword(), hostPort)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return "", fmt.Errorf("controller: 解析源库连接参数失败，host=%s: %w", hostPort, err)
	}
	defer func(cleanup func() error) { _ = cleanup() }(db.Close)

	connCtx, cancel := context.WithTimeout(ctx, m.opts.MySQLConnectTimeout)
	defer cancel()

	gset, err := position.ResolveLatest(connCtx, db)
	if err != nil {
		return "", fmt.Errorf("controller: 按 Kind=LATEST 读取源库当前位点失败，host=%s: %w", hostPort, err)
	}
	return gset, nil
}

// resolveBinlogPosStartGTID resolves a binlog file boundary against the actual source.
// Unavailable files, invalid boundaries and purged required GTIDs fail explicitly.
func (m *Module) resolveBinlogPosStartGTID(ctx context.Context, ep *iproto.EndpointConn, serverID uint32, sourceFile string, sourceOffset uint64) (string, error) {
	if sourceOffset > math.MaxUint32 {
		return "", fmt.Errorf("controller: BINLOG_POS 的 source_offset=%d 超出 uint32 范围，无法换算", sourceOffset)
	}
	port, err := toPort16(ep.GetPort())
	if err != nil {
		return "", fmt.Errorf("controller: BINLOG_POS 探测连接端口非法: %w", err)
	}

	hostPort := fmt.Sprintf("%s:%d", ep.GetHost(), ep.GetPort())
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/", ep.GetUsername(), ep.GetPassword(), hostPort)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return "", fmt.Errorf("controller: 解析源库连接参数失败，host=%s: %w", hostPort, err)
	}
	defer func(cleanup func() error) { _ = cleanup() }(db.Close)

	resolveCtx, cancel := context.WithTimeout(ctx, m.opts.BinlogPosResolveTimeout)
	defer cancel()

	conn := position.Conn{
		ServerID: serverID,
		Host:     ep.GetHost(),
		Port:     port,
		User:     ep.GetUsername(),
		Password: ep.GetPassword(),
	}
	gset, err := position.ResolveFileOffset(resolveCtx, db, conn, sourceFile, uint32(sourceOffset))
	if err != nil {
		// 原样透传底层错误文本（含 *position.BoundaryError 给出的前后边界提示）：
		// 与 resolveLatestStartGTID"允许原样透传底层错误文本"同一条既定行为，
		// 该函数文档已经论证过为什么这样做不会带出 dsn/密码，这里不重复。
		return "", fmt.Errorf("controller: 按 Kind=BINLOG_POS 换算位点失败，host=%s file=%s offset=%d: %w",
			hostPort, sourceFile, sourceOffset, err)
	}
	return gset, nil
}

// finishApply runs in the actor, committing successful configuration and runtime
// intent before reporting completion.
func (m *Module) finishApply(item *iproto.PipelineConfigItem, applyErr, publishErr error, cmdCtx *cmdContext, preUnloadFailed bool) {
	id := item.GetPipelineId()

	key := keyOf(item)
	delete(m.pending, key)

	defer m.reevaluateTrailing(key)

	defer func() {
		outcome := applyErr
		if outcome == nil && m.runtimeFaults[key] {
			outcome = fmt.Errorf("controller: pipeline failed during module startup")
		}
		m.completeCmdSide(cmdCtx, outcome, resultingStateOnStart(outcome))
	}()

	if publishErr != nil {
		slog.Warn("publish apply/cmd result failed",
			slog.String("pipeline_id", id), slog.Bool("cmd_triggered", cmdCtx != nil), slog.Any("error", publishErr))
	}
	if applyErr != nil {
		slog.Warn("apply pipeline config failed",
			slog.String("pipeline_id", id), slog.Uint64("config_version", item.GetConfigVersion()),
			slog.Any("error", applyErr))

		m.reportPipelineStatus(item, iproto.PipelineState_PIPELINE_STATE_ERROR, applyErr.Error())
		// A failed startup load has no in-process applied record. Preserve the
		// last confirmed configuration from disk as stopped, so STOP/PAUSE can
		// still report this role and clear its retained ERROR status.
		if _, exists := m.applied[key]; !exists && !preUnloadFailed {
			if prior, known := m.priorRunApplied[key]; known && prior.GetCommon().GetRunId() == item.GetCommon().GetRunId() {
				m.applied[key] = appliedPipeline{PipelineConfigItem: prior.PipelineConfigItem}
				if err := persistAppliedState(m.opts.Store, m.applied, m.now().UnixMilli()); err != nil {
					slog.Warn("persist stopped configuration after startup failure failed", slog.String("pipeline_id", id), slog.Any("error", err))
				}
			}
		}

		if prior, existed := m.applied[key]; existed && (prior.running || prior.unloadUncertain) {

			updated := appliedPipeline{PipelineConfigItem: prior.PipelineConfigItem, running: false, paused: false}
			if preUnloadFailed {
				updated = appliedPipeline{PipelineConfigItem: prior.PipelineConfigItem, running: true, unloadUncertain: true}
			}
			m.applied[key] = updated
			if err := persistAppliedState(m.opts.Store, m.applied, m.now().UnixMilli()); err != nil {
				slog.Warn("persist applied state failed", slog.String("pipeline_id", id), slog.Any("error", err))
			}
		}
		return
	}

	// Record running only after successful assembly or runtime update.
	m.applied[key] = appliedPipeline{PipelineConfigItem: item, running: true}
	if err := persistAppliedState(m.opts.Store, m.applied, m.now().UnixMilli()); err != nil {
		slog.Warn("persist applied state failed", slog.String("pipeline_id", id), slog.Any("error", err))
	}
	slog.Info("pipeline config applied", slog.String("pipeline_id", id),
		slog.Uint64("config_version", item.GetConfigVersion()))
}

// finishUnload 回到事件循环执行，成功时把这条 pipeline 从内存权威状态与持久化
// 快照里一并移除。
func (m *Module) finishUnload(item *iproto.PipelineConfigItem, unloadErr, publishErr error) {
	id := item.GetPipelineId()
	key := keyOf(item)
	delete(m.pending, key)
	// 见 finishApply 同一行的说明：必须在 delete 之后、用 defer 覆盖全部返回路径。
	defer m.reevaluateTrailing(key)

	if publishErr != nil {
		slog.Warn("publish config.apply.result failed",
			slog.String("pipeline_id", id), slog.Any("error", publishErr))
	}
	if unloadErr != nil {
		slog.Warn("unload pipeline failed",
			slog.String("pipeline_id", id), slog.Any("error", unloadErr))
		// A failed unload or durable deletion write remains uncertain and retryable.
		if prior, exists := m.applied[key]; exists && prior.running {
			prior.unloadUncertain = true
			m.applied[key] = prior
		}
		if !item.GetDeleted() {
			m.reportPipelineStatus(item, iproto.PipelineState_PIPELINE_STATE_ERROR, unloadErr.Error())
		}
		return
	}
	delete(m.runtimeInstances, key)
	delete(m.runtimeFaults, key)

	if _, existed := m.applied[key]; !existed {
		return
	}
	if err := deleteAppliedPipeline(m.opts.Store, m.applied, key, m.now().UnixMilli()); err != nil {
		slog.Warn("delete applied state failed", slog.String("pipeline_id", id), slog.Any("error", err))
	}
	m.dropCmdDedupIfNoSideLeft(id)
	slog.Info("pipeline config unloaded",
		slog.String("pipeline_id", id), slog.Any("role", item.GetRole()))
}

// m.applied 的键是 appliedKey{pipelineID, role}（按侧），而 cmdDedupSlot 的键是
// (pipelineID, cmdType)（不分侧：一条 cmd.pipeline 对两侧是同一条指令，
// 见 cmdFanout"一条消息，两侧执行，一个 ack"）。自环拓扑下卸掉源侧时目标侧还在跑,
// 那时清掉去重表就会让目标侧那条指令的重复投递重新执行一次。
//
// persistCmdDedup 走 PutCritical（同步阻塞），而本方法在事件循环里执行——
// 在这里落盘就是 禁止的同步等待。而这里不需要立即落盘：persistCmdDedup 是
// 整篇覆盖，写的是调用那一刻的完整内存表，所以下一次任何指令收敛时残留自然被
// 带走。
//
// 代价是明确且有界的：若此后该节点再无任何 cmd.pipeline 收敛，残留会留在磁盘上。
// 残留无害——cmdPipelineKey 含 ConfigVersion，Manager 重建同名 pipeline 用的是
// 新的（严格递增的）config_version，旧记录命不中；上界是"曾存在过的 pipeline 数 ×
// PipelineCmdType 取值数"。
func (m *Module) dropCmdDedupIfNoSideLeft(pipelineID string) {
	for k := range m.applied {
		if k.pipelineID == pipelineID {
			// 还有别的侧在 applied 里，这条 pipeline 没有真正消失。
			return
		}
	}
	if n := dropCmdDedupFor(m.cmdPipelineResolved, pipelineID); n > 0 {
		slog.Info("cmd dedup slots dropped with pipeline",
			slog.String("pipeline_id", pipelineID), slog.Int("slots", n))
	}
}

// finishStop 回到事件循环执行 changeStop 的收尾：成功时把 m.applied[id] 的 running
// 位改写为 false，item/key 都保留——与 finishUnload 的关键区别、也是 changeStop
// 相对 changeUnload 存在的唯一理由：deleteAppliedPipeline（finishUnload 用的那个）
// 连 key 一起删除，本函数用 persistAppliedState 整体重新持久化，key 继续存在。
//
// 卸载失败时不改动 m.applied：保留卸载前的状态（不管那一刻是 running:true 还是
// 幂等重复调用时本来就已经是 running:false），与 finishApply/finishUnload"失败不
// 覆盖已确认状态"同一条纪律——STOP 失败不应该让节点凭空"忘记"这条 pipeline 之前
// 的真实运行状态。
func (m *Module) finishStop(item *iproto.PipelineConfigItem, unloadErr, publishErr error, cmdCtx *cmdContext) {
	id := item.GetPipelineId()
	key := keyOf(item)
	delete(m.pending, key)
	// 见 finishApply 同一行的说明：必须在 delete 之后、用 defer 覆盖全部返回路径。
	defer m.reevaluateTrailing(key)

	defer m.completeCmdSide(cmdCtx, unloadErr, resultingStateOnStop(cmdCtxType(cmdCtx), unloadErr))

	if publishErr != nil {
		slog.Warn("publish apply/cmd result failed",
			slog.String("pipeline_id", id), slog.Bool("cmd_triggered", cmdCtx != nil), slog.Any("error", publishErr))
	}
	if unloadErr != nil {
		slog.Warn("stop pipeline failed", slog.String("pipeline_id", id), slog.Any("error", unloadErr))

		m.reportPipelineStatus(item, iproto.PipelineState_PIPELINE_STATE_ERROR, unloadErr.Error())
		return
	}

	cmdType := cmdCtxType(cmdCtx)

	paused := cmdType == iproto.PipelineCmdType_PIPELINE_CMD_TYPE_PAUSE
	delete(m.runtimeInstances, key)
	delete(m.runtimeFaults, key)
	m.applied[key] = appliedPipeline{PipelineConfigItem: item, running: false, paused: paused}
	if err := persistAppliedState(m.opts.Store, m.applied, m.now().UnixMilli()); err != nil {
		slog.Warn("persist applied state failed", slog.String("pipeline_id", id), slog.Any("error", err))
	}

	targetState := resultingStateOnStop(cmdType, nil)
	m.reportPipelineStatus(item, targetState, "")
	// message 保持字面量、不因 STOP/PAUSE 分叉成两条消息，
	// 具体报的是哪个目标状态放进 resulting_state 字段。
	slog.Info("pipeline module unloaded, configuration retained",
		slog.String("pipeline_id", id), slog.Any("resulting_state", targetState))
}

// 调用 startApply 前 m.pending[id] 必须已经清除（finishApply/finishUnload 都在
// delete(m.pending, id) 之后才 defer 本方法），否则会被当成"仍在装配中"误判跳过、
// 这次补偿本身又被跳过、又记一次 trailingItem——不会死循环（因为不会有新的
// AsyncGo 完成来清除它），但会让这条 pipeline 卡住，直到下一次真正的外部触发。
//
// trailingItem 里没有记录时是最常见的路径（没有发生尾沿丢失），直接返回，不做
// 任何事——这不是一个通用的周期性重扫入口，只在 finishApply/finishUnload 的
// 收尾时被动检查一次。
func (m *Module) reevaluateTrailing(key appliedKey) {
	if m.identityRetired {
		delete(m.trailingItem, key)
		m.stopRetiredPipelines()
		return
	}
	item, ok := m.trailingItem[key]
	if !ok {
		return
	}
	delete(m.trailingItem, key)
	slog.Info("re-evaluating pipeline after trailing config change observed while busy",
		slog.String("pipeline_id", key.pipelineID), slog.Any("role", key.role))
	if plan, needed := m.planForItem(item); needed {
		m.startApply(plan)
	}
}
