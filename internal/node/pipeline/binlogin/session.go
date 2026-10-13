package binlogin

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"
	"uuid"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"google.golang.org/protobuf/proto"

	"github.com/xmapst/xdts/internal/dblineage"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/mysqlx"
	"github.com/xmapst/xdts/internal/node/resourcequota"
	"github.com/xmapst/xdts/internal/rule"
)

var errSessionPanic = errors.New("binlogin: pipeline session panicked")

func (m *Module) runSessionSafely(ctx context.Context, startGTID string) (sessionErr error) {
	defer m.rejectPendingRuntimeUpdates()
	defer func() {
		if recover() != nil {
			// A panic value can contain credentials or a SQL payload.
			sessionErr = errSessionPanic
		}
	}()
	defer func() {
		if m.publishWindow != nil {
			_ = m.publishWindow.pause()
		}
	}()
	return m.runSessionFn(ctx, startGTID)
}

// scheduleSession 在事件循环里发起一次新的 dump 会话（AsyncGo 的 f/cb 模式，见
// internal/node/pipeline/binlogin/binlogin.go 包文档"阻塞 IO 与 actor 状态边界"）。只应从事件循环调用（OnInit、
// 重连定时器回调、handleSessionEnded），调用时同步读取 m.readGTIDSet 作为本次
// 会话的起点快照——一旦交给 AsyncGo，会话 goroutine 就不再读事件循环拥有的任何
// 字段（m.opts 是唯一例外：构造后不再修改，天然可跨 goroutine 安全读取）。
func (m *Module) scheduleSession() {
	if m.stopping {
		return
	}
	// 用 published 而不是 read 作为下一次会话（含首次会话与每一次断线重连）的
	// 起点：published 保证"这之前的一切都已经被下游确认接纳"，read 可能领先于
	// published（上一次会话某次 Sink 投递失败、会话因此中断的场景，见 dispatchOne），
	// 用 read 起步会跳过那笔从未被确认的事务，是静默丢失。理由与
	// resolveStartGTIDSet（OnInit 时的等价决策）完全一致，这里是同一条决策在
	// "本模块实例存活期间的后续会话"上的延伸——两处都不能从 read 起步。
	startGTID := m.publishedGTIDSet
	ctx := m.sessionCtx
	startedAt := m.now()

	var sessionErr error
	m.streamWorkers.Add(1)
	m.AsyncGo("binlogin.dump", func() {
		defer m.streamWorkers.Done()
		sessionErr = m.runSessionSafely(ctx, startGTID)
	}, func() {
		m.handleSessionEnded(sessionErr, m.now().Sub(startedAt))
	})
}

// handleSessionEnded 运行在事件循环（AsyncGo 的 cb），依据会话结束原因决定下一步：
// 停机（ctx 取消）什么都不做，OnDestroy 已经在处理收尾；purge 转 Error 并停止重连
// （NODE-BIN-08"提示修改位点或重新初始化"，需要人工介入）；其余错误按指数退避
// 安排下一次重连尝试。
func (m *Module) handleSessionEnded(err error, elapsed time.Duration) {
	if m.stopping {
		return
	}
	if err == nil {
		// runSession 只在 ctx 取消或遇到错误时返回；理论上不会拿到 nil。防御性地
		// 按最小退避重连，而不是 panic 或陷入忙等。
		slog.Warn("session exited unexpectedly, reconnecting with minimum backoff",
			slog.String("pipeline_id", m.opts.PipelineID))
		m.armReconnect(m.opts.ReconnectInitialBackoff)
		return
	}
	if errors.Is(err, context.Canceled) {
		return // 停机路径：OnDestroy 已取消 sessionCtx，不需要也不应该再重连
	}
	m.stats.telemetryErrors.Add(1)
	if errors.Is(err, errSessionPanic) {
		m.setPipelineState(iproto.PipelineState_PIPELINE_STATE_ERROR, err.Error())
		m.reportErrorEvent("PIPELINE_SESSION_PANIC", "源端会话异常，已释放本会话资源并按恢复策略重连", true)
	}
	if errors.Is(err, resourcequota.ErrExceeded) {
		m.setPipelineState(iproto.PipelineState_PIPELINE_STATE_ERROR, err.Error())
		m.reportErrorEvent("NODE_RESOURCE_QUOTA_EXCEEDED", err.Error(), false)
		m.sessionCancel()
		return
	}
	if errors.Is(err, errSchemaHistory) {
		m.setPipelineState(iproto.PipelineState_PIPELINE_STATE_ERROR, err.Error())
		m.reportErrorEvent("SCHEMA_HISTORY_UNAVAILABLE", "表结构无法安全恢复或保存，请检查本地存储、源库只读查询权限和历史位点；历史缺失时补充匹配的结构历史或重新初始化", false)
		m.sessionCancel()
		return
	}
	if errors.Is(err, rule.ErrRowEvaluation) || errors.Is(err, rule.ErrRowExpression) {
		m.setPipelineState(iproto.PipelineState_PIPELINE_STATE_ERROR, "行过滤无法安全执行，请修正规则或源表后恢复")
		m.reportErrorEvent("ROW_FILTER_FAILED", "行过滤无法安全执行，请检查列是否存在、类型是否匹配以及表达式限制", false)
		m.sessionCancel()
		return
	}
	if errors.Is(err, rule.ErrTransformConfig) || errors.Is(err, rule.ErrTransformEvaluation) {
		m.setPipelineState(iproto.PipelineState_PIPELINE_STATE_ERROR, "清洗转换无法安全执行，请修正规则或源表后恢复")
		m.reportErrorEvent("TRANSFORM_FAILED", "清洗转换失败，请检查列类型、键列和规则限制", false)
		m.sessionCancel()
		return
	}
	if errors.Is(err, errSnapshotDDLIncompatible) {
		m.setPipelineState(iproto.PipelineState_PIPELINE_STATE_ERROR, err.Error())
		m.reportErrorEvent("SNAPSHOT_DDL_INCOMPATIBLE", err.Error(), false)
		m.sessionCancel()
		return
	}

	if kind, ok := classifyGTIDFatalError(err); ok {
		m.stats.purgeDetected.Add(1) // 三种归类共用同一个计数语义（"1236 致命错误"整体计数），不拆分 Stats 字段——未来真有需要按类型分别观测时再加，这里不为了这次分类顺手扩大统计结构。
		switch kind {
		case gtidFatalBehindMaster:
			slog.Error("gtid behind new master, pipeline moving to error",
				slog.String("pipeline_id", m.opts.PipelineID), slog.Any("error", err))
			m.reportErrorEvent("GTID_BEHIND_MASTER",
				fmt.Sprintf("新主库缺少本地位点所需的 GTID，疑似故障转移到落后副本；"+
					"是否有数据在故障转移中丢失需要人工核实（当前没有可靠的协议信号能自动判定），"+
					"确认无误后再恢复该 pipeline: %v", err), false)
		case gtidFatalPurged:
			slog.Error("required binlog already purged, pipeline moving to error",
				slog.String("pipeline_id", m.opts.PipelineID), slog.Any("error", err))
			m.reportErrorEvent("BINLOG_PURGED",
				fmt.Sprintf("所需 binlog 已被清理，请修改起始位点或重新初始化该 pipeline: %v", err), false)
		default: // gtidFatalUnclassified：错误文本无法判定具体原因，fail-safe 按最保守方式处理（不得当成"未知，继续重连"放过）。
			slog.Error("unclassified fatal mysql 1236 binlog error, pipeline moving to error",
				slog.String("pipeline_id", m.opts.PipelineID), slog.Any("error", err))
			m.reportErrorEvent("GTID_FATAL_UNCLASSIFIED",
				fmt.Sprintf("MySQL 返回致命 binlog 读取错误（1236），但无法从错误文本判定具体原因，"+
					"按最保守方式处理，需人工核实后再恢复该 pipeline: %v", err), false)
		}
		m.setPipelineState(iproto.PipelineState_PIPELINE_STATE_ERROR, err.Error())
		m.sessionCancel()
		return
	}

	if elapsed >= stableSessionThreshold {
		m.backoffAttempt = 0
	}
	delay := backoffDelay(m.backoffAttempt, m.opts.ReconnectInitialBackoff, m.opts.ReconnectMaxBackoff, m.opts.ReconnectMultiplier)
	if m.backoffAttempt < 30 { // 避免 backoffAttempt 无界增长（长期故障场景下的防御）
		m.backoffAttempt++
	}
	m.stats.reconnects.Add(1)
	slog.Warn("session interrupted, scheduling reconnect with backoff",
		slog.String("pipeline_id", m.opts.PipelineID), slog.Duration("delay", delay), slog.Any("error", err))
	m.armReconnect(delay)
}

func (m *Module) armReconnect(delay time.Duration) {
	m.NewTimer(timerKindReconnect, delay)
}

// backoffDelay 计算第 attempt 次重连（0-based）应等待的时长：initial * multiplier^attempt，
// 上限 max。
func backoffDelay(attempt int, initial, max time.Duration, multiplier float64) time.Duration {
	if attempt <= 0 || initial <= 0 {
		return initial
	}
	d := float64(initial)
	for range attempt {
		d *= multiplier
		if d >= float64(max) {
			return max
		}
	}
	return time.Duration(d)
}

type gtidFatalErrorKind int

const (
	gtidFatalUnclassified gtidFatalErrorKind = iota
	// gtidFatalPurged 表示确认为"所需 binlog 已被清理"（NODE-BIN-08）。
	gtidFatalPurged
	// gtidFatalBehindMaster 表示确认为"新主库缺少本地位点所需的 GTID"，与 purge 是
	// 完全不同的运维场景（NODE-BIN-10）：purge 通常需要修改起始位点或重新初始化；
	// 新主库落后则更可能是故障转移到了落后副本，是否有数据在故障转移中丢失需要
	// 人工核实——目前没有协议层信号能自动判定这一点（已确认，见调研记录），本包
	// 只负责把这种情况和普通 purge 区分开、给出更准确的诊断信息，不负责自动判定
	// 数据是否丢失。
	gtidFatalBehindMaster
)

// classifyGTIDFatalError 判定 err 是否为 MySQL 1236 错误，并尽力从错误文本区分
// purge 与"新主库落后"两种服务端场景。ok=false 表示这根本不是 1236 错误（调用方
// 应按其它错误类型处理，不属于本函数覆盖范围）；ok=true 时 kind 给出细分归类，
// 三种取值里 gtidFatalUnclassified 的调用方处理义务见该常量的文档——不是"可以
// 随便处理"的兜底分支。
//
// 关键词弱匹配（strings.Contains，不要求完整句子），因为 MySQL 版本、locale 差异
// 都可能改变实际措辞；不硬编码完整错误字符串。
func classifyGTIDFatalError(err error) (kind gtidFatalErrorKind, ok bool) {
	var myErr *gmysql.MyError
	if !errors.As(err, &myErr) || myErr.Code != gmysql.ER_MASTER_FATAL_ERROR_READING_BINLOG {
		return gtidFatalUnclassified, false
	}
	msg := strings.ToLower(myErr.Message)
	switch {
	case strings.Contains(msg, "purged"):
		return gtidFatalPurged, true
	case strings.Contains(msg, "more gtid"): // 覆盖 "more GTIDs than the master/source has" 两种措辞
		return gtidFatalBehindMaster, true
	default:
		return gtidFatalUnclassified, true
	}
}

// runSession 是一次完整的 dump 会话：连接、StartSyncGTID、循环 GetEvent 直到出错或
// ctx 取消。运行在 AsyncGo 的独立 goroutine 上，不触碰事件循环拥有的任何 Module 字段
// BinlogSyncer 不复用，每次会话重新构造，以免 StartSyncGTID 返回
// errSyncRunning），因此每次会话都重新构造一个。
func (m *Module) runSession(ctx context.Context, startGTID string) error {
	startGTID, err := m.resumePublications(ctx, startGTID)
	if err != nil {
		return err
	}
	historyCtx, historyCancel := context.WithTimeout(ctx, schemaHistoryTimeout)
	err = m.restoreSchemaHistory(historyCtx, startGTID)
	historyCancel()
	if err != nil {
		return err
	}
	catalog, err := m.newSessionSchemaCatalog(ctx)
	if err != nil {
		return err
	}
	defer func(cleanup func() error) { _ = cleanup() }(catalog.db.Close)
	if expected := m.sourceRuntime.DatabaseRoute.SourceServerUUID; expected != "" {
		var actual string
		identityCtx, cancelIdentity := context.WithTimeout(ctx, schemaHistoryTimeout)
		err := catalog.db.QueryRowContext(identityCtx, "SELECT @@server_uuid").Scan(&actual)
		cancelIdentity()
		if err != nil || actual != expected {
			return fmt.Errorf("%w: source database identity changed; probe and refresh configuration", errSchemaHistory)
		}
	}
	if err := catalog.prepareBaseline(startGTID); err != nil {
		return err
	}
	historyCtx, historyCancel = context.WithTimeout(ctx, schemaHistoryTimeout)
	err = m.commitSchemaHistory(historyCtx, startGTID)
	historyCancel()
	if err != nil {
		return err
	}
	cfg := mysqlx.NewBinlogSyncerConfig(m.opts.ServerID, m.opts.Host, m.opts.Port, m.opts.User, m.opts.Password)
	cfg.HeartbeatPeriod = m.opts.HeartbeatPeriod
	cfg.TLSConfig = m.opts.TLSConfig
	cfg.ReadTimeout = 30 * time.Second
	// Keep third-party parsed-event lookahead bounded while the current
	// transaction owns the pipeline's accounted business buffer.
	cfg.EventCacheCount = 1
	cfg.RowsEventDecodeFunc = decodeSourceRows
	// 自行实现指数退避重连（NODE-BIN-08），不依赖库内固定 1s 重试；ERR_HEADER 协议
	// 错误（含 purge）本就不会被库内重试覆盖（已通过源码核实），这里显式关闭库内
	// 重试只是为了统一由本包一处决定退避节奏，避免两套重试策略互相干扰。
	cfg.DisableRetrySync = true

	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()

	startSet, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, startGTID)
	if err != nil {
		return fmt.Errorf("binlogin: 解析起始 GTID 集合 %q 失败: %w", startGTID, err)
	}
	streamer, err := syncer.StartSyncGTID(startSet)
	if err != nil {
		return fmt.Errorf("binlogin: StartSyncGTID 失败: %w", err)
	}

	// localRead/localPublished 是本次会话私有的位点累积副本（会话 goroutine 独占
	// 读写，不与事件循环共享），每完成一个事务/空事务/水位刷新就 Update 一次，再
	// 通过 emitProgress 把最新值自投递回事件循环——这是本包"位点只能由事件循环
	// 持有权威副本"这条纪律唯一需要绕一层的地方，绕的方式是"会话自己维护只写不
	// 共享的草稿，写完立即上报"，而不是直接读写事件循环字段。两者从同一个 startGTID
	// 起步（此刻相等，见 internal/node/pipeline/binlogin/binlogin.go OnInit 的注释），此后随 dispatchOne 的
	// "先 read 后 published"顺序而可能分裂——必须各自
	// 解析出独立的 GTIDSet 实例，不能共享底层 map，否则 Update 一个会连带改到
	// 另一个。
	localRead, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, startGTID)
	if err != nil {
		return fmt.Errorf("binlogin: 解析起始 GTID 集合 %q 失败: %w", startGTID, err)
	}
	localPublished, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, startGTID)
	if err != nil {
		return fmt.Errorf("binlogin: 解析起始 GTID 集合 %q 失败: %w", startGTID, err)
	}

	agg := newAggregator(m.sourceRuntime.TableFilter, m.sourceRuntime.ColumnFilter, m.sourceRuntime.OriginResolver, m.opts.Center, m.schemaTracker)
	agg.ddlDependencies = m.opts.DDLSourceDependencies
	agg.rowFilter = m.sourceRuntime.RowFilter
	agg.transforms = m.sourceRuntime.Transforms
	agg.databaseRoute = m.sourceRuntime.DatabaseRoute
	if agg.databaseRoute.Persistent {
		if err := dblineage.New(catalog.db).Validate(ctx); err != nil {
			return err
		}
		groupCtx, cancelGroup := context.WithTimeout(ctx, dblineage.Timeout)
		var name, group string
		err := catalog.db.QueryRowContext(groupCtx, "SHOW VARIABLES LIKE 'group_replication_group_name'").Scan(&name, &group)
		cancelGroup()
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("binlogin: read group lineage identity failed")
		}
		if group != "" {
			id, err := uuid.Parse(group)
			if err != nil || id == uuid.Nil() {
				return fmt.Errorf("binlogin: invalid group lineage identity")
			}
			agg.nativeGroupUUID = id.String()
		}
		agg.loadLineage = func(gtid string) (iproto.StoredDatabaseLineage, bool, error) {
			return dblineage.New(catalog.db).Load(ctx, gtid)
		}
		agg.lineageAllowed = m.originTxnAllowed
	}
	agg.ddlColumnPolicy = iproto.CloneDDLColumnPolicy(m.sourceRuntime.DDLColumnPolicy)
	agg.budget = m.opts.Budget
	defer agg.reset()
	agg.ensureSchema = catalog.ensureTableMetadata
	agg.schemaReadPosition = localRead.String
	agg.onTablesFiltered = func() { m.stats.tablesFiltered.Add(1) }

	// A single bounded read deadline covers successive ready events. Creating
	// and stopping a timer for every event adds work to a busy row stream.
	readDeadline := &sourceReadDeadline{}
	readDeadline.renew(ctx)
	defer readDeadline.close()
	for {
		if err := m.pollPublications(ctx); err != nil {
			return err
		}
		if err := m.applyRuntimeUpdate(agg); err != nil {
			return err
		}
		ev, err := streamer.GetEvent(readDeadline.context)
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			readDeadline.renew(ctx)
			continue
		}
		if err != nil {
			return err // 含 context.Canceled（停机）与真实错误（含 purge）
		}
		m.streamReadyOnce.Do(func() { close(m.streamReady) })
		if err := m.handleEvent(ctx, agg, localRead, localPublished, ev); err != nil {
			return err
		}
	}
}

// handleEvent 分派单个顶层 BinlogEvent：心跳（NODE-BIN-07）单独处理，
// TRANSACTION_PAYLOAD_EVENT 需要先解包再把内层事件逐个递归喂给同一套分派逻辑
// （TransactionPayloadEvent.Decode 产出的内层事件不会重新出现在 streamer.GetEvent
// 的 channel 里，必须调用方自己展开——已通过源码核实），其余事件交给聚合器。
func (m *Module) handleEvent(ctx context.Context, agg *aggregator, localRead, localPublished gmysql.GTIDSet, ev *replication.BinlogEvent) error {
	switch ev.Header.EventType {
	case replication.HEARTBEAT_EVENT, replication.HEARTBEAT_LOG_EVENT_V2:
		if err := m.drainPublicationWindow(ctx); err != nil {
			return err
		}
		// 水位消息携带 published（不是 read）：这是对外可见、会被下游用于计算
		// 延迟的位点声明，必须只声明已经确认过的进度（与检查点、resolveStartGTIDSet
		// 同一条"对外一律只暴露 published"的纪律）。空闲期间不会有 Sink 调用
		// 正在飞行中的中间态，此刻 read 与 published 必然相等，这里的选择不影响
		// 数值，只是在语义上选对将来仍然正确的那一个。
		if err := m.sendWatermark(ctx, localPublished.String()); err != nil {
			return err
		}
		agg.lastWatermark = m.now()
		return nil
	case replication.TRANSACTION_PAYLOAD_EVENT:
		tp, ok := ev.Event.(*replication.TransactionPayloadEvent)
		if !ok {
			return fmt.Errorf("binlogin: TRANSACTION_PAYLOAD_EVENT 的 Event 字段类型不是 "+
				"*replication.TransactionPayloadEvent（实际 %T）", ev.Event)
		}
		for _, inner := range tp.Events {
			if err := m.dispatchOne(ctx, agg, localRead, localPublished, inner); err != nil {
				return err
			}
		}
		return nil
	default:
		return m.dispatchOne(ctx, agg, localRead, localPublished, ev)
	}
}

// dispatchOne 把一个（可能来自 TransactionPayloadEvent 内层展开的）事件喂给聚合器，
// 事务/空事务结束时推进 localRead/localPublished 并上报下游/进展。
func (m *Module) dispatchOne(ctx context.Context, agg *aggregator, localRead, localPublished gmysql.GTIDSet, ev *replication.BinlogEvent) error {
	if err := m.applyRuntimeUpdate(agg); err != nil {
		return err
	}
	result, err := agg.feed(ev)
	defer result.buffer.Release()
	if err != nil {
		return err
	}
	if _, isGTID := ev.Event.(*replication.GTIDEvent); isGTID {
		preview := &iproto.Txn{Gtid: agg.gtid}
		agg.applyOrigin(preview)
		// Durable metadata arrives after GTID; incomplete preview history cannot
		// reject rows before the reserved record has restored the full route.
		agg.originRejected = !agg.databaseRoute.Persistent && !m.originTxnAllowed(preview, agg.originUnknown)
	}
	if result.txn.GetDdl() != nil && m.opts.Budget != nil {
		ddlBuffer, err := m.opts.Budget.Reserve(uint64(proto.Size(result.txn))*4 + 256)
		if err != nil {
			return err
		}
		defer ddlBuffer.Release()
	}
	if result.txn.GetDdl() != nil && m.opts.SnapshotDDLGuard != nil {
		// A filtered source DDL still changes the snapshot's actual table.
		if err := m.opts.SnapshotDDLGuard.Check(result.gtid, result.txn.Ddl.Schema, result.txn.Ddl.Statement); err != nil {
			return fmt.Errorf("%w: %w", errSnapshotDDLIncompatible, err)
		}
	}
	if result.txn != nil && !m.allowOriginResult(result) {
		result.kind = resultSkip
		result.schemaChanged = result.txn.GetDdl() != nil
	}
	switch result.kind {
	case resultNone:
		return nil
	case resultSkip:
		if err := m.drainPublicationWindow(ctx); err != nil {
			return err
		}
		// 空事务/跳过事件：没有数据需要投递下游，不存在"读到但未确认"的窗口，
		// read 与 published 同步推进。
		if err := localRead.Update(result.gtid); err != nil {
			return fmt.Errorf("binlogin: 累积 read GTID 集合失败: %w", err)
		}
		if result.schemaChanged {
			if err := m.persistSchemaHistory(localRead.String()); err != nil {
				return err
			}
		}
		if err := localPublished.Update(result.gtid); err != nil {
			return fmt.Errorf("binlogin: 累积 published GTID 集合失败: %w", err)
		}
		if m.opts.SnapshotDDLGuard != nil {
			m.opts.SnapshotDDLGuard.Observe(localPublished.String())
		}
		m.emitProgress(localRead.String(), localPublished.String(), result.txn.GetCommitTsMs(), 0)
		// A busy server may never send idle heartbeats when all transactions
		// touch other tables. Publish the confirmed filtered position periodically
		// so downstream snapshot catchup can reach H without waiting for idle IO.
		if agg.lastWatermark.IsZero() || m.now().Sub(agg.lastWatermark) >= m.opts.HeartbeatPeriod {
			if err := m.sendWatermark(ctx, localPublished.String()); err != nil {
				return err
			}
			agg.lastWatermark = m.now()
		}
		return nil
	case resultTxn:
		if guard, ok := m.opts.SnapshotDDLGuard.(interface{ RecordCreation(*iproto.Txn) error }); ok {
			creation := result.txn
			if creation.GetDdl() != nil && m.sourceRuntime.ShadowTablePolicy != nil {
				creation = proto.Clone(creation).(*iproto.Txn)
				creation.ShadowTablePolicy = iproto.CloneShadowTablePolicy(m.sourceRuntime.ShadowTablePolicy)
			}
			if err := guard.RecordCreation(creation); err != nil {
				return err
			}
		}
		// 先推进 read：此刻只代表"已经从 binlog 读到这笔事务"，不代表下游已经
		// 接纳——read 领先于 published 的窗口从这里开始，直到下面 emitTxn 成功
		// 才收窄。如果 emitTxn 失败，这个窗口会一直保留到会话结束。
		if err := localRead.Update(result.gtid); err != nil {
			return fmt.Errorf("binlogin: 累积 read GTID 集合失败: %w", err)
		}
		if result.txn.Ddl != nil {

			m.reportDDLEvent(result.txn.Ddl.Schema, result.txn.Ddl.Statement, result.txn.Gtid)
			// Persist schema with the session-owned observed GTIDs, including this DDL.
			// Publication may lag; actor mirrors cannot establish the session's schema boundary.
			if err := m.persistSchemaHistory(localRead.String()); err != nil {
				return err
			}
			if result.schemaWarning != "" {
				slog.Warn("ddl schema parse produced a warning",
					slog.String("pipeline_id", m.opts.PipelineID),
					slog.String("gtid", result.txn.Gtid),
					slog.String("warning", result.schemaWarning))
			}
		}
		// Publish the read/published split before a potentially blocked send.
		m.emitProgress(localRead.String(), localPublished.String(), result.txn.GetCommitTsMs(), 0)
		asynchronous, err := m.publishTransaction(ctx, localRead, localPublished, result.txn)
		if err != nil {
			// published 原地不动：这里仍然 emitProgress 一次，让事件循环的
			// readGTIDSet 诊断字段反映真实进度，但 publishedGTIDSet 传的是
			// localPublished 当前（未变化）的值，绝不能在这里伪造成已发布——
			// 调用方（下一次 scheduleSession/resolveStartGTIDSet）依赖这个字段
			// 判断安全恢复点。
			m.emitProgress(localRead.String(), localPublished.String(), result.txn.GetCommitTsMs(), 0)
			return err
		}
		if asynchronous {
			return nil
		}
		if err := localPublished.Update(result.gtid); err != nil {
			return fmt.Errorf("binlogin: 累积 published GTID 集合失败: %w", err)
		}
		if m.opts.SnapshotDDLGuard != nil {
			m.opts.SnapshotDDLGuard.Observe(localPublished.String())
		}
		m.emitProgress(localRead.String(), localPublished.String(), result.txn.GetCommitTsMs(), len(result.txn.Rows))
		return nil
	default:
		return nil
	}
}

func (m *Module) emitTxn(ctx context.Context, txn *iproto.Txn) error {
	if m.sourceRuntime.ShadowTablePolicy != nil {
		txn = proto.Clone(txn).(*iproto.Txn)
		txn.ShadowTablePolicy = iproto.CloneShadowTablePolicy(m.sourceRuntime.ShadowTablePolicy)
	}
	if txn.Ddl != nil || len(txn.Rows) <= m.opts.MaxRowsPerChunk {
		if err := m.sendBatch(ctx, singleTxnBatch(m.opts.PipelineID, m.opts.RunID, m.epoch, m.batchSeq.Add(1), txn)); err != nil {
			return err
		}
		m.stats.txnsEmitted.Add(1)
		if txn.Ddl != nil {
			m.stats.ddlEmitted.Add(1)
		} else {
			m.stats.rowsEmitted.Add(int64(len(txn.Rows)))
		}
		return nil
	}

	rows := txn.Rows
	total := (len(rows) + m.opts.MaxRowsPerChunk - 1) / m.opts.MaxRowsPerChunk
	for i := range total {
		start := i * m.opts.MaxRowsPerChunk
		end := min(start+m.opts.MaxRowsPerChunk, len(rows))
		chunkTxn := &iproto.Txn{
			ShadowTablePolicy: iproto.CloneShadowTablePolicy(txn.GetShadowTablePolicy()),
			Gtid:              txn.Gtid,
			OriginCenter:      txn.OriginCenter,
			Path:              slices.Clone(txn.Path),
			EndpointPath:      slices.Clone(txn.EndpointPath),
			LineageVersion:    txn.LineageVersion,
			Hops:              txn.Hops,
			CommitTsMs:        txn.CommitTsMs,
			LastCommitted:     txn.LastCommitted,
			SequenceNumber:    txn.SequenceNumber,
			Rows:              rows[start:end],
		}
		batch := singleTxnBatch(m.opts.PipelineID, m.opts.RunID, m.epoch, m.batchSeq.Add(1), chunkTxn)
		batch.Chunk = &iproto.Chunk{Index: uint32(i), Total: uint32(total)}
		if err := m.sendBatch(ctx, batch); err != nil {
			return err
		}
	}
	m.stats.txnsEmitted.Add(1)
	m.stats.rowsEmitted.Add(int64(len(txn.Rows)))
	return nil
}

func singleTxnBatch(pipelineID string, runID, epoch, seq uint64, txn *iproto.Txn) *iproto.DataBatch {
	return &iproto.DataBatch{
		PipelineId:   pipelineID,
		RunId:        runID,
		Epoch:        epoch,
		BatchSeq:     seq,
		BeginGtidSet: txn.Gtid,
		EndGtidSet:   txn.Gtid,
		Txns:         []*iproto.Txn{txn},
	}
}

// sendBatch 是**所有** DataBatch 的唯一漏斗（emitTxn 的单批路径与大事务分片循环都
// 经过它），因此增量限流（SourceRuleSet.incremental_rate_limit）的等待点在这里，
// 而不在 dispatchOne/emitTxn：放在事务层只会为整笔事务等一次，然后把它的 N 个分片
// 瞬时打出去——长期均速对、瞬时突发不对，而突发正是限流要防的东西。
//
// 等待必须发生在 withSendTimeout **之前**：SendTimeout（默认 30s）是给 Sink 投递本身
// 设的上限，而一次正常的限流等待完全可以超过 30s（低速率 + 大分片）。如果把限流等待
// 套进 send 超时 ctx，一次符合配置的等待会被误报成投递超时，进而触发退避重连——
// 把限流变成故障。
//
// 限流等待被 ctx 取消时原样 %w 包裹返回：handleSessionEnded 用
// errors.Is(err, context.Canceled) 判断"这是停机而不是故障"，包裹不破坏这个判断，
// 但换成 errors.New 就会让一次正常关停变成一次退避重连。
//
// sendWatermark 刻意不限流：水位消息不含行、字节数接近 0，且每个心跳周期只有一条；
// 压住它只会推迟空闲位点声明（下游据此算延迟），没有任何带宽收益。
func (m *Module) sendBatch(ctx context.Context, batch *iproto.DataBatch) error {
	if err := m.limiter.waitForBatch(ctx, batch); err != nil {
		return fmt.Errorf("binlogin: 增量限流等待被中断（batch_seq=%d）: %w", batch.BatchSeq, err)
	}
	sendCtx, cancel := m.withSendTimeout(ctx)
	defer cancel()
	return m.publishBatch(sendCtx, batch)
}

func (m *Module) publishBatch(ctx context.Context, batch *iproto.DataBatch) error {
	if err := m.opts.Sink.SendBatch(ctx, batch); err != nil {
		return fmt.Errorf("binlogin: 投递 DataBatch 失败（batch_seq=%d）: %w", batch.BatchSeq, err)
	}
	m.stats.batchesSent.Add(1)
	return nil
}

// sendWatermark 投递一次空闲水位消息（NODE-BIN-07）并顺带把当前位点上报给事件循环
// （心跳期间没有新事务，但目标端仍需要刷新的时间戳来计算延迟，见 Watermark 的用途）。
// gtidSet 调用方传入 localPublished（见 handleEvent），空闲期间不存在 Sink 调用
// 正在飞行中的中间态，此刻 read 与 published 数值相等，emitProgress 两个位点参数
// 都传 gtidSet 是如实反映这一点，不是偷懒省略 read。
func (m *Module) sendWatermark(ctx context.Context, gtidSet string) error {
	sendCtx, cancel := m.withSendTimeout(ctx)
	defer cancel()
	tsMs := m.now().UnixMilli()
	wm := &iproto.Watermark{
		PipelineId: m.opts.PipelineID,
		RunId:      m.opts.RunID,
		GtidSet:    gtidSet,
		SourceTsMs: tsMs,
	}
	if err := m.opts.Sink.SendWatermark(sendCtx, wm); err != nil {
		return fmt.Errorf("binlogin: 投递 Watermark 失败: %w", err)
	}
	m.stats.watermarksSent.Add(1)
	m.emitProgress(gtidSet, gtidSet, tsMs, 0)
	return nil
}

func (m *Module) withSendTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if m.opts.SendTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, m.opts.SendTimeout)
}

// sourceReadDeadline belongs to the serial source session; renewal always
// releases the previous timer and session exit releases the current timer.
type sourceReadDeadline struct {
	context context.Context
	cancel  context.CancelFunc
}

func (r *sourceReadDeadline) renew(parent context.Context) {
	r.close()
	r.context, r.cancel = context.WithTimeout(parent, 100*time.Millisecond)
}

func (r *sourceReadDeadline) close() {
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
}
