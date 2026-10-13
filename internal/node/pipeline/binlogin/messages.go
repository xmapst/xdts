package binlogin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"uuid"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdoc"
	"github.com/xmapst/xhive/chanrpc"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/reporter"
	"github.com/xmapst/xdts/internal/node/safeerror"
	"github.com/xmapst/xdts/internal/node/store"
)

const checkpointCollection = "checkpoints"

func checkpointID(pipelineID string) string {
	return pipelineID + ".source"
}

const schemaHistoryCollection = "schema_history"

// progressMsg 是会话 goroutine（AsyncGo 的 f）向本模块事件循环自投递的内部消息，
// 用于把"读取并成功交给 Sink"的最新位点安全地写回事件循环私有状态（见 internal/node/pipeline/binlogin/binlogin.go
// 包文档"阻塞 IO 与 actor 状态边界"）。不是外部模块间协议，因此不需要像
// reporter.StatusReport 那样导出。
//
// readGTIDSet 与 publishedGTIDSet 两个字段独立传递（不是一份共享值）：会话
// goroutine 在 Sink 调用失败时仍会上报这次消息（见 internal/node/pipeline/binlogin/session.go dispatchOne），
// 此时 readGTIDSet 已经推进但 publishedGTIDSet 传的是旧值——如果这里只有一个
// 字段，就没有办法表达"读到了但没确认"这个中间状态本身（见 internal/node/pipeline/binlogin/binlogin.go 包文档
// "位点语义：read 与 published 的分裂"）。
type progressMsg struct {
	readGTIDSet      string
	publishedGTIDSet string
	sourceEventTsMs  int64
	txnRows          int // 0 表示本次只是心跳/水位刷新/投递失败重试前的中间状态，没有新确认的事务
}

func (m *Module) onProgress(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
	req, ok := ci.Request.(progressMsg)
	if !ok {
		return nil
	}
	if iproto.PipelineState(m.pipelineState.Load()) == iproto.PipelineState_PIPELINE_STATE_ERROR && (req.txnRows > 0 || req.publishedGTIDSet != m.publishedGTIDSet) {
		m.setPipelineState(iproto.PipelineState_PIPELINE_STATE_RUNNING, "")
	}
	m.readGTIDSet = req.readGTIDSet
	m.publishedGTIDSet = req.publishedGTIDSet
	m.lastEventTsMs = req.sourceEventTsMs
	return nil
}

// emitProgress 由会话 goroutine 调用，把最新位点自投递回事件循环。使用 selfClient
// （而不是 m.Skeleton.Cast）是因为 selfClient 直接持有 m.ChanRPC()，不依赖
// defaultApp 这个包级单例——会话 goroutine 与测试驱动模块的方式都不经过
// xhive.Run/defaultApp（见既有 store/reporter/logship 测试的一致做法），
// selfClient 保证生产路径与测试路径用同一条投递方式。
func (m *Module) emitProgress(readGTIDSet, publishedGTIDSet string, sourceEventTsMs int64, txnRows int) {
	m.selfClient.Cast(m.ChanRPC(), progressMsg{
		readGTIDSet:      readGTIDSet,
		publishedGTIDSet: publishedGTIDSet,
		sourceEventTsMs:  sourceEventTsMs,
		txnRows:          txnRows,
	})
}

// resolveStartGTIDSet 在 OnInit 阶段确定起始位点：优先用 Options.StartGTIDSet
// （NODE-POS-04：调用方已完成文件+偏移/时间点换算），否则尝试从 Store 的
// checkpoints 集合恢复上次持久化的 published_gtid_set；两者都拿不到则报错——
// 不得静默从空集合开始，那等价于把整份数据当新事务重新同步一遍（C-23 的位点只能
// 是显式的 GTID 集合，不允许隐式默认值）。
// Only a checkpoint with this run identity is a recovery source; a new run
// requires its explicit starting point.
//
// 本函数只读 checkpoint 文档的 published_gtid_set，完全不看 schema_history 落盘
// 到了哪个 GTID。这两份持久化状态必须维持"(位点, 结构) 是一致的一对"这个不变量，
// 本函数是这个不变量里"位点"那一半的唯一恢复入口——persistSchemaHistory 每条
// DDL 同步落盘、本函数依赖的 checkpoint 只周期性落盘，两者不耦合，因此
// schema_history 可能领先于这里恢复出的位点。重放这个位点到那条 DDL 之间的
// 事务时会用"过新"的结构解码，可能造成永久性列值丢失，不会被后续重放自愈。
// 完整的失败场景、触发条件与修复方向见 persistSchemaHistory 的文档。
func (m *Module) resolveStartGTIDSet() (string, error) {
	if m.opts.RequireEpochRange && !m.opts.Snapshotting {
		var doc map[string]any
		if m.opts.Store == nil {
			return "", errors.New("binlogin: source recovery requires store")
		}
		if err := m.opts.Store.Get(context.Background(), checkpointCollection, checkpointID(m.opts.PipelineID), &doc); err != nil {
			return "", err
		}
		revision, valid := store.CheckpointPositionRevision(doc)
		set, present := store.CheckpointPublishedGTID(doc)
		if !store.MatchesCheckpointRun(doc, m.opts.RunID) || !valid || revision != m.opts.PositionRevision || !present {
			return "", errors.New("binlogin: source recovery checkpoint changed")
		}
		if _, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, set); err != nil {
			return "", err
		}
		return set, nil
	}
	if m.opts.PositionRevision > 0 {
		if m.opts.PositionRevision > math.MaxInt64 {
			return "", errors.New("binlogin: position revision exceeds storage range")
		}
		if m.opts.Store == nil {
			return "", errors.New("binlogin: position revision requires persistent checkpoint")
		}
		var doc map[string]any
		err := m.opts.Store.Get(context.Background(), checkpointCollection, checkpointID(m.opts.PipelineID), &doc)
		if err != nil && !errors.Is(err, xdoc.ErrNotFound) {
			return "", err
		}
		if err == nil {
			run, valid := store.CheckpointRun(doc)
			if !valid || run > m.opts.RunID {
				return "", errors.New("binlogin: invalid or future checkpoint run")
			}
		}
		if err == nil && store.MatchesCheckpointRun(doc, m.opts.RunID) {
			revision, valid := store.CheckpointPositionRevision(doc)
			if !valid || revision > m.opts.PositionRevision {
				return "", errors.New("binlogin: invalid or future position revision")
			}
			if revision == m.opts.PositionRevision {
				set, ok := store.CheckpointPublishedGTID(doc)
				if !ok {
					return "", errors.New("binlogin: revised checkpoint position missing")
				}
				if _, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, set); err != nil {
					return "", err
				}
				return set, nil
			}
		}
		if !m.opts.StartGTIDSetProvided {
			return "", errors.New("binlogin: new position revision requires explicit start")
		}
		return m.opts.StartGTIDSet, nil
	}
	if m.opts.StartGTIDSet != "" || m.opts.StartGTIDSetProvided {
		return m.opts.StartGTIDSet, nil
	}
	if m.opts.Store == nil {
		return "", errors.New("binlogin: 未提供 StartGTIDSet 且未注入 Store，无法确定起始位点")
	}

	var doc map[string]any
	err := m.opts.Store.Get(context.Background(), checkpointCollection, checkpointID(m.opts.PipelineID), &doc)
	if err != nil {
		return "", fmt.Errorf("binlogin: 未提供 StartGTIDSet 且读取检查点失败: %w", err)
	}
	if !store.MatchesCheckpointRun(doc, m.opts.RunID) {
		return "", errors.New("binlogin: 检查点不属于当前运行，必须提供本次运行的起点")
	}
	gset, ok := store.CheckpointPublishedGTID(doc)
	if !ok {
		return "", errors.New("binlogin: 未提供 StartGTIDSet 且检查点里没有可用的 published_gtid_set")
	}
	return gset, nil
}

// 关键写入必须携带检查点文档的全部字段，不能只写 epoch：真实 store 的 Upsert 是
// 整篇改写（已读 xdoc github.com/xmapst/xdoc/collection.go 源码确认，不是按字段合并），只传 epoch 会把
// 同一篇文档里的 read_gtid_set/published_gtid_set/batch_seq 一并冲掉。
//
// startGTID 是 resolveStartGTIDSet 已经决定的本次运行起点，read/published 在这一刻
// 相等（见 OnInit 调用处的注释），随文档一起写入是为了不丢失这次"顺带"落盘的机会
// ——不这样做的话，epoch 分配到落地之间没有其它字段变化，checkpoint 文档在这个
// 时间点上原本就应该是这个值。
func (m *Module) resolveEpoch(startGTID string) (uint64, error) {
	var stored uint64
	if m.opts.Store != nil {
		var doc map[string]any
		err := m.opts.Store.Get(context.Background(), checkpointCollection, checkpointID(m.opts.PipelineID), &doc)
		if err == nil {
			var valid bool
			stored, valid = store.CheckpointEpoch(doc)
			if !valid {
				return 0, errors.New("binlogin: invalid checkpoint epoch")
			}
			if m.opts.RequireEpochRange {
				ceiling, valid := store.CheckpointEpochCeiling(doc)
				if !valid || ceiling <= stored {
					return 0, errors.New("binlogin: durable source epoch range exhausted")
				}
				m.epochCeiling = ceiling
			}
		} else if m.opts.RequireEpochRange || m.opts.PositionRevision > 0 && !errors.Is(err, xdoc.ErrNotFound) {
			return 0, err
		}
		// 读取失败（典型情况：首次运行，文档尚不存在）不是致命错误，stored
		// 保持零值——与 resolveStartGTIDSet 依赖 Options.StartGTIDSet 区分
		// "首次运行"是同一类容忍，这里没有等价的显式信号，统一按"没有更早的
		// epoch"处理是可接受的保守选择：宁可让新 epoch 从 1 开始，也不能让
		// 一次暂时性读取故障挡住启动。
	}
	if stored >= math.MaxInt64 {
		return 0, errors.New("binlogin: checkpoint epoch exhausted")
	}
	newEpoch := stored + 1

	if m.opts.Store == nil {
		// 未注入 Store：epoch 只能在内存里分配，重启后无法恢复真实的上一个值
		// ——与 resolveStartGTIDSet"未注入 Store 时必须显式给 StartGTIDSet"是
		// 同一条"未接入持久化"的过渡态约定，调用方需自行承担不持久化的后果。
		return newEpoch, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultEpochCommitTimeout)
	defer cancel()
	doc := map[string]any{
		"run_id":                   m.opts.RunID,
		"position_revision":        m.opts.PositionRevision,
		"read_gtid_set":            startGTID,
		"published_gtid_set":       startGTID,
		"published_gtid_set_known": true,
		"epoch":                    int64(newEpoch),
		"batch_seq":                int64(m.batchSeq.Load()),
		"updated_at_ms":            m.now().UnixMilli(),
	}
	if m.opts.RequireEpochRange {
		doc["epoch_ceiling"] = int64(m.epochCeiling)
	}
	if err := m.opts.Store.PutCritical(ctx, store.Op{
		Collection: checkpointCollection,
		ID:         checkpointID(m.opts.PipelineID),
		Doc:        doc,
		Critical:   true,
	}); err != nil {
		return 0, fmt.Errorf("binlogin: 关键写入新 epoch 失败（NODE-XDOC-04 要求先落盘再使用，未落盘不得启动会话）: %w", err)
	}
	return newEpoch, nil
}

// persistCheckpoint 周期性保存 read/published 位点、epoch 和 batch_seq。
// 关键身份切换使用独立的确认持久化路径。
func (m *Module) persistCheckpoint() {
	if m.opts.Store == nil {
		return
	}
	doc := map[string]any{
		"run_id":                   m.opts.RunID,
		"position_revision":        m.opts.PositionRevision,
		"read_gtid_set":            m.readGTIDSet,
		"published_gtid_set":       m.publishedGTIDSet,
		"published_gtid_set_known": true,
		"epoch":                    int64(m.epoch),
		"batch_seq":                int64(m.batchSeq.Load()),
		"updated_at_ms":            m.now().UnixMilli(),
	}
	if m.opts.RequireEpochRange {
		doc["epoch_ceiling"] = int64(m.epochCeiling)
	}
	if err := m.opts.Store.Put(store.Op{
		Collection: checkpointCollection,
		ID:         checkpointID(m.opts.PipelineID),
		Doc:        doc,
	}); err != nil {
		slog.Warn("write checkpoint failed", slog.String("pipeline_id", m.opts.PipelineID), slog.Any("error", err))
	}
}

// 可以在 OnInit（事件循环）里同步调用：这是一次本地 xdoc 读取，不是网络 IO，
// 且 store.Module.Get 本身已经文档化为"可从任意 goroutine 安全调用"（不违反
// ADR-06"阻塞 IO 不能在事件循环里做"——那条约束针对的是连接源库这类真正的
// 网络阻塞调用，见包文档"阻塞 IO 与 actor 状态边界"）。
func (m *Module) loadSchemaHistory(startGTIDSet string) error {
	ctx, cancel := context.WithTimeout(context.Background(), schemaHistoryTimeout)
	defer cancel()
	return m.restoreSchemaHistory(ctx, startGTIDSet)
}

// schemaValidThroughStart 判定一份结构快照是否没有领先于本次恢复位点，即
// startGTIDSet ⊇ validThrough。
//
// 判据是包含而不是相等：干净退出时 OnDestroy 会在最后一条 DDL 之后再落一次
// checkpoint，恢复位点因此通常晚于快照位点，相等会把正常情形全部作废、等于这个
// 功能从未生效。
//
// 它比必要条件更严：startGTIDSet 与 validThrough 都含非 DDL 事务的 GTID，所以一份
// 其实可用的快照也可能因为"读到了一些与结构无关的事务"而被作废。严的方向是安全
// 方向（作废 ⇒ 空 Tracker ⇒ 多传一列），不额外区分。
func schemaValidThroughStart(startGTIDSet, validThrough string) bool {
	if validThrough == "" {
		return true
	}
	startSet, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, startGTIDSet)
	if err != nil {
		return false
	}
	snapSet, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, validThrough)
	if err != nil {
		return false
	}
	return startSet.Contain(snapSet)
}

// persistSchemaHistory critically commits the complete schema version before
// advancing its head or any checkpoint. The boundary uses observed GTIDs, including
// the DDL just applied; publication GTIDs can lag behind that structure.
func (m *Module) persistSchemaHistory(validThroughGTIDSet string) error {
	parent := m.sessionCtx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, schemaHistoryTimeout)
	defer cancel()
	return m.commitSchemaHistory(ctx, validThroughGTIDSet)
}

// setPipelineState 更新本模块对自身 pipeline 运行状态的认知，并通过 reporter
// 上报（NODE-RPT-02：状态变化即时上报）。reporter 模块本身是否真的能收到这条
// Cast，取决于调用方是否已经把两者一起注册进同一个 xhive 应用（生产路径成立，
// 见 internal/node/reporter/messages.go 的文档："未来的 binlog_in...通过 m.Cast(reporter.ModuleName,...)"
// ——本包正是那份注释预告的调用方）；本包自身的单元测试（不经过 xhive.Run/
// defaultApp）验证的是状态转移与事件构造本身，不验证 Cast 是否真的送达一个真实
// reporter 实例，这与 reporter 包自己的测试策略一致。
// Production status carries the configuration version injected by the Loader.
func (m *Module) setPipelineState(state iproto.PipelineState, lastError string) {
	m.pipelineState.Store(int32(state))
	m.lastError = lastError
	status := buildPipelineStatus(m.opts.PipelineID, m.opts.RunID, state, lastError, m.now().UnixMilli())
	status.ConfigVersion = m.runtimeVersion.Load()
	status.RuntimeInstanceId = m.opts.InstanceID
	if m.opts.InstanceID != "" {
		m.Cast("controller", iproto.PipelineRuntimeStatus{PipelineID: m.opts.PipelineID, Role: iproto.PipelineRole_PIPELINE_ROLE_SOURCE, RunID: m.opts.RunID, ConfigVersion: m.runtimeVersion.Load(), InstanceID: m.opts.InstanceID, State: state})
	}
	m.Cast(reporter.ModuleName, reporter.StatusReport{Status: status})
}

func buildPipelineStatus(pipelineID string, runID uint64, state iproto.PipelineState, lastError string, nowMs int64) *iproto.PipelineStatus {
	return &iproto.PipelineStatus{
		PipelineId:    pipelineID,
		Role:          iproto.PipelineRole_PIPELINE_ROLE_SOURCE,
		State:         state,
		ConfigVersion: 0, // setPipelineState replaces this with the configured version.
		RunId:         runID,
		LastError:     safeerror.PublicText(lastError),
		TsMs:          nowMs,
	}
}

func (m *Module) reportErrorEvent(code, message string, retryable bool) {
	m.Cast(reporter.ModuleName, reporter.EventReport{
		Event: buildErrorEvent(m.opts.PipelineID, code, message, retryable, m.now().UnixMilli()),
	})
}

func buildErrorEvent(pipelineID, code, message string, retryable bool, nowMs int64) *iproto.Event {
	publicMessage := safeerror.PublicText(message)
	return &iproto.Event{
		EventId:    uuid.New().String(),
		Kind:       iproto.EventKind_EVENT_KIND_ERROR,
		PipelineId: pipelineID,
		TsMs:       nowMs,
		Level:      "ERROR",
		Message:    publicMessage,
		Detail: &iproto.Event_Error{
			Error: &iproto.ErrorEventDetail{
				Code:      code,
				Detail:    publicMessage,
				Retryable: retryable,
			},
		},
	}
}

func (m *Module) reportDDLEvent(schema, statement, gtid string) {
	m.Cast(reporter.ModuleName, reporter.EventReport{
		Event: &iproto.Event{
			EventId:    uuid.New().String(),
			Kind:       iproto.EventKind_EVENT_KIND_DDL,
			PipelineId: m.opts.PipelineID,
			TsMs:       m.now().UnixMilli(),
			Level:      "INFO",
			Message:    "检测到 DDL",
			Detail: &iproto.Event_Ddl{
				Ddl: &iproto.DDLEventDetail{
					Schema:    schema,
					Statement: statement,
					Gtid:      gtid,
				},
			},
		},
	})
}
