package binlogin

import (
	"fmt"
	"strings"
	"time"
	"uuid"

	"github.com/go-mysql-org/go-mysql/replication"
	"google.golang.org/protobuf/proto"

	"github.com/xmapst/xdts/internal/dblineage"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/mysqlx"
	"github.com/xmapst/xdts/internal/node/resourcequota"
	"github.com/xmapst/xdts/internal/node/schema"
	"github.com/xmapst/xdts/internal/rule"
)

// resultKind 是 aggregator.feed 的产出种类。
type resultKind int

const (
	// resultNone：事件已消费但没有产出（表元数据、非事务边界事件等）。
	resultNone resultKind = iota
	// resultSkip：一个事务/空事务结束但没有任何行变更需要下发（全部行被 TableFilter
	// 剔除，或者 QueryEvent 直接是 COMMIT/ROLLBACK 没有实质内容）——NODE-BIN-05
	// "被过滤的事务同样推进位点"的精神同样适用于这种表级过滤场景：不下发，但位点
	// 必须照常推进，否则重连后会反复重放同一个空事务。
	resultSkip
	// resultTxn：一个完整事务（DML 或 DDL）已聚合完成，可以下发。
	resultTxn
)

// feedResult 是 aggregator.feed 的返回值。
type feedResult struct {
	buffer *resourcequota.Buffer // ownership transfers to dispatch until delivery completes
	kind   resultKind
	gtid   string      // resultSkip/resultTxn 均非空：本次结束的事务的 GTID，供调用方推进位点
	txn    *iproto.Txn // 仅 resultTxn 非空

	schemaWarning string
	schemaChanged bool
	originUnknown bool
}

// aggregatorState 是事务聚合状态机的状态。
type aggregatorState int

const (
	stateIdle             aggregatorState = iota // 等待下一个 GTIDEvent
	stateAwaitBeginOrDDL                         // 已见 GTIDEvent，等待 QueryEvent(BEGIN/DDL/COMMIT)
	stateAccumulatingRows                        // 已见 BEGIN，正在累积 TableMap/RowsEvent，等待 XID
)

// tableMapInfo 缓存一次 TableMapEvent 的解析结果，供该表后续 RowsEvent 复用
// （mysqlx.BuildColumnMeta 的既定用法：每张表调用一次，同一会话内所有后续 RowsEvent
// 复用）。nil 表示该 TableID 对应的表被 TableFilter 拒绝，后续 RowsEvent 直接跳过、
// 不解码（NODE-BIN-06）。
type tableMapInfo struct {
	schema, table string
	meta          []mysqlx.ColumnMeta
	primaryKeyIdx []uint64
	generated     []bool
	lineage       bool
	shape         *replication.TableMapEvent
	projection    *sourceRowProjection
}

// aggregator 是一次 binlog 会话内的事务聚合状态机：接收已解析的 replication 事件，
// 按事务边界（GTID → BEGIN/DDL → ... → XID）聚合成 iproto.Txn。不依赖网络、不依赖
// xhive，纯函数式地通过 feed 驱动，因此可以完全脱离真实 MySQL 做单元测试（构造合成
// 的 replication 事件直接喂给它）。
//
// 一个 aggregator 实例只服务一次会话：TableID → tableMapInfo 的缓存假定同一个
// TableID 在整个会话内语义稳定，重连后由新会话构造新的 aggregator，不跨会话复用
// 缓存——这与 go-mysql 自身按连接维护 table map 缓存的方式一致。
type aggregator struct {
	budget *resourcequota.Budget
	buffer *resourcequota.Buffer
	// Owned by this session's event reader, including heartbeat processing.
	lastWatermark   time.Time
	tableFilter     ITableFilter
	columnFilter    IColumnFilter
	rowFilter       iproto.ISourceRowPlan
	transforms      *rule.TransformPlan
	ddlColumnPolicy *iproto.DDLColumnPolicy
	ddlDependencies bool
	origin          IOriginResolver
	center          string
	databaseRoute   iproto.DatabaseLineageRoute
	nativeGroupUUID string
	loadLineage     func(string) (iproto.StoredDatabaseLineage, bool, error)
	lineageAllowed  func(*iproto.Txn, bool) bool
	storedLineage   *iproto.Txn
	schemaTrack     ISchemaTracker
	// Confined to the serial binlog session. History precedes current catalog.
	ensureSchema       func(schema, table, position string, completeRequired bool) error
	schemaReadPosition func() string

	state  aggregatorState
	tables map[uint64]*tableMapInfo

	// 当前事务累积状态。
	gtid           string
	sourceUUID     string
	lastCommitted  int64
	sequenceNumber int64
	commitTsMs     int64
	rows           []*iproto.RowChange
	savepoints     []transactionSavepoint
	originUnknown  bool
	originRejected bool

	// onTablesFiltered 供调用方统计因表过滤被跳过解码的 RowsEvent 次数，可为空。
	onTablesFiltered func()
}

func newAggregator(tableFilter ITableFilter, columnFilter IColumnFilter, origin IOriginResolver, center string, schemaTracker ISchemaTracker) *aggregator {
	effectiveColumnFilter := columnFilter
	if schemaTracker != nil {
		effectiveColumnFilter = generatedColumnFilter{user: columnFilter, schema: schemaTracker}
	}
	return &aggregator{
		tableFilter:  tableFilter,
		columnFilter: effectiveColumnFilter,
		origin:       origin,
		center:       center,
		schemaTrack:  schemaTracker,
		tables:       make(map[uint64]*tableMapInfo),
	}
}

func (a *aggregator) reset() {
	for _, info := range a.tables {
		if info != nil {
			info.shape = nil
			info.projection = nil
		}
	}
	a.buffer.Release()
	a.buffer = nil
	a.state = stateIdle
	a.gtid = ""
	a.sourceUUID = ""
	a.lastCommitted = 0
	a.sequenceNumber = 0
	a.commitTsMs = 0
	a.rows = nil
	a.savepoints = nil
	a.originUnknown = false
	a.originRejected = false
	a.storedLineage = nil
}

// feed 消费一个已解析的 replication.BinlogEvent。ev 必须已经完成
// TRANSACTION_PAYLOAD_EVENT 解包——调用方（会话循环，见 internal/node/pipeline/binlogin/session.go 的 handleEvent）
// 负责把 TransactionPayloadEvent.Events 里的每个内层事件单独传进来，aggregator 本身
// 不感知压缩事务负载这层包装；心跳事件同理由调用方在 Header.EventType 层面单独拦截，
// 永远不会走到这里。
func (a *aggregator) feed(ev *replication.BinlogEvent) (feedResult, error) {
	switch e := ev.Event.(type) {
	case *replication.GTIDEvent:
		return a.feedGTID(e)
	case *replication.QueryEvent:
		return a.feedQuery(e)
	case *replication.TableMapEvent:
		return feedResult{}, a.feedTableMap(e)
	case *replication.RowsEvent:
		changeType, ok := rowsEventChangeType(ev.Header.EventType)
		if !ok {
			return feedResult{}, fmt.Errorf("binlogin: 不支持的 RowsEvent 类型: %s", ev.Header.EventType)
		}
		return feedResult{}, a.feedRows(e, changeType)
	case *replication.XIDEvent:
		return a.feedXID()
	default:

		return feedResult{}, nil
	}
}

func (a *aggregator) feedGTID(e *replication.GTIDEvent) (feedResult, error) {
	if a.state != stateIdle {
		return feedResult{}, fmt.Errorf("binlogin: 在事务 %q 未结束时收到新的 GTIDEvent", a.gtid)
	}
	gset, err := e.GTIDNext()
	if err != nil {
		return feedResult{}, fmt.Errorf("binlogin: 解析 GTIDEvent 失败: %w", err)
	}
	srcUUID, err := uuid.Parse(string(e.SID))
	if err != nil {
		return feedResult{}, fmt.Errorf("binlogin: 解析 GTIDEvent 来源 UUID 失败: %w", err)
	}
	a.gtid = gset.String()
	a.sourceUUID = srcUUID.String()
	a.lastCommitted = e.LastCommitted
	a.sequenceNumber = e.SequenceNumber
	a.commitTsMs = e.OriginalCommitTime().UnixMilli()
	if e.OriginalCommitTimestamp == 0 {
		a.commitTsMs = 0
	}
	a.state = stateAwaitBeginOrDDL
	return feedResult{}, nil
}

func (a *aggregator) feedQuery(e *replication.QueryEvent) (feedResult, error) {
	q := strings.TrimSpace(string(e.Query))
	upper := strings.ToUpper(q)

	switch a.state {
	case stateAwaitBeginOrDDL:
		switch upper {
		case "BEGIN":
			a.state = stateAccumulatingRows
			return feedResult{}, nil
		case "COMMIT", "ROLLBACK":
			// BEGIN 都没出现就直接 COMMIT/ROLLBACK：MySQL 正常不会为真正空事务写
			// binlog，这里仍防御性地按空事务处理，不报错拒绝（宁可多推进一次位点，
			// 也不要因为一个理论上不该出现、但无害的序列而中断整个会话）。
			gtid := a.gtid
			txn := &iproto.Txn{Gtid: gtid, CommitTsMs: a.commitTsMs}
			a.applyOrigin(txn)
			unknown := a.originUnknown
			a.reset()
			return feedResult{kind: resultSkip, gtid: gtid, txn: txn, originUnknown: unknown}, nil
		default:
			// DDL（隐式提交）：GTID 之后直接跟一条非 BEGIN/COMMIT/ROLLBACK 的
			// QueryEvent，本身就是该事务的提交点。
			//
			// Update schema history even for excluded DDL. Scope is independently
			// parsed below; observational AffectedTables never controls execution.
			if err := a.loadDDLLineage(); err != nil {
				return feedResult{}, err
			}
			schemaWarning := ""
			var affectedTables []string
			var dependencies []*iproto.DDLTableDependencies
			if a.schemaTrack != nil && !reservedLineageDDL(string(e.Schema), q) {
				if tracker, ok := a.schemaTrack.(interface {
					DependenciesBeforeDDL(string, string) []schema.OriginalTableDependencies
				}); a.ddlDependencies && ok {
					for _, original := range tracker.DependenciesBeforeDDL(string(e.Schema), q) {
						dependencies = append(dependencies, iproto.SourceDependencies(original.Schema, original.Table, original.Dependencies))
					}
				}
				affectedTables, schemaWarning = a.schemaTrack.ApplyDDL(string(e.Schema), q, a.gtid, a.commitTsMs)
			}
			allowed, err := a.allowDDL(string(e.Schema), q)
			if err != nil {
				return feedResult{}, err
			}
			if !allowed {
				gtid := a.gtid
				txn := &iproto.Txn{Gtid: gtid, CommitTsMs: a.commitTsMs, Ddl: &iproto.DDL{Schema: string(e.Schema), Statement: q}}
				a.applyOrigin(txn)
				unknown := a.originUnknown
				a.reset()
				return feedResult{kind: resultSkip, gtid: gtid, txn: txn, originUnknown: unknown, schemaWarning: schemaWarning, schemaChanged: true}, nil
			}
			txn := &iproto.Txn{
				Gtid:           a.gtid,
				CommitTsMs:     a.commitTsMs,
				LastCommitted:  a.lastCommitted,
				SequenceNumber: a.sequenceNumber,
				Ddl: &iproto.DDL{
					Schema:             string(e.Schema),
					Statement:          q,
					CommitTsMs:         a.commitTsMs,
					AffectedTables:     affectedTables,
					ColumnPolicy:       iproto.CloneDDLColumnPolicy(a.ddlColumnPolicy),
					SourceDependencies: dependencies,
				},
			}
			a.applyOrigin(txn)
			unknown := a.originUnknown
			gtid := a.gtid
			a.reset()
			return feedResult{kind: resultTxn, gtid: gtid, txn: txn, schemaWarning: schemaWarning, originUnknown: unknown}, nil
		}
	case stateAccumulatingRows:
		if handled, err := a.feedSavepoint(q); handled || err != nil {
			return feedResult{}, err
		}
		if upper != "COMMIT" && upper != "ROLLBACK" {
			// Row events and explicitly parsed savepoint statements are accepted;
			// 出现其它 QueryEvent 视为流异常，明确拒绝而不是静默忽略（AGENTS.md
			//"边界输入必须校验"）。
			return feedResult{}, fmt.Errorf("binlogin: 事务累积行期间收到意外的 QueryEvent: %q", q)
		}
		// 非 XA 事务（例如混合非事务性引擎）用 QueryEvent(COMMIT) 而不是 XIDEvent
		// 提交，NODE-BIN-02 明确把 COMMIT 列为 QueryEvent 的合法取值之一。与
		// feedXID 共用同一套"有行就下发、无行就跳过"的收尾逻辑。
		if upper == "ROLLBACK" {
			clear(a.rows)
			a.rows = nil
		}
		return a.finishTxn(), nil
	default:
		return feedResult{}, fmt.Errorf("binlogin: 在状态 %d 下收到意外的 QueryEvent: %q", a.state, q)
	}
}

func (a *aggregator) feedTableMap(e *replication.TableMapEvent) error {
	if reservedLineageTable(e) {
		if e.ColumnCount != 2 {
			return fmt.Errorf("binlogin: incompatible lineage table map")
		}
		a.tables[e.TableID] = &tableMapInfo{lineage: true}
		return nil
	}
	if dblineage.Reserved(string(e.Schema)) {
		a.tables[e.TableID] = nil
		return nil
	}
	if a.originRejected {
		a.tables[e.TableID] = nil
		return nil
	}
	schema, table := string(e.Schema), string(e.Table)
	if !a.tableFilter.MatchTable(schema, table) {
		a.tables[e.TableID] = nil
		return nil
	}
	if info := a.tables[e.TableID]; a.state == stateAccumulatingRows && info != nil && !info.lineage && tableMapShapesEqual(info.shape, e) {
		return nil
	}
	if a.ensureSchema != nil {
		position, err := a.schemaReplayPosition()
		if err != nil {
			return err
		}
		completeRequired := len(e.ColumnNameString()) != int(e.ColumnCount)
		if a.transforms.HasTable(schema, table) {
			completeRequired = true
		}
		enums, sets := e.EnumStrValueMap(), e.SetStrValueMap()
		for i := range int(e.ColumnCount) {
			if (e.IsEnumColumn(i) && len(enums[i]) == 0) || (e.IsSetColumn(i) && len(sets[i]) == 0) {
				completeRequired = true
			}
		}
		if err := a.ensureSchema(schema, table, position, completeRequired); err != nil {
			return err
		}
	}
	meta, primaryKey, err := a.buildSchemaMetadata(e)
	if err != nil {
		return fmt.Errorf("binlogin: 解析表 %s.%s 的列元数据失败: %w", schema, table, err)
	}
	a.tables[e.TableID] = &tableMapInfo{
		schema:        schema,
		table:         table,
		meta:          meta,
		primaryKeyIdx: primaryKey,
	}
	if a.schemaTrack != nil {
		info := a.tables[e.TableID]
		info.generated = make([]bool, len(info.meta))
		for i, column := range info.meta {
			info.generated[i] = a.schemaTrack.IsGenerated(schema, table, column.Name)
		}
	}
	if a.state == stateAccumulatingRows {
		if a.budget != nil {
			if a.buffer == nil {
				a.buffer, err = a.budget.Reserve(0)
				if err != nil {
					return err
				}
			}
			if err := a.buffer.Grow(tableMapShapeBytes(e)); err != nil {
				return err
			}
		}
		a.tables[e.TableID].shape = cloneTableMapShape(e)
	}
	return nil
}

func (a *aggregator) feedRows(e *replication.RowsEvent, changeType iproto.ChangeType) error {
	if a.state != stateAccumulatingRows {
		return fmt.Errorf("binlogin: 在状态 %d 下收到意外的 RowsEvent（table_id=%d）", a.state, e.TableID)
	}
	if a.originRejected && !a.databaseRoute.Persistent {
		return nil
	}
	info, ok := a.tables[e.TableID]
	if !ok {
		return fmt.Errorf("binlogin: RowsEvent（table_id=%d）之前没有出现过对应的 TableMapEvent", e.TableID)
	}
	if info != nil && info.lineage {
		return a.feedLineageRows(e, changeType)
	}
	if a.originRejected {
		return nil
	}
	if info == nil {
		if a.onTablesFiltered != nil {
			a.onTablesFiltered()
		}
		return nil // 表被 TableFilter 拒绝，尽早丢弃，不解码（NODE-BIN-06）
	}
	if !a.allowRowOperation(changeType) {
		return nil
	}
	var changes []*iproto.RowChange
	var err error
	if info.shape != nil && (a.rowFilter == nil || !a.rowFilter.HasRowFilters(info.schema, info.table)) {
		if info.projection == nil {
			var projection *sourceRowProjection
			projection, err = buildSourceRowProjection(info, a.columnFilter)
			if err == nil && a.budget != nil {
				if a.buffer == nil {
					a.buffer, err = a.budget.Reserve(0)
				}
				if err == nil {
					err = a.buffer.Grow(uint64(128 + 64*len(info.meta) + 32*len(info.primaryKeyIdx)))
				}
			}
			if err == nil {
				info.projection = projection
			}
		}
		if err == nil {
			changes, err = buildProjectedRowChanges(e, changeType, info, info.projection)
		}
	} else {
		changes, err = buildFilteredRowChanges(e, changeType, info, a.columnFilter, a.rowFilter)
	}
	if err != nil {
		return fmt.Errorf("binlogin: 解码表 %s.%s 的行变更失败: %w", info.schema, info.table, err)
	}
	if err := a.transformRows(info, changes); err != nil {
		return err
	}
	if a.budget != nil && len(changes) > 0 {
		if a.buffer == nil {
			a.buffer, err = a.budget.Reserve(0)
			if err != nil {
				return err
			}
		}
		for _, change := range changes {
			// Cover decoded protobuf objects, row pointer arrays and encoding
			// copies retained until synchronous publisher confirmation returns.
			cost := uint64(proto.Size(change))*4 + 256
			cost += uint64(len(change.GetBefore().GetValues())+len(change.GetAfter().GetValues())) * 192
			if err := a.buffer.Grow(cost); err != nil {
				return err
			}
		}
	}
	a.rows = append(a.rows, changes...)
	return nil
}

func (a *aggregator) feedXID() (feedResult, error) {
	if a.state != stateAccumulatingRows {
		return feedResult{}, fmt.Errorf("binlogin: 在状态 %d 下收到意外的 XIDEvent", a.state)
	}
	return a.finishTxn(), nil
}

// finishTxn 是事务收尾的共用逻辑：XIDEvent 与 QueryEvent(COMMIT/ROLLBACK) 都在
// stateAccumulatingRows 状态下走到这里——有行就下发（resultTxn），没有行（全部被
// TableFilter 剔除，或事务本身没有实际写入）就只推进位点（resultSkip）。
func (a *aggregator) finishTxn() feedResult {
	gtid := a.gtid
	if len(a.rows) == 0 {
		txn := &iproto.Txn{Gtid: gtid, CommitTsMs: a.commitTsMs}
		a.applyOrigin(txn)
		unknown := a.originUnknown
		a.reset()
		return feedResult{kind: resultSkip, gtid: gtid, txn: txn, originUnknown: unknown}
	}
	txn := &iproto.Txn{
		Gtid:           a.gtid,
		CommitTsMs:     a.commitTsMs,
		LastCommitted:  a.lastCommitted,
		SequenceNumber: a.sequenceNumber,
		Rows:           a.rows,
	}
	a.applyOrigin(txn)
	unknown := a.originUnknown
	buffer := a.buffer
	a.buffer = nil // result owns the rows until dispatch's publisher returns
	a.reset()
	return feedResult{kind: resultTxn, gtid: gtid, txn: txn, originUnknown: unknown, buffer: buffer}
}

// applyOrigin 填充 Txn 的来源中心和血缘路径；未知 UUID 保留告警标记。
func (a *aggregator) applyOrigin(txn *iproto.Txn) {
	if a.applyStoredOrigin(txn) {
		return
	}
	center, path, hops, ok := a.origin.Resolve(a.sourceUUID)
	if !ok {
		a.originUnknown = true
		center, path, hops = a.center, []string{a.center}, 1
	}
	txn.OriginCenter = center
	txn.Path = append([]string(nil), path...)
	txn.Hops = hops
	if a.databaseRoute.SourceServerUUID != "" {
		if resolver, ok := a.origin.(iproto.ISourceEndpointPlan); ok {
			if origin, known := resolver.ResolveEndpoint(a.sourceUUID); known && origin != a.databaseRoute.SourceServerUUID {
				txn.EndpointPath = append(txn.EndpointPath, origin)
			}
		}
		txn.EndpointPath = append(txn.EndpointPath, a.databaseRoute.SourceServerUUID)
		if uint32(len(txn.EndpointPath)) > txn.Hops {
			txn.Hops = uint32(len(txn.EndpointPath))
		}
	}
	if a.isNativeLineageRoot() {
		txn.OriginCenter, txn.Path, txn.EndpointPath, txn.Hops = a.center, []string{a.center}, []string{a.databaseRoute.SourceServerUUID}, 1
		txn.LineageVersion = iproto.StoredLineageVersion
		a.originUnknown = false
	}
}
