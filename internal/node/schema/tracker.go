package schema

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	parsermysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/xmapst/xdts/internal/schemamig"
)

// Tracker 是 binlog_in 用来增量维护表结构的内存模型（见包文档）。零值不可用，
// 用 NewTracker 构造。
type Tracker struct {
	tables map[string]TableStructure

	caseSensitive bool
}

func NewTracker(caseSensitive bool) *Tracker {
	return &Tracker{tables: make(map[string]TableStructure), caseSensitive: caseSensitive}
}

func (t *Tracker) tableKey(schema, table string) string {
	if t.caseSensitive {
		return schema + "." + table
	}
	return strings.ToLower(schema) + "." + strings.ToLower(table)
}

// identEqual 按 t.caseSensitive 比较一对库名/表名标识符，语义与 tableKey 一致
// （对同一对输入应该产出与"折算成同一个 tableKey"相同的结论）。目前只用于
// applyAlterTable 判断 RENAME 是否真的改变了表的库/表身份，不用于列名——列名
// 比较固定用 strings.EqualFold，不受 caseSensitive 影响，见 internal/node/schema/types.go。
func (t *Tracker) identEqual(a, b string) bool {
	if t.caseSensitive {
		return a == b
	}
	return strings.EqualFold(a, b)
}

// Lookup 返回某张表当前已知的结构，ok=false 表示本 Tracker 从未见过它（可能
// 从未被 DDL/schema_history 触达，也可能已被 DROP）。
func (t *Tracker) Lookup(schema, table string) (TableStructure, bool) {
	st, ok := t.tables[t.tableKey(schema, table)]
	return st, ok
}

// IsGenerated 判定某列当前是否已知为生成列（STORED/VIRTUAL）。表或列未知时
// 保守返回 false——与"未接线默认放行"是同一种失败方向：宁可暂时多传一列，
// 也不要在结构未知时武断地把一个普通列误判成生成列而丢弃真实数据。
func (t *Tracker) IsGenerated(schema, table, column string) bool {
	st, ok := t.Lookup(schema, table)
	if !ok {
		return false
	}
	i := st.columnIndex(column)
	if i < 0 {
		return false
	}
	return st.Columns[i].Generated
}

// Len 返回当前已跟踪的表数量，供 OnInit 判断"是否是从 schema_history 恢复了
// 非空状态"（见 internal/node/pipeline/binlogin 的 Warn 日志）。
func (t *Tracker) Len() int { return len(t.tables) }

// Snapshot is a deep-copyable table structure archive.
// CaseSensitive must match the receiving Tracker before table keys are rebuilt.
// Tables use a slice because xdoc map keys cannot contain dots. The caller supplies
// ValidThroughGTIDSet so persisted structure can be checked against checkpoints.
// Published xdoc keys use Go field names; json tags do not change disk keys.
type Snapshot struct {
	CaseSensitive bool `json:"case_sensitive"`
	// Tables 是全部已知表结构，顺序不承载语义（Restore 按 Schema/Table 重算键）。
	Tables []TableStructure `json:"tables"`
	// ValidThroughGTIDSet 是"本份结构反映到哪个 GTID 集合为止"，由调用方填写。
	// 校验发生在 binlogin.loadSchemaHistory（本包不引入 GTID 集合代数）。
	ValidThroughGTIDSet string `json:"valid_through_gtid_set"`
}

// Snapshot 返回当前全部已知表结构的深拷贝快照 + 生成这份快照时的 caseSensitive
// 策略（见 Snapshot 类型文档），供持久化到 schema_history。
// Tables 与 Tracker 内部状态互不共享底层数组，调用方（含序列化发生在稍后批次提交的
// store 模块）可以安全长期持有。
func (t *Tracker) Snapshot() Snapshot {
	tables := make([]TableStructure, 0, len(t.tables))
	for _, v := range t.tables {
		tables = append(tables, v.clone())
	}
	// 按 (Schema, Table) 定序：map 遍历序随机，而这份快照会被整篇落盘。不定序的话
	// 内容一字未改的两次 Snapshot 会产出不同的字节串，任何"内容是否变了"的判断
	// （包括将来可能加的 digest/校验和）都会被这个随机序毒害。
	sort.Slice(tables, func(i, j int) bool {
		if tables[i].Schema != tables[j].Schema {
			return tables[i].Schema < tables[j].Schema
		}
		return tables[i].Table < tables[j].Table
	})
	return Snapshot{CaseSensitive: t.caseSensitive, Tables: tables}
}

// RestoreOutcome 是 Tracker.Restore 的结果，供调用方（binlogin.loadSchemaHistory）
// 决定要不要额外记一条"为什么最终是空的"日志——RestoreEmpty 与
// RestoreDiscardedPolicyMismatch 在 Restore 之后 t.Len()==0 这一点上表现相同，但
// 原因截然不同，不应该被同一条 Warn 文案盖住（见 Snapshot 类型文档）。
type RestoreOutcome int

const (
	// RestoreEmpty 表示传入的快照本身没有内容（Tables 长度为 0），没有可恢复的东西
	// ——通常对应"从未持久化过"或"上一次持久化时 Tracker 本来就是空的"。
	RestoreEmpty RestoreOutcome = iota
	// RestoreApplied 表示快照非空且 CaseSensitive 与当前 Tracker 一致，已成功恢复。
	RestoreApplied
	// RestoreDiscardedPolicyMismatch 表示快照非空，但它的 CaseSensitive 与当前
	// Tracker 的策略不一致，已整体作废（Tracker 保持恢复前的状态，不是"部分应用"）
	// ——调用方应该按"这条 pipeline 需要重放 DDL 重建结构"处理，与真正的首次运行
	// 走同一条已有路径，但原因不同，值得单独记录。
	RestoreDiscardedPolicyMismatch
)

// Restore 用一份持久化快照替换当前全部状态。由启动阶段或串行会话所有者在
// 开始处理事件之前调用；空快照清空全部表，表示所选位点没有已知结构。
func (t *Tracker) Restore(snapshot Snapshot) RestoreOutcome {
	if len(snapshot.Tables) == 0 {
		t.tables = make(map[string]TableStructure)
		return RestoreEmpty
	}
	if snapshot.CaseSensitive != t.caseSensitive {
		return RestoreDiscardedPolicyMismatch
	}
	t.tables = make(map[string]TableStructure, len(snapshot.Tables))
	for _, v := range snapshot.Tables {
		// 重算键而不是落盘键：见 Snapshot 类型文档"为什么 Tables 是数组"。这一步
		// 必须在上面 CaseSensitive 相等检查**之后**——策略一致才意味着 tableKey
		// 算法一致。
		t.tables[t.tableKey(v.Schema, v.Table)] = v.clone()
	}
	return RestoreApplied
}

// ApplyDDL 解析一条 DDL 语句并增量更新内存结构。defaultSchema 是该连接当前的
// 默认库（对应 binlog QueryEvent.Schema），供未显式带库名的语句使用；gtid/atMs
// 写入受影响表结构的 UpdatedGTID/UpdatedAtMs。
//
// 返回值 affectedTables 是尽力而为的受影响表清单（docs/xdts-design.md proto/data.proto
// DDL.affected_tables 的字面定义："尽力而为，仅供路由/统计/界面展示使用，
// 不作为执行依据"），warning 非空表示这条语句解析失败或属于本包明确不追踪的
// 派生定义（CREATE TABLE ... LIKE/AS SELECT）——两种情况调用方都应该记录但
// 绝不能因此中断会话：DDL 事务本该怎么透传还怎么透传，本包只是尽力附加
// 结构跟踪副作用。语句本身如果压根不是表结构语句（视图/存储过程/触发器/
// GRANT/SET 等），静默返回 (nil, "")，不算异常。
func (t *Tracker) ApplyDDL(defaultSchema, statement, gtid string, atMs int64) (affectedTables []string, warning string) {
	stmt, err := parseOneStmt(statement)
	if err != nil {
		return nil, fmt.Sprintf("解析失败，跳过结构跟踪（不影响该 DDL 语句本身的转发）: %v", err)
	}
	if create, ok := stmt.(*ast.CreateTableStmt); ok {
		affectedTables, warning = t.applyCreateTable(defaultSchema, create, gtid, atMs)
		if warning == "" {
			schemaName, tableName := resolveTableName(defaultSchema, create.Table)
			key := t.tableKey(schemaName, tableName)
			value := t.tables[key]
			if create.ReferTable == nil {
				value.DDLDependencies = schemamig.CaptureTableDependencies(statement)
			}
			t.tables[key] = value
		}
		return affectedTables, warning
	}
	var changedTable *ast.TableName
	switch s := stmt.(type) {
	case *ast.AlterTableStmt:
		changedTable = s.Table
	case *ast.CreateIndexStmt:
		changedTable = s.Table
	case *ast.DropIndexStmt:
		changedTable = s.Table
	}
	if changedTable != nil {
		schemaName, tableName := resolveTableName(defaultSchema, changedTable)
		key := t.tableKey(schemaName, tableName)
		if value, ok := t.tables[key]; ok {
			value.DDLDependencies = schemamig.AdvanceTableDependencies(value.DDLDependencies, statement)
			t.tables[key] = value
		}
	}

	switch n := stmt.(type) {
	case *ast.DropTableStmt:
		return t.applyDropTable(defaultSchema, n)
	case *ast.AlterTableStmt:
		return t.applyAlterTable(defaultSchema, n, gtid, atMs)
	case *ast.RenameTableStmt:
		return t.applyRenameTable(defaultSchema, n)
	case *ast.TruncateTableStmt:
		_, table := resolveTableName(defaultSchema, n.Table)
		return []string{table}, "" // TRUNCATE 不改变列结构
	case *ast.CreateIndexStmt:
		return t.applyCreateIndex(defaultSchema, n, gtid, atMs)
	case *ast.DropIndexStmt:
		return t.applyDropIndex(defaultSchema, n, gtid, atMs)
	default:
		// 非表结构语句（CREATE VIEW/PROCEDURE/FUNCTION/TRIGGER/EVENT、
		// CREATE/DROP DATABASE、GRANT、SET、FLUSH 等）：不产生 TableMapEvent/
		// RowsEvent，也不影响生成列判定，本包明确不关心，不是漏判。
		return nil, ""
	}
}

func resolveTableName(defaultSchema string, tn *ast.TableName) (schemaName, table string) {
	schemaName = tn.Schema.O
	if schemaName == "" {
		schemaName = defaultSchema
	}
	return schemaName, tn.Name.O
}

func (t *Tracker) applyCreateTable(defaultSchema string, ct *ast.CreateTableStmt, gtid string, atMs int64) ([]string, string) {
	schemaName, tableName := resolveTableName(defaultSchema, ct.Table)

	if ct.ReferTable != nil {
		refSchema, refTable := resolveTableName(defaultSchema, ct.ReferTable)
		if source, ok := t.Lookup(refSchema, refTable); ok && source.MetadataComplete {
			source.Schema, source.Table = schemaName, tableName
			source.UpdatedGTID, source.UpdatedAtMs = gtid, atMs
			// LIKE omits foreign keys and generates new CHECK identities. Column
			// and row key metadata remain exact; named DDL dependencies do not.
			source.DDLDependencies.Complete = false
			t.tables[t.tableKey(schemaName, tableName)] = source.clone()
			return []string{tableName}, ""
		}
	}
	if ct.ReferTable != nil || ct.Select != nil {

		return []string{tableName}, "CREATE TABLE ... LIKE/AS SELECT 不追踪列结构（已知范围边界）"
	}

	cols := make([]ColumnStructure, 0, len(ct.Cols))
	for _, col := range ct.Cols {
		cols = append(cols, columnFromDef(col))
	}
	pk, uniques := keysFromColumnsAndConstraints(ct.Cols, ct.Constraints)

	t.tables[t.tableKey(schemaName, tableName)] = TableStructure{
		Schema:           schemaName,
		Table:            tableName,
		MetadataComplete: true,
		Columns:          cols,
		PrimaryKey:       pk,
		UniqueKeys:       uniques,
		UpdatedGTID:      gtid,
		UpdatedAtMs:      atMs,
	}
	return []string{tableName}, ""
}

func (t *Tracker) applyDropTable(defaultSchema string, dt *ast.DropTableStmt) ([]string, string) {
	if dt.IsView {
		return nil, "" // DROP VIEW 不是表结构变更
	}
	tables := make([]string, 0, len(dt.Tables))
	for _, tn := range dt.Tables {
		schemaName, tableName := resolveTableName(defaultSchema, tn)
		delete(t.tables, t.tableKey(schemaName, tableName))
		tables = append(tables, tableName)
	}
	return tables, ""
}

func (t *Tracker) applyRenameTable(defaultSchema string, rt *ast.RenameTableStmt) ([]string, string) {
	affected := make([]string, 0, len(rt.TableToTables)*2)
	for _, tt := range rt.TableToTables {
		oldSchema, oldTable := resolveTableName(defaultSchema, tt.OldTable)
		newSchema, newTable := resolveTableName(defaultSchema, tt.NewTable)
		t.renameTable(oldSchema, oldTable, newSchema, newTable)
		affected = append(affected, oldTable, newTable)
	}
	return affected, ""
}

func (t *Tracker) renameTable(oldSchema, oldTable, newSchema, newTable string) {
	oldKey := t.tableKey(oldSchema, oldTable)
	st, ok := t.tables[oldKey]
	delete(t.tables, oldKey)
	if !ok {
		// 没有旧结构可迁移（这张表在本 Tracker 里从未出现过，例如重启后还
		// 没见过它）：仍然在新 key 下留一条空结构占位，而不是完全不处理——
		// 空结构下 IsGenerated 对它继续保守返回 false，行为等价于"未知"，
		// 不会引入错误判定。
		st = TableStructure{}
	}
	st.Schema, st.Table = newSchema, newTable
	t.tables[t.tableKey(newSchema, newTable)] = st
}

func (t *Tracker) applyAlterTable(defaultSchema string, at *ast.AlterTableStmt, gtid string, atMs int64) ([]string, string) {
	schemaName, tableName := resolveTableName(defaultSchema, at.Table)
	st, existed := t.tables[t.tableKey(schemaName, tableName)]
	if !existed {
		st = TableStructure{Schema: schemaName, Table: tableName}
	}

	var warnings []string
	currentSchema, currentTable := schemaName, tableName
	for _, spec := range at.Specs {
		if spec.Tp == ast.AlterTableRenameTable && spec.NewTable != nil {
			// 同一条 ALTER TABLE 可以用逗号级联多个动作，RENAME 通常排在
			// 最后但语法上不禁止排在中间；后续子句仍作用于同一张表，只是
			// 归属的 key 变了，循环结束后统一挪库，这里先记下新名。
			currentSchema, currentTable = resolveTableName(defaultSchema, spec.NewTable)
			continue
		}
		if w := applySpec(&st, spec); w != "" {
			warnings = append(warnings, w)
		}
	}

	// 这里保留字面 != 而不是 identEqual：无论判断成"变了"还是"没变"，
	// 下面几行都会用 t.tableKey(currentSchema, currentTable) 覆盖写入——
	// tableKey 自己已经按 caseSensitive 折算，一次多余的 delete+overwrite
	// 与直接 overwrite 到同一个 key 的最终状态完全相同，不影响正确性，
	// 不需要在这里重复接线 caseSensitive。
	if currentSchema != schemaName || currentTable != tableName {
		delete(t.tables, t.tableKey(schemaName, tableName))
	}
	st.Schema, st.Table = currentSchema, currentTable
	st.UpdatedGTID, st.UpdatedAtMs = gtid, atMs
	// The column tracker also owns physical FIRST/AFTER ordering. Dependencies
	// must describe that same version rather than an append-only column list.
	st.DDLDependencies.Columns = make([]string, len(st.Columns))
	for i, column := range st.Columns {
		st.DDLDependencies.Columns[i] = column.Name
	}
	if !st.MetadataComplete {
		st.DDLDependencies.Complete = false
	}
	t.tables[t.tableKey(currentSchema, currentTable)] = st

	affected := []string{tableName}
	// 用 identEqual（而不是字面 !=）判断是否真的换了身份：与 tableKey 同一条
	// caseSensitive 策略，否则 caseSensitive=false 时一次纯粹的大小写改名
	// （如 RENAME TO T1，源端实际不区分）会被误报成两张不同的表。库名/表名
	// 两部分对称地用同一个策略比较，不做"库名折叠、表名不折叠"这种不一致
	// 处理。
	if !t.identEqual(currentTable, tableName) || !t.identEqual(currentSchema, schemaName) {
		affected = append(affected, currentTable)
	}
	return affected, strings.Join(warnings, "; ")
}

// applySpec 应用单个 AlterTableSpec。只显式处理会改变"列集合 / 主键 / 唯一键"
// 这三项跟踪目标的子句类型；其余（分区、外键、TTL、masking policy、
// statistics、TiFlash 副本、ALGORITHM/LOCK/FORCE 提示等）明确忽略、不警告
// （见包文档"已知范围边界"）。真正意料之外、未来 tidb parser 新增的子句类型
// 才会落到 default 分支产生 warning。
func applySpec(st *TableStructure, spec *ast.AlterTableSpec) string {
	switch spec.Tp {
	case ast.AlterTableAddColumns:
		for _, col := range spec.NewColumns {
			if warning := placeColumn(st, columnFromDef(col), spec.Position); warning != "" {
				return warning
			}
		}
		for _, cst := range spec.NewConstraints {
			applyConstraint(st, cst)
		}
	case ast.AlterTableDropColumn:
		if spec.OldColumnName != nil {
			st.removeColumn(spec.OldColumnName.Name.O)
		}
	case ast.AlterTableModifyColumn:
		for _, col := range spec.NewColumns {
			if warning := placeColumn(st, columnFromDef(col), spec.Position); warning != "" {
				return warning
			}
		}
	case ast.AlterTableChangeColumn:
		if spec.OldColumnName != nil && len(spec.NewColumns) == 1 {
			st.renameColumn(spec.OldColumnName.Name.O, spec.NewColumns[0].Name.Name.O)
			if warning := placeColumn(st, columnFromDef(spec.NewColumns[0]), spec.Position); warning != "" {
				return warning
			}
		}
	case ast.AlterTableRenameColumn:
		if spec.OldColumnName != nil && spec.NewColumnName != nil {
			st.renameColumn(spec.OldColumnName.Name.O, spec.NewColumnName.Name.O)
		}
	case ast.AlterTableAddConstraint:
		if spec.Constraint != nil {
			applyConstraint(st, spec.Constraint)
		}
	case ast.AlterTableDropPrimaryKey:
		st.PrimaryKey = nil
	case ast.AlterTableDropIndex:
		st.removeUniqueKeyByName(spec.Name)
	case ast.AlterTableOption, ast.AlterTableAlterColumn, ast.AlterTableLock,
		ast.AlterTableAlgorithm, ast.AlterTableRenameIndex, ast.AlterTableForce,
		ast.AlterTableOrderByColumns, ast.AlterTableEnableKeys, ast.AlterTableDisableKeys,
		ast.AlterTableDropForeignKey, ast.AlterTableWriteable, ast.AlterTableIndexInvisible,
		ast.AlterTableAttributes, ast.AlterTableCache, ast.AlterTableNoCache,
		ast.AlterTableStatsOptions, ast.AlterTableAddStatistics, ast.AlterTableDropStatistics,
		ast.AlterTableSetTiFlashReplica, ast.AlterTableWithValidation, ast.AlterTableWithoutValidation,
		ast.AlterTableSecondaryLoad, ast.AlterTableSecondaryUnload, ast.AlterTableAlterCheck,
		ast.AlterTableDropCheck:
		// 已知且明确不影响列/主键/唯一键集合，忽略。
	default:
		if isPartitionSpec(spec.Tp) {
			return "" // 分区维护类子句统一忽略，不逐个枚举（见包文档）
		}
		st.MetadataComplete = false
		return fmt.Sprintf("ALTER TABLE 子句类型 %d 不在本包已识别范围内，可能遗漏结构变更（尽力而为，已知边界）", spec.Tp)
	}
	return ""
}

// isPartitionSpec 判断是否属于分区维护类子句——这类子句数量多（十几种）且都
// 不改变列集合，逐个在 switch 里枚举没有信息量，这里用一个独立判定函数收敛。
func isPartitionSpec(tp ast.AlterTableType) bool {
	switch tp {
	case ast.AlterTablePartitionAttributes, ast.AlterTablePartitionOptions,
		ast.AlterTableCoalescePartitions, ast.AlterTableDropPartition,
		ast.AlterTableTruncatePartition, ast.AlterTablePartition,
		ast.AlterTableRemovePartitioning, ast.AlterTableRebuildPartition,
		ast.AlterTableReorganizePartition, ast.AlterTableCheckPartitions,
		ast.AlterTableExchangePartition, ast.AlterTableOptimizePartition,
		ast.AlterTableRepairPartition, ast.AlterTableImportPartitionTablespace,
		ast.AlterTableDiscardPartitionTablespace, ast.AlterTableAddPartitions,
		ast.AlterTableDropFirstPartition, ast.AlterTableAddLastPartition,
		ast.AlterTableReorganizeLastPartition, ast.AlterTableReorganizeFirstPartition,
		ast.AlterTableImportTablespace, ast.AlterTableDiscardTablespace,
		ast.AlterTableRemoveTTL:
		return true
	default:
		return false
	}
}

func columnFromDef(col *ast.ColumnDef) ColumnStructure {
	cs := ColumnStructure{Name: col.Name.Name.O}
	if col.Tp != nil {
		cs.MySQLType = col.Tp.GetType()
		cs.Unsigned = parsermysql.HasUnsignedFlag(col.Tp.GetFlag())
		switch cs.MySQLType {
		case parsermysql.TypeEnum:
			cs.EnumValues = append([]string(nil), col.Tp.GetElems()...)
		case parsermysql.TypeSet:
			cs.SetValues = append([]string(nil), col.Tp.GetElems()...)
		}
	}
	for _, opt := range col.Options {
		if opt.Tp == ast.ColumnOptionGenerated {
			cs.Generated = true
			cs.GeneratedStored = opt.Stored
		}
	}
	return cs
}

// Physical order is required when a TableMap omits its optional column names.
// Missing AFTER references invalidate fallback metadata instead of guessing.
func placeColumn(st *TableStructure, column ColumnStructure, position *ast.ColumnPosition) string {
	if position == nil || position.Tp == ast.ColumnPositionNone {
		st.upsertColumn(column)
		return ""
	}
	if i := st.columnIndex(column.Name); i >= 0 {
		st.Columns = append(st.Columns[:i], st.Columns[i+1:]...)
	}
	index := 0
	if position.Tp == ast.ColumnPositionAfter {
		if position.RelativeColumn == nil || st.columnIndex(position.RelativeColumn.Name.O) < 0 {
			st.MetadataComplete = false
			st.upsertColumn(column)
			return "ALTER TABLE AFTER 的参照列缺失，列顺序元数据不可用于解码兜底"
		}
		index = st.columnIndex(position.RelativeColumn.Name.O) + 1
	} else if position.Tp != ast.ColumnPositionFirst {
		st.MetadataComplete = false
		st.upsertColumn(column)
		return "ALTER TABLE 列位置无法识别，列顺序元数据不可用于解码兜底"
	}
	st.Columns = append(st.Columns, ColumnStructure{})
	copy(st.Columns[index+1:], st.Columns[index:len(st.Columns)-1])
	st.Columns[index] = column
	return ""
}

// keysFromColumnsAndConstraints 从 CREATE TABLE 的列级内联选项与表级约束里
// 提取主键与唯一键集合（"唯一键集合...须由 DDL 经 parser
// 解析而来"）。
func keysFromColumnsAndConstraints(cols []*ast.ColumnDef, constraints []*ast.Constraint) (pk []string, uniques []UniqueKey) {
	for _, col := range cols {
		name := col.Name.Name.O
		for _, opt := range col.Options {
			switch opt.Tp {
			case ast.ColumnOptionPrimaryKey:
				pk = append(pk, name)
			case ast.ColumnOptionUniqKey:
				uniques = append(uniques, UniqueKey{Columns: []string{name}})
			}
		}
	}
	for _, cst := range constraints {
		names := indexPartColumnNames(cst.Keys)
		switch cst.Tp {
		case ast.ConstraintPrimaryKey:
			pk = append(pk, names...)
		case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			uniques = append(uniques, UniqueKey{Name: cst.Name, Columns: names})
		}
	}
	return pk, uniques
}

func applyConstraint(st *TableStructure, cst *ast.Constraint) {
	names := indexPartColumnNames(cst.Keys)
	switch cst.Tp {
	case ast.ConstraintPrimaryKey:
		st.PrimaryKey = names
	case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
		st.UniqueKeys = append(st.UniqueKeys, UniqueKey{Name: cst.Name, Columns: names})
		// ConstraintKey/ConstraintIndex（普通非唯一索引）、ConstraintForeignKey、
		// ConstraintFulltext、ConstraintCheck、ConstraintVector 等与本包跟踪的
		// "唯一键集合"无关，不处理。
	}
}

func indexPartColumnNames(keys []*ast.IndexPartSpecification) []string {
	var out []string
	for _, k := range keys {
		if k.Column == nil {
			continue // 表达式索引部分（函数索引），没有列名可用，跳过（尽力而为）
		}
		out = append(out, k.Column.Name.O)
	}
	return out
}

func (t *Tracker) applyCreateIndex(defaultSchema string, ci *ast.CreateIndexStmt, gtid string, atMs int64) ([]string, string) {
	schemaName, tableName := resolveTableName(defaultSchema, ci.Table)
	if ci.KeyType != ast.IndexKeyTypeUnique {
		return []string{tableName}, "" // 非唯一索引，不影响本包跟踪的唯一键集合
	}
	st := t.tables[t.tableKey(schemaName, tableName)]
	st.Schema, st.Table = schemaName, tableName
	st.UniqueKeys = append(st.UniqueKeys, UniqueKey{Name: ci.IndexName, Columns: indexPartColumnNames(ci.IndexPartSpecifications)})
	st.UpdatedGTID, st.UpdatedAtMs = gtid, atMs
	t.tables[t.tableKey(schemaName, tableName)] = st
	return []string{tableName}, ""
}

func (t *Tracker) applyDropIndex(defaultSchema string, di *ast.DropIndexStmt, gtid string, atMs int64) ([]string, string) {
	schemaName, tableName := resolveTableName(defaultSchema, di.Table)
	st, ok := t.tables[t.tableKey(schemaName, tableName)]
	if !ok {
		return []string{tableName}, ""
	}
	st.removeUniqueKeyByName(di.IndexName)
	st.UpdatedGTID, st.UpdatedAtMs = gtid, atMs
	t.tables[t.tableKey(schemaName, tableName)] = st
	return []string{tableName}, ""
}
