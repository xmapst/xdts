package schemamig

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// ObjectKind 是结构迁移覆盖的六类对象（需求 MGR-SCH-01），
// 与 internal/manager/model.ObjectType 的取值语义一一对应（两个包不互相
// 依赖，故各自定义类型；schema/foreign_key 不在本包范围——库级操作由调用方
// 直接下发 CREATE DATABASE，外键随全量完成后单独处理，不走本管线）。
type ObjectKind int

const (
	ObjectUnknown ObjectKind = iota
	ObjectTable
	ObjectView
	ObjectProcedure
	ObjectFunction
	ObjectTrigger
	ObjectEvent
)

// String 供日志与阻断原因文案使用。
func (k ObjectKind) String() string {
	switch k {
	case ObjectTable:
		return "table"
	case ObjectView:
		return "view"
	case ObjectProcedure:
		return "procedure"
	case ObjectFunction:
		return "function"
	case ObjectTrigger:
		return "trigger"
	case ObjectEvent:
		return "event"
	default:
		return "unknown"
	}
}

// SchemaTable 是一个用 (库名, 表名) 定位的键，用于 Mapping 里按表限定的规则
// （列过滤）。不是用作 Go 原生 map 索引（那样只能精确比较）——tableTarget/
// dropColumnSet 对 Tables/DropColumns 做的是线性扫描 + foldEqual，比较策略
// 由调用方通过 Options.SourceCaseSensitive 传入的源端 lower_case_table_names
// 决定：true（源端 `=0`，敏感）精确比较，false（源端 1/2，或未探测）按
// strings.EqualFold，见 internal/schemamig/ident.go。
type SchemaTable struct {
	Schema string
	Table  string
}

// Mapping 是一次改写要应用的库 / 表 / 列规则（需求 MGR-SCH-02）。
type Mapping struct {
	// TableAliases resolves physical tool objects before column lookup and
	// supplies their mapped physical names. The resolver must be immutable.
	TableAliases ITableAliasMapping
	// ColumnFilter is an immutable compiled source policy. It is evaluated
	// before renaming, on columns bound by the parsed table definition.
	ColumnFilter IColumnFilter
	Columns      map[SchemaTable]map[string]string
	// Schemas 是库名重映射：源库名 -> 目标库名，按调用方传入的
	// Options.SourceCaseSensitive 决定 key 匹配是否区分大小写（见
	// SchemaTable 类型注释）。未出现在此表中的库名保持原样（未做映射不是
	// 阻断条件）。
	Schemas map[string]string
	// Tables 是表名重映射（同库内改名），key 是改写前的 (库, 表)——库名部分
	// 填源库名（未经过 Schemas 映射前的原始值），值是新表名；库名映射与
	// 表名映射各自独立生效、按需组合。只对 ObjectTable/ObjectView 生效。
	Tables map[SchemaTable]string
	// DropColumns 是要从表定义中删除的列，按 (源库, 源表) 限定。只对
	// ObjectTable 生效。
	DropColumns map[SchemaTable][]string
}

type IColumnFilter interface {
	Allow(schema, table, column string) bool
	FiltersTable(schema, table string) bool
}

type ITableAliasMapping interface {
	ResolveTable(schema, table string) (original, mapped string, found bool)
}

func (m Mapping) columnTarget(schema, table, column string, sensitive bool) string {
	if m.TableAliases != nil {
		if original, _, found := m.TableAliases.ResolveTable(schema, table); found {
			table = original
		}
	}
	for ref, cols := range m.Columns {
		if foldEqual(ref.Schema, schema, sensitive) && foldEqual(ref.Table, table, sensitive) {
			return lookupFold(cols, column, column, false)
		}
	}
	return column
}

// TargetNames resolves all names from the same source identity.
func (m Mapping) TargetNames(schema, table string, columns []string, sensitive bool) (string, string, []string) {
	out := make([]string, len(columns))
	for i, col := range columns {
		out[i] = m.columnTarget(schema, table, col, sensitive)
	}
	return m.schemaTarget(schema, sensitive), m.tableTarget(schema, table, sensitive), out
}

// schemaTarget 返回 schema 的映射目标；未命中时原样返回。caseSensitive 见
// SchemaTable 类型注释（调用方传 Options.SourceCaseSensitive）。
func (m Mapping) schemaTarget(schema string, caseSensitive bool) string {
	return lookupFold(m.Schemas, schema, schema, caseSensitive)
}

// tableTarget 返回 (schema, table) 的表名映射目标（使用改写前的库名做 key）；
// 未命中时原样返回原表名。caseSensitive 见 SchemaTable 类型注释。
func (m Mapping) tableTarget(schema, table string, caseSensitive bool) string {
	if m.TableAliases != nil {
		if _, mapped, found := m.TableAliases.ResolveTable(schema, table); found {
			return mapped
		}
	}
	for k, v := range m.Tables {
		if foldEqual(k.Schema, schema, caseSensitive) && foldEqual(k.Table, table, caseSensitive) {
			return v
		}
	}
	return table
}

// dropColumnSet 返回 (schema, table) 要过滤的列名集合（小写规范化，供大小写
// 不敏感成员判断——列名比较恒定不敏感，不受 lower_case_table_names 影响，
// 见 foldKey 注释）。caseSensitive 只控制 (schema, table) 这一层的匹配，见
// SchemaTable 类型注释。
func (m Mapping) dropColumnSet(schema, table string, caseSensitive bool) map[string]bool {
	if m.TableAliases != nil {
		if original, _, found := m.TableAliases.ResolveTable(schema, table); found {
			table = original
		}
	}
	for k, v := range m.DropColumns {
		if foldEqual(k.Schema, schema, caseSensitive) && foldEqual(k.Table, table, caseSensitive) {
			set := make(map[string]bool, len(v))
			for _, c := range v {
				set[foldKey(c)] = true
			}
			return set
		}
	}
	return nil
}

// Options 是改写行为的可配置项（哪些表选项要清洗、DEFINER 兜底账号等）。
type Options struct {
	// SourceDefaultSchema is the binlog Query event's unqualified-name context.
	SourceDefaultSchema string
	// DefinerFallback 是策略配置的兜底 DEFINER 账号，支持两种形式（见
	// internal/schemamig/definer.go 的 resolveDefinerFallback，两条解析路线共用同一份判定）：
	//   - "user@host"，两侧允许已经带反引号也允许裸标识符，例如
	//     "app_sync@%" 或 "`app_sync`@`%`"——引号是可选的，本包会统一
	//     解析出用户名/主机名再按需要重新加引号，调用方不需要自己转义。
	//   - "CURRENT_USER"（不区分大小写，也接受 "CURRENT_USER()"）。
	// 只有 DefinerAccountExists 为 false 时才会启用；为空字符串且需要
	// 兜底时视为配置缺失，返回阻断而不是产出一个没有 DEFINER 的非法 DDL。
	DefinerFallback string
	// GuardVariable 是触发器守卫使用的会话变量名（需求 MGR-SCH-10 / O-07），
	// 零值时使用 DefaultGuardVariable（"@xdts_apply"）。
	GuardVariable string
	// StripTableOptions 是要从表定义中清洗掉的表选项类型（需求 MGR-SCH-03：
	// AUTO_INCREMENT=N / DATA DIRECTORY / TABLESPACE / ENCRYPTION 等环境
	// 相关选项）。零值（nil）时使用 DefaultStripTableOptions。
	StripTableOptions []ast.TableOptionType
	// DefinerAccountExists 表示目标端已存在与源端 DEFINER 同名的账号——由
	// 调用方通过探测任务（`SHOW CREATE ...` 之外的另一次探测）预先查好，
	// 本包不做数据库 IO。为 true 时保留源端 DEFINER 不变（需求
	// MGR-SCH-09：「目标存在同名账号则保留，否则改写为策略配置的账号」）。
	DefinerAccountExists bool
	// TargetVectorType 表示目标实例是否支持 VECTOR 列类型（对应
	// mysqlx.Caps.VectorType，本包不直接依赖 mysqlx 以保持解耦，由调用方
	// 传布尔值）。源端定义含 VECTOR 列而目标不支持时阻断（需求
	// MGR-SCH-03， 版本矩阵表 VECTOR 一行）。
	TargetVectorType bool
	// TargetInvisibleColumn 表示目标实例是否支持列级 INVISIBLE 属性
	// （MySQL 8.0.23+，含 GIPK 8.0.30+ 自动生成的 my_row_id 列——GIPK 只是
	// 不可见列的一个特例，见 internal/schemamig/invisible_column.go）。源端定义含不可见列而
	// 目标不支持时阻断；目标支持时，不可见标记会被完整保留（strip 解析前
	// 剥离、restore 后按列名回填，见 internal/schemamig/invisible_column.go）。与
	// TargetVectorType 同一种模式：本包不做数据库 IO，由调用方探测目标
	// 版本后传入。
	TargetInvisibleColumn bool
	// SourceCaseSensitive 表示源端 lower_case_table_names 是否为 `=0`
	// （大小写敏感）。对应 mysqlx.LowerCaseTableNames 探测结果里的
	// Sensitive 取值；本包不直接依赖 mysqlx 以保持解耦（与
	// TargetVectorType/TargetInvisibleColumn 同一种模式），由调用方转换成
	// 布尔值传入。控制 Mapping.Schemas/Tables/DropColumns 里 (库名, 表名)
	// 部分的匹配策略（见 SchemaTable 类型注释、internal/schemamig/ident.go）——true 时按精确
	// 比较，false 时按 strings.EqualFold。
	//
	// 只影响库名/表名匹配，不影响列名——MySQL 列名比较不受
	// lower_case_table_names 约束，dropColumnSet 对列名的折叠固定不敏感，
	// 不受这个字段控制。
	SourceCaseSensitive bool
}

// DefaultStripTableOptions 是 环境相关表选项默认清单。
func DefaultStripTableOptions() []ast.TableOptionType {
	return []ast.TableOptionType{
		ast.TableOptionAutoIncrement,
		ast.TableOptionDataDirectory,
		ast.TableOptionIndexDirectory,
		ast.TableOptionTablespace,
		ast.TableOptionEncryption,
	}
}

// DefaultGuardVariable 是触发器守卫默认使用的会话变量名（O-07 业务方已
// 确认的方案）。
const DefaultGuardVariable = "@xdts_apply"

func (o Options) stripSet() map[ast.TableOptionType]bool {
	list := o.StripTableOptions
	if list == nil {
		list = DefaultStripTableOptions()
	}
	set := make(map[ast.TableOptionType]bool, len(list))
	for _, tp := range list {
		set[tp] = true
	}
	return set
}

func (o Options) guardVariable() string {
	if o.GuardVariable == "" {
		return DefaultGuardVariable
	}
	return o.GuardVariable
}

// Request 是一次改写调用的完整输入。
type Request struct {
	// DefaultSchema binds an unqualified object in a binlog Query event.
	DefaultSchema string
	// Kind 由调用方指定（调用方发起探测任务时已经知道自己请求的是
	// `SHOW CREATE TABLE` 还是 `SHOW CREATE PROCEDURE` 等，不依赖本包做
	// 文本嗅探猜测类型——嗅探容易被关键字出现在注释/字符串里的边界情况
	// 误导，而调用方本来就确定地知道）。
	Kind ObjectKind
	// DDL 是源端 `SHOW CREATE ...` 返回的原始定义文本。
	DDL string
	// Mapping 与 Options 见各自类型定义。
	Mapping Mapping
	Options Options
}

// Result 是一次改写调用的结果。恰好二选一：Blocked=true 时 DDL 为空、
// Reason 与 OriginalDDL 有效；Blocked=false 时 DDL 有效、Reason/OriginalDDL
// 为空。
type Result struct {
	// Reason 是阻断原因，措辞可直接展示给用户（不含内部堆栈/包名等实现
	// 细节）。仅 Blocked=true 时有效。
	Reason string

	OriginalDDL string
	// DDL 是改写后的单条语句文本（不含 DELIMITER 包装）。仅 Blocked=false
	// 时有效。
	DDL string
	// Blocked 为 true 表示"无法安全改写"，这是本包的第一原则性结果
	// （internal/schemamig/doc.go）。
	Blocked bool
	// NeedsDelimiter 为 true 表示该 DDL 内部含有以分号分隔的复合语句体
	// （存储过程 / 函数 / 触发器 / 事件），交付为 .sql 文本供 DBA 执行时
	// 必须先过 WrapDelimiter（需求 MGR-SCH-10/11， 第二条）；
	// 表 / 视图恒为 false。程序化单条 Exec 不受此项影响——DDL 字段本身
	// 就是一条完整、可直接 Exec 的语句，NeedsDelimiter 只影响"存成 .sql
	// 文件用 mysql 客户端执行"这条交付路径。
	NeedsDelimiter bool
}

func blocked(reason, originalDDL string) Result {
	return Result{Blocked: true, Reason: reason, OriginalDDL: originalDDL}
}
