package binlogin

import (
	"context"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/schema"
	"github.com/xmapst/xdts/internal/node/store"
)

type IDDLGuard interface {
	Check(gtid, schema, ddl string) error
	Observe(string)
	Failure() error
}

// ISink 是 binlog_in 向下游投递数据面消息的最小接口。生产装配由
// internal/node/controller/loader.go 将 amqpout.Module 注入 Options.Sink，
// 源侧过滤和转换在投递前执行；测试可注入独立接收器。
//
// Send* waits for broker acceptance or cancellation. With a configured window,
// independent immutable batches can be sent concurrently; only the source
// session advances the continuous published prefix. Implementations must be
// safe for concurrent calls. Watermarks and fragmented transactions remain
// serial barriers after draining the window.
type ISink interface {
	// SendBatch 投递一个 DataBatch（含普通批次与大事务分片）。
	SendBatch(ctx context.Context, batch *iproto.DataBatch) error
	// SendWatermark 投递一个空闲水位消息（NODE-BIN-07）。
	SendWatermark(ctx context.Context, wm *iproto.Watermark) error
}

// ITableFilter selects source tables before row decoding. A nil filter permits all tables.
type ITableFilter interface {
	// MatchTable 返回 false 表示该表不在同步范围内，其对应的 RowsEvent 不解码、不产生
	// RowChange（NODE-BIN-06"尽早丢弃，减少解码与传输开销"）。
	MatchTable(schema, table string) bool
}

type allowAllTableFilter struct{}

func (allowAllTableFilter) MatchTable(_, _ string) bool { return true }

// IColumnFilter 决定某一列是否应写入 RowChange.column_names/values。
type IColumnFilter interface {
	// Allow 返回 false 表示该列应从本次行变更里剔除。
	Allow(schema, table, column string) bool
}

type allowAllColumnFilter struct{}

func (allowAllColumnFilter) Allow(_, _, _ string) bool { return true }

// 与 ITableFilter/IColumnFilter/IOriginResolver 不同，这不是一个"用户可配置的
// 策略"，而是纯基础设施（只有一种正确实现），因此 Module 在 New() 里总是
// 无条件构造一个真实的 schema.Tracker，不提供"为空时退化"的默认值、也不需要
// 调用方注入——Options 里没有这个字段。
type ISchemaTracker interface {
	Lookup(schema, table string) (schema.TableStructure, bool)
	// ApplyDDL 解析一条 DDL 并更新内存结构；warning 非空表示解析失败或该语句
	// 属于本包明确不追踪的种类（见 internal/node/schema 包文档），调用方应当
	// 记录但绝不能因此中断会话。
	ApplyDDL(defaultSchema, statement, gtid string, atMs int64) (affectedTables []string, warning string)
	// IsGenerated 判定某列当前是否已知为生成列。表/列结构未知时保守返回
	// false（宁可多传一列，也不武断丢弃真实数据）。
	IsGenerated(schemaName, table, column string) bool
}

// generatedColumnFilter 把用户 IColumnFilter 与 ISchemaTracker 的生成列判定
// 组合成 aggregator 实际使用的单一 IColumnFilter："两者都放行才放行"——用户
// 策略与生成列剔除是两个独立、正交的过滤维度，互不替代。
type generatedColumnFilter struct {
	user   IColumnFilter
	schema ISchemaTracker
}

func (f generatedColumnFilter) Allow(schemaName, table, column string) bool {
	if f.schema != nil && f.schema.IsGenerated(schemaName, table, column) {
		return false
	}
	return f.user == nil || f.user.Allow(schemaName, table, column)
}

// IOriginResolver 依据 GTID 来源 UUID 解出事务的来源中心与已知血缘路径。
// Controller 提供来自 CommonConfig.origin_map 的不可变实现。
// 未注入时 localOriginResolver 使用 path=[Center]、hops=1，并在启动时告警。
type IOriginResolver interface {
	// Resolve 依据 binlog 事件里的来源 server UUID（GTIDEvent.SID 解出的文本形式）
	// 给出来源中心与血缘路径；ok=false 时调用方按 localOriginResolver 的默认值处理
	// （不阻断事务聚合——NODE-BIN-05 本身要求"被过滤的事务同样推进位点"，来源无法判定
	// 时同理不能卡住整条流水线）。
	Resolve(sourceUUID string) (center string, path []string, hops uint32, ok bool)
}

type IOriginFilter interface {
	AllowOrigin(center string, hops uint32, path []string) bool
}

type localOriginResolver struct{ center string }

func (r localOriginResolver) Resolve(_ string) (string, []string, uint32, bool) {
	if r.center == "" {
		return "", nil, 0, false
	}
	return r.center, []string{r.center}, 1, true
}

// iStoreAccess 是本包持久化/恢复检查点所需的最小 store 访问接口，*store.Client
// 天然实现它（与 reporter.iStoreAccess 同一模式，两包各自独立定义、不共享类型，
// 理由见 logship.iPublisher 的同类注释：避免通过共享内部接口类型制造隐性耦合）。
// 为空时检查点只在内存，不落盘、也无法在重启后恢复起始位点（此时 Options.StartGTIDSet
// 必须显式给出，否则 OnInit 失败，见 resolveStartGTIDSet）。
type iStoreAccess interface {
	Put(op store.Op) error
	Get(ctx context.Context, collection, id string, out any) error
	PutCritical(ctx context.Context, op store.Op) error
}
