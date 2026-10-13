package pipelinespec

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/xmapst/xdts/internal/iproto"
)

// protoMarshalOptions 使用 snake_case 字段名和枚举名称序列化规则。
// 不设置 EmitUnpopulated，保留可选字段缺席与显式零值的区别。
var protoMarshalOptions = protojson.MarshalOptions{UseProtoNames: true}

// StrategyRulesDoc 按执行位置组织 source/target 规则。
// 协议叶子消息使用 protojson 的 snake_case 字段名和枚举名称，外层容器使用 encoding/json。
type StrategyRulesDoc struct {
	Source *StrategySourceRules `json:"source,omitempty"`
	Target *StrategyTargetRules `json:"target,omitempty"`
}

// StrategySourceRules 组织源端过滤、来源策略、全量参数和发布窗口。
// 列过滤、行过滤、影子表规则在源端读取路径编译和执行。
type StrategySourceRules struct {
	ObjectFilters        []*iproto.ObjectFilterRule      `json:"object_filters,omitempty"`
	OperationFilter      *iproto.OperationFilter         `json:"operation_filter,omitempty"`
	ColumnFilters        []*iproto.ColumnFilterRule      `json:"column_filters,omitempty"`
	RowFilters           []*iproto.RowFilterRule         `json:"row_filters,omitempty"`
	ShadowTables         []*iproto.ShadowTableRule       `json:"shadow_tables,omitempty"`
	IncrementalRateLimit *iproto.RateLimit               `json:"incremental_rate_limit,omitempty"`
	OriginPolicy         *iproto.OriginPolicy            `json:"origin_policy,omitempty"`
	Snapshot             *iproto.SnapshotParams          `json:"snapshot,omitempty"`
	IncrementalWindow    *iproto.IncrementalWindowPolicy `json:"incremental_window,omitempty"`
}

// StrategyTargetRules 打包目标端执行的规则与策略：TargetRuleSet 的 2 个
// 字段 + 四个目标端策略（write/conflict/ddl/error，均为 TargetSideConfig
// 的兄弟字段，存储层同样按"目标端配置"归到一组）。
//
// Transforms 按 execution_side 投影到源端或目标端，缺席默认目标端。
// Mappings 在保存及组装时编译校验，独立投影到目标端。
type StrategyTargetRules struct {
	Mappings       *iproto.MappingRules           `json:"mappings,omitempty"`
	Transforms     []*iproto.CleanseTransformRule `json:"transforms,omitempty"`
	WritePolicy    *iproto.WritePolicy            `json:"write_policy,omitempty"`
	ConflictPolicy *iproto.ConflictPolicy         `json:"conflict_policy,omitempty"`
	DDLPolicy      *iproto.DDLPolicy              `json:"ddl_policy,omitempty"`
	ErrorPolicy    *iproto.ErrorPolicy            `json:"error_policy,omitempty"`

	// 它不在 SnapshotParams 里：proto/config.proto 的 message SnapshotParams
	// 文档注释写明 write_concurrency
	// 是目标端独占、不随源端那份下发给源节点，所以在协议里是 write_policy 的
	// 兄弟。前端把它放在 snapshotParams.writeConcurrency 下，出站映射需要把它
	// 提出来放到 target 这一层，不是改个名。
	//
	// 类型是标量 uint32 而不是消息指针，所以这一个字段走不了
	// unmarshalProtoPtr，也区分不了"没配"与"配了 0"——这跟 rate_limit
	// 那种消息型字段正相反（见 marshalTargetRules 附近的说明）。对调优参数
	// 而言这是对的：0 就是"按默认来"，proto3 非 optional 标量本来就这个语义。
	SnapshotWriteConcurrency uint32 `json:"snapshot_write_concurrency,omitempty"`
}

// rawRulesDoc/rawSourceRules/rawTargetRules 是 ParseStrategyRules 的中间
// 表示：先用 encoding/json 把外层"这个 key 底下有没有值、值的原始字节是
// 什么"这层容器结构剥开，再对每个叶子字段单独调用 protojson.Unmarshal。
//
// 这一步用 encoding/json（不是 protojson）是安全的，且必须这样做：
// StrategyRulesDoc/StrategySourceRules/StrategyTargetRules 本身是本包
// 手写的容器类型，不是 proto.Message，protojson.Unmarshal 只能作用在真正
// 的 proto 消息上，无法直接喂给容器整体；容器这一层只涉及"某个 key 是否
// 存在、原始字节切到哪里为止"，不经过任何枚举字段，与方言无关，两种
// json 实现在这一层行为一致。真正需要按 protojson 方言解析的是每个叶子
// 值（如 origin_policy 那个 JSON 对象本身），在 parseSourceRules/
// parseTargetRules 里逐个调用 unmarshalProtoPtr/unmarshalProtoList 处理。
type rawRulesDoc struct {
	Source json.RawMessage `json:"source,omitempty"`
	Target json.RawMessage `json:"target,omitempty"`
}

type rawSourceRules struct {
	ObjectFilters        []json.RawMessage `json:"object_filters,omitempty"`
	OperationFilter      json.RawMessage   `json:"operation_filter,omitempty"`
	ColumnFilters        []json.RawMessage `json:"column_filters,omitempty"`
	RowFilters           []json.RawMessage `json:"row_filters,omitempty"`
	ShadowTables         []json.RawMessage `json:"shadow_tables,omitempty"`
	IncrementalRateLimit json.RawMessage   `json:"incremental_rate_limit,omitempty"`
	OriginPolicy         json.RawMessage   `json:"origin_policy,omitempty"`
	Snapshot             json.RawMessage   `json:"snapshot,omitempty"`
	IncrementalWindow    json.RawMessage   `json:"incremental_window,omitempty"`
}

type rawTargetRules struct {
	Mappings       json.RawMessage   `json:"mappings,omitempty"`
	Transforms     []json.RawMessage `json:"transforms,omitempty"`
	WritePolicy    json.RawMessage   `json:"write_policy,omitempty"`
	ConflictPolicy json.RawMessage   `json:"conflict_policy,omitempty"`
	DDLPolicy      json.RawMessage   `json:"ddl_policy,omitempty"`
	ErrorPolicy    json.RawMessage   `json:"error_policy,omitempty"`

	// 标量，不是 json.RawMessage：它不是 proto 消息，不需要经 protojson 叶子
	// 解析，见 StrategyTargetRules 同名字段的说明。
	SnapshotWriteConcurrency uint32 `json:"snapshot_write_concurrency,omitempty"`
}

// unmarshalStrictContainer rejects unknown container fields and trailing JSON.
// Source and target rule documents are decoded separately against their contracts.
func unmarshalStrictContainer(raw []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// unmarshalProtoPtr 把一段 protojson 方言的 JSON 解析成 *T（T 是某个
// iproto 生成的消息类型）。raw 为空（字段没出现）返回 (nil, nil)，对应
// "这个可选字段没有配置"，不是错误。
//
// 严格模式（不设 DiscardUnknown）：遇到叶子消息里出现不认识的字段直接
// 报错，不静默丢弃——跟"P1/P2 类别在保存前校验里拒绝"是同一个原则的
// 两处体现，都是"配了但不处理"绝不能悄悄发生。
func unmarshalProtoPtr[T any, PT interface {
	*T
	proto.Message
}](raw json.RawMessage) (PT, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var v T
	pv := PT(&v)
	if err := protojson.Unmarshal(raw, pv); err != nil {
		return nil, err
	}
	return pv, nil
}

// unmarshalProtoList 对一组 protojson 方言的 JSON 元素逐个调用
// unmarshalProtoPtr，用于 repeated 字段（如 object_filters）——这类字段
// 直接是容器的顶层字段（不是嵌套在某一个 proto 消息内部），protojson
// 没有机会替我们处理数组切分，必须先用 encoding/json 拆成
// []json.RawMessage（安全，见 rawSourceRules 的文档注释），再逐个转换。
func unmarshalProtoList[T any, PT interface {
	*T
	proto.Message
}](raws []json.RawMessage) ([]PT, error) {
	if len(raws) == 0 {
		return nil, nil
	}
	out := make([]PT, 0, len(raws))
	for i, raw := range raws {
		v, err := unmarshalProtoPtr[T, PT](raw)
		if err != nil {
			return nil, fmt.Errorf("第 %d 项: %w", i, err)
		}
		out = append(out, v)
	}
	return out, nil
}

// marshalProtoPtr 把 *T（T 是某个 iproto 生成的消息类型）序列化成
// protojson{UseProtoNames: true} 方言的原始字节。msg 为 nil（可选字段
// 没有配置）返回 (nil, nil)，配合 rawXxx 系列 json.RawMessage 字段的
// omitempty，不会在输出里凭空生造一个空对象。
func marshalProtoPtr[T any, PT interface {
	*T
	proto.Message
}](msg PT) (json.RawMessage, error) {
	if msg == nil {
		return nil, nil
	}
	return protoMarshalOptions.Marshal(msg)
}

// marshalProtoList 是 marshalProtoPtr 的 repeated 字段版本，跟
// unmarshalProtoList 对称：repeated 字段是容器的顶层字段，不是嵌套在
// 某一个 proto 消息内部，protojson 没有机会替我们处理数组，逐个转换后
// 用 encoding/json 拼回数组（这一步同样只涉及数组语法，不经过任何枚举
// 字段，安全）。
func marshalProtoList[T any, PT interface {
	*T
	proto.Message
}](msgs []PT) ([]json.RawMessage, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	out := make([]json.RawMessage, 0, len(msgs))
	for i, m := range msgs {
		raw, err := marshalProtoPtr[T, PT](m)
		if err != nil {
			return nil, fmt.Errorf("第 %d 项: %w", i, err)
		}
		out = append(out, raw)
	}
	return out, nil
}

// doc 为 nil 时按空文档处理，产出 "{}"。
func MarshalStrategyRules(doc *StrategyRulesDoc) ([]byte, error) {
	if doc == nil {
		doc = &StrategyRulesDoc{}
	}
	var env rawRulesDoc
	if doc.Source != nil {
		raw, err := marshalSourceRules(doc.Source)
		if err != nil {
			return nil, fmt.Errorf("pipelinespec: 序列化 source 规则失败: %w", err)
		}
		env.Source = raw
	}
	if doc.Target != nil {
		raw, err := marshalTargetRules(doc.Target)
		if err != nil {
			return nil, fmt.Errorf("pipelinespec: 序列化 target 规则失败: %w", err)
		}
		env.Target = raw
	}

	out, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("pipelinespec: 序列化外层结构失败: %w", err)
	}
	// protojson 可能输出非紧凑 JSON；最终对整体调用 json.Compact。
	var buf bytes.Buffer
	if err := json.Compact(&buf, out); err != nil {
		return nil, fmt.Errorf("pipelinespec: 压缩落库格式失败: %w", err)
	}
	return buf.Bytes(), nil
}

func marshalSourceRules(s *StrategySourceRules) (json.RawMessage, error) {
	var rs rawSourceRules
	var err error
	if rs.ObjectFilters, err = marshalProtoList[iproto.ObjectFilterRule](s.ObjectFilters); err != nil {
		return nil, fmt.Errorf("object_filters: %w", err)
	}
	if rs.OperationFilter, err = marshalProtoPtr[iproto.OperationFilter](s.OperationFilter); err != nil {
		return nil, fmt.Errorf("operation_filter: %w", err)
	}
	if rs.ColumnFilters, err = marshalProtoList[iproto.ColumnFilterRule](s.ColumnFilters); err != nil {
		return nil, fmt.Errorf("column_filters: %w", err)
	}
	if rs.RowFilters, err = marshalProtoList[iproto.RowFilterRule](s.RowFilters); err != nil {
		return nil, fmt.Errorf("row_filters: %w", err)
	}
	if rs.ShadowTables, err = marshalProtoList[iproto.ShadowTableRule](s.ShadowTables); err != nil {
		return nil, fmt.Errorf("shadow_tables: %w", err)
	}
	if rs.IncrementalRateLimit, err = marshalProtoPtr[iproto.RateLimit](s.IncrementalRateLimit); err != nil {
		return nil, fmt.Errorf("incremental_rate_limit: %w", err)
	}
	if rs.IncrementalWindow, err = marshalProtoPtr[iproto.IncrementalWindowPolicy](s.IncrementalWindow); err != nil {
		return nil, fmt.Errorf("incremental_window: %w", err)
	}
	if rs.OriginPolicy, err = marshalProtoPtr[iproto.OriginPolicy](s.OriginPolicy); err != nil {
		return nil, fmt.Errorf("origin_policy: %w", err)
	}
	if rs.Snapshot, err = marshalProtoPtr[iproto.SnapshotParams](s.Snapshot); err != nil {
		return nil, fmt.Errorf("snapshot: %w", err)
	}
	return json.Marshal(rs)
}

func marshalTargetRules(t *StrategyTargetRules) (json.RawMessage, error) {
	var rt rawTargetRules
	var err error
	if rt.Mappings, err = marshalProtoPtr[iproto.MappingRules](t.Mappings); err != nil {
		return nil, fmt.Errorf("mappings: %w", err)
	}
	if rt.Transforms, err = marshalProtoList[iproto.CleanseTransformRule](t.Transforms); err != nil {
		return nil, fmt.Errorf("transforms: %w", err)
	}
	if rt.WritePolicy, err = marshalProtoPtr[iproto.WritePolicy](t.WritePolicy); err != nil {
		return nil, fmt.Errorf("write_policy: %w", err)
	}
	if rt.ConflictPolicy, err = marshalProtoPtr[iproto.ConflictPolicy](t.ConflictPolicy); err != nil {
		return nil, fmt.Errorf("conflict_policy: %w", err)
	}
	if rt.DDLPolicy, err = marshalProtoPtr[iproto.DDLPolicy](t.DDLPolicy); err != nil {
		return nil, fmt.Errorf("ddl_policy: %w", err)
	}
	if rt.ErrorPolicy, err = marshalProtoPtr[iproto.ErrorPolicy](t.ErrorPolicy); err != nil {
		return nil, fmt.Errorf("error_policy: %w", err)
	}
	rt.SnapshotWriteConcurrency = t.SnapshotWriteConcurrency
	return json.Marshal(rt)
}

// 方言：（MGR-STR-06）——落库/前端
// 提交的 JSON 是 protojson{UseProtoNames: true}，不是 encoding/json；
// digest 计算（internal/manager/pipelinespec/digest.go）用另一种方言（encoding/json 打在生成码上），
// 两者刻意不同，理由见 internal/manager/pipelinespec/digest.go 与，不是本函数需要关心的事。
func ParseStrategyRules(raw []byte) (*StrategyRulesDoc, error) {
	if len(raw) == 0 {
		return &StrategyRulesDoc{}, nil
	}
	var env rawRulesDoc
	if err := unmarshalStrictContainer(raw, &env); err != nil {
		return nil, fmt.Errorf("pipelinespec: 解析 StrategyVersion.Rules 外层结构失败: %w", err)
	}
	doc := &StrategyRulesDoc{}
	if len(env.Source) > 0 {
		sr, err := parseSourceRules(env.Source)
		if err != nil {
			return nil, fmt.Errorf("pipelinespec: 解析 source 规则失败: %w", err)
		}
		doc.Source = sr
	}
	if len(env.Target) > 0 {
		tr, err := parseTargetRules(env.Target)
		if err != nil {
			return nil, fmt.Errorf("pipelinespec: 解析 target 规则失败: %w", err)
		}
		doc.Target = tr
	}
	return doc, nil
}

func parseSourceRules(raw json.RawMessage) (*StrategySourceRules, error) {
	var rs rawSourceRules
	if err := unmarshalStrictContainer(raw, &rs); err != nil {
		return nil, fmt.Errorf("外层结构: %w", err)
	}
	out := &StrategySourceRules{}
	var err error
	if out.ObjectFilters, err = unmarshalProtoList[iproto.ObjectFilterRule](rs.ObjectFilters); err != nil {
		return nil, fmt.Errorf("object_filters: %w", err)
	}
	if out.OperationFilter, err = unmarshalProtoPtr[iproto.OperationFilter](rs.OperationFilter); err != nil {
		return nil, fmt.Errorf("operation_filter: %w", err)
	}
	if out.ColumnFilters, err = unmarshalProtoList[iproto.ColumnFilterRule](rs.ColumnFilters); err != nil {
		return nil, fmt.Errorf("column_filters: %w", err)
	}
	if out.RowFilters, err = unmarshalProtoList[iproto.RowFilterRule](rs.RowFilters); err != nil {
		return nil, fmt.Errorf("row_filters: %w", err)
	}
	if out.ShadowTables, err = unmarshalProtoList[iproto.ShadowTableRule](rs.ShadowTables); err != nil {
		return nil, fmt.Errorf("shadow_tables: %w", err)
	}
	if out.IncrementalRateLimit, err = unmarshalProtoPtr[iproto.RateLimit](rs.IncrementalRateLimit); err != nil {
		return nil, fmt.Errorf("incremental_rate_limit: %w", err)
	}
	if out.IncrementalWindow, err = unmarshalProtoPtr[iproto.IncrementalWindowPolicy](rs.IncrementalWindow); err != nil {
		return nil, fmt.Errorf("incremental_window: %w", err)
	}
	if out.OriginPolicy, err = unmarshalProtoPtr[iproto.OriginPolicy](rs.OriginPolicy); err != nil {
		return nil, fmt.Errorf("origin_policy: %w", err)
	}
	if out.Snapshot, err = unmarshalProtoPtr[iproto.SnapshotParams](rs.Snapshot); err != nil {
		return nil, fmt.Errorf("snapshot: %w", err)
	}
	return out, nil
}

func parseTargetRules(raw json.RawMessage) (*StrategyTargetRules, error) {
	var rt rawTargetRules
	if err := unmarshalStrictContainer(raw, &rt); err != nil {
		return nil, fmt.Errorf("外层结构: %w", err)
	}
	out := &StrategyTargetRules{}
	var err error
	if out.Mappings, err = unmarshalProtoPtr[iproto.MappingRules](rt.Mappings); err != nil {
		return nil, fmt.Errorf("mappings: %w", err)
	}
	if out.Transforms, err = unmarshalProtoList[iproto.CleanseTransformRule](rt.Transforms); err != nil {
		return nil, fmt.Errorf("transforms: %w", err)
	}
	if out.WritePolicy, err = unmarshalProtoPtr[iproto.WritePolicy](rt.WritePolicy); err != nil {
		return nil, fmt.Errorf("write_policy: %w", err)
	}
	if out.ConflictPolicy, err = unmarshalProtoPtr[iproto.ConflictPolicy](rt.ConflictPolicy); err != nil {
		return nil, fmt.Errorf("conflict_policy: %w", err)
	}
	if out.DDLPolicy, err = unmarshalProtoPtr[iproto.DDLPolicy](rt.DDLPolicy); err != nil {
		return nil, fmt.Errorf("ddl_policy: %w", err)
	}
	if out.ErrorPolicy, err = unmarshalProtoPtr[iproto.ErrorPolicy](rt.ErrorPolicy); err != nil {
		return nil, fmt.Errorf("error_policy: %w", err)
	}
	out.SnapshotWriteConcurrency = rt.SnapshotWriteConcurrency
	return out, nil
}

// ValidateObjectFilterRules validates matching modes, patterns and rule actions
// before a persisted strategy can be assembled for either role.
func ValidateObjectFilterRules(doc *StrategyRulesDoc) error {
	if doc == nil || doc.Source == nil {
		return nil
	}
	var errs []error
	for i, r := range doc.Source.ObjectFilters {
		if r == nil {
			// unmarshalProtoList 不会产出 nil 元素，这里防御性跳过而不是
			// panic，不给一个理论上不会发生的输入形状制造额外的失败面。
			continue
		}
		if r.GetAction() == iproto.FilterAction_FILTER_ACTION_UNSPECIFIED {
			errs = append(errs, fmt.Errorf("source.object_filters[%d]: action 未指定", i))
		}
		if r.GetSchemaMatch() == iproto.MatchType_MATCH_TYPE_UNSPECIFIED {
			errs = append(errs, fmt.Errorf("source.object_filters[%d]: schema_match 未指定", i))
		}
		if r.GetSchemaPattern() == "" {
			errs = append(errs, fmt.Errorf("source.object_filters[%d]: schema_pattern 不能为空", i))
		}
		// table_match/table_pattern 是一对：都留空表示"只按库匹配"
		// （合法），但只填一个说明用户填了模式串却没选匹配方式（或者反过来
		// 选了匹配方式却没填模式串），两种情况节点侧 compilePattern 都会
		// 报错，不是一个能自愈的状态。
		tableMatchSet := r.GetTableMatch() != iproto.MatchType_MATCH_TYPE_UNSPECIFIED
		tablePatternSet := r.GetTablePattern() != ""
		if tableMatchSet || tablePatternSet {
			if !tableMatchSet {
				errs = append(errs, fmt.Errorf("source.object_filters[%d]: table_pattern 已填但 table_match 未指定", i))
			}
			if !tablePatternSet {
				errs = append(errs, fmt.Errorf("source.object_filters[%d]: table_match 已填但 table_pattern 为空", i))
			}
		}
	}
	return errors.Join(errs...)
}

func ValidateColumnFilterRules(doc *StrategyRulesDoc) error {
	if doc == nil || doc.Source == nil {
		return nil
	}
	_, err := iproto.CompileColumnFilters(doc.Source.ColumnFilters, false)
	return err
}

func ValidateRowFilterRules(doc *StrategyRulesDoc) error {
	if doc == nil || doc.Source == nil {
		return nil
	}
	_, err := iproto.CompileRowFilters(doc.Source.RowFilters, false)
	return err
}
