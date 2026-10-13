package rule

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Compile 把一份 RuleSet 编译成可复用的 Plan。是纯计算：不读写
// 任何外部状态，两次用相同输入调用会产出行为相同（但不是同一个指针）的 Plan。
//
// 正则/glob 在这里一次性编译进 compiledObjectRule，Plan 的匹配方法只执行
// regexp.MatchString，不重新编译（NODE-TF-01）。
//
// 失败必须返回非 nil error、不产出半成品 Plan（MGR-STR-02"保存前校验"依赖这一点：
// Manager 用本函数的返回值判断一份策略能不能保存，errors.Join 收集到的全部问题都要
// 能展示给用户，不是拿到第一个就短路丢弃其余信息）。
func Compile(rules RuleSet, side Side, caseSensitive bool) (*Plan, error) {
	if !side.Valid() {
		return nil, fmt.Errorf("rule: 无效的 side %q", side)
	}

	var errs []error

	if side == SideTarget {
		if len(rules.RowFilters) > 0 {
			errs = append(errs, errors.New("rule: target cannot compile source row filters"))
		}
		if len(rules.ColumnFilters) > 0 {
			errs = append(errs, errors.New("rule: 目标端编译不接受 column_filters（源端专属字段）"))
		}
		if len(rules.ObjectFilters) > 0 {
			errs = append(errs, errors.New("rule: 目标端编译不接受 object_filters（源端专属字段）"))
		}
		if rules.Operation != nil {
			errs = append(errs, errors.New("rule: 目标端编译不接受 operation（源端专属字段）"))
		}
		if rules.Origin != nil {
			errs = append(errs, errors.New("rule: 目标端编译不接受 origin（源端专属字段）"))
		}
	}

	compiledObjects, objErrs := compileObjectFilters(rules.ObjectFilters, caseSensitive)
	errs = append(errs, objErrs...)
	compiledColumns, colErrs := compileColumnFilters(rules.ColumnFilters, caseSensitive)
	errs = append(errs, colErrs...)

	compiledRows, rowErr := compileRowFilters(rules.RowFilters, caseSensitive)
	errs = append(errs, rowErr)
	if err := validateOriginPolicy(rules.Origin); err != nil {
		errs = append(errs, err)
	}

	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	var operation *OperationFilter
	if rules.Operation != nil {
		value := *rules.Operation
		operation = &value
	}
	var origin *OriginPolicy
	if rules.Origin != nil {
		value := *rules.Origin
		value.Centers = append([]string(nil), rules.Origin.Centers...)
		origin = &value
	}
	return &Plan{
		rows:      compiledRows,
		side:      side,
		objects:   compiledObjects,
		columns:   compiledColumns,
		operation: operation,
		origin:    origin,
	}, nil
}

// compiledObjectRule 是 ObjectFilterRule 编译后的形态：schema/table 两个模式都已经
// 落成 iMatcher（EXACT 是大小写不敏感的字符串比较，GLOB/REGEX 统一落成预编译的
// *regexp.Regexp，MatchTable 的热路径只调用 iMatcher，不做任何解析/编译工作）。
type compiledObjectRule struct {
	action FilterAction
	schema iMatcher
	table  iMatcher // nil 表示"只按库匹配"（原规则 TableMatch=Unspecified 且 TablePattern 为空）
}

// iMatcher 是编译后的模式匹配器，MatchString 必须是纯函数、不分配、不做 IO
// （NODE-TF-01"热路径不做重复的正则编译、不用反射"——iMatcher 本身在 Compile 阶段
// 一次构造完成，Plan 的匹配方法只调用它）。
type iMatcher interface {
	MatchString(s string) bool
}

type exactMatcher struct {
	pattern       string
	caseSensitive bool
}

func (m exactMatcher) MatchString(s string) bool {
	if m.caseSensitive {
		return m.pattern == s
	}
	return strings.EqualFold(m.pattern, s)
}

// compileObjectFilters validates and precompiles matching policy outside the hot path.
func compileObjectFilters(rules []ObjectFilterRule, caseSensitive bool) ([]compiledObjectRule, []error) {
	if len(rules) == 0 {
		return nil, nil
	}
	out := make([]compiledObjectRule, 0, len(rules))
	var errs []error
	for i, r := range rules {
		if r.Action == FilterActionUnspecified {
			errs = append(errs, fmt.Errorf("rule: object_filters[%d]: action 未指定", i))
			continue
		}
		schemaM, err := compilePattern(r.SchemaMatch, r.SchemaPattern, caseSensitive)
		if err != nil {
			errs = append(errs, fmt.Errorf("rule: object_filters[%d]: schema_pattern: %w", i, err))
			continue
		}
		var tableM iMatcher
		if r.TableMatch != MatchUnspecified || r.TablePattern != "" {
			tableM, err = compilePattern(r.TableMatch, r.TablePattern, caseSensitive)
			if err != nil {
				errs = append(errs, fmt.Errorf("rule: object_filters[%d]: table_pattern: %w", i, err))
				continue
			}
		}
		out = append(out, compiledObjectRule{action: r.Action, schema: schemaM, table: tableM})
	}
	if len(errs) > 0 {
		return nil, errs
	}
	return out, nil
}

// compilePattern 把一个 (MatchType, pattern) 对编译成 iMatcher。GLOB 按 shell 通配符
// 语义翻译成等价正则（* 任意长度任意字符，? 恰好一个字符，其余字符转义后原样匹配）
// 再一次性 regexp.Compile；REGEX 直接编译调用方给的正则；EXACT 不需要编译。
// caseSensitive 语义见 Compile 与 exactMatcher 文档：false（默认/未探测）时 GLOB/
// REGEX 加 `(?i)` 前缀，true 时不加，与 EXACT 分支的策略保持一致。
//
// 空 pattern 一律拒绝（MatchUnspecified 之外的取值）：这里的空串不是"库/表级
// 只按库匹配"的哨兵——那个哨兵由调用方（compileObjectFilters）在决定要不要调用
// 本函数之前就判断掉了（TableMatch=Unspecified 且 TablePattern="" 才会跳过整个
// table 分支），一旦真的调用到这里，说明调用方已经认定"这是一条要编译的模式"，
// 此时的空串只可能是配置错误（比如显式选了 EXACT 但忘了填 pattern）。
func compilePattern(mt MatchType, pattern string, caseSensitive bool) (iMatcher, error) {
	if mt == MatchUnspecified {
		return nil, errors.New("match_type 未指定")
	}
	if pattern == "" {
		return nil, errors.New("pattern 不能为空")
	}
	switch mt {
	case MatchExact:
		return exactMatcher{pattern: pattern, caseSensitive: caseSensitive}, nil
	case MatchGlob:
		src := "^" + globToRegex(pattern) + "$"
		if !caseSensitive {
			src = "(?i)" + src
		}
		re, err := regexp.Compile(src)
		if err != nil {
			return nil, fmt.Errorf("glob 模式翻译成正则后编译失败: %w", err)
		}
		return re, nil
	case MatchRegex:
		src := "^(?:" + pattern + ")$"
		if !caseSensitive {
			src = "(?i)" + src
		}
		re, err := regexp.Compile(src)
		if err != nil {
			return nil, fmt.Errorf("正则编译失败: %w", err)
		}
		return re, nil
	default:
		return nil, fmt.Errorf("未知的 match_type %d", mt)
	}
}

// globToRegex 把 * / ? 之外的字符全部转义，* 翻译成 .*，? 翻译成 .——不支持字符类
// （[...]）等更高级的 glob 语法，够用即可：这里只是给 schema/table 名字做通配，不是
// 通用文件名匹配。
func globToRegex(pattern string) string {
	var b strings.Builder
	for _, r := range pattern {
		switch r {
		case '*':
			b.WriteString(".*")
		case '?':
			b.WriteByte('.')
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	return b.String()
}

// validateOriginPolicy 校验 OriginPolicy 本身自洽：INCLUDE_ORIGINS/EXCLUDE_ORIGINS
// 模式下 Centers 不能为空（否则规则没有意义，多半是配置遗漏），其余模式下 Centers
// 应为空（避免"填了但没用到"的误导性配置）。
func validateOriginPolicy(op *OriginPolicy) error {
	if op == nil {
		return nil
	}
	switch op.Mode {
	case OriginModeLocalOnly, OriginModeAll:
		if len(op.Centers) > 0 {
			return fmt.Errorf("rule: origin: mode=%d 不使用 centers，但传入了 %d 个", op.Mode, len(op.Centers))
		}
	case OriginModeIncludeOrigins, OriginModeExcludeOrigins:
		if len(op.Centers) == 0 {
			return fmt.Errorf("rule: origin: mode=%d 要求 centers 非空", op.Mode)
		}
	case OriginModeUnspecified:
		return errors.New("rule: origin: mode 未指定")
	default:
		return fmt.Errorf("rule: origin: 未知的 mode %d", op.Mode)
	}
	return nil
}
