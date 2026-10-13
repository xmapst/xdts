// internal/schemamig/invisible_column.go 处理 MySQL 列级 INVISIBLE 属性（8.0.23+）。GIPK
// （Generated Invisible Primary Key，8.0.30+ 自动生成的 my_row_id 列）
// 只是不可见列的一个特例——源端 SHOW CREATE TABLE 里 GIPK 生成的列和
// 用户手工 ALTER TABLE ... ALTER COLUMN c SET INVISIBLE 出来的列，文本
// 形态完全一样，本文件不为 GIPK 单独开路径，两者共用这一套处理。
//
// === 为什么必须做文本手术，不能走 AST（实测结论） ===
// tidb parser（github.com/pingcap/tidb/pkg/parser）的语法文件（parser.y）
// 里，VISIBLE/INVISIBLE 只在 IndexInvisible 产生式出现（索引级可见性，
// KEY idx(col) INVISIBLE），完全没有列级产生式——不是"版本注释语法没
// 适配"，是这条语法规则压根不存在：已用真实探针验证过，去掉
// /*!80023 ... */ 版本注释包装、裸写 col ... INVISIBLE 依然
// 解析失败。因此没有 AST 字段可以承载这个属性，只能在解析前从文本里
// 剥离、改写完成后按列名把原始标记文本插回去，这一点也是 charset_registry
// .go 那套"补登记表"方案完全用不上的地方——INVISIBLE 不是"parser 不
// 认识这个名字"，是语法产生式本身不存在，没有可以补登记的入口。
package schemamig

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
)

// reInvisibleColumnMarker 匹配列级 INVISIBLE 标记的两种源端形态：
// SHOW CREATE TABLE 的真实输出固定带版本注释包装（/*!80023 INVISIBLE
// */），但本函数也兼容裸 INVISIBLE（不假设输入一定来自 SHOW CREATE
// TABLE）。
var reInvisibleColumnMarker = regexp.MustCompile(`(?i)/\*!\d+\s+INVISIBLE\s*\*/|\bINVISIBLE\b`)

// invisibleColumnMarker 记录一处从源端 DDL 剥离出来的列级 INVISIBLE
// 标记——属于哪一列（裸列名，不带反引号）、原始标记文本是什么。
type invisibleColumnMarker struct {
	column string
	text   string
}

// stripInvisibleColumnMarkers 从源端 DDL 里剥离全部列级 INVISIBLE 标记，
// 返回剥离后的文本与每个标记归属的列；ok=false 表示存在标记但没能安全
// 识别出归属列（比如列定义里出现了未预期的括号/引号结构），调用方此时
// 应该按"无法安全改写"阻断，不能在不确定归属的情况下猜测着继续。
//
// 按列归属：不使用整表范围的猜测，而是从每个标记出现的位置往前找最近
// 的顶层分隔边界（列表左括号或顶层逗号，用括号深度 + 反引号状态跳过嵌套
// 内容），边界后的第一个反引号标识符就是这一列的列名——SHOW CREATE
// TABLE 的固定格式保证了每个列定义都以 "`列名` 类型..." 开头。
//
// 已知边界情况：反向扫描只跟踪括号深度与反引号状态，不跟踪字符串字面量
// （单引号）内容——如果同一列定义里、INVISIBLE 标记之前还有一个带圆括号
// 的字符串字面量（例如极不常见的 `COMMENT 'note (draft)'` 出现在
// INVISIBLE 之前的列选项里），括号计数可能被打乱。这种组合本身就很罕见
// （GIPK 生成的列不会有这种内容），且 internal/schemamig/table.go 在最终产物上做了"必须能
// 重新解析"的兜底校验，真出现这种情况会在那一步被拦下阻断，不会静默
// 产出错误 DDL。
func stripInvisibleColumnMarkers(ddl string) (marked string, markers []invisibleColumnMarker, ok bool) {
	marked = ddl
	for {
		loc := reInvisibleColumnMarker.FindStringIndex(marked)
		if loc == nil {
			break
		}
		col, found := columnNameBeforeBoundary(marked, loc[0])
		if !found {
			return ddl, nil, false
		}
		markers = append(markers, invisibleColumnMarker{column: col, text: marked[loc[0]:loc[1]]})
		before := strings.TrimRight(marked[:loc[0]], " ")
		marked = before + marked[loc[1]:]
	}
	return marked, markers, true
}

// columnNameBeforeBoundary 从 pos 往前扫描，找到"顶层"（不在括号/反引号
// 内）的最近分隔边界（列表开头的 '(' 或顶层逗号 ','），返回边界之后
// 出现的第一个反引号标识符。
func columnNameBeforeBoundary(s string, pos int) (string, bool) {
	depth := 0
	inBacktick := false
	for i := pos - 1; i >= 0; i-- {
		c := s[i]
		if inBacktick {
			if c == '`' {
				inBacktick = false
			}
			continue
		}
		switch c {
		case '`':
			inBacktick = true
		case ')':
			depth++
		case '(':
			if depth == 0 {
				return firstBacktickIdent(s[i+1 : pos])
			}
			depth--
		case ',':
			if depth == 0 {
				return firstBacktickIdent(s[i+1 : pos])
			}
		}
	}
	return "", false
}

func firstBacktickIdent(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false
	}
	if s[0] == '`' {
		var name strings.Builder
		for i := 1; i < len(s); i++ {
			if s[i] == '`' {
				if i+1 < len(s) && s[i+1] == '`' {
					name.WriteByte('`')
					i++
					continue
				}
				return name.String(), name.Len() > 0
			}
			name.WriteByte(s[i])
		}
		return "", false
	}
	end := len(s)
	for i, c := range s {
		if unicode.IsSpace(c) {
			end = i
			break
		}
		if !unicode.IsLetter(c) && !unicode.IsDigit(c) && c != '_' && c != '$' {
			return "", false
		}
	}
	return s[:end], end > 0
}

// survivingInvisibleColumnMarkers 从 markers 里过滤出归属列仍然存在于 cols
// 里的那些。cols 应该传改写各步骤（列过滤 / 映射等）完成之后的最终列
// AST 节点——不是随便哪个中间态。用途：判断"改写最终产物里到底还有没有
// 不可见列"不能只看源端 DDL 曾经出现过 INVISIBLE，列存在与否要看最终
// 产物；列的消失途径不止 DropColumns 一种（将来可能有别的过滤/投影
// 机制），这里按"是否还在最终列清单里"统一判断，不绑死在某个具体删除
// 机制上——rewriteTable 的目标能力门槛检查、以及下面 reinsert...
// 函数的回填范围，都基于这份过滤结果，两处判断口径保持一致。
func survivingInvisibleColumnMarkers(markers []invisibleColumnMarker, cols []*ast.ColumnDef) []invisibleColumnMarker {
	if len(markers) == 0 {
		return nil
	}
	present := make(map[string]bool, len(cols))
	for _, col := range cols {
		present[col.Name.Name.O] = true
	}
	var out []invisibleColumnMarker
	for _, m := range markers {
		if present[m.column] {
			out = append(out, m)
		}
	}
	return out
}

// reinsertInvisibleColumnMarkers 在 restore 出来的 DDL 里，按列名把
// stripInvisibleColumnMarkers 记录的标记重新插回对应列定义的末尾。cols
// 是改写完成、已经过滤/映射之后的列 AST 节点——归属列已经不在 cols 里的
// 标记（见 survivingInvisibleColumnMarkers）直接跳过，不做任何插入（列
// 本身都不在了，标记自然不需要保留）。
//
// 定位方式：把这一列单独 Restore 成文本，去完整 DDL 里找这段文本作为
// 定位锚点——不用手写文本拼接或做括号深度扫描去猜这一列的定义边界，
// 直接复用 tidb 自己的 Restore 保证格式（大小写、空格）跟整条语句的
// restore 结果完全一致；*ast.ColumnDef 自己实现了 Restore(ctx)，可以
// 脱离整条语句单独调用（已用真实探针验证）。锚点文本以反引号包裹的
// 列名开头，同一张表内列名唯一，理论上只会命中一次；如果命中次数不是
// 恰好一次，返回 error 而不是猜一个位置插入——按"无法安全改写一律
// 阻断"处理，调用方应把这种情况当成整表阻断。
func reinsertInvisibleColumnMarkers(ddl string, cols []*ast.ColumnDef, markers []invisibleColumnMarker) (string, error) {
	markers = survivingInvisibleColumnMarkers(markers, cols)
	if len(markers) == 0 {
		return ddl, nil
	}
	byColumn := make(map[string]*ast.ColumnDef, len(cols))
	for _, col := range cols {
		byColumn[col.Name.Name.O] = col
	}
	for _, m := range markers {
		col := byColumn[m.column] // survivingInvisibleColumnMarkers 已保证存在。
		var buf strings.Builder
		ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &buf)
		if err := col.Restore(ctx); err != nil {
			return "", fmt.Errorf("schemamig: 单独渲染列 %s 失败: %w", m.column, err)
		}
		anchor := buf.String()
		if count := strings.Count(ddl, anchor); count != 1 {
			return "", fmt.Errorf("schemamig: 列 %s 的定位锚点在改写产物里出现 %d 次（预期恰好 1 次），无法安全回填 INVISIBLE 标记", m.column, count)
		}
		ddl = strings.Replace(ddl, anchor, anchor+" "+m.text, 1)
	}
	return ddl, nil
}
