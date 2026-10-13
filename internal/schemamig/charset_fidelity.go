// Preserve utf8mb3 names that TiDB otherwise normalizes to utf8 aliases.
// Replace charset/collation identifiers with registered markers before parsing,
// then restore their original names. Replacements must leave quoted user content
// and unrelated identifiers intact. The restored DDL is reparsed before acceptance.
package schemamig

import (
	"regexp"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/charset"
)

// 四个哨兵名字：刻意用不像任何真实内容的拼写，四个互不为彼此子串，回填
// 阶段可以按任意顺序做全文字面量替换，不用担心相互覆盖。
const (
	utf8mb3CharsetMarker        = "xdtsutf8mb3csmarker"
	utf8mb3BinMarker            = "xdtsutf8mb3binmarker"
	utf8mb3UnicodeCIMarker      = "xdtsutf8mb3unicidmarker"
	utf8mb3GeneralCIMarker      = "xdtsutf8mb3gencidmarker"
	utf8mb3MarkerCharsetSortlen = 1
)

// registerUTF8MB3FidelityMarkers 注册上面四个哨兵名字，供 internal/schemamig/charset_registry.go
// 的 init() 调用。哨兵字符集本身没有特殊语义要求，只需要能被
// charset.GetCharsetInfo / charset.GetCollationByName 正常解析、且不会
// 触发 tidb 内部对 utf8mb3 那四个名字的别名重定向（重定向按精确字符串
// 比较，哨兵拼写不是 "utf8mb3"/"utf8mb3_bin"/... 本身，天然不会触发）。
func registerUTF8MB3FidelityMarkers() {
	charset.AddCharset(&charset.Charset{
		Name:             utf8mb3CharsetMarker,
		DefaultCollation: utf8mb3GeneralCIMarker,
		Collations:       make(map[string]*charset.Collation),
		Maxlen:           3, // 与真实 utf8mb3 一致，纯粹为了信息准确，本包不依赖这个值做任何计算。
	})
	for i, name := range []string{utf8mb3BinMarker, utf8mb3UnicodeCIMarker, utf8mb3GeneralCIMarker} {
		charset.AddCollation(&charset.Collation{
			// 用一段不会与 internal/schemamig/charset_registry.go 已登记 ID、也不会与 tidb
			// 内置 ID 冲突的高位区间；具体数值无实际意义，只要求互不相同
			// 且不与既有登记冲突。
			ID:           900000 + i,
			CharsetName:  utf8mb3CharsetMarker,
			Name:         name,
			Sortlen:      utf8mb3MarkerCharsetSortlen,
			PadAttribute: charset.PadNone,
		})
	}
}

// reCharsetUTF8MB3 只匹配紧跟在 CHARACTER SET / CHARSET 关键字之后（等号
// 可选）的裸 utf8mb3——不匹配 utf8mb3_xxx 这种排序规则形态（那是
// reCollateUTF8MB3 的职责），避免两个正则的匹配范围重叠。
var reCharsetUTF8MB3 = regexp.MustCompile(`(?i)((?:CHARACTER\s+SET|CHARSET)\s*=?\s*)utf8mb3\b`)

// reCollateUTF8MB3 只匹配紧跟在 COLLATE 关键字之后（等号可选）的这三个
// 精确排序规则名——不是 utf8mb3_ 前缀的任意排序规则，只有这三个会被 tidb
// 内部别名重定向；utf8mb3 家族其余那些走 internal/schemamig/charset_registry.go 的登记表方案
// 即可保真，不需要哨兵替换。
//
// 具体条数（42 条缺口 / 其中 28 条属 utf8mb3 家族 / 25 登记 + 3 哨兵 /
// missingCollations 共 39 条）以 internal/schemamig/charset_registry.go 开头那段为准，本文件
// 不复述——那边连"42 与 39 不是同一件事"都写了，这里再抄一份只是多一个会漂
// 的副本。
//
// 这么做的理由：升级依赖的人改的是 go.mod，他不会来搜 utf8mb3，写在这里的
// 提醒对他等于不存在；而他一定会跑 `go test ./internal/schemamig/`。上面那段散文
// 保留是因为它解释了为什么要管这件事，断言只管发现它变了。
var reCollateUTF8MB3 = regexp.MustCompile(`(?i)(COLLATE\s*=?\s*)utf8mb3_(bin|unicode_ci|general_ci)\b`)

// stripUTF8MB3Markers 把源 DDL 文本里 CHARACTER SET/CHARSET/COLLATE 子句
// 位置上的 utf8mb3 相关名字替换成哨兵名字，返回替换后的文本。没有命中时
// 原样返回输入（不分配新字符串之外没有其他副作用）。
func stripUTF8MB3Markers(ddl string) string {
	ddl = reCharsetUTF8MB3.ReplaceAllString(ddl, "${1}"+utf8mb3CharsetMarker)
	ddl = reCollateUTF8MB3.ReplaceAllStringFunc(ddl, func(m string) string {
		sub := reCollateUTF8MB3.FindStringSubmatch(m)
		switch sub[2] {
		case "bin":
			return sub[1] + utf8mb3BinMarker
		case "unicode_ci":
			return sub[1] + utf8mb3UnicodeCIMarker
		default: // "general_ci"
			return sub[1] + utf8mb3GeneralCIMarker
		}
	})
	return ddl
}

// utf8mb3MarkerReplacement 把一个哨兵名字（预编译为不区分大小写的正则）
// 和它对应的真实拼写配对。
type utf8mb3MarkerReplacement struct {
	pattern *regexp.Regexp
	real    string
}

// utf8mb3MarkerReplacements 四个哨兵各自的换回规则。正则在包初始化时
// 一次性编译，不在每次调用 restoreUTF8MB3Markers 时重新编译。
var utf8mb3MarkerReplacements = []utf8mb3MarkerReplacement{
	{regexp.MustCompile(`(?i)` + utf8mb3BinMarker), "utf8mb3_bin"},
	{regexp.MustCompile(`(?i)` + utf8mb3UnicodeCIMarker), "utf8mb3_unicode_ci"},
	{regexp.MustCompile(`(?i)` + utf8mb3GeneralCIMarker), "utf8mb3_general_ci"},
	{regexp.MustCompile(`(?i)` + utf8mb3CharsetMarker), "utf8mb3"},
}

// restoreUTF8MB3Markers 把哨兵名字替换回真实的 utf8mb3 拼写。哨兵名字是
// 本文件发明的专用字符串，不会在真实用户内容里天然出现，这一步做全文
// 替换是安全的，不需要像 stripUTF8MB3Markers 那样做关键字锚定；但必须
// 不区分大小写匹配——tidb Restore 对表级选项（DEFAULT CHARACTER SET=...
// / DEFAULT COLLATE=...）统一转成大写，对列级属性（`col` ... CHARACTER
// SET ... COLLATE ...）保留原样大小写，哨兵名字在这两个位置会分别以
// 大写、小写两种形态出现（已实测确认），必须都能命中；替换后的大小写
// 跟随命中文本本身的大小写，不强行统一成某一种，保持跟 DDL 里其余部分
// 同一种风格（大写表级选项旁边混一个小写真实名字会显得不一致，虽然不
// 影响正确性，但没有理由制造这种不一致）。
func restoreUTF8MB3Markers(ddl string) string {
	for _, r := range utf8mb3MarkerReplacements {
		ddl = r.pattern.ReplaceAllStringFunc(ddl, func(matched string) string {
			if matched == strings.ToUpper(matched) {
				return strings.ToUpper(r.real)
			}
			return r.real
		})
	}
	return ddl
}
