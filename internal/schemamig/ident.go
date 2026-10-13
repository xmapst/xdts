package schemamig

import "strings"

func foldEqual(a, b string, caseSensitive bool) bool {
	if caseSensitive {
		return a == b
	}
	return strings.EqualFold(a, b)
}

// foldKey 把列名规范化为大小写不敏感比较用的 map key。恒定折叠，不受
// lower_case_table_names 影响——MySQL 列名的大小写比较不受这个变量约束，
// 这一点与库名/表名（foldEqual/lookupFold）是两条独立的规则，不要合并处理。
func foldKey(s string) string {
	return strings.ToLower(s)
}

// lookupFold 在一个 string->string 的库名映射里按 caseSensitive 指定的策略
// 查找 key（语义同 foldEqual），未命中时返回 fallback。只用于库名查找
// （Mapping.Schemas），不用于列名。
func lookupFold(m map[string]string, key, fallback string, caseSensitive bool) string {
	for k, v := range m {
		if foldEqual(k, key, caseSensitive) {
			return v
		}
	}
	return fallback
}

// unquoteMySQLIdent 去掉 MySQL 反引号标识符的引用并还原被转义的反引号
// （“ ` “ -> “ ` “）；非反引号包裹的输入原样返回。ANTLR 路线下从
// token 文本里取出的标识符可能带引号，需要先还原成"裸名字"才能与
// Mapping 里配置的库名做比较。
func unquoteMySQLIdent(s string) string {
	if len(s) < 2 || s[0] != '`' || s[len(s)-1] != '`' {
		return s
	}
	inner := s[1 : len(s)-1]
	return strings.ReplaceAll(inner, "``", "`")
}

// quoteMySQLIdent 把一个裸名字包装成反引号标识符，内部的反引号按 MySQL
// 规则转义成两个反引号。restore 阶段统一用反引号引用，与
// format.DefaultRestoreFlags 的 RestoreNameBackQuotes 保持一致的风格。
func quoteMySQLIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}
