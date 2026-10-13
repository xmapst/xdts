package schemamig

import (
	"fmt"
	"strings"
)

// definerAccount 是 Options.DefinerFallback 解析后的规范化表示，是 MGR-SCH-09
// 兜底 DEFINER 账号在 tidb AST 路线（VIEW，构造 *auth.UserIdentity）与 ANTLR
// 路线（存储过程/函数/触发器/事件，token 级文本替换）之间唯一共用的判定
// 逻辑——两条路线各自负责把这个中间表示渲染成自己需要的形式（AST 字段 /
// SQL 文本），但"输入字符串到底表示什么账号"这一步只有这一份实现，不允许
// 出现两条路线判断不一致的情况（真实 MySQL 执行发现过一次：ANTLR 路线对
// CURRENT_USER 是意外地能跑通、对 user@host 又因为不加引号在 host 含 "%"
// 时直接语法错误；tidb 路线则反过来拒绝 CURRENT_USER。这两个问题的根因都是
// 同一件事分别实现了两次、判断标准不一致）。
type definerAccount struct {
	currentUser bool
	user, host  string
}

// resolveDefinerFallback 解析 Options.DefinerFallback，支持两种合法形式：
//   - CURRENT_USER（不区分大小写，允许带一对空括号 "CURRENT_USER()"，两种
//     写法在 MySQL DEFINER 子句语法里都合法）——特殊值，不能被当成用户名去
//     拆分成 user@host。
//   - "user@host"：按"不在反引号内的第一个 @"切分，两侧允许已经带反引号，
//     也允许裸标识符；不校验 host 部分是否是合法的 IP/域名/通配符模式
//     （"%"、"192.168.1.%" 都是合法 MySQL host），交给目标 MySQL 在真正
//     CREATE 时校验。
func resolveDefinerFallback(s string) (definerAccount, error) {
	trimmed := strings.TrimSpace(s)
	if isCurrentUserLiteral(trimmed) {
		return definerAccount{currentUser: true}, nil
	}
	user, host, err := splitUserHost(trimmed)
	if err != nil {
		return definerAccount{}, err
	}
	return definerAccount{user: user, host: host}, nil
}

func isCurrentUserLiteral(s string) bool {
	return strings.EqualFold(s, "CURRENT_USER") || strings.EqualFold(s, "CURRENT_USER()")
}

// splitUserHost 把 "user@host" 形式拆成用户名与主机名。按"不在反引号内的
// 第一个 @"切分——用户名/主机名允许包含 @ 的唯一方式是被反引号包住，本
// 函数据此正确处理这种情况，不会在反引号内的 @ 处误切分。
func splitUserHost(s string) (user, host string, err error) {
	inQuote := false
	splitAt := -1
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '`':
			inQuote = !inQuote
		case '@':
			if !inQuote {
				splitAt = i
			}
		}
		if splitAt >= 0 {
			break
		}
	}
	if splitAt < 0 {
		return "", "", fmt.Errorf("DefinerFallback %q 既不是 CURRENT_USER，也不是 `user`@`host` 形式（缺少未加引号的 @ 分隔符）", s)
	}
	return unquoteMySQLIdent(s[:splitAt]), unquoteMySQLIdent(s[splitAt+1:]), nil
}

// text 把账号渲染成可以直接拼进 SQL 文本的形式，供 ANTLR 路线使用——那条
// 路线是 token 级文本替换，不经过任何会自动加引号的 AST 节点，因此必须
// 自己负责给 user/host 各自补上反引号（真实 MySQL 执行验证过：host 是
// "%" 这类通配符时不加引号会直接 Error 1064，这是本函数存在的直接原因）。
// tidb 路线不使用这个方法——它构造 *auth.UserIdentity 交给 tidb 自己的
// Restore 逻辑加引号，见 internal/schemamig/view.go 的 applyDefiner。
func (a definerAccount) text() string {
	if a.currentUser {
		return "CURRENT_USER"
	}
	return quoteMySQLIdent(a.user) + "@" + quoteMySQLIdent(a.host)
}
