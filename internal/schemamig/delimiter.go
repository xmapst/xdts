package schemamig

import (
	"fmt"
	"strings"
)

// delimiterToken 是包装用的自定义分隔符。用 "$$" 而非默认的 ";" 是标准
// MySQL 客户端惯例（`mysqldump`/官方文档示例一致用法），选它只是为了可读性
// 与常见工具输出保持一致，换成其它不含分号、不与语句体冲突的 token 同样
// 正确。
const delimiterToken = "$$"

// WrapDelimiter 把一条改写后的 DDL 包装成可以被
// `mysql < file.sql`（或任何按默认 ";" 分隔符切分语句的客户端）正确整体
// 执行的文本（需求 MGR-SCH-10：「生成的 DDL 必须包 DELIMITER 包装」，设计
// 第二条——实测把未包装的触发器 DDL 存成 .sql 文件执行会报
// `ERROR 1064`，因为客户端按默认分隔符把过程体内部的分号提前切断了）。
//
// 只应该用在 Result.NeedsDelimiter=true 的场景（存储过程 / 函数 / 触发器 /
// 事件）；表 / 视图的 DDL 本身就是单条语句，包装是无意义的噪音，调用方应该
// 先检查 NeedsDelimiter 再决定是否调用本函数——这正是 Result 携带该字段的
// 原因，不要求调用方自己重新判断对象类型。
//
// ddl 结尾允许带或不带分号，输出统一为:
//
//	DELIMITER $$
//	<ddl>$$
//	DELIMITER ;
func WrapDelimiter(ddl string) string {
	trimmed := strings.TrimRight(ddl, " \t\r\n;")
	// mysql recognizes delimiters inside stored bodies too. Choose one that
	// never occurs in the statement, including quoted strings and comments.
	token := delimiterToken
	for i := 1; strings.Contains(trimmed, token); i++ {
		token = fmt.Sprintf("$$%d$$", i)
	}
	var b strings.Builder
	b.WriteString("DELIMITER ")
	b.WriteString(token)
	b.WriteByte('\n')
	b.WriteString(trimmed)
	b.WriteString(token)
	b.WriteByte('\n')
	b.WriteString("DELIMITER ;\n")
	return b.String()
}
