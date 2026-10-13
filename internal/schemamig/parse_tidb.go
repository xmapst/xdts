// internal/schemamig/parse_tidb.go 提供表 / 视图改写共用的 tidb parser 封装（ADR-08 分层解析器
// 的表/视图分支）。
package schemamig

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"

	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

// parseOneTiDBStmt 用 tidb parser 解析恰好一条语句（SHOW CREATE 系列返回的
// 文本本来就是单条语句，不带结尾分号也能解析）。
func parseOneTiDBStmt(ddl string) (ast.StmtNode, error) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(ddl, "", "")
	if err != nil {
		if normalized, ok := explicitViewCheckOption(ddl); ok {
			stmt, err = p.ParseOneStmt(normalized, "", "")
		}
	}
	if err != nil {
		return nil, fmt.Errorf("解析失败: %w", err)
	}
	return stmt, nil
}

// restoreTiDB 把 AST 还原成 SQL 文本。统一使用
// format.DefaultRestoreFlags（反引号引用标识符、关键字大写、字符串单引号），
// 与 SHOW CREATE 系列原生输出风格一致。
func restoreTiDB(node ast.Node) (string, error) {
	var sb strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := node.Restore(ctx); err != nil {
		return "", fmt.Errorf("restore 失败: %w", err)
	}
	return sb.String(), nil
}
