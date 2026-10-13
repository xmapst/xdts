package schema

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/xmapst/xdts/internal/schemamig"

	// tidb parser 在独立模块（脱离完整 tidb 编译单元）场景下必须注册一个
	// "字面量值驱动"才能构造字面量表达式节点，否则 parser.New() 会直接 panic
	// （"no parser driver (forgotten import?)"，internal/schemamig/parse_tidb.go
	// 已经实测复现并记录过这一点）。结构跟踪解析全部表DDL；CREATE的
	// 不可见列兼容复用schemamig的只读元数据解析，执行SQL仍由改写器负责。
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

// parseOneStmt 解析恰好一条语句。binlog QueryEvent.Query 本来就是单条语句，
// 不带结尾分号也能解析。
func parseOneStmt(ddl string) (ast.StmtNode, error) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(ddl, "", "")
	if err != nil {
		if table, tableErr := schemamig.ParseTableDefinition(ddl); tableErr == nil {
			return table, nil
		}
	}
	if err != nil {
		return nil, fmt.Errorf("schema: 解析 DDL 失败: %w", err)
	}
	return stmt, nil
}
