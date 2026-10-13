package schemamig

import (
	"fmt"

	"github.com/antlr4-go/antlr/v4"
	"github.com/bytebase/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
)

// rewriteView 实现 ObjectView 的改写管线：解析 → 视图自身的库/表映射 →
// DEFINER 处理 → SELECT 体内表引用的库映射 → restore。
func rewriteView(req Request) (Result, error) {
	stmt, err := parseOneTiDBStmt(req.DDL)
	if err != nil {
		return blocked(err.Error(), req.DDL), nil
	}
	cv, ok := stmt.(*ast.CreateViewStmt)
	if !ok {
		return Result{}, fmt.Errorf("%w: 期望 CREATE VIEW，实际是 %T", ErrDDLKindMismatch, stmt)
	}

	srcSchema := cv.ViewName.Schema.O
	if srcSchema == "" {
		srcSchema = req.DefaultSchema
	}
	srcName := cv.ViewName.Name.O
	newSchema := req.Mapping.schemaTarget(srcSchema, req.Options.SourceCaseSensitive)
	newName := req.Mapping.tableTarget(srcSchema, srcName, req.Options.SourceCaseSensitive)
	cv.ViewName.Schema = ast.NewCIStr(newSchema)
	cv.ViewName.Name = ast.NewCIStr(newName)

	if reason, err := applyDefiner(cv.Definer, req.Options, func(u *auth.UserIdentity) { cv.Definer = u }); err != nil {
		return Result{}, err
	} else if reason != "" {
		return blocked(reason, req.DDL), nil
	}

	if cv.Select != nil {
		refs := &viewReferences{schema: srcSchema, refs: map[SchemaTable]bool{}, ctes: map[string]bool{}}
		cv.Select.Accept(refs)
		if len(refs.ctes) != 0 || req.Mapping.ColumnFilter != nil || len(req.Mapping.Columns) != 0 || len(req.Mapping.DropColumns) != 0 {
			if reason := rewriteViewColumns(cv, req.Mapping, srcSchema, srcName, req.Options.SourceCaseSensitive); reason != "" {
				return blocked(reason, req.DDL), nil
			}
		} else {
			cv.Select.Accept(&tableNameRewriter{mapping: req.Mapping, caseSensitive: req.Options.SourceCaseSensitive, defaultSchema: srcSchema})
		}
	}

	ddl, err := restoreMySQLView(cv, req.DDL)
	if err != nil {
		return blocked(err.Error(), req.DDL), nil
	}
	return Result{DDL: ddl}, nil
}

func restoreMySQLView(view *ast.CreateViewStmt, original string) (string, error) {
	ddl, err := restoreTiDB(view)
	if err != nil {
		return "", err
	}
	// TiDB Restore omits CASCADED, while MySQL requires WITH CHECK OPTION
	// to retain enforcement. Read only a parsed view-tail option, never text
	// inside a query expression or literal.
	if view.CheckOption == ast.CheckOptionCascaded {
		parsed, err := parseRoutine(original)
		if err != nil {
			return "", fmt.Errorf("无法确认视图 CHECK OPTION 的完整语义: %w", err)
		}
		finder := &viewCheckFinder{BaseMySQLParserListener: &mysql.BaseMySQLParserListener{}}
		antlr.ParseTreeWalkerDefault.Walk(finder, parsed.tree)
		if finder.present {
			ddl += " WITH CASCADED CHECK OPTION"
		}
	}
	return ddl, nil
}

type viewCheckFinder struct {
	*mysql.BaseMySQLParserListener
	present bool
}

func (f *viewCheckFinder) EnterViewCheckOption(*mysql.ViewCheckOptionContext) {
	f.present = true
}

// applyDefiner 是需求 MGR-SCH-09 的通用判定："目标存在同名账号则保留，
// 否则改写为策略配置的账号"。set 由调用方传入用来把结果写回具体 AST 节点
// （View/存储过程/函数/触发器/事件的 Definer 字段类型不完全一样，但判定
// 逻辑一致）。返回非空 reason 表示应该阻断（策略要求改写但没配置兜底账号，
// 或调用方没有提供预检查结果）。
func applyDefiner(current *auth.UserIdentity, opts Options, set func(*auth.UserIdentity)) (string, error) {
	if opts.DefinerAccountExists || current == nil {
		return "", nil // 目标已有同名账号（或源端定义本来就没有 DEFINER），保留不变。
	}
	if opts.DefinerFallback == "" {
		return "目标端缺少与源端 DEFINER 同名的账号，且未配置兜底 DEFINER 账号（需求 MGR-SCH-09 / PRE-15）", nil
	}
	acc, err := resolveDefinerFallback(opts.DefinerFallback)
	if err != nil {
		return "", fmt.Errorf("schemamig: 解析 DefinerFallback 失败: %w", err)
	}
	if acc.currentUser {
		// auth.UserIdentity.CurrentUser=true 时 tidb 自己的 Restore 只输出
		// 关键字 CURRENT_USER，不看 Username/Hostname，两者留空即可。
		set(&auth.UserIdentity{CurrentUser: true})
		return "", nil
	}
	set(&auth.UserIdentity{Username: acc.user, Hostname: acc.host})
	return "", nil
}

// tableNameRewriter 是 ast.Visitor，改写"已带库限定名"的两类节点：
//   - *ast.TableName（Schema.O != ""）：FROM/JOIN 子句里的表引用。
//   - *ast.ColumnName（Schema.O != ""）：SELECT 列表 / WHERE / GROUP BY 等
//     位置的 `db.table.column` 三段式列引用——SHOW CREATE VIEW 的真实输出
//     固定把列引用展开成这种全限定形式（真实 MySQL 执行验证过：只改
//     TableName 不改 ColumnName 时，改写产物在目标库执行报
//     Error 1054 Unknown column，因为 SELECT 列表里的 `db.table.col` 还
//     停留在源库名，而 FROM 子句已经指向目标库，两边对不上）。
//
// 不带库限定的引用（用当前会话默认库解析，或者是表别名限定，比如视图用了
// `FROM t_main AS t` 时列引用会是 `t`.`col`，Table.O="t"、Schema.O=""）
// 保持原样——列引用的库限定符和表别名限定符在 tidb AST 里分别对应
// ColumnName.Schema 是否非空，天然可以区分，不需要额外判断"这段前缀到底是
// 库名还是别名"。
type tableNameRewriter struct {
	mapping       Mapping
	caseSensitive bool // Options.SourceCaseSensitive，见 types.go 类型注释。
	defaultSchema string
}

func (v *tableNameRewriter) Enter(n ast.Node) (ast.Node, bool) {
	switch node := n.(type) {
	case *ast.TableName:
		if node.Schema.O == "" && v.defaultSchema != "" {
			node.Schema = ast.NewCIStr(v.defaultSchema)
		}
		if node.Schema.O != "" {
			srcSchema := node.Schema.O
			newTable := v.mapping.tableTarget(srcSchema, node.Name.O, v.caseSensitive)
			newSchema := v.mapping.schemaTarget(srcSchema, v.caseSensitive)
			node.Schema = ast.NewCIStr(newSchema)
			node.Name = ast.NewCIStr(newTable)
		}
	case *ast.ColumnName:
		if node.Schema.O != "" {
			srcSchema := node.Schema.O
			newTable := v.mapping.tableTarget(srcSchema, node.Table.O, v.caseSensitive)
			newSchema := v.mapping.schemaTarget(srcSchema, v.caseSensitive)
			node.Schema = ast.NewCIStr(newSchema)
			node.Table = ast.NewCIStr(newTable)
		}
	case *ast.FuncCallExpr:
		if node.Schema.O != "" {
			node.Schema = ast.NewCIStr(v.mapping.schemaTarget(node.Schema.O, v.caseSensitive))
		}
	}
	return n, false
}

func (v *tableNameRewriter) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}
