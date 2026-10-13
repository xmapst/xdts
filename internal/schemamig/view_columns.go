package schemamig

import "github.com/pingcap/tidb/pkg/parser/ast"

// viewColumns binds source names before any table is renamed. Each instance
// owns one SELECT scope; unsupported scopes are rejected rather than guessed.
type viewColumns struct {
	mapping     Mapping
	sensitive   bool
	schema      string
	tables      []viewColumnTable
	aliases     map[string]string
	selects     int
	aliasClause bool
	reason      string
}

type viewColumnTable struct {
	SchemaTable
	alias string
}

func rewriteSimpleViewColumns(view *ast.CreateViewStmt, mapping Mapping, schema, name string, sensitive bool) string {
	selectStmt, ok := view.Select.(*ast.SelectStmt)
	if !ok || selectStmt.With != nil {
		return "视图列规则需要可绑定的单个 SELECT 作用域"
	}
	v := &viewColumns{mapping: mapping, sensitive: sensitive, schema: schema, aliases: map[string]string{}}
	if selectStmt.From != nil {
		v.collect(selectStmt.From.TableRefs)
	}
	if v.reason != "" {
		return v.reason
	}
	for i, col := range view.Cols {
		if !v.allowed(schema, name, col.O) {
			return "视图输出引用被过滤列"
		}
		view.Cols[i] = ast.NewCIStr(mapping.columnTarget(schema, name, col.O, sensitive))
	}
	for _, field := range selectStmt.Fields.Fields {
		if field.WildCard != nil {
			return "视图通配符需实际列元数据才能安全应用列规则"
		}
		output := field.AsName.O
		if output == "" {
			if col, ok := field.Expr.(*ast.ColumnNameExpr); ok {
				output = col.Name.Name.O
			}
		}
		if output != "" {
			if len(view.Cols) == 0 && !v.allowed(schema, name, output) {
				return "视图输出引用被过滤列"
			}
			target := output
			if len(view.Cols) == 0 {
				target = mapping.columnTarget(schema, name, output, sensitive)
			}
			// Keep the public view name when its underlying column is renamed.
			field.AsName = ast.NewCIStr(target)
			v.aliases[foldKey(output)] = target
		} else {
			if len(view.Cols) == 0 {
				return "视图表达式输出需显式别名或列列表才能保留公开列名"
			}
		}
	}
	selectStmt.Accept(v)
	return v.reason
}

func (v *viewColumns) collect(node ast.ResultSetNode) {
	switch n := node.(type) {
	case nil:
	case *ast.Join:
		if n.NaturalJoin || len(n.Using) != 0 {
			v.reason = "视图 NATURAL/USING 连接需实际列依赖才能安全应用列规则"
			return
		}
		v.collect(n.Left)
		v.collect(n.Right)
	case *ast.TableSource:
		t, ok := n.Source.(*ast.TableName)
		if !ok {
			v.reason = "视图派生表的列作用域暂不能安全绑定"
			return
		}
		schema := t.Schema.O
		if schema == "" {
			schema = v.schema
		}
		v.tables = append(v.tables, viewColumnTable{SchemaTable{schema, t.Name.O}, n.AsName.O})
	default:
		v.reason = "视图表作用域暂不能安全绑定"
	}
}

func (v *viewColumns) allowed(schema, table, column string) bool {
	return !v.mapping.dropColumnSet(schema, table, v.sensitive)[foldKey(column)] && (v.mapping.ColumnFilter == nil || v.mapping.ColumnFilter.Allow(schema, table, column))
}

func (v *viewColumns) bind(c *ast.ColumnName) (viewColumnTable, bool) {
	var matched []viewColumnTable
	for _, table := range v.tables {
		if c.Schema.O != "" {
			if table.alias == "" && foldEqual(c.Schema.O, table.Schema, v.sensitive) && foldEqual(c.Table.O, table.Table, v.sensitive) {
				matched = append(matched, table)
			}
		} else if c.Table.O == "" || table.alias != "" && foldEqual(c.Table.O, table.alias, v.sensitive) || table.alias == "" && foldEqual(c.Table.O, table.Table, v.sensitive) {
			matched = append(matched, table)
		}
	}
	if len(matched) != 1 {
		v.reason = "视图列引用无法唯一绑定来源表"
		return viewColumnTable{}, false
	}
	return matched[0], true
}

func (v *viewColumns) Enter(node ast.Node) (ast.Node, bool) {
	if v.reason != "" {
		return node, true
	}
	switch n := node.(type) {
	case *ast.SelectStmt:
		v.selects++
		if v.selects > 1 {
			v.reason = "视图嵌套 SELECT 的列作用域暂不能安全绑定"
			return node, true
		}
	case *ast.GroupByClause, *ast.HavingClause, *ast.OrderByClause:
		v.aliasClause = true
	case *ast.ColumnName:
		if v.aliasClause && n.Schema.O == "" && n.Table.O == "" {
			if alias, ok := v.aliases[foldKey(n.Name.O)]; ok {
				// GROUP BY can prefer an underlying column over a select alias.
				for _, table := range v.tables {
					if v.mapping.columnTarget(table.Schema, table.Table, n.Name.O, v.sensitive) != n.Name.O {
						v.reason = "视图输出别名与来源列映射存在歧义"
						return node, true
					}
				}
				n.Name = ast.NewCIStr(alias)
				return node, true
			}
		}
		table, ok := v.bind(n)
		if !ok {
			return node, true
		}
		if !v.allowed(table.Schema, table.Table, n.Name.O) {
			v.reason = "视图表达式引用被过滤列"
			return node, true
		}
		n.Name = ast.NewCIStr(v.mapping.columnTarget(table.Schema, table.Table, n.Name.O, v.sensitive))
		if n.Table.O != "" && table.alias == "" {
			n.Table = ast.NewCIStr(v.mapping.tableTarget(table.Schema, table.Table, v.sensitive))
			if n.Schema.O != "" {
				n.Schema = ast.NewCIStr(v.mapping.schemaTarget(table.Schema, v.sensitive))
			}
		}
	case *ast.TableName:
		schema := n.Schema.O
		if schema == "" {
			schema = v.schema
		}
		n.Name = ast.NewCIStr(v.mapping.tableTarget(schema, n.Name.O, v.sensitive))
		n.Schema = ast.NewCIStr(v.mapping.schemaTarget(schema, v.sensitive))
	case *ast.FuncCallExpr:
		if n.Schema.O != "" {
			n.Schema = ast.NewCIStr(v.mapping.schemaTarget(n.Schema.O, v.sensitive))
		}
	}
	return node, false
}

func (v *viewColumns) Leave(node ast.Node) (ast.Node, bool) {
	switch node.(type) {
	case *ast.GroupByClause, *ast.HavingClause, *ast.OrderByClause:
		v.aliasClause = false
	}
	return node, true
}
