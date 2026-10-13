package schemamig

import (
	"fmt"
	"maps"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

type queryOutput struct{ source, target string }
type queryTable struct {
	SchemaTable
	alias   string
	derived bool
	outputs []queryOutput
}

// Each SELECT owns its bindings; only an explicit parent allows correlation.
type queryBinder struct {
	mapping   Mapping
	schema    string
	sensitive bool
	depth     int
	reason    string
}
type queryScope struct {
	binder      *queryBinder
	parent      *queryScope
	tables      []queryTable
	ctes        map[string][]queryOutput
	aliases     map[string]string
	aliasClause bool
}

func rewriteViewColumns(view *ast.CreateViewStmt, mapping Mapping, schema, name string, sensitive bool) string {
	if selectStmt, ok := view.Select.(*ast.SelectStmt); ok && selectStmt.With == nil {
		probe := &nestedQueryProbe{}
		selectStmt.Accept(probe)
		if probe.selects == 1 {
			return rewriteSimpleViewColumns(view, mapping, schema, name, sensitive)
		}
	}
	b := &queryBinder{mapping: mapping, schema: schema, sensitive: sensitive}
	out := b.query(view.Select, nil, nil, schema, name, view.Cols)
	if b.reason != "" {
		return b.reason
	}
	if len(view.Cols) > 0 {
		if len(out) != len(view.Cols) {
			return "视图公开列数与 SELECT 不一致"
		}
		for i, col := range view.Cols {
			view.Cols[i] = ast.NewCIStr(mapping.columnTarget(schema, name, col.O, sensitive))
		}
	}
	return ""
}

type nestedQueryProbe struct{ selects int }

func (p *nestedQueryProbe) Enter(n ast.Node) (ast.Node, bool) {
	if _, ok := n.(*ast.SelectStmt); ok {
		p.selects++
	}
	return n, false
}
func (*nestedQueryProbe) Leave(n ast.Node) (ast.Node, bool) { return n, true }
func (b *queryBinder) allowed(schema, table, column string) bool {
	return !b.mapping.dropColumnSet(schema, table, b.sensitive)[foldKey(column)] && (b.mapping.ColumnFilter == nil || b.mapping.ColumnFilter.Allow(schema, table, column))
}

func (b *queryBinder) query(node ast.Node, parent *queryScope, inherited map[string][]queryOutput, publicSchema, publicName string, explicit []ast.CIStr) []queryOutput {
	if b.reason != "" || node == nil {
		return nil
	}
	b.depth++
	defer func() { b.depth-- }()
	if b.depth > 64 {
		b.reason = "视图查询嵌套超过64层"
		return nil
	}
	ctes := maps.Clone(inherited)
	if ctes == nil {
		ctes = map[string][]queryOutput{}
	}
	switch n := node.(type) {
	case *ast.SelectStmt:
		b.with(n.With, parent, ctes)
		s := &queryScope{binder: b, parent: parent, ctes: ctes, aliases: map[string]string{}}
		if n.From != nil {
			s.collect(n.From.TableRefs)
		}
		if n.Fields == nil {
			b.reason = "视图 SELECT 缺少输出列表"
			return nil
		}
		if len(explicit) > 0 && len(explicit) != len(n.Fields.Fields) {
			b.reason = "视图公开列数与 SELECT 不一致"
			return nil
		}
		out := make([]queryOutput, 0, len(n.Fields.Fields))
		for i, field := range n.Fields.Fields {
			if field.WildCard != nil {
				b.reason = "视图通配符需原列元数据才能安全应用列规则"
				return nil
			}
			output := field.AsName.O
			if output == "" {
				if col, ok := field.Expr.(*ast.ColumnNameExpr); ok {
					output = col.Name.Name.O
				}
			}
			internalOutput := output
			if len(explicit) > 0 {
				output = explicit[i].O
			}
			if output == "" {
				b.reason = "视图表达式输出需显式别名或列列表"
				return nil
			}
			target := output
			if publicName != "" {
				if !b.allowed(publicSchema, publicName, output) {
					b.reason = "视图输出引用被过滤列"
					return nil
				}
				target = b.mapping.columnTarget(publicSchema, publicName, output, b.sensitive)
			}
			if _, exists := s.aliases[foldKey(output)]; exists {
				b.reason = "视图输出列名重复"
				return nil
			}
			s.aliases[foldKey(output)] = target
			if len(explicit) == 0 {
				field.AsName = ast.NewCIStr(target)
			} else {
				if internalOutput != "" {
					field.AsName = ast.NewCIStr(internalOutput)
				}
				delete(s.aliases, foldKey(output))
				if internalOutput != "" {
					s.aliases[foldKey(internalOutput)] = internalOutput
				}
			}
			out = append(out, queryOutput{output, target})
			s.expression(field.Expr, false)
		}
		s.expression(n.Where, false)
		if n.GroupBy != nil {
			s.expression(n.GroupBy, true)
		}
		if n.Having != nil {
			s.expression(n.Having, true)
		}
		if n.OrderBy != nil {
			s.expression(n.OrderBy, true)
		}
		for i := range n.WindowSpecs {
			s.expression(&n.WindowSpecs[i], false)
		}
		if n.Limit != nil {
			s.expression(n.Limit, false)
		}
		return out
	case *ast.SetOprStmt:
		b.with(n.With, parent, ctes)
		out := b.query(n.SelectList, parent, ctes, publicSchema, publicName, explicit)
		b.setOrder(n.OrderBy, n.Limit, out)
		return out
	case *ast.SetOprSelectList:
		b.with(n.With, parent, ctes)
		var out []queryOutput
		for i, part := range n.Selects {
			schema, name, cols := "", "", []ast.CIStr(nil)
			if i == 0 {
				schema, name, cols = publicSchema, publicName, explicit
			}
			next := b.query(part, parent, ctes, schema, name, cols)
			if i == 0 {
				out = next
			} else if len(next) != len(out) {
				b.reason = "视图集合操作的输出列数不一致"
				return nil
			}
		}
		b.setOrder(n.OrderBy, n.Limit, out)
		return out
	case *ast.SubqueryExpr:
		return b.query(n.Query, parent, ctes, publicSchema, publicName, explicit)
	default:
		b.reason = fmt.Sprintf("视图查询作用域不能绑定：%T", node)
		return nil
	}
}
func (b *queryBinder) with(with *ast.WithClause, parent *queryScope, ctes map[string][]queryOutput) {
	if with == nil {
		return
	}
	for _, cte := range with.CTEs {
		key := cte.Name.O
		if !b.sensitive {
			key = foldKey(key)
		}
		if with.IsRecursive || cte.IsRecursive {
			b.reason = "递归 CTE 的列依赖尚不能安全绑定"
			return
		}
		ctes[key] = b.query(cte.Query, parent, ctes, "", "", cte.ColNameList)
	}
}
func (b *queryBinder) setOrder(order *ast.OrderByClause, limit *ast.Limit, out []queryOutput) {
	s := &queryScope{binder: b, aliases: map[string]string{}}
	for _, col := range out {
		s.aliases[foldKey(col.source)] = col.target
	}
	if order != nil {
		s.expression(order, true)
	}
	if limit != nil {
		s.expression(limit, false)
	}
}
func (s *queryScope) collect(node ast.ResultSetNode) {
	b := s.binder
	if b.reason != "" {
		return
	}
	switch n := node.(type) {
	case nil:
	case *ast.Join:
		if n.NaturalJoin || len(n.Using) > 0 {
			b.reason = "视图 NATURAL/USING 连接需原列依赖才能安全绑定"
			return
		}
		s.collect(n.Left)
		s.collect(n.Right)
		if n.On != nil {
			s.expression(n.On, false)
		}
	case *ast.TableSource:
		t := queryTable{alias: n.AsName.O}
		if physical, ok := n.Source.(*ast.TableName); ok {
			key := physical.Name.O
			if !b.sensitive {
				key = foldKey(key)
			}
			if outputs, exists := s.ctes[key]; physical.Schema.O == "" && exists {
				t.Table = physical.Name.O
				t.derived = true
				t.outputs = outputs
			} else {
				t.Schema, t.Table = physical.Schema.O, physical.Name.O
				if t.Schema == "" {
					t.Schema = b.schema
				}
				physical.Schema = ast.NewCIStr(b.mapping.schemaTarget(t.Schema, b.sensitive))
				physical.Name = ast.NewCIStr(b.mapping.tableTarget(t.Schema, t.Table, b.sensitive))
			}
		} else {
			if t.alias == "" {
				b.reason = "视图派生表缺少别名"
				return
			}
			t.derived = true
			t.outputs = b.query(n.Source, nil, s.ctes, "", "", nil)
		}
		for _, previous := range s.tables {
			left, right := previous.alias, t.alias
			if left == "" {
				left = previous.Table
			}
			if right == "" {
				right = t.Table
			}
			if foldEqual(left, right, b.sensitive) {
				b.reason = "视图来源表别名重复"
				return
			}
		}
		s.tables = append(s.tables, t)
	default:
		b.reason = "视图表作用域暂不能安全绑定"
	}
}
func (s *queryScope) expression(node ast.Node, alias bool) {
	if node == nil || s.binder.reason != "" {
		return
	}
	old := s.aliasClause
	s.aliasClause = alias
	node.Accept(s)
	s.aliasClause = old
}
func (s *queryScope) bind(c *ast.ColumnName) (queryTable, bool) {
	if c.Schema.O == "" && c.Table.O == "" && s.parent != nil {
		for _, table := range s.tables {
			if !table.derived {
				s.binder.reason = "关联子查询未限定列需原列元数据才能安全绑定"
				return queryTable{}, false
			}
		}
	}
	var matched []queryTable
	for _, table := range s.tables {
		if c.Schema.O != "" {
			if !table.derived && table.alias == "" && foldEqual(c.Schema.O, table.Schema, s.binder.sensitive) && foldEqual(c.Table.O, table.Table, s.binder.sensitive) {
				matched = append(matched, table)
			}
		} else if c.Table.O == "" || table.alias != "" && foldEqual(c.Table.O, table.alias, s.binder.sensitive) || table.alias == "" && foldEqual(c.Table.O, table.Table, s.binder.sensitive) {
			if c.Table.O == "" && table.derived {
				found := false
				for _, col := range table.outputs {
					found = found || foldKey(col.source) == foldKey(c.Name.O)
				}
				if !found {
					continue
				}
			}
			matched = append(matched, table)
		}
	}
	if len(matched) == 1 {
		return matched[0], true
	}
	if len(matched) == 0 && s.parent != nil {
		return s.parent.bind(c)
	}
	s.binder.reason = "视图列引用无法唯一绑定来源表"
	return queryTable{}, false
}
func (s *queryScope) Enter(node ast.Node) (ast.Node, bool) {
	b := s.binder
	if b.reason != "" {
		return node, true
	}
	switch n := node.(type) {
	case *ast.SubqueryExpr:
		b.query(n.Query, s, s.ctes, "", "", nil)
		return node, true
	case *ast.ColumnName:
		if s.aliasClause && n.Schema.O == "" && n.Table.O == "" {
			if alias, ok := s.aliases[foldKey(n.Name.O)]; ok {
				for _, table := range s.tables {
					if !table.derived && b.mapping.columnTarget(table.Schema, table.Table, n.Name.O, b.sensitive) != n.Name.O {
						b.reason = "视图输出别名与来源列映射存在歧义"
						return node, true
					}
				}
				n.Name = ast.NewCIStr(alias)
				return node, true
			}
		}
		table, ok := s.bind(n)
		if !ok {
			return node, true
		}
		if table.derived {
			for _, col := range table.outputs {
				if foldKey(col.source) == foldKey(n.Name.O) {
					n.Name = ast.NewCIStr(col.target)
					return node, true
				}
			}
			b.reason = "视图引用未知派生输出列"
			return node, true
		}
		if !b.allowed(table.Schema, table.Table, n.Name.O) {
			b.reason = "视图表达式引用被过滤列"
			return node, true
		}
		n.Name = ast.NewCIStr(b.mapping.columnTarget(table.Schema, table.Table, n.Name.O, b.sensitive))
		if n.Table.O != "" && table.alias == "" {
			n.Table = ast.NewCIStr(b.mapping.tableTarget(table.Schema, table.Table, b.sensitive))
			if n.Schema.O != "" {
				n.Schema = ast.NewCIStr(b.mapping.schemaTarget(table.Schema, b.sensitive))
			}
		}
	case *ast.FuncCallExpr:
		if n.Schema.O != "" {
			n.Schema = ast.NewCIStr(b.mapping.schemaTarget(n.Schema.O, b.sensitive))
		}
	}
	return node, false
}
func (s *queryScope) Leave(node ast.Node) (ast.Node, bool) { return node, true }
