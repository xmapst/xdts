package schemamig

import (
	"maps"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// CTE visibility follows lexical scope and declaration order. A same-named
// physical table in an earlier/inner query must not disappear from the plan.
type scopedViewReferences struct {
	schema string
	refs   map[SchemaTable]bool
	ctes   map[string]bool
	stack  []map[string]bool
}

func (v *scopedViewReferences) Enter(node ast.Node) (ast.Node, bool) {
	var with *ast.WithClause
	scope := false
	switch n := node.(type) {
	case *ast.SelectStmt:
		scope = true
		with = n.With
	case *ast.SetOprStmt:
		scope = true
		with = n.With
	case *ast.SetOprSelectList:
		scope = true
		with = n.With
	case *ast.WithClause:
		return node, true
	case *ast.TableName:
		if n.Schema.O == "" && v.ctes[foldKey(n.Name.O)] {
			return node, true
		}
		schema := n.Schema.O
		if schema == "" {
			schema = v.schema
		}
		v.refs[SchemaTable{schema, n.Name.O}] = true
	}
	if scope {
		v.stack = append(v.stack, v.ctes)
		v.ctes = maps.Clone(v.ctes)
		if v.ctes == nil {
			v.ctes = map[string]bool{}
		}
		if with != nil {
			for _, cte := range with.CTEs {
				key := foldKey(cte.Name.O)
				if with.IsRecursive || cte.IsRecursive {
					v.ctes[key] = true
				}
				cte.Query.Accept(v)
				v.ctes[key] = true
			}
		}
	}
	return node, false
}
func (v *scopedViewReferences) Leave(node ast.Node) (ast.Node, bool) {
	switch node.(type) {
	case *ast.SelectStmt, *ast.SetOprStmt, *ast.SetOprSelectList:
		v.ctes = v.stack[len(v.stack)-1]
		v.stack = v.stack[:len(v.stack)-1]
	}
	return node, true
}
