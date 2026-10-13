package schemamig

import "github.com/pingcap/tidb/pkg/parser/ast"

// columnPolicyMapping creates private projection maps from AST column names;
// SQL literals and index names never participate. Referenced columns bind to
// the referenced table rather than the table being altered.
func columnPolicyMapping(mapping Mapping, node ast.Node, schema, table string, sensitive bool) Mapping {
	if mapping.ColumnFilter == nil {
		return mapping
	}
	copy := mapping
	copy.DropColumns = make(map[SchemaTable][]string, len(mapping.DropColumns)+1)
	for ref, columns := range mapping.DropColumns {
		copy.DropColumns[ref] = append([]string(nil), columns...)
	}
	v := &columnPolicyNames{mapping: &copy, schema: schema, table: table, sensitive: sensitive}
	node.Accept(v)
	return copy
}

type columnPolicyNames struct {
	mapping       *Mapping
	schema, table string
	sensitive     bool
}

func (v *columnPolicyNames) add(schema, table, column string) {
	if v.mapping.ColumnFilter.Allow(schema, table, column) {
		return
	}
	ref := SchemaTable{Schema: schema, Table: table}
	for existing := range v.mapping.DropColumns {
		if foldEqual(existing.Schema, schema, v.sensitive) && foldEqual(existing.Table, table, v.sensitive) {
			ref = existing
			break
		}
	}
	for _, name := range v.mapping.DropColumns[ref] {
		if foldEqual(name, column, false) {
			return
		}
	}
	v.mapping.DropColumns[ref] = append(v.mapping.DropColumns[ref], column)
}

func (v *columnPolicyNames) Enter(n ast.Node) (ast.Node, bool) {
	switch x := n.(type) {
	case *ast.ReferenceDef:
		if x.Table != nil {
			schema := x.Table.Schema.O
			if schema == "" {
				schema = v.schema
			}
			for _, key := range x.IndexPartSpecifications {
				if key.Column != nil {
					v.add(schema, x.Table.Name.O, key.Column.Name.O)
				}
			}
		}
		return n, true
	case *ast.ColumnName:
		v.add(v.schema, v.table, x.Name.O)
	}
	return n, false
}
func (v *columnPolicyNames) Leave(n ast.Node) (ast.Node, bool) { return n, true }
