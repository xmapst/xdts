package schemamig

import "github.com/pingcap/tidb/pkg/parser/ast"

// renameDefinitionColumns visits one table definition. References use the
// referenced table's column rules, never the owning table's column rules.
type definitionColumns struct {
	mapping       Mapping
	schema, table string
	sensitive     bool
}

func (v *definitionColumns) Enter(n ast.Node) (ast.Node, bool) {
	switch x := n.(type) {
	case *ast.ReferenceDef:
		if x.Table == nil {
			return n, true
		}
		schema := x.Table.Schema.O
		if schema == "" {
			schema = v.schema
		}
		for _, key := range x.IndexPartSpecifications {
			if key.Column != nil {
				key.Column.Name = ast.NewCIStr(v.mapping.columnTarget(schema, x.Table.Name.O, key.Column.Name.O, v.sensitive))
			}
		}
		x.Table.Name = ast.NewCIStr(v.mapping.tableTarget(schema, x.Table.Name.O, v.sensitive))
		x.Table.Schema = ast.NewCIStr(v.mapping.schemaTarget(schema, v.sensitive))
		return n, true
	case *ast.ColumnName:
		x.Name = ast.NewCIStr(v.mapping.columnTarget(v.schema, v.table, x.Name.O, v.sensitive))
	}
	return n, false
}
func (v *definitionColumns) Leave(n ast.Node) (ast.Node, bool) { return n, true }
