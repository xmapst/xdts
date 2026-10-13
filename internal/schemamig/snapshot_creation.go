package schemamig

import "github.com/pingcap/tidb/pkg/parser/ast"

// SnapshotCreation accepts only self-contained physical table definitions.
// LIKE and SELECT require state that is unavailable before incremental replay.
func SnapshotCreation(schema, ddl string) (SchemaTable, bool) {
	stmt, err := parseOneTiDBStmt(ddl)
	if err != nil {
		return SchemaTable{}, false
	}
	create, ok := stmt.(*ast.CreateTableStmt)
	if !ok || create.TemporaryKeyword != ast.TemporaryNone || create.Select != nil || create.ReferTable != nil || create.Table == nil || len(create.Cols) == 0 {
		return SchemaTable{}, false
	}
	if create.Table.Schema.O != "" {
		schema = create.Table.Schema.O
	}
	if schema == "" || create.Table.Name.O == "" {
		return SchemaTable{}, false
	}
	return SchemaTable{Schema: schema, Table: create.Table.Name.O}, true
}
