package schema

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/xmapst/xdts/internal/schemamig"
)

type OriginalTableDependencies struct {
	Schema, Table string
	Dependencies  schemamig.TableDependencies
}

// DependenciesBeforeDDL is called by the session owner before ApplyDDL.
// Missing definitions remain absent; live post-DDL catalogs cannot fill them.
func (t *Tracker) DependenciesBeforeDDL(defaultSchema, ddl string) []OriginalTableDependencies {
	stmt, err := parseOneStmt(ddl)
	if err != nil {
		return nil
	}
	var name *ast.TableName
	switch s := stmt.(type) {
	case *ast.AlterTableStmt:
		name = s.Table
	case *ast.CreateIndexStmt:
		name = s.Table
	case *ast.DropIndexStmt:
		name = s.Table
	default:
		return nil
	}
	schemaName, tableName := resolveTableName(defaultSchema, name)
	table, known := t.Lookup(schemaName, tableName)
	if !known || !table.DDLDependencies.Complete {
		return nil
	}
	return []OriginalTableDependencies{{Schema: schemaName, Table: tableName, Dependencies: table.DDLDependencies.Clone()}}
}
