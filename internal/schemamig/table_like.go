package schemamig

import "github.com/pingcap/tidb/pkg/parser/ast"

// LIKE clones the already projected target table. This is safe only when both
// physical names resolve to the same logical source table (online tool copies),
// or neither table has any column projection or mapping.
func rewriteLikeTable(req Request, create *ast.CreateTableStmt, schema, table string) (Result, error) {
	referenceSchema := create.ReferTable.Schema.O
	if referenceSchema == "" {
		referenceSchema = req.DefaultSchema
	}
	referenceTable := create.ReferTable.Name.O
	logical := func(db, name string) string {
		if req.Mapping.TableAliases != nil {
			if original, _, found := req.Mapping.TableAliases.ResolveTable(db, name); found {
				return original
			}
		}
		return name
	}
	same := foldEqual(schema, referenceSchema, req.Options.SourceCaseSensitive) && foldEqual(logical(schema, table), logical(referenceSchema, referenceTable), req.Options.SourceCaseSensitive)
	if !same {
		projected := func(db, name string) bool {
			if len(req.Mapping.dropColumnSet(db, name, req.Options.SourceCaseSensitive)) > 0 || req.Mapping.ColumnFilter != nil && req.Mapping.ColumnFilter.FiltersTable(db, name) {
				return true
			}
			for ref, columns := range req.Mapping.Columns {
				if len(columns) > 0 && foldEqual(ref.Schema, db, req.Options.SourceCaseSensitive) && foldEqual(ref.Table, name, req.Options.SourceCaseSensitive) {
					return true
				}
			}
			return false
		}
		if projected(schema, table) || projected(referenceSchema, referenceTable) {
			return blocked("CREATE TABLE LIKE 的两表列策略不同，需原物理定义", req.DDL), nil
		}
	}
	create.Table.Schema = ast.NewCIStr(req.Mapping.schemaTarget(schema, req.Options.SourceCaseSensitive))
	create.Table.Name = ast.NewCIStr(req.Mapping.tableTarget(schema, table, req.Options.SourceCaseSensitive))
	create.ReferTable.Schema = ast.NewCIStr(req.Mapping.schemaTarget(referenceSchema, req.Options.SourceCaseSensitive))
	create.ReferTable.Name = ast.NewCIStr(req.Mapping.tableTarget(referenceSchema, referenceTable, req.Options.SourceCaseSensitive))
	ddl, err := restoreTiDB(create)
	if err != nil {
		return blocked(err.Error(), req.DDL), nil
	}
	return Result{DDL: ddl}, nil
}
