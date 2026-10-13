package schemamig

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// ForeignKey is one explicitly named table constraint, restored as a single
// ALTER statement. Table identity is separate from its schema-wide symbol.
type ForeignKey struct {
	Schema    string
	Table     string
	Name      string
	Reference SchemaTable
	DDL       string
}

func foreignKeyFromConstraint(schema, table string, c *ast.Constraint) (ForeignKey, error) {
	if c == nil || c.Tp != ast.ConstraintForeignKey || c.Name == "" || c.Refer == nil || c.Refer.Table == nil || len(c.Keys) == 0 || len(c.Keys) != len(c.Refer.IndexPartSpecifications) {
		return ForeignKey{}, fmt.Errorf("invalid named foreign key")
	}
	refSchema := c.Refer.Table.Schema.O
	if refSchema == "" {
		refSchema = schema
	}
	c.Refer.Table.Schema = ast.NewCIStr(refSchema)
	stmt := &ast.AlterTableStmt{Table: &ast.TableName{Schema: ast.NewCIStr(schema), Name: ast.NewCIStr(table)}, Specs: []*ast.AlterTableSpec{{Tp: ast.AlterTableAddConstraint, Constraint: c}}}
	ddl, err := restoreTiDB(stmt)
	return ForeignKey{Schema: schema, Table: table, Name: c.Name, Reference: SchemaTable{Schema: refSchema, Table: c.Refer.Table.Name.O}, DDL: restoreUTF8MB3Markers(ddl)}, err
}

// SplitTableForeignKeys preserves column markers while removing only foreign
// keys. Ordinary supporting indexes stay on the base table.
func SplitTableForeignKeys(schema, ddl string) (string, []ForeignKey, error) {
	marked, markers, ok := stripInvisibleColumnMarkers(stripUTF8MB3Markers(ddl))
	if !ok {
		return "", nil, fmt.Errorf("cannot locate invisible columns")
	}
	n, err := parseOneTiDBStmt(marked)
	if err != nil {
		return "", nil, err
	}
	ct, ok := n.(*ast.CreateTableStmt)
	if !ok || ct.Select != nil || ct.ReferTable != nil {
		return "", nil, ErrDDLKindMismatch
	}
	if ct.Table.Schema.O != "" {
		schema = ct.Table.Schema.O
	}
	if schema == "" {
		return "", nil, fmt.Errorf("foreign key table schema required")
	}
	ct.Table.Schema = ast.NewCIStr(schema)
	keys := []ForeignKey{}
	kept := make([]*ast.Constraint, 0, len(ct.Constraints))
	names := map[string]bool{}
	for _, c := range ct.Constraints {
		if c.Tp != ast.ConstraintForeignKey {
			kept = append(kept, c)
			continue
		}
		fk, err := foreignKeyFromConstraint(schema, ct.Table.Name.O, c)
		if err != nil {
			return "", nil, err
		}
		if names[strings.ToLower(fk.Name)] {
			return "", nil, fmt.Errorf("duplicate foreign key symbol")
		}
		names[strings.ToLower(fk.Name)] = true
		keys = append(keys, fk)
	}
	ct.Constraints = kept
	base, err := restoreTiDB(ct)
	if err != nil {
		return "", nil, err
	}
	base, err = reinsertInvisibleColumnMarkers(base, ct.Cols, markers)
	return restoreUTF8MB3Markers(base), keys, err
}

// ForeignKeyIdentity accepts exactly one ALTER TABLE ADD named FOREIGN KEY.
// It cannot carry an unrelated ALTER specification or another statement.
func ForeignKeyIdentity(schema, ddl string) (ForeignKey, error) {
	n, err := parseOneTiDBStmt(stripUTF8MB3Markers(ddl))
	if err != nil {
		return ForeignKey{}, err
	}
	stmt, ok := n.(*ast.AlterTableStmt)
	if !ok || len(stmt.Specs) != 1 || stmt.Specs[0].Tp != ast.AlterTableAddConstraint {
		return ForeignKey{}, ErrDDLKindMismatch
	}
	if stmt.Table.Schema.O != "" {
		schema = stmt.Table.Schema.O
	}
	if schema == "" {
		return ForeignKey{}, fmt.Errorf("foreign key schema required")
	}
	return foreignKeyFromConstraint(schema, stmt.Table.Name.O, stmt.Specs[0].Constraint)
}

func CompareForeignKeys(expected, actual ForeignKey) []Difference {
	if expected.DDL == actual.DDL {
		return nil
	}
	return []Difference{{Path: "foreign_keys." + expected.Name, Expected: expected.DDL, Actual: actual.DDL}}
}
