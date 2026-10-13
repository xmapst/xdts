package schemamig

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// InitializationTableProperties reads explicit columns and complete keys from
// CREATE TABLE. Nullable, prefix and expression-only unique keys cannot locate
// every row for the snapshot reader.
func InitializationTableProperties(ddl string) (usableKey bool, engine string, err error) {
	marked, _, ok := stripInvisibleColumnMarkers(stripUTF8MB3Markers(ddl))
	if !ok {
		return false, "", ErrDDLKindMismatch
	}
	n, err := parseOneTiDBStmt(marked)
	if err != nil {
		return false, "", err
	}
	table, ok := n.(*ast.CreateTableStmt)
	if !ok || len(table.Cols) == 0 || table.Select != nil || table.ReferTable != nil {
		return false, "", ErrDDLKindMismatch
	}
	nonnull := map[string]bool{}
	columns := map[string]bool{}
	for _, col := range table.Cols {
		key := strings.ToLower(col.Name.Name.O)
		if columns[key] {
			return false, "", ErrDDLKindMismatch
		}
		columns[key] = true
		for _, option := range col.Options {
			if option.Tp == ast.ColumnOptionNotNull || option.Tp == ast.ColumnOptionPrimaryKey {
				nonnull[key] = true
			}
			if option.Tp == ast.ColumnOptionPrimaryKey {
				usableKey = true
			}
		}
		for _, option := range col.Options {
			if option.Tp == ast.ColumnOptionUniqKey && nonnull[key] {
				usableKey = true
			}
		}
	}
	for _, constraint := range table.Constraints {
		if constraint.Tp != ast.ConstraintPrimaryKey && constraint.Tp != ast.ConstraintUniq && constraint.Tp != ast.ConstraintUniqKey && constraint.Tp != ast.ConstraintUniqIndex {
			continue
		}
		complete := len(constraint.Keys) != 0
		seen := map[string]bool{}
		for _, part := range constraint.Keys {
			if part.Column == nil || part.Length > 0 || part.Expr != nil {
				complete = false
				continue
			}
			key := strings.ToLower(part.Column.Name.O)
			if !columns[key] || seen[key] || (!nonnull[key] && constraint.Tp != ast.ConstraintPrimaryKey) {
				complete = false
			}
			seen[key] = true
		}
		usableKey = usableKey || complete
	}
	for _, option := range table.Options {
		if option.Tp == ast.TableOptionEngine {
			engine = option.StrValue
		}
	}
	return usableKey, engine, nil
}
