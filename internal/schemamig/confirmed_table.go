package schemamig

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// ConfirmedTableDefinitionEqual checks a complete SHOW CREATE definition.
// Only the row-dependent AUTO_INCREMENT counter is ignored. Unlike write
// compatibility, extra columns, keys, storage options and constraints matter.
func ConfirmedTableDefinitionEqual(expected, actual string) (bool, error) {
	normalize := func(ddl string) (string, error) {
		marked, markers, ok := stripInvisibleColumnMarkers(stripUTF8MB3Markers(ddl))
		if !ok {
			return "", fmt.Errorf("invalid invisible markers")
		}
		n, err := parseOneTiDBStmt(marked)
		if err != nil {
			return "", err
		}
		table, ok := n.(*ast.CreateTableStmt)
		if !ok || table.Select != nil || table.ReferTable != nil || table.TemporaryKeyword != ast.TemporaryNone {
			return "", ErrDDLKindMismatch
		}
		table.IfNotExists = false
		table.Table.Schema = ast.NewCIStr("")
		opts := table.Options[:0]
		for _, option := range table.Options {
			if option.Tp != ast.TableOptionAutoIncrement {
				opts = append(opts, option)
			}
		}
		table.Options = opts
		text, err := restoreTiDB(table)
		if err != nil {
			return "", err
		}
		text, err = reinsertInvisibleColumnMarkers(text, table.Cols, markers)
		return restoreUTF8MB3Markers(text), err
	}
	a, err := normalize(expected)
	if err != nil {
		return false, err
	}
	b, err := normalize(actual)
	return a == b, err
}
