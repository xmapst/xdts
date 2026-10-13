package schemamig

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// WithTableCollation supplements SHOW CREATE's omitted table default using
// authoritative information_schema metadata. The caller retains the raw DDL.
func WithTableCollation(ddl, collation string) (string, error) {
	if collation == "" {
		return "", fmt.Errorf("table collation metadata required")
	}
	if len(collation) > 64 {
		return "", fmt.Errorf("invalid table collation")
	}
	for _, c := range collation {
		if c != '_' && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') {
			return "", fmt.Errorf("invalid table collation")
		}
	}
	marked, invisible, ok := stripInvisibleColumnMarkers(stripUTF8MB3Markers(ddl))
	if !ok {
		return "", fmt.Errorf("cannot locate invisible columns")
	}
	n, err := parseOneTiDBStmt(marked)
	if err != nil {
		return "", err
	}
	ct, ok := n.(*ast.CreateTableStmt)
	if !ok {
		return "", ErrDDLKindMismatch
	}
	charset := ""
	for _, opt := range ct.Options {
		if opt.Tp == ast.TableOptionCharset {
			charset = restoreUTF8MB3Markers(opt.StrValue)
		}
		if opt.Tp == ast.TableOptionCollate {
			if !strings.EqualFold(restoreUTF8MB3Markers(opt.StrValue), collation) {
				return "", fmt.Errorf("table collation metadata disagrees with definition")
			}
			return ddl, nil
		}
	}
	if charset == "" || !strings.HasPrefix(strings.ToLower(collation), strings.ToLower(charset)+"_") {
		return "", fmt.Errorf("table collation charset disagrees with definition")
	}
	ct.Options = append(ct.Options, &ast.TableOption{Tp: ast.TableOptionCollate, StrValue: collation})
	out, err := restoreTiDB(ct)
	if err != nil {
		return "", err
	}
	out, err = reinsertInvisibleColumnMarkers(out, ct.Cols, invisible)
	return restoreUTF8MB3Markers(out), err
}
