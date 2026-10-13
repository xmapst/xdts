package schemamig

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// ParseTableDefinition parses a CREATE solely for structural metadata. TiDB
// does not support MySQL column visibility; removing that attribute here keeps
// physical columns, generated expressions and keys. The result must never be
// restored as executable migration SQL, whose fidelity belongs to Rewrite.
func ParseTableDefinition(ddl string) (*ast.CreateTableStmt, error) {
	stmt, err := parseOneTiDBStmt(ddl)
	if err != nil {
		stmt, err = parseOneTiDBStmt(metadataColumnVisibility(ddl))
	}
	if err != nil {
		return nil, err
	}
	create, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		return nil, ErrDDLKindMismatch
	}
	return create, nil
}

// Attribute removal is lexical: quoted names/values and ordinary comments
// remain byte-for-byte, including literal words and parentheses.
func metadataColumnVisibility(ddl string) string {
	var out strings.Builder
	for pos := 0; pos < len(ddl); {
		start := pos
		c := ddl[pos]
		if c == '\'' || c == '"' || c == '`' {
			pos++
			for pos < len(ddl) {
				if ddl[pos] == '\\' && c != '`' && pos+1 < len(ddl) {
					pos += 2
					continue
				}
				if ddl[pos] == c {
					pos++
					if pos < len(ddl) && ddl[pos] == c {
						pos++
						continue
					}
					break
				}
				pos++
			}
		} else if strings.HasPrefix(ddl[pos:], "/*") {
			end := strings.Index(ddl[pos+2:], "*/")
			if end < 0 {
				out.WriteString(ddl[pos:])
				break
			}
			pos += end + 4
			comment := ddl[start:pos]
			if loc := reInvisibleColumnMarker.FindStringIndex(comment); loc != nil && loc[0] == 0 && loc[1] == len(comment) {
				out.WriteByte(' ')
				continue
			}
		} else if c == '#' || strings.HasPrefix(ddl[pos:], "-- ") {
			end := strings.IndexByte(ddl[pos:], '\n')
			if end < 0 {
				out.WriteString(ddl[pos:])
				break
			}
			pos += end + 1
		} else if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' {
			pos++
			for pos < len(ddl) && (ddl[pos] >= 'a' && ddl[pos] <= 'z' || ddl[pos] >= 'A' && ddl[pos] <= 'Z' || ddl[pos] >= '0' && ddl[pos] <= '9' || ddl[pos] == '_' || ddl[pos] == '$') {
				pos++
			}
			if strings.EqualFold(ddl[start:pos], "INVISIBLE") {
				out.WriteByte(' ')
				continue
			}
		} else {
			pos++
		}
		out.WriteString(ddl[start:pos])
	}
	return out.String()
}
