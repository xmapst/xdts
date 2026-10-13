package schemamig

import (
	"errors"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// ViewPlaceholder retains name/security options and replaces only the SELECT.
// Column names come from actual source metadata, never guessed from SELECT *.
func ViewPlaceholder(ddl string, columns []string) (string, string, error) {
	n, err := parseOneTiDBStmt(ddl)
	if err != nil {
		return "", "", err
	}
	v, ok := n.(*ast.CreateViewStmt)
	if !ok || len(columns) == 0 || len(columns) > 4096 {
		return "", "", errors.New("view placeholder requires actual columns")
	}
	v.OrReplace = true
	final, err := restoreMySQLView(v, ddl)
	if err != nil {
		return "", "", err
	}
	seen := map[string]bool{}
	var fields []string
	for _, name := range columns {
		if name == "" || !utf8.ValidString(name) || utf8.RuneCountInString(name) > 64 || strings.ContainsRune(name, 0) || seen[strings.ToLower(name)] {
			return "", "", errors.New("invalid view placeholder column")
		}
		seen[strings.ToLower(name)] = true
		fields = append(fields, "1 AS `"+strings.ReplaceAll(name, "`", "``")+"`")
	}
	selectNode, err := parseOneTiDBStmt("SELECT " + strings.Join(fields, ",") + " WHERE (0 <> 0)")
	if err != nil {
		return "", "", err
	}
	v.Select = selectNode
	v.Cols = nil
	v.OrReplace = false
	v.Algorithm = ast.AlgorithmUndefined
	v.CheckOption = ast.CheckOptionCascaded
	placeholder, err := restoreTiDB(v)
	return placeholder, final, err
}

// ViewFunctionReferences enumerates only schema-qualified or generic calls.
// The plan intersects generic names with selected stored functions.
type ViewFunctionReference struct {
	SchemaTable
	Qualified bool
}

func ViewFunctionReferences(schema, ddl string) ([]ViewFunctionReference, error) {
	n, err := parseOneTiDBStmt(ddl)
	if err != nil {
		return nil, err
	}
	v, ok := n.(*ast.CreateViewStmt)
	if !ok {
		return nil, ErrDDLKindMismatch
	}
	visitor := &viewFunctions{schema: schema, refs: map[ViewFunctionReference]bool{}}
	v.Select.Accept(visitor)
	var out []ViewFunctionReference
	for ref := range visitor.refs {
		out = append(out, ref)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Schema != out[j].Schema {
			return out[i].Schema < out[j].Schema
		}
		if out[i].Table != out[j].Table {
			return out[i].Table < out[j].Table
		}
		return !out[i].Qualified && out[j].Qualified
	})
	return out, nil
}

type viewFunctions struct {
	schema string
	refs   map[ViewFunctionReference]bool
}

func (v *viewFunctions) Enter(n ast.Node) (ast.Node, bool) {
	if f, ok := n.(*ast.FuncCallExpr); ok && (f.Schema.O != "" || f.Tp == ast.FuncCallExprTypeGeneric) {
		schema := f.Schema.O
		if schema == "" {
			schema = v.schema
		}
		v.refs[ViewFunctionReference{Schema: schema, Table: f.FnName.O, Qualified: f.Schema.O != ""}] = true
	}
	return n, false
}
func (v *viewFunctions) Leave(n ast.Node) (ast.Node, bool) { return n, true }

func ViewIsReplacement(ddl string) (bool, error) {
	n, err := parseOneTiDBStmt(ddl)
	if err != nil {
		return false, err
	}
	v, ok := n.(*ast.CreateViewStmt)
	if !ok {
		return false, ErrDDLKindMismatch
	}
	return v.OrReplace, nil
}
