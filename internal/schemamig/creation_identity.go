package schemamig

import (
	"fmt"

	"github.com/bytebase/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// CreationIdentity is stricter than DDLObjects: only a single CREATE definition
// is accepted, and a trigger is identified by its own name rather than its table.
func CreationIdentity(defaultSchema, ddl string) (kind ObjectKind, schema, name string, err error) {
	marked, _, ok := stripInvisibleColumnMarkers(stripUTF8MB3Markers(ddl))
	if !ok {
		return 0, "", "", fmt.Errorf("invalid invisible markers")
	}
	n, parseErr := parseOneTiDBStmt(marked)
	// TiDB parses CREATE PROCEDURE too, but the routine identity and body
	// belong to the same ANTLR path as the established routine rewriter.
	if _, ok := n.(*ast.ProcedureInfo); ok {
		parseErr = ErrDDLKindMismatch
	}
	if parseErr == nil {
		var ref *ast.TableName
		switch x := n.(type) {
		case *ast.CreateDatabaseStmt:
			return ObjectUnknown, x.Name.O, x.Name.O, nil
		case *ast.CreateTableStmt:
			if x.Select != nil || x.ReferTable != nil || x.TemporaryKeyword != ast.TemporaryNone {
				return 0, "", "", ErrDDLKindMismatch
			}
			kind = ObjectTable
			ref = x.Table
		case *ast.CreateViewStmt:
			kind = ObjectView
			ref = x.ViewName
		default:
			return 0, "", "", ErrDDLKindMismatch
		}
		schema = ref.Schema.O
		if schema == "" {
			schema = defaultSchema
		}
		return kind, schema, ref.Name.O, nil
	}
	pr, err := parseRoutine(ddl)
	if err != nil {
		return 0, "", "", err
	}
	if len(pr.tree.AllQuery()) != 1 {
		return 0, "", "", ErrDDLKindMismatch
	}
	var q mysql.IQualifiedIdentifierContext
	switch {
	case findCreateProcedure(pr.tree) != nil:
		kind = ObjectProcedure
		q = findCreateProcedure(pr.tree).ProcedureName().QualifiedIdentifier()
	case findCreateFunction(pr.tree) != nil:
		kind = ObjectFunction
		q = findCreateFunction(pr.tree).FunctionName().QualifiedIdentifier()
	case findCreateTrigger(pr.tree) != nil:
		kind = ObjectTrigger
		q = findCreateTrigger(pr.tree).TriggerName().QualifiedIdentifier()
	case findCreateEvent(pr.tree) != nil:
		kind = ObjectEvent
		q = findCreateEvent(pr.tree).EventName().QualifiedIdentifier()
	default:
		return 0, "", "", ErrDDLKindMismatch
	}
	schema = defaultSchema
	name = unquoteMySQLIdent(q.Identifier().GetText())
	if dot := q.DotIdentifier(); dot != nil {
		schema = name
		name = unquoteMySQLIdent(dot.Identifier().GetText())
	}
	return kind, schema, name, nil
}
