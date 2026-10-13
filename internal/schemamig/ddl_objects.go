package schemamig

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/bytebase/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// DDLObjects returns authoritative primary objects touched by a DDL. References
// in expressions or foreign keys do not change the scope of the operation.
// known=false means the caller cannot decide scope from this parser.
func DDLObjects(schema, ddl string) (objects []SchemaTable, known bool) {
	stmt, err := parseOneTiDBStmt(stripUTF8MB3Markers(ddl))
	if err != nil {
		marked, _, ok := stripInvisibleColumnMarkers(stripUTF8MB3Markers(ddl))
		if !ok {
			return nil, false
		}
		stmt, err = parseOneTiDBStmt(marked)
	}
	if err != nil {
		return routineDDLObjects(schema, ddl)
	}
	if _, ok := stmt.(*ast.ProcedureInfo); ok {
		return routineDDLObjects(schema, ddl)
	}
	add := func(t *ast.TableName) {
		if t == nil {
			return
		}
		db := t.Schema.O
		if db == "" {
			db = schema
		}
		objects = append(objects, SchemaTable{Schema: db, Table: t.Name.O})
	}
	switch s := stmt.(type) {
	case *ast.CreateTableStmt:
		add(s.Table)
	case *ast.CreateViewStmt:
		add(s.ViewName)
	case *ast.AlterTableStmt:
		add(s.Table)
		for _, spec := range s.Specs {
			if spec.NewTable != nil {
				add(spec.NewTable)
			}
		}
	case *ast.DropTableStmt:
		for _, table := range s.Tables {
			add(table)
		}
	case *ast.TruncateTableStmt:
		add(s.Table)
	case *ast.CreateIndexStmt:
		add(s.Table)
	case *ast.DropIndexStmt:
		add(s.Table)
	case *ast.RenameTableStmt:
		for _, pair := range s.TableToTables {
			add(pair.OldTable)
			add(pair.NewTable)
		}
	case *ast.CreateDatabaseStmt:
		objects = append(objects, SchemaTable{Schema: s.Name.O})
	case *ast.DropDatabaseStmt:
		objects = append(objects, SchemaTable{Schema: s.Name.O})
	case *ast.AlterDatabaseStmt:
		db := s.Name.O
		if s.AlterDefaultDatabase {
			db = schema
		}
		objects = append(objects, SchemaTable{Schema: db})
	case *ast.DropProcedureStmt:
		add(s.ProcedureName)
	default:
		return nil, false
	}
	return objects, len(objects) > 0
}

type routineObjects struct {
	*mysql.BaseMySQLParserListener
	schema  string
	objects []SchemaTable
}

func routineDDLObjects(schema, ddl string) ([]SchemaTable, bool) {
	p, err := parseRoutine(ddl)
	if err != nil || len(p.tree.AllQuery()) != 1 {
		return nil, false
	}
	f := &routineObjects{BaseMySQLParserListener: &mysql.BaseMySQLParserListener{}, schema: schema}
	antlr.ParseTreeWalkerDefault.Walk(f, p.tree)
	return f.objects, len(f.objects) > 0
}
func (f *routineObjects) add(q mysql.IQualifiedIdentifierContext) {
	if q == nil || q.Identifier() == nil {
		return
	}
	schema, name := f.schema, unquoteMySQLIdent(q.Identifier().GetText())
	if dot := q.DotIdentifier(); dot != nil && dot.Identifier() != nil {
		schema = name
		name = unquoteMySQLIdent(dot.Identifier().GetText())
	}
	f.objects = append(f.objects, SchemaTable{Schema: schema, Table: name})
}
func (f *routineObjects) EnterCreateTrigger(c *mysql.CreateTriggerContext) {
	f.add(c.TableRef().QualifiedIdentifier())
}
func (f *routineObjects) EnterCreateProcedure(c *mysql.CreateProcedureContext) {
	f.add(c.ProcedureName().QualifiedIdentifier())
}
func (f *routineObjects) EnterCreateFunction(c *mysql.CreateFunctionContext) {
	f.add(c.FunctionName().QualifiedIdentifier())
}
func (f *routineObjects) EnterCreateEvent(c *mysql.CreateEventContext) {
	f.add(c.EventName().QualifiedIdentifier())
}
func (f *routineObjects) EnterDropEvent(c *mysql.DropEventContext) {
	f.add(c.EventRef().QualifiedIdentifier())
}
func (f *routineObjects) EnterDropFunction(c *mysql.DropFunctionContext) {
	f.add(c.FunctionRef().QualifiedIdentifier())
}
func (f *routineObjects) EnterDropTrigger(c *mysql.DropTriggerContext) {
	f.add(c.TriggerRef().QualifiedIdentifier())
}
func (f *routineObjects) EnterAlterProcedure(c *mysql.AlterProcedureContext) {
	f.add(c.ProcedureRef().QualifiedIdentifier())
}
func (f *routineObjects) EnterAlterFunction(c *mysql.AlterFunctionContext) {
	f.add(c.FunctionRef().QualifiedIdentifier())
}
func (f *routineObjects) EnterAlterView(c *mysql.AlterViewContext) {
	f.add(c.ViewRef().QualifiedIdentifier())
}
func (f *routineObjects) EnterAlterEvent(c *mysql.AlterEventContext) {
	f.add(c.EventRef().QualifiedIdentifier())
	if rename := c.Identifier(); rename != nil && len(f.objects) > 0 {
		f.objects = append(f.objects, SchemaTable{Schema: f.schema, Table: unquoteMySQLIdent(rename.GetText())})
	}
}
