package schemamig

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/bytebase/parser/mysql"
)

// ALTER changes routine characteristics, or an event's schedule and body.
// MySQL has no ALTER TRIGGER; replacing its body requires DROP and CREATE.
type routineAlterFinder struct {
	*mysql.BaseMySQLParserListener
	count int
	event *mysql.AlterEventContext
}

func findRoutineAlter(tree antlr.ParseTree) *routineAlterFinder {
	f := &routineAlterFinder{BaseMySQLParserListener: &mysql.BaseMySQLParserListener{}}
	antlr.ParseTreeWalkerDefault.Walk(f, tree)
	return f
}
func (f *routineAlterFinder) EnterAlterProcedure(*mysql.AlterProcedureContext) { f.count++ }
func (f *routineAlterFinder) EnterAlterFunction(*mysql.AlterFunctionContext)   { f.count++ }
func (f *routineAlterFinder) EnterAlterEvent(n *mysql.AlterEventContext) {
	f.count++
	f.event = n
}

func rewriteRoutineAlter(req IncrementalRequest, parsed *parsedRoutine, f *routineAlterFinder) (Result, error) {
	rw := antlr.NewTokenStreamRewriter(parsed.stream)
	if event := f.event; event != nil {
		if body := event.CompoundStatement(); body != nil && hasDynamicSQL(body) {
			return blocked(dynamicSQLBlockReason, req.DDL), nil
		}
		if reason, err := applyDefinerANTLR(event.DefinerClause(), req.Options, rw); err != nil {
			return Result{}, err
		} else if reason != "" {
			return blocked(reason, req.DDL), nil
		}
		if reason := rewriteRoutineColumns(event, req.Mapping, req.Options, rw); reason != "" {
			return blocked(reason, req.DDL), nil
		}
		if rename := event.Identifier(); rename != nil {
			// MySQL resolves an unqualified rename destination in the session's
			// default database, even when the original event is qualified.
			name := req.Mapping.tableTarget(req.Schema, unquoteMySQLIdent(rename.GetText()), req.Options.SourceCaseSensitive)
			rw.ReplaceDefault(rename.GetStart().GetTokenIndex(), rename.GetStop().GetTokenIndex(), quoteMySQLIdent(name))
		}
		if en := event.ENABLE_SYMBOL(); en != nil {
			i := en.GetSymbol().GetTokenIndex()
			rw.ReplaceDefault(i, i, "DISABLE")
		} else if event.DISABLE_SYMBOL() == nil {
			switch {
			case event.COMMENT_SYMBOL() != nil:
				rw.InsertBeforeDefault(event.COMMENT_SYMBOL().GetSymbol().GetTokenIndex(), "DISABLE ")
			case event.DO_SYMBOL() != nil:
				rw.InsertBeforeDefault(event.DO_SYMBOL().GetSymbol().GetTokenIndex(), "DISABLE ")
			default:
				rw.InsertAfterDefault(event.GetStop().GetTokenIndex(), " DISABLE")
			}
		}
	}
	antlr.ParseTreeWalkerDefault.Walk(newQualifiedNameRewriter(req.Mapping, req.Options, rw), parsed.tree)
	q := parsed.tree.AllQuery()[0]
	return Result{DDL: rewrittenTextRange(rw, q.GetStart().GetTokenIndex(), q.GetStop().GetTokenIndex()-1)}, nil
}
