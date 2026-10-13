package schemamig

import (
	"fmt"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/bytebase/parser/mysql"
)

type viewAlterFinder struct {
	*mysql.BaseMySQLParserListener
	view *mysql.AlterViewContext
}

func (f *viewAlterFinder) EnterAlterView(view *mysql.AlterViewContext) { f.view = view }

// alterViewDefinition changes only the parsed operation token so the existing
// CREATE VIEW AST can bind columns and accounts. Execution retains ALTER.
func alterViewDefinition(ddl string) (string, bool) {
	parsed, err := parseRoutine(ddl)
	if err != nil || len(parsed.tree.AllQuery()) != 1 {
		return "", false
	}
	f := &viewAlterFinder{BaseMySQLParserListener: &mysql.BaseMySQLParserListener{}}
	antlr.ParseTreeWalkerDefault.Walk(f, parsed.tree)
	if f.view == nil {
		return "", false
	}
	statement, ok := f.view.GetParent().(*mysql.AlterStatementContext)
	if !ok || statement.ALTER_SYMBOL() == nil {
		return "", false
	}
	rw := antlr.NewTokenStreamRewriter(parsed.stream)
	i := statement.ALTER_SYMBOL().GetSymbol().GetTokenIndex()
	rw.ReplaceDefault(i, i, "CREATE")
	query := parsed.tree.AllQuery()[0]
	return rewrittenTextRange(rw, query.GetStart().GetTokenIndex(), query.GetStop().GetTokenIndex()-1), true
}

func rewriteAlterView(req IncrementalRequest, definition string) (Result, error) {
	result, err := rewriteView(Request{Kind: ObjectView, DefaultSchema: req.Schema, DDL: definition, Mapping: req.Mapping, Options: req.Options})
	if err != nil || result.Blocked {
		if result.Blocked {
			result.OriginalDDL = req.DDL
		}
		return result, err
	}
	if !strings.HasPrefix(result.DDL, "CREATE ") {
		return Result{}, fmt.Errorf("schemamig: unexpected rewritten view operation")
	}
	result.DDL = "ALTER " + strings.TrimPrefix(result.DDL, "CREATE ")
	return result, nil
}

// AlterViewDefiner uses the same parsed account identity as CREATE VIEW.
func AlterViewDefiner(ddl string) (*DefinerAccount, error) {
	definition, ok := alterViewDefinition(ddl)
	if !ok {
		return nil, nil
	}
	return DefinitionDefiner(ObjectView, definition)
}
