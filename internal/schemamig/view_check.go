package schemamig

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/bytebase/parser/mysql"
)

type implicitViewCheckFinder struct {
	*mysql.BaseMySQLParserListener
	option *mysql.ViewCheckOptionContext
}

func (f *implicitViewCheckFinder) EnterViewCheckOption(option *mysql.ViewCheckOptionContext) {
	if option.CASCADED_SYMBOL() == nil && option.LOCAL_SYMBOL() == nil {
		f.option = option
	}
}

// MySQL permits WITH CHECK OPTION as shorthand for CASCADED. TiDB requires
// the explicit keyword; insert it only at the parsed view-tail CHECK token.
func explicitViewCheckOption(ddl string) (string, bool) {
	parsed, err := parseRoutine(ddl)
	if err != nil || len(parsed.tree.AllQuery()) != 1 {
		return "", false
	}
	f := &implicitViewCheckFinder{BaseMySQLParserListener: &mysql.BaseMySQLParserListener{}}
	antlr.ParseTreeWalkerDefault.Walk(f, parsed.tree)
	if f.option == nil {
		return "", false
	}
	rw := antlr.NewTokenStreamRewriter(parsed.stream)
	rw.InsertBeforeDefault(f.option.CHECK_SYMBOL().GetSymbol().GetTokenIndex(), "CASCADED ")
	query := parsed.tree.AllQuery()[0]
	return rewrittenTextRange(rw, query.GetStart().GetTokenIndex(), query.GetStop().GetTokenIndex()-1), true
}
