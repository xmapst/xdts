package schemamig

import (
	"strings"

	"github.com/antlr4-go/antlr/v4"
	mysql "github.com/bytebase/parser/mysql"
)

// TriggerSuppressesApply accepts a whole-body guard on the standard apply
// variable. Declarations and alternative branches are conservatively rejected:
// their expressions or handlers can execute outside that guard.
func TriggerSuppressesApply(ddl string) (bool, error) {
	pr, err := parseRoutine(ddl)
	if err != nil {
		return false, err
	}
	// Versioned executable comments can be hidden by the parser's server
	// version. They cannot prove suppression on the actual target server.
	for _, token := range pr.stream.GetAllTokens() {
		if token.GetTokenType() == mysql.MySQLLexerVERSION_COMMENT_START || token.GetTokenType() == mysql.MySQLLexerMYSQL_COMMENT_START {
			return false, nil
		}
	}
	ct := findCreateTrigger(pr.tree)
	if ct == nil {
		return false, ErrDDLKindMismatch
	}
	body := ct.CompoundStatement()
	var block mysql.IBeginEndBlockContext
	if body.UnlabeledBlock() != nil {
		block = body.UnlabeledBlock().BeginEndBlock()
	} else if body.LabeledBlock() != nil {
		block = body.LabeledBlock().BeginEndBlock()
	}
	if block != nil {
		if decl := block.SpDeclarations(); decl != nil && decl.GetText() != "" {
			return false, nil
		}
		list := block.CompoundStatementList()
		if list == nil {
			return true, nil
		}
		if len(list.AllCompoundStatement()) != 1 {
			return false, nil
		}
		body = list.CompoundStatement(0)
	}
	if body.IfStatement() == nil {
		return false, nil
	}
	guard := body.IfStatement().IfBody()
	if guard == nil || guard.ELSE_SYMBOL() != nil || guard.ELSEIF_SYMBOL() != nil || guard.IfBody() != nil || guard.Expr() == nil {
		return false, nil
	}
	var tokens []string
	for _, token := range pr.stream.GetAllTokens() {
		if token.GetChannel() == antlr.TokenDefaultChannel && token.GetTokenIndex() >= guard.Expr().GetStart().GetTokenIndex() && token.GetTokenIndex() <= guard.Expr().GetStop().GetTokenIndex() {
			tokens = append(tokens, strings.ToUpper(token.GetText()))
		}
	}
	return len(tokens) == 3 && tokens[0] == "@XDTS_APPLY" && tokens[1] == "IS" && tokens[2] == "NULL", nil
}
