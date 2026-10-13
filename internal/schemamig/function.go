package schemamig

import (
	"fmt"

	"github.com/antlr4-go/antlr/v4"
	mysql "github.com/bytebase/parser/mysql"
)

type createFunctionFinder struct {
	*mysql.BaseMySQLParserListener
	ctx *mysql.CreateFunctionContext
}

func findCreateFunction(tree antlr.ParseTree) *mysql.CreateFunctionContext {
	f := &createFunctionFinder{BaseMySQLParserListener: &mysql.BaseMySQLParserListener{}}
	antlr.ParseTreeWalkerDefault.Walk(f, tree)
	return f.ctx
}

func (f *createFunctionFinder) EnterCreateFunction(ctx *mysql.CreateFunctionContext) {
	f.ctx = ctx
}

// rewriteFunction 实现 ObjectFunction 的改写管线，步骤与 rewriteProcedure
// 完全对称（需求 MGR-SCH-08/09）；FUNCTION 与 PROCEDURE 在 bytebase 语法里
// 是两条独立的产生式（createFunction/createProcedure），所以是两个函数而
// 不是共用一个类型分支，但内部逻辑没有分叉。
func rewriteFunction(req Request) (Result, error) {
	pr, err := parseRoutine(req.DDL)
	if err != nil {
		return blocked(err.Error(), req.DDL), nil
	}
	cf := findCreateFunction(pr.tree)
	if cf == nil {
		return Result{}, fmt.Errorf("%w: 期望 CREATE FUNCTION", ErrDDLKindMismatch)
	}

	if hasDynamicSQL(cf.CompoundStatement()) {
		return blocked(dynamicSQLBlockReason, req.DDL), nil
	}

	rewriter := antlr.NewTokenStreamRewriter(pr.stream)
	if reason, err := applyDefinerANTLR(cf.DefinerClause(), req.Options, rewriter); err != nil {
		return Result{}, err
	} else if reason != "" {
		return blocked(reason, req.DDL), nil
	}

	if reason := rewriteRoutineColumns(cf, req.Mapping, req.Options, rewriter); reason != "" {
		return blocked(reason, req.DDL), nil
	}

	antlr.ParseTreeWalkerDefault.Walk(newQualifiedNameRewriter(req.Mapping, req.Options, rewriter), cf)

	startIdx := findCreateSymbolIndex(pr.tree)
	stopIdx := cf.GetStop().GetTokenIndex()
	return Result{DDL: rewrittenTextRange(rewriter, startIdx, stopIdx), NeedsDelimiter: true}, nil
}
