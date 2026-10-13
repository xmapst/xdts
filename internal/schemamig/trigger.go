package schemamig

import (
	"fmt"

	"github.com/antlr4-go/antlr/v4"
	mysql "github.com/bytebase/parser/mysql"
)

type createTriggerFinder struct {
	*mysql.BaseMySQLParserListener
	ctx *mysql.CreateTriggerContext
}

func findCreateTrigger(tree antlr.ParseTree) *mysql.CreateTriggerContext {
	f := &createTriggerFinder{BaseMySQLParserListener: &mysql.BaseMySQLParserListener{}}
	antlr.ParseTreeWalkerDefault.Walk(f, tree)
	return f.ctx
}

func (f *createTriggerFinder) EnterCreateTrigger(ctx *mysql.CreateTriggerContext) {
	f.ctx = ctx
}

// rewriteTrigger 实现 ObjectTrigger 的改写管线：解析 → 动态 SQL 阻断 →
// DEFINER 处理 → 库限定名改写 → 触发器守卫包裹。
func rewriteTrigger(req Request) (Result, error) {
	pr, err := parseRoutine(req.DDL)
	if err != nil {
		return blocked(err.Error(), req.DDL), nil
	}
	ct := findCreateTrigger(pr.tree)
	if ct == nil {
		return Result{}, fmt.Errorf("%w: 期望 CREATE TRIGGER", ErrDDLKindMismatch)
	}

	body := ct.CompoundStatement()
	if hasDynamicSQL(body) {
		return blocked(dynamicSQLBlockReason, req.DDL), nil
	}

	rewriter := antlr.NewTokenStreamRewriter(pr.stream)
	if reason, err := applyDefinerANTLR(ct.DefinerClause(), req.Options, rewriter); err != nil {
		return Result{}, err
	} else if reason != "" {
		return blocked(reason, req.DDL), nil
	}
	if reason := rewriteRoutineColumns(ct, req.Mapping, req.Options, rewriter); reason != "" {
		return blocked(reason, req.DDL), nil
	}

	antlr.ParseTreeWalkerDefault.Walk(newQualifiedNameRewriter(req.Mapping, req.Options, rewriter), ct)

	startIdx := findCreateSymbolIndex(pr.tree)
	ddl, err := wrapTriggerGuard(ct, body, startIdx, req.Options.guardVariable(), rewriter)
	if err != nil {
		return Result{}, err
	}
	return Result{DDL: ddl, NeedsDelimiter: true}, nil
}

// wrapTriggerGuard wraps executable trigger statements in the session guard.
// DECLARE statements remain at the start of compound blocks. Boundary insertions
// avoid overlap with qualified-name rewrites; empty bodies need no guard.
func wrapTriggerGuard(ct *mysql.CreateTriggerContext, body mysql.ICompoundStatementContext, createSymbolIdx int, guardVar string, rewriter *antlr.TokenStreamRewriter) (string, error) {
	var beb mysql.IBeginEndBlockContext
	switch {
	case body.UnlabeledBlock() != nil:
		beb = body.UnlabeledBlock().BeginEndBlock()
	case body.LabeledBlock() != nil:
		beb = body.LabeledBlock().BeginEndBlock()
	}

	ctStop := ct.GetStop().GetTokenIndex()

	if beb != nil {
		stmtList := beb.CompoundStatementList()
		if stmtList == nil {
			return rewrittenTextRange(rewriter, createSymbolIdx, ctStop), nil // 空 BEGIN END，无需守卫。
		}
		rewriter.InsertBeforeDefault(stmtList.GetStart().GetTokenIndex(), "IF "+guardVar+" IS NULL THEN ")
		rewriter.InsertAfterDefault(stmtList.GetStop().GetTokenIndex(), " END IF;")
		return rewrittenTextRange(rewriter, createSymbolIdx, ctStop), nil
	}

	// 非 BEGIN...END 单语句体：三段拼接，避免登记重叠 Replace。起始边界用
	// CREATE_SYMBOL 的下标（见 findCreateSymbolIndex 的注释），不能用
	// ct.GetStart()——那是 DEFINER 子句或 TRIGGER 关键字的位置，缺开头的
	// "CREATE "。
	ctStart := createSymbolIdx
	bodyStart := body.GetStart().GetTokenIndex()
	bodyStop := body.GetStop().GetTokenIndex()

	var prefix, suffix string
	if bodyStart > ctStart {
		prefix = rewriter.GetText(antlr.DefaultProgramName, antlr.NewInterval(ctStart, bodyStart-1))
	}
	if bodyStop < ctStop {
		suffix = rewriter.GetText(antlr.DefaultProgramName, antlr.NewInterval(bodyStop+1, ctStop))
	}
	bodyText := rewriter.GetText(antlr.DefaultProgramName, antlr.NewInterval(bodyStart, bodyStop))

	wrapped := fmt.Sprintf("BEGIN\nIF %s IS NULL THEN\n%s;\nEND IF;\nEND", guardVar, bodyText)
	return prefix + wrapped + suffix, nil
}
