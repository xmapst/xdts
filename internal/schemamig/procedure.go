package schemamig

import (
	"fmt"

	"github.com/antlr4-go/antlr/v4"
	mysql "github.com/bytebase/parser/mysql"
)

// createProcedureFinder 在解析出的语法树里定位唯一的 CreateProcedureContext
// 节点。SHOW CREATE PROCEDURE 的返回文本恒定只含一条 CREATE PROCEDURE 语句，
// 因此"唯一"是前置条件而不是需要处理的多值情况；万一解析出多条或者根本
// 没有命中（说明调用方传错了 Kind，或者文本其实是别的对象类型），
// findCreateProcedure 返回 nil，由调用方转成 ErrDDLKindMismatch。
type createProcedureFinder struct {
	*mysql.BaseMySQLParserListener
	ctx *mysql.CreateProcedureContext
}

func findCreateProcedure(tree antlr.ParseTree) *mysql.CreateProcedureContext {
	f := &createProcedureFinder{BaseMySQLParserListener: &mysql.BaseMySQLParserListener{}}
	antlr.ParseTreeWalkerDefault.Walk(f, tree)
	return f.ctx
}

func (f *createProcedureFinder) EnterCreateProcedure(ctx *mysql.CreateProcedureContext) {
	f.ctx = ctx
}

// rewriteProcedure 实现 ObjectProcedure 的改写管线：解析 → 动态 SQL 阻断 →
// DEFINER 处理 → 库限定名改写 → 取回改写后文本。
func rewriteProcedure(req Request) (Result, error) {
	pr, err := parseRoutine(req.DDL)
	if err != nil {
		return blocked(err.Error(), req.DDL), nil
	}
	cp := findCreateProcedure(pr.tree)
	if cp == nil {
		return Result{}, fmt.Errorf("%w: 期望 CREATE PROCEDURE", ErrDDLKindMismatch)
	}

	if hasDynamicSQL(cp.CompoundStatement()) {
		return blocked(dynamicSQLBlockReason, req.DDL), nil
	}

	rewriter := antlr.NewTokenStreamRewriter(pr.stream)
	if reason, err := applyDefinerANTLR(cp.DefinerClause(), req.Options, rewriter); err != nil {
		return Result{}, err
	} else if reason != "" {
		return blocked(reason, req.DDL), nil
	}

	if reason := rewriteRoutineColumns(cp, req.Mapping, req.Options, rewriter); reason != "" {
		return blocked(reason, req.DDL), nil
	}

	antlr.ParseTreeWalkerDefault.Walk(newQualifiedNameRewriter(req.Mapping, req.Options, rewriter), cp)

	startIdx := findCreateSymbolIndex(pr.tree)
	stopIdx := cp.GetStop().GetTokenIndex()
	return Result{DDL: rewrittenTextRange(rewriter, startIdx, stopIdx), NeedsDelimiter: true}, nil
}

const dynamicSQLBlockReason = "存储程序体内出现 PREPARE/EXECUTE（含 DEALLOCATE/DROP PREPARE）动态 SQL，改写无法静态保证语义正确，按需求 MGR-SCH-08 阻断，请人工处理原文"
