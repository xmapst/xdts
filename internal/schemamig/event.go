package schemamig

import (
	"fmt"

	"github.com/antlr4-go/antlr/v4"
	"github.com/bytebase/parser/mysql"
)

type createEventFinder struct {
	*mysql.BaseMySQLParserListener
	ctx *mysql.CreateEventContext
}

func findCreateEvent(tree antlr.ParseTree) *mysql.CreateEventContext {
	f := &createEventFinder{BaseMySQLParserListener: &mysql.BaseMySQLParserListener{}}
	antlr.ParseTreeWalkerDefault.Walk(f, tree)
	return f.ctx
}

func (f *createEventFinder) EnterCreateEvent(ctx *mysql.CreateEventContext) {
	f.ctx = ctx
}

// rewriteEvent 实现 ObjectEvent 的改写管线：解析 → 动态 SQL 阻断 → DEFINER
// 处理 → 库限定名改写 → 强制 DISABLE（需求 MGR-SCH-11：「目标端以 DISABLE
// 状态创建；启用由人工决定」）。
func rewriteEvent(req Request) (Result, error) {
	pr, err := parseRoutine(req.DDL)
	if err != nil {
		return blocked(err.Error(), req.DDL), nil
	}
	ec := findCreateEvent(pr.tree)
	if ec == nil {
		return Result{}, fmt.Errorf("%w: 期望 CREATE EVENT", ErrDDLKindMismatch)
	}

	if body := ec.CompoundStatement(); body != nil && hasDynamicSQL(body) {
		return blocked(dynamicSQLBlockReason, req.DDL), nil
	}

	rewriter := antlr.NewTokenStreamRewriter(pr.stream)
	if reason, err := applyDefinerANTLR(ec.DefinerClause(), req.Options, rewriter); err != nil {
		return Result{}, err
	} else if reason != "" {
		return blocked(reason, req.DDL), nil
	}
	if reason := rewriteRoutineColumns(ec, req.Mapping, req.Options, rewriter); reason != "" {
		return blocked(reason, req.DDL), nil
	}

	antlr.ParseTreeWalkerDefault.Walk(newQualifiedNameRewriter(req.Mapping, req.Options, rewriter), ec)

	forceEventDisable(ec, rewriter)

	startIdx := findCreateSymbolIndex(pr.tree)
	stopIdx := ec.GetStop().GetTokenIndex()
	return Result{DDL: rewrittenTextRange(rewriter, startIdx, stopIdx), NeedsDelimiter: true}, nil
}

// forceEventDisable 把事件的 ENABLE/DISABLE 子句强制改成 DISABLE（需求
// MGR-SCH-11）。语法上这个子句整体可选
// （`(ENABLE_SYMBOL | DISABLE_SYMBOL (ON_SYMBOL SLAVE_SYMBOL)?)?`），三种
// 源端形态分别处理：
//   - 已经是 DISABLE（不管有没有 `ON SLAVE`）：不用改，本来就满足要求。
//   - 是 ENABLE：整体替换成 DISABLE。
//   - 缺省（既不写 ENABLE 也不写 DISABLE，MySQL 默认按 ENABLE 处理）：在
//     COMMENT 子句之前插入（没有 COMMENT 子句时在 DO 之前插入）——用
//     COMMENT_SYMBOL()/DO_SYMBOL() 是否存在来确定插入点，这两者不可能同时
//     缺失（DO 恒定存在，是 createEvent 产生式的必需部分）。
func forceEventDisable(ec *mysql.CreateEventContext, rewriter *antlr.TokenStreamRewriter) {
	if ec.DISABLE_SYMBOL() != nil {
		return
	}
	if en := ec.ENABLE_SYMBOL(); en != nil {
		idx := en.GetSymbol().GetTokenIndex()
		rewriter.ReplaceDefault(idx, idx, "DISABLE")
		return
	}
	insertBefore := ec.DO_SYMBOL().GetSymbol()
	if cs := ec.COMMENT_SYMBOL(); cs != nil {
		insertBefore = cs.GetSymbol()
	}
	rewriter.InsertBeforeDefault(insertBefore.GetTokenIndex(), "DISABLE ")
}
