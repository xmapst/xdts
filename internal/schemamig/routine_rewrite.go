// internal/schemamig/routine_rewrite.go 提供存储过程 / 函数 / 触发器 / 事件四类对象共用的改写
// 步骤：动态 SQL 检测（第一原则性安全阀）、DEFINER 替换、库限定名改写。
// 触发器守卫包裹（internal/schemamig/trigger.go）与事件 DISABLE 强制（internal/schemamig/event.go）是各自专属的
// 步骤，不在这里。
package schemamig

import (
	"fmt"

	"github.com/antlr4-go/antlr/v4"
	mysql "github.com/bytebase/parser/mysql"
)

// dynamicSQLListener 检测语法树里任意位置出现的 PreparedStatementContext
// （bytebase 语法：PREPARE ... FROM / EXECUTE ... / DEALLOCATE|DROP PREPARE
// 三种形式共用这一个产生式，见 MySQLParser.g4 的 preparedStatement 规则）。
// 用语法树节点类型判定而不是文本关键字扫描，字符串字面量与注释里出现
// "PREPARE"/"EXECUTE" 不会误触发——这两种文本在词法层根本不会产生
// PreparedStatementContext 节点。
type dynamicSQLListener struct {
	*mysql.BaseMySQLParserListener
	found bool
}

func newDynamicSQLListener() *dynamicSQLListener {
	return &dynamicSQLListener{BaseMySQLParserListener: &mysql.BaseMySQLParserListener{}}
}

func (l *dynamicSQLListener) EnterPreparedStatement(_ *mysql.PreparedStatementContext) {
	l.found = true
}

func hasDynamicSQL(tree antlr.ParseTree) bool {
	l := newDynamicSQLListener()
	antlr.ParseTreeWalkerDefault.Walk(l, tree)
	return l.found
}

// qualifiedNameRewriter 改写语法树里"两段式限定名"的库名/对象名部分
// （bytebase 语法的 qualifiedIdentifier: identifier dotIdentifier?，被
// tableRef/procedureRef/functionRef/triggerRef/procedureName/functionName/
// triggerName/eventName 复用）。只在 DotIdentifier() != nil（即确实是
// `schema.object` 两段式）时才处理，单段标识符（列名、别名、局部变量、
// CTE 名）原样跳过——dotIdentifier 是可选产生式，不带点号时压根不会进这个
// 分支，不需要额外判断"是不是别名"，语法结构本身已经把这类风险排除在外
type qualifiedNameRewriter struct {
	*mysql.BaseMySQLParserListener
	mapping       Mapping
	caseSensitive bool
	defaultSchema string
	rewriter      *antlr.TokenStreamRewriter
}

func newQualifiedNameRewriter(mapping Mapping, opts Options, rewriter *antlr.TokenStreamRewriter) *qualifiedNameRewriter {
	return &qualifiedNameRewriter{BaseMySQLParserListener: &mysql.BaseMySQLParserListener{}, mapping: mapping, caseSensitive: opts.SourceCaseSensitive, defaultSchema: opts.SourceDefaultSchema, rewriter: rewriter}
}

func (l *qualifiedNameRewriter) EnterQualifiedIdentifier(ctx *mysql.QualifiedIdentifierContext) {
	// Field references belong to the column binder. Treating alias.column as
	// schema.object would overlap its replacement and change alias semantics.
	switch ctx.GetParent().(type) {
	case *mysql.FieldIdentifierContext:
		if l.mapping.ColumnFilter != nil || len(l.mapping.Columns) != 0 || len(l.mapping.DropColumns) != 0 {
			return
		}
	}
	dot := ctx.DotIdentifier()
	if dot == nil {
		if l.defaultSchema == "" || ctx.Identifier() == nil {
			return
		}
		// Only grammar contexts that name actual objects receive a default DB;
		// local variables, column names and aliases remain unchanged.
		switch ctx.GetParent().(type) {
		case *mysql.TableRefContext, *mysql.ProcedureNameContext, *mysql.FunctionNameContext, *mysql.TriggerNameContext, *mysql.EventNameContext, *mysql.ProcedureRefContext, *mysql.FunctionRefContext, *mysql.TriggerRefContext, *mysql.EventRefContext:
			name := unquoteMySQLIdent(ctx.Identifier().GetText())
			name = l.mapping.tableTarget(l.defaultSchema, name, l.caseSensitive)
			schema := l.mapping.schemaTarget(l.defaultSchema, l.caseSensitive)
			l.rewriter.ReplaceDefault(ctx.GetStart().GetTokenIndex(), ctx.GetStop().GetTokenIndex(), quoteMySQLIdent(schema)+"."+quoteMySQLIdent(name))
		}
		return
	}
	schemaIDCtx := ctx.Identifier()
	nameIDCtx := dot.Identifier()
	if schemaIDCtx == nil || nameIDCtx == nil {
		return
	}

	schema := unquoteMySQLIdent(schemaIDCtx.GetText())
	name := unquoteMySQLIdent(nameIDCtx.GetText())

	if newSchema := l.mapping.schemaTarget(schema, l.caseSensitive); newSchema != schema {
		l.replace(schemaIDCtx, newSchema)
	}
	if newName := l.mapping.tableTarget(schema, name, l.caseSensitive); newName != name {
		l.replace(nameIDCtx, newName)
	}
}

func (l *qualifiedNameRewriter) replace(idCtx mysql.IIdentifierContext, newVal string) {
	start := idCtx.GetStart().GetTokenIndex()
	stop := idCtx.GetStop().GetTokenIndex()
	l.rewriter.ReplaceDefault(start, stop, quoteMySQLIdent(newVal))
}

// applyDefinerANTLR 是需求 MGR-SCH-09 在 ANTLR 路线上的实现：目标已有同名
// 账号（或源端定义没有 DEFINER 子句）则不动；否则要求已配置兜底账号，
// 在 rewriter 里登记一次整段替换。渲染成替换文本这一步交给
// definerAccount.text()（internal/schemamig/definer.go），不在这里重复实现——这是与 tidb
// 路线（internal/schemamig/view.go 的 applyDefiner）共用的同一份 DefinerFallback 判定逻辑，
// 只是最终落地形式不同：tidb 路线构造类型化的 *auth.UserIdentity 交给
// tidb 自己的 Restore 加引号，这里没有这样的 AST 节点可用，是纯 token 级
// 文本替换，必须自己负责加引号（真实 MySQL 执行验证过：这里如果直接用
// DefinerFallback 原始文本不加引号替换，host 含 "%" 这类通配符时会直接
// Error 1064）。
func applyDefinerANTLR(definerCtx mysql.IDefinerClauseContext, opts Options, rewriter *antlr.TokenStreamRewriter) (reason string, err error) {
	if definerCtx == nil || opts.DefinerAccountExists {
		return "", nil
	}
	if opts.DefinerFallback == "" {
		return "目标端缺少与源端 DEFINER 同名的账号，且未配置兜底 DEFINER 账号（需求 MGR-SCH-09 / PRE-15）", nil
	}
	acc, err := resolveDefinerFallback(opts.DefinerFallback)
	if err != nil {
		return "", fmt.Errorf("schemamig: 解析 DefinerFallback 失败: %w", err)
	}
	userCtx := definerCtx.User()
	if userCtx == nil {
		return "", fmt.Errorf("schemamig: DEFINER 子句缺少 user 节点（语法要求 definerClause 必含 user，不应发生）")
	}
	start := userCtx.GetStart().GetTokenIndex()
	stop := userCtx.GetStop().GetTokenIndex()
	rewriter.ReplaceDefault(start, stop, acc.text())
	return "", nil
}

// rewrittenTextRange 返回 [startIdx, stopIdx] token 区间在改写后的文本
// （应用 rewriter 已登记的全部替换 / 插入操作）。
func rewrittenTextRange(rewriter *antlr.TokenStreamRewriter, startIdx, stopIdx int) string {
	return rewriter.GetText(antlr.DefaultProgramName, antlr.NewInterval(startIdx, stopIdx))
}

// createStatementFinder 定位语法树里 CREATE_SYMBOL 这个 token。
//
// 这不是多余的一步：bytebase 语法里 CREATE_SYMBOL 属于父规则
// createStatement（`createStatement: CREATE_SYMBOL ( createProcedure | ...
// | createTrigger | createEvent | ... )`），不属于
// createProcedure/createFunction/createTrigger/createEvent 这几个子规则
// 自己——这几个子规则的 GetStart() 直接从 DEFINER 子句或对象类型关键字
// （PROCEDURE/FUNCTION/TRIGGER/EVENT）开始，不含开头的 "CREATE "。
// 如果只用子规则自己的 token 区间拼文本，改写结果会整体缺失开头的
// "CREATE " 关键字，产出一段根本不是合法 DDL 语句的文本——这是本包在
// 编写阶段的手动抽样打印检查中发现并修复的一处真实缺陷，不是理论推演。
type createStatementFinder struct {
	*mysql.BaseMySQLParserListener
	createSymbol antlr.Token
}

func (f *createStatementFinder) EnterCreateStatement(ctx *mysql.CreateStatementContext) {
	if f.createSymbol == nil {
		f.createSymbol = ctx.CREATE_SYMBOL().GetSymbol()
	}
}

// findCreateSymbolIndex 返回整棵语法树里 CREATE_SYMBOL 的 token 下标，
// 作为改写结果的正确起始边界。SHOW CREATE 系列的返回文本恒定只含一条
// CREATE 语句，因此树里只会有一个 CreateStatementContext，不存在多值歧义。
func findCreateSymbolIndex(tree antlr.ParseTree) int {
	f := &createStatementFinder{BaseMySQLParserListener: &mysql.BaseMySQLParserListener{}}
	antlr.ParseTreeWalkerDefault.Walk(f, tree)
	return f.createSymbol.GetTokenIndex()
}
