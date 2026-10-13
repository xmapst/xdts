package schemamig

import (
	"slices"

	"strings"

	"github.com/antlr4-go/antlr/v4"
	mysql "github.com/bytebase/parser/mysql"
)

type routineColumnScope struct {
	node    antlr.Tree
	tables  []queryTable
	aliases map[string]bool
}
type routineColumnRewriter struct {
	*mysql.BaseMySQLParserListener
	mapping    Mapping
	opts       Options
	rewriter   *antlr.TokenStreamRewriter
	parameters map[string]bool
	blocks     map[antlr.Tree]map[string]bool
	scopes     []routineColumnScope
	trigger    SchemaTable
	reason     string
}

// Column contexts and local declarations are supplied by the grammar. Text
// literals, function names, and relation identifiers never enter this pass.
func rewriteRoutineColumns(tree antlr.ParseTree, mapping Mapping, opts Options, rw *antlr.TokenStreamRewriter) string {
	if mapping.ColumnFilter == nil && len(mapping.Columns) == 0 && len(mapping.DropColumns) == 0 {
		return ""
	}
	v := &routineColumnRewriter{BaseMySQLParserListener: &mysql.BaseMySQLParserListener{}, mapping: mapping, opts: opts, rewriter: rw, parameters: map[string]bool{}, blocks: map[antlr.Tree]map[string]bool{}}
	var declarations func(antlr.Tree)
	declarations = func(node antlr.Tree) {
		switch n := node.(type) {
		case *mysql.FunctionParameterContext:
			v.parameters[foldKey(unquoteMySQLIdent(n.ParameterName().GetText()))] = true
		case *mysql.BeginEndBlockContext:
			locals := map[string]bool{}
			if n.SpDeclarations() != nil {
				var collect func(antlr.Tree)
				collect = func(t antlr.Tree) {
					if _, nested := t.(*mysql.BeginEndBlockContext); nested {
						return
					}
					if decl, ok := t.(*mysql.VariableDeclarationContext); ok {
						for _, id := range decl.IdentifierList().AllIdentifier() {
							locals[foldKey(unquoteMySQLIdent(id.GetText()))] = true
						}
					}
					for _, child := range t.GetChildren() {
						collect(child)
					}
				}
				collect(n.SpDeclarations())
			}
			v.blocks[node] = locals
		case *mysql.CreateTriggerContext:
			v.trigger = v.relation(n.TableRef().QualifiedIdentifier())
		}
		for _, child := range node.GetChildren() {
			declarations(child)
		}
	}
	declarations(tree)
	antlr.ParseTreeWalkerDefault.Walk(v, tree)
	return v.reason
}
func (v *routineColumnRewriter) relation(q mysql.IQualifiedIdentifierContext) SchemaTable {
	ref := SchemaTable{Schema: v.opts.SourceDefaultSchema}
	if q == nil || q.Identifier() == nil {
		return ref
	}
	ref.Table = unquoteMySQLIdent(q.Identifier().GetText())
	if dot := q.DotIdentifier(); dot != nil {
		ref.Schema = ref.Table
		ref.Table = unquoteMySQLIdent(dot.Identifier().GetText())
	}
	return ref
}
func (v *routineColumnRewriter) local(node antlr.Tree, name string) bool {
	if v.parameters[foldKey(name)] {
		return true
	}
	for parent := node.GetParent(); parent != nil; parent = parent.GetParent() {
		if v.blocks[parent][foldKey(name)] {
			return true
		}
	}
	return false
}
func (v *routineColumnRewriter) EnterEveryRule(node antlr.ParserRuleContext) {
	if v.reason != "" {
		return
	}
	switch n := node.(type) {
	case *mysql.SimpleStatementContext:
		if n.SelectStatement() == nil && n.InsertStatement() == nil && n.ReplaceStatement() == nil && n.UpdateStatement() == nil && n.DeleteStatement() == nil && n.SetStatement() == nil && n.DoStatement() == nil && n.CallStatement() == nil {
			v.reason = "存储程序子语句缺少安全列绑定实现"
		}
	case *mysql.QuerySpecificationContext, *mysql.UpdateStatementContext, *mysql.DeleteStatementContext, *mysql.InsertStatementContext, *mysql.ReplaceStatementContext:
		scope := routineColumnScope{node: node, aliases: map[string]bool{}}
		var collect func(antlr.Tree)
		collect = func(t antlr.Tree) {
			if t != node {
				switch t.(type) {
				case *mysql.QuerySpecificationContext, *mysql.SubqueryContext:
					return
				}
			}
			switch c := t.(type) {
			case *mysql.SelectAliasContext:
				if c.Identifier() != nil {
					scope.aliases[foldKey(unquoteMySQLIdent(c.Identifier().GetText()))] = true
				}
			case *mysql.SingleTableContext:
				table := queryTable{SchemaTable: v.relation(c.TableRef().QualifiedIdentifier())}
				if c.TableAlias() != nil {
					table.alias = unquoteMySQLIdent(c.TableAlias().Identifier().GetText())
				}
				scope.tables = append(scope.tables, table)
			case *mysql.DerivedTableContext:
				v.reason = "存储程序派生表列绑定尚未实现"
			case *mysql.WithClauseContext:
				v.reason = "存储程序 CTE 列绑定尚未实现"
			case *mysql.TableWildContext:
				v.reason = "存储程序通配符需原列元数据"
			case *mysql.SelectItemListContext:
				for _, child := range c.GetChildren() {
					if terminal, ok := child.(antlr.TerminalNode); ok && terminal.GetText() == "*" {
						v.reason = "存储程序通配符需原列元数据"
					}
				}
			}
			for _, child := range t.GetChildren() {
				collect(child)
			}
		}
		collect(node)
		if statement, ok := node.(*mysql.DeleteStatementContext); ok && statement.TableRef() != nil {
			table := queryTable{SchemaTable: v.relation(statement.TableRef().QualifiedIdentifier())}
			if statement.TableAlias() != nil {
				table.alias = unquoteMySQLIdent(statement.TableAlias().Identifier().GetText())
			}
			scope.tables = append(scope.tables, table)
		}
		if insert, ok := node.(*mysql.InsertStatementContext); ok {
			scope.tables = append(scope.tables, queryTable{SchemaTable: v.relation(insert.TableRef().QualifiedIdentifier())})
			if constructor := insert.InsertFromConstructor(); constructor != nil && constructor.Fields() == nil {
				v.reason = "存储程序隐式 INSERT 列需原物理定义"
			}
			if query := insert.InsertQueryExpression(); query != nil && query.Fields() == nil {
				v.reason = "存储程序隐式 INSERT SELECT 列需原物理定义"
			}
		}
		if replace, ok := node.(*mysql.ReplaceStatementContext); ok {
			scope.tables = append(scope.tables, queryTable{SchemaTable: v.relation(replace.TableRef().QualifiedIdentifier())})
			if constructor := replace.InsertFromConstructor(); constructor != nil && constructor.Fields() == nil {
				v.reason = "存储程序隐式 REPLACE 列需原物理定义"
			}
			if query := replace.InsertQueryExpression(); query != nil && query.Fields() == nil {
				v.reason = "存储程序隐式 REPLACE SELECT 列需原物理定义"
			}
		}
		v.scopes = append(v.scopes, scope)
	}
}
func (v *routineColumnRewriter) ExitEveryRule(node antlr.ParserRuleContext) {
	if len(v.scopes) > 0 && v.scopes[len(v.scopes)-1].node == node {
		v.scopes = v.scopes[:len(v.scopes)-1]
	}
}
func routineIdentifierParts(node antlr.Tree) []string {
	var out []string
	var walk func(antlr.Tree)
	walk = func(t antlr.Tree) {
		if id, ok := t.(*mysql.IdentifierContext); ok {
			out = append(out, unquoteMySQLIdent(id.GetText()))
			return
		}
		for _, child := range t.GetChildren() {
			walk(child)
		}
	}
	walk(node)
	return out
}
func (v *routineColumnRewriter) bind(node antlr.ParserRuleContext, assignment bool) {
	if v.reason != "" {
		return
	}
	parts := routineIdentifierParts(node)
	if len(parts) == 0 || len(parts) > 3 {
		v.reason = "存储程序列标识符无法绑定"
		return
	}
	column := parts[len(parts)-1]
	if len(parts) == 1 && len(v.scopes) > 0 && v.scopes[len(v.scopes)-1].aliases[foldKey(column)] {
		for parent := node.GetParent(); parent != nil; parent = parent.GetParent() {
			switch parent.(type) {
			case *mysql.OrderClauseContext, *mysql.GroupByClauseContext, *mysql.HavingClauseContext:
				v.reason = "存储程序输出别名与来源列需原列定义才能安全区分"
				return
			case *mysql.QuerySpecificationContext:
				parent = nil
			}
			if parent == nil {
				break
			}
		}
	}
	if len(parts) == 1 && !assignment && v.local(node, column) {
		return
	}
	if len(parts) == 2 && (foldKey(parts[0]) == "new" || foldKey(parts[0]) == "old") {
		if v.trigger.Table == "" {
			v.reason = "NEW/OLD 缺少触发器来源表"
			return
		}
		v.replace(node, parts, v.trigger, column)
		return
	}
	var matched []queryTable
	for _, v0 := range slices.Backward(v.scopes) {
		for _, table := range v0.tables {
			switch len(parts) {
			case 1:
				matched = append(matched, table)
			case 2:
				qualifier := table.alias
				if qualifier == "" {
					qualifier = table.Table
				}
				if foldEqual(parts[0], qualifier, v.opts.SourceCaseSensitive) {
					matched = append(matched, table)
				}
			case 3:
				if table.alias == "" && foldEqual(parts[0], table.Schema, v.opts.SourceCaseSensitive) && foldEqual(parts[1], table.Table, v.opts.SourceCaseSensitive) {
					matched = append(matched, table)
				}
			}
		}
		if len(matched) > 0 {
			break
		}
	}
	if len(matched) != 1 {
		v.reason = "存储程序列引用无法唯一绑定来源表或局部变量"
		return
	}
	if len(parts) == 1 && len(v.scopes) > 1 {
		v.reason = "存储程序关联未限定列需原列元数据"
		return
	}
	v.replace(node, parts, matched[0].SchemaTable, column)
}
func (v *routineColumnRewriter) replace(node antlr.ParserRuleContext, parts []string, ref SchemaTable, column string) {
	if v.mapping.dropColumnSet(ref.Schema, ref.Table, v.opts.SourceCaseSensitive)[foldKey(column)] || v.mapping.ColumnFilter != nil && !v.mapping.ColumnFilter.Allow(ref.Schema, ref.Table, column) {
		v.reason = "存储程序引用被过滤列"
		return
	}
	parts[len(parts)-1] = v.mapping.columnTarget(ref.Schema, ref.Table, column, v.opts.SourceCaseSensitive)
	if len(parts) == 3 {
		parts[0] = v.mapping.schemaTarget(ref.Schema, v.opts.SourceCaseSensitive)
		parts[1] = v.mapping.tableTarget(ref.Schema, ref.Table, v.opts.SourceCaseSensitive)
	} else if len(parts) == 2 && foldKey(parts[0]) != "new" && foldKey(parts[0]) != "old" && foldEqual(parts[0], ref.Table, v.opts.SourceCaseSensitive) {
		aliased := false
		for _, v0 := range slices.Backward(v.scopes) {
			for _, table := range v0.tables {
				if table.alias != "" && foldEqual(parts[0], table.alias, v.opts.SourceCaseSensitive) {
					aliased = true
				}
			}
			if aliased {
				break
			}
		}
		if !aliased {
			parts[0] = v.mapping.tableTarget(ref.Schema, ref.Table, v.opts.SourceCaseSensitive)
		}
	}
	var text strings.Builder
	for i, part := range parts {
		if i > 0 {
			text.WriteString(".")
		}
		text.WriteString(quoteMySQLIdent(part))
	}
	v.rewriter.ReplaceDefault(node.GetStart().GetTokenIndex(), node.GetStop().GetTokenIndex(), text.String())
}
func (v *routineColumnRewriter) EnterSimpleIdentifier(node *mysql.SimpleIdentifierContext) {
	if _, columnRef := node.GetParent().(*mysql.ColumnRefContext); columnRef {
		return
	}
	v.bind(node, false)
}
func (v *routineColumnRewriter) ExitSelectItem(node *mysql.SelectItemContext) {
	if v.reason != "" || node.SelectAlias() != nil || node.Expr() == nil {
		return
	}
	var column *mysql.ColumnRefContext
	var find func(antlr.Tree)
	find = func(t antlr.Tree) {
		if c, ok := t.(*mysql.ColumnRefContext); ok {
			if column != nil {
				column = nil
				return
			}
			column = c
			return
		}
		for _, child := range t.GetChildren() {
			find(child)
		}
	}
	find(node.Expr())
	if column == nil || node.Expr().GetStart().GetTokenIndex() != column.GetStart().GetTokenIndex() || node.Expr().GetStop().GetTokenIndex() != column.GetStop().GetTokenIndex() {
		return
	}
	parts := routineIdentifierParts(column)
	if len(parts) > 0 {
		v.rewriter.InsertAfterDefault(node.GetStop().GetTokenIndex(), " AS "+quoteMySQLIdent(parts[len(parts)-1]))
	}
}
func (v *routineColumnRewriter) EnterColumnRef(node *mysql.ColumnRefContext) {
	assignment := false
	switch node.GetParent().(type) {
	case *mysql.UpdateElementContext, *mysql.InsertIdentifierContext:
		assignment = true
	}
	v.bind(node, assignment)
}
func (v *routineColumnRewriter) EnterInternalVariableName(node *mysql.InternalVariableNameContext) {
	parts := routineIdentifierParts(node)
	if len(parts) == 2 && (foldKey(parts[0]) == "new" || foldKey(parts[0]) == "old") {
		v.bind(node, true)
	}
}
