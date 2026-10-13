package schemamig

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/parser/ast"
	tidbmysql "github.com/pingcap/tidb/pkg/parser/mysql"
)

// rewriteTable 实现 ObjectTable 的改写管线：utf8mb3 保真 / INVISIBLE 标记
// 剥离（文本层，解析前）→ 解析 → VECTOR 版本适配 → 列过滤（含索引联动与
// 主键保护）与库 / 表映射 → 不可见列版本适配（基于列过滤后的最终结果
// 判断，见下方门槛检查处的说明）→ 表选项清洗 → restore → INVISIBLE 标记
// 回填 / utf8mb3 拼写回填（文本层，解析后）→ 产物自洽性校验。
func rewriteTable(req Request) (Result, error) {
	// 解析前的文本手术：两处都是"tidb parser 本身处理不了这段语法/会
	// 静默改写"的问题，必须在喂给 parser 之前处理，互相独立、顺序不影响
	// 正确性。详细原因见 internal/schemamig/charset_fidelity.go / internal/schemamig/invisible_column.go。
	markedDDL := stripUTF8MB3Markers(req.DDL)
	markedDDL, invisibleMarkers, ok := stripInvisibleColumnMarkers(markedDDL)
	if !ok {
		return blocked("列定义包含无法安全定位归属列的 INVISIBLE 标记，无法安全改写", req.DDL), nil
	}

	stmt, err := parseOneTiDBStmt(markedDDL)
	if err != nil {
		return blocked(err.Error(), req.DDL), nil
	}
	ct, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		return Result{}, fmt.Errorf("%w: 期望 CREATE TABLE，实际是 %T", ErrDDLKindMismatch, stmt)
	}

	srcSchema := ct.Table.Schema.O
	if srcSchema == "" {
		srcSchema = req.DefaultSchema
	}
	srcTable := ct.Table.Name.O
	if ct.ReferTable != nil {
		return rewriteLikeTable(req, ct, srcSchema, srcTable)
	}
	req.Mapping = columnPolicyMapping(req.Mapping, ct, srcSchema, srcTable, req.Options.SourceCaseSensitive)

	// 版本适配：VECTOR 类型。放在前面，任何一列命中即阻断，
	// 不必等后续步骤白做功。
	for _, col := range ct.Cols {
		if col.Tp != nil && col.Tp.GetType() == tidbmysql.TypeTiDBVectorFloat32 && !req.Options.TargetVectorType {
			return blocked(fmt.Sprintf(
				"列 %s.%s.%s 是 VECTOR 类型，目标实例不支持该类型",
				srcSchema, srcTable, col.Name.Name.O), req.DDL), nil
		}
	}

	// 列过滤：先算出要删的列集合，再检查主键是否受影响（必须先检查、
	// 后删除——一旦发现主键受影响就整体阻断，不做部分删除再回滚）。
	dropSet := req.Mapping.dropColumnSet(srcSchema, srcTable, req.Options.SourceCaseSensitive)
	if len(dropSet) > 0 {
		if reason, hit := primaryKeyReferencesDropped(ct, dropSet); hit {
			return blocked(reason, req.DDL), nil
		}
		for _, col := range ct.Cols {
			if !dropSet[foldKey(col.Name.Name.O)] && columnDefinitionReferencesDropped(col, dropSet) {
				return blocked("保留列的表达式引用被过滤列，不能安全投影建表", req.DDL), nil
			}
		}
	}
	for _, cst := range ct.Constraints {
		if !constraintReferencesDropped(cst, dropSet) && referencesExcludedForeignColumn(cst, req.Mapping, srcSchema, req.Options.SourceCaseSensitive) {
			return blocked("外键引用被过滤的目标列，不能安全投影建表", req.DDL), nil
		}
	}

	// 应用库 / 表映射。
	newSchema := req.Mapping.schemaTarget(srcSchema, req.Options.SourceCaseSensitive)
	newTable := req.Mapping.tableTarget(srcSchema, srcTable, req.Options.SourceCaseSensitive)
	ct.Table.Schema = ast.NewCIStr(newSchema)
	ct.Table.Name = ast.NewCIStr(newTable)

	if len(dropSet) > 0 {
		ct.Cols = filterColumns(ct.Cols, dropSet)
		ct.Constraints = filterConstraints(ct.Constraints, dropSet)
		if len(ct.Cols) == 0 {
			return blocked("列过滤移除了全部列，不能创建目标表", req.DDL), nil
		}
	}
	ct.Accept(&definitionColumns{mapping: req.Mapping, schema: srcSchema, table: srcTable, sensitive: req.Options.SourceCaseSensitive})
	for i := range invisibleMarkers {
		invisibleMarkers[i].column = req.Mapping.columnTarget(srcSchema, srcTable, invisibleMarkers[i].column, req.Options.SourceCaseSensitive)
	}

	// 版本适配：不可见列（含 GIPK 自动生成的列，见本函数顶部说明）。判断
	// 放在列过滤完成之后——按"改写最终产物里是否还有不可见列"判断，不是
	// "源端 DDL 曾经出现过 INVISIBLE"就武断要求目标支持：如果不可见列本身
	// 已经被列过滤删掉，最终产物根本不含这一列，目标是否支持 INVISIBLE
	// 与这次改写无关，不应该因为它曾经存在过就阻断一张实际能在目标端正常
	// 执行的表。survivingInvisibleColumnMarkers 按"列是否还在最终 ct.Cols
	// 里"统一判断，不绑死在 DropColumns 这一种删除机制上。
	survivingMarkers := survivingInvisibleColumnMarkers(invisibleMarkers, ct.Cols)
	if len(survivingMarkers) > 0 && !req.Options.TargetInvisibleColumn {
		return blocked(fmt.Sprintf(
			"表 %s.%s 改写后仍含 %d 个不可见列（含 GIPK 自动生成的列），目标实例不支持列级 INVISIBLE 属性（需要 MySQL 8.0.23+）",
			srcSchema, srcTable, len(survivingMarkers)), req.DDL), nil
	}

	// 表选项清洗（AUTO_INCREMENT=N / DATA DIRECTORY / TABLESPACE
	// / ENCRYPTION 等环境相关选项）。
	strip := req.Options.stripSet()
	if len(strip) > 0 {
		ct.Options = filterTableOptions(ct.Options, strip)
	}

	ddl, err := restoreTiDB(ct)
	if err != nil {
		return blocked(err.Error(), req.DDL), nil
	}

	// 解析后的文本手术：按列名把 INVISIBLE 标记回填（用改写完成后的
	// ct.Cols，天然处理了被 DropColumns 过滤掉的列——不在 ct.Cols 里的
	// 标记直接跳过），再把 utf8mb3 的哨兵名字换回真实拼写。
	ddl, err = reinsertInvisibleColumnMarkers(ddl, ct.Cols, invisibleMarkers)
	if err != nil {
		return blocked(err.Error(), req.DDL), nil
	}
	ddl = restoreUTF8MB3Markers(ddl)

	// 兜底：产物必须能重新解析，做不到就阻断而不是交出可能有问题的产物
	// （"保真 > 阻断 > 绝不静默改名"）。重新解析前要用
	// 同一套 marker 机制剥离一遍——产物里现在装的是真实的 utf8mb3/
	// INVISIBLE 拼写，跟源端 DDL 一样过不了原生 tidb parser，这一步验证
	// 的是"这份产物如果再喂回本函数，还能正常处理"，不是直接拿生 parser
	// 验证。
	verifyDDL := stripUTF8MB3Markers(ddl)
	verifyDDL, _, verifyOK := stripInvisibleColumnMarkers(verifyDDL)
	if !verifyOK {
		return blocked("改写产物里的 INVISIBLE 标记无法重新定位归属列，无法安全改写", req.DDL), nil
	}
	if _, err := parseOneTiDBStmt(verifyDDL); err != nil {
		return blocked(fmt.Sprintf("改写产物无法重新解析，无法安全改写: %v", err), req.DDL), nil
	}

	return Result{DDL: ddl}, nil
}

func primaryKeyReferencesDropped(ct *ast.CreateTableStmt, dropSet map[string]bool) (string, bool) {
	for _, col := range ct.Cols {
		if !dropSet[foldKey(col.Name.Name.O)] {
			continue
		}
		for _, opt := range col.Options {
			if opt.Tp == ast.ColumnOptionPrimaryKey {
				return fmt.Sprintf("列 %s 是内联主键（PRIMARY KEY 列选项），不能被列过滤删除", col.Name.Name.O), true
			}
		}
	}
	for _, cst := range ct.Constraints {
		if cst.Tp != ast.ConstraintPrimaryKey {
			continue
		}
		for _, key := range cst.Keys {
			if key.Column == nil {
				continue // 表达式索引部分（函数索引），没有列名可比对，跳过。
			}
			if dropSet[foldKey(key.Column.Name.O)] {
				return fmt.Sprintf("主键包含被过滤列 %s，不能安全改写", key.Column.Name.O), true
			}
		}
	}
	return "", false
}

// filterColumns 删除 dropSet 命中的列定义。
func filterColumns(cols []*ast.ColumnDef, dropSet map[string]bool) []*ast.ColumnDef {
	kept := cols[:0:0]
	for _, col := range cols {
		if dropSet[foldKey(col.Name.Name.O)] {
			continue
		}
		kept = append(kept, col)
	}
	return kept
}

// filterConstraints 删除引用了被过滤列的索引类约束（需求 MGR-SCH-02："列
// 过滤时一并去掉引用被过滤列的索引"）。PRIMARY KEY 类型的约束理论上已经在
// primaryKeyReferencesDropped 那一步被拦截返回阻断，走不到这里；这里仍然
// 保留 PRIMARY KEY 分支只是防御性的（万一调用方绕过检查直接调用本函数），
// 不代表这条路径预期会被触发。
func filterConstraints(constraints []*ast.Constraint, dropSet map[string]bool) []*ast.Constraint {
	kept := constraints[:0:0]
	for _, cst := range constraints {
		if constraintReferencesDropped(cst, dropSet) {
			continue
		}
		kept = append(kept, cst)
	}
	return kept
}

func constraintReferencesDropped(cst *ast.Constraint, dropSet map[string]bool) bool {
	// 注意：cst.Refer（外键约束才有）里的 IndexPartSpecifications 是被引用的
	// 外部表的列，不是本表的列，不能拿来跟本表的 dropSet 比较——本表这一侧
	// 引用的列已经在 cst.Keys 里，下面这个循环已经覆盖，不需要也不应该再看
	// cst.Refer。
	for _, key := range cst.Keys {
		if key.Column != nil && dropSet[foldKey(key.Column.Name.O)] {
			return true
		}
		if key.Expr != nil {
			for name := range collectColumnNames(key.Expr) {
				if dropSet[name] {
					return true
				}
			}
		}
	}

	if cst.Tp == ast.ConstraintCheck && cst.Expr != nil {
		for name := range collectColumnNames(cst.Expr) {
			if dropSet[name] {
				return true
			}
		}
	}
	return false
}

// columnNameCollector 是 ast.Visitor，收集表达式树里出现的全部列名
// （*ast.ColumnNameExpr），用于判断 CHECK 约束表达式是否引用了某个列。
type columnNameCollector struct {
	names map[string]bool
}

func collectColumnNames(expr ast.ExprNode) map[string]bool {
	c := &columnNameCollector{names: map[string]bool{}}
	expr.Accept(c)
	return c.names
}

func (v *columnNameCollector) Enter(n ast.Node) (ast.Node, bool) {
	if cn, ok := n.(*ast.ColumnNameExpr); ok {
		v.names[foldKey(cn.Name.Name.O)] = true
	}
	return n, false
}

func (v *columnNameCollector) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}

// filterTableOptions 删除 strip 命中的表选项类型。
func filterTableOptions(opts []*ast.TableOption, strip map[ast.TableOptionType]bool) []*ast.TableOption {
	kept := opts[:0:0]
	for _, opt := range opts {
		if strip[opt.Tp] {
			continue
		}
		kept = append(kept, opt)
	}
	return kept
}
