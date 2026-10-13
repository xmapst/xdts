package schemamig

import "github.com/pingcap/tidb/pkg/parser/ast"

// projectAlterColumns consumes source names. It does not reorder operations;
// mixed statements either project completely or return a blocking reason.
func projectAlterColumns(stmt *ast.AlterTableStmt, drops map[string]bool) string {
	kept := make([]*ast.AlterTableSpec, 0, len(stmt.Specs))
	for _, spec := range stmt.Specs {
		switch spec.Tp {
		case ast.AlterTableAddColumns, ast.AlterTableModifyColumn, ast.AlterTableAlterColumn:
			columns := make([]*ast.ColumnDef, 0, len(spec.NewColumns))
			for _, col := range spec.NewColumns {
				if drops[foldKey(col.Name.Name.O)] {
					for _, option := range col.Options {
						if option.Tp == ast.ColumnOptionPrimaryKey {
							return "主键引用被过滤列，不能安全投影 ALTER"
						}
					}
					continue
				}
				if columnDefinitionReferencesDropped(col, drops) {
					return "保留列的表达式引用被过滤列，不能安全投影 ALTER"
				}
				columns = append(columns, col)
			}
			spec.NewColumns = columns
			for _, constraint := range spec.NewConstraints {
				if constraint.Tp == ast.ConstraintPrimaryKey && constraintReferencesDropped(constraint, drops) {
					return "主键引用被过滤列，不能安全投影 ALTER"
				}
			}
			spec.NewConstraints = filterConstraints(spec.NewConstraints, drops)
			if len(columns)+len(spec.NewConstraints) == 0 {
				continue
			}
		case ast.AlterTableDropColumn:
			if spec.OldColumnName != nil && drops[foldKey(spec.OldColumnName.Name.O)] {
				continue
			}
		case ast.AlterTableRenameColumn, ast.AlterTableChangeColumn:
			if spec.OldColumnName == nil {
				return "列重命名缺少原列身份"
			}
			newName := spec.NewColumnName
			if spec.Tp == ast.AlterTableChangeColumn && len(spec.NewColumns) == 1 {
				newName = spec.NewColumns[0].Name
			}
			if newName == nil {
				return "列重命名缺少新列身份"
			}
			oldDropped, newDropped := drops[foldKey(spec.OldColumnName.Name.O)], drops[foldKey(newName.Name.O)]
			if oldDropped != newDropped {
				return "列重命名跨越列过滤范围，需重新初始化目标结构"
			}
			if oldDropped {
				for _, col := range spec.NewColumns {
					for _, option := range col.Options {
						if option.Tp == ast.ColumnOptionPrimaryKey {
							return "主键引用被过滤列，不能安全投影 ALTER"
						}
					}
				}
				continue
			}
			for _, col := range spec.NewColumns {
				if columnDefinitionReferencesDropped(col, drops) {
					return "保留列的表达式引用被过滤列，不能安全投影 ALTER"
				}
			}
		case ast.AlterTableAddConstraint:
			if spec.Constraint != nil && constraintReferencesDropped(spec.Constraint, drops) {
				if spec.Constraint.Tp == ast.ConstraintPrimaryKey {
					return "主键引用被过滤列，不能安全投影 ALTER"
				}
				continue
			}
		case ast.AlterTableLock, ast.AlterTableAlgorithm:
			// Retained only when at least one actual alteration remains below.
		case ast.AlterTableOption:
			// Table options do not bind column names; preserve their values.
		case ast.AlterTableDropIndex, ast.AlterTableDropPrimaryKey, ast.AlterTableDropForeignKey,
			ast.AlterTableDropCheck, ast.AlterTableAlterCheck, ast.AlterTableRenameIndex, ast.AlterTableIndexInvisible,
			ast.AlterTablePartition, ast.AlterTableRemovePartitioning, ast.AlterTableAddPartitions,
			ast.AlterTableDropPartition, ast.AlterTableTruncatePartition, ast.AlterTableCoalescePartitions,
			ast.AlterTableReorganizePartition, ast.AlterTableRebuildPartition, ast.AlterTableCheckPartitions,
			ast.AlterTableOptimizePartition, ast.AlterTableRepairPartition:
			// Original source dependencies were validated before this pass.
		default:
			// Index/foreign-key drops and partition changes require prior table
			// definitions to distinguish objects omitted by column filtering.
			return "该 ALTER 操作需原表依赖定义才能安全投影列过滤"
		}
		if spec.Position != nil && spec.Position.RelativeColumn != nil && drops[foldKey(spec.Position.RelativeColumn.Name.O)] {
			return "列位置依赖被过滤列，不能安全投影 ALTER"
		}
		kept = append(kept, spec)
	}
	actual := false
	for _, spec := range kept {
		if spec.Tp != ast.AlterTableLock && spec.Tp != ast.AlterTableAlgorithm {
			actual = true
			break
		}
	}
	if !actual {
		kept = nil
	}
	stmt.Specs = kept
	return ""
}

func columnDefinitionReferencesDropped(column *ast.ColumnDef, drops map[string]bool) bool {
	for _, option := range column.Options {
		if option.Expr != nil {
			for name := range collectColumnNames(option.Expr) {
				if drops[name] {
					return true
				}
			}
		}
	}
	return false
}

func referencesExcludedForeignColumn(constraint *ast.Constraint, mapping Mapping, defaultSchema string, sensitive bool) bool {
	ref := constraint.Refer
	if ref == nil || ref.Table == nil {
		return false
	}
	schema := ref.Table.Schema.O
	if schema == "" {
		schema = defaultSchema
	}
	drops := mapping.dropColumnSet(schema, ref.Table.Name.O, sensitive)
	return constraintReferencesDropped(&ast.Constraint{Keys: ref.IndexPartSpecifications}, drops)
}
