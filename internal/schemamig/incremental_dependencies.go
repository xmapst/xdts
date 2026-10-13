package schemamig

import "github.com/pingcap/tidb/pkg/parser/ast"

func dependencyExcluded(obj DDLDependency, drops map[string]bool) bool {
	for _, column := range obj.Columns {
		if drops[foldKey(column)] {
			return true
		}
	}
	return false
}

// Resolve named operations before ordinary projection, updating one private
// source model for each original clause in its original order.
func projectAlterDependencies(stmt *ast.AlterTableStmt, original *TableDependencies, drops map[string]bool) string {
	kept := make([]*ast.AlterTableSpec, 0, len(stmt.Specs))
	for _, spec := range stmt.Specs {
		kind, name := "", ""
		switch spec.Tp {
		case ast.AlterTableDropIndex:
			kind, name = "index", spec.Name
		case ast.AlterTableDropPrimaryKey:
			kind, name = "index", "PRIMARY"
		case ast.AlterTableDropForeignKey:
			kind, name = "foreign_key", spec.Name
		case ast.AlterTableDropCheck, ast.AlterTableAlterCheck:
			kind, name = "check", spec.Name
			if spec.Constraint != nil {
				name = spec.Constraint.Name
			}
		case ast.AlterTableRenameIndex:
			kind, name = "index", spec.FromKey.O
		case ast.AlterTableIndexInvisible:
			kind, name = "index", spec.IndexName.O
		}
		include := true
		if kind != "" {
			if original == nil || !original.Complete {
				return "该 ALTER 操作需原表依赖定义才能安全投影列过滤"
			}
			index := original.object(kind, name)
			if index < 0 {
				return "ALTER 的具名对象不在原表依赖中"
			}
			include = !dependencyExcluded(original.Objects[index], drops)
			if kind == "index" && name == "PRIMARY" && !include {
				return "主键引用被过滤列，不能安全投影 ALTER"
			}
		}
		switch spec.Tp {
		case ast.AlterTablePartition, ast.AlterTableRemovePartitioning, ast.AlterTableAddPartitions,
			ast.AlterTableDropPartition, ast.AlterTableTruncatePartition, ast.AlterTableCoalescePartitions,
			ast.AlterTableReorganizePartition, ast.AlterTableRebuildPartition, ast.AlterTableCheckPartitions,
			ast.AlterTableOptimizePartition, ast.AlterTableRepairPartition:
			if original == nil || !original.Complete {
				return "分区操作需原表列依赖定义"
			}
			for _, column := range original.PartitionColumns {
				if drops[foldKey(column)] {
					return "分区引用被过滤列，不能安全投影 ALTER"
				}
			}
			if spec.Partition != nil {
				for _, column := range dependencyColumns(spec.Partition) {
					if drops[foldKey(column)] {
						return "新分区引用被过滤列，不能安全投影 ALTER"
					}
				}
			}
		}
		if original != nil {
			text, err := restoreTiDB(&ast.AlterTableStmt{Table: stmt.Table, Specs: []*ast.AlterTableSpec{spec}})
			if err != nil {
				return "无法维护原表依赖"
			}
			next := AdvanceTableDependencies(*original, text)
			*original = next
		}
		if include {
			kept = append(kept, spec)
		}
	}
	stmt.Specs = kept
	return ""
}
