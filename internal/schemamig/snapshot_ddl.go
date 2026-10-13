package schemamig

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// SnapshotDDLCompatible accepts only changes for which the immutable chunk
// boundaries and row identity remain valid. Unknown changes stop the run.
func SnapshotDDLCompatible(ddl string) (bool, string) {
	stmt, err := parseOneTiDBStmt(ddl)
	if err != nil {
		return false, "全量期间 DDL 无法解析，请对受影响表重新全量"
	}
	switch s := stmt.(type) {
	case *ast.AnalyzeTableStmt:
		return true, "" // Statistics do not change the row or chunk identity.
	case *ast.CreateIndexStmt:
		if s.KeyType == ast.IndexKeyTypeUnique {
			return false, "全量期间新增唯一索引，请对该表重新全量"
		}
		return true, ""
	case *ast.DropIndexStmt:
		return true, ""
	case *ast.AlterTableStmt:
		for _, spec := range s.Specs {
			switch spec.Tp {
			case ast.AlterTableAddColumns:
				for _, col := range spec.NewColumns {
					notNull, hasDefault := false, false
					for _, option := range col.Options {
						switch option.Tp {
						case ast.ColumnOptionNotNull:
							notNull = true
						case ast.ColumnOptionDefaultValue:
							hasDefault = true
						case ast.ColumnOptionPrimaryKey, ast.ColumnOptionUniqKey:
							return false, "全量期间新增唯一定位列，请重新全量"
						}
					}
					if notNull && !hasDefault {
						return false, "全量期间新增非空且无默认值列，请重新全量"
					}
				}
			case ast.AlterTableDropColumn, ast.AlterTableDropIndex, ast.AlterTableLock, ast.AlterTableAlgorithm:
			case ast.AlterTableAddConstraint:
				if spec.Constraint == nil || spec.Constraint.Tp != ast.ConstraintKey && spec.Constraint.Tp != ast.ConstraintIndex {
					return false, "全量期间新增唯一索引或约束，请重新全量"
				}
			default:
				return false, fmt.Sprintf("全量期间结构变更不兼容（%v），请对受影响表重新全量", spec.Tp)
			}
		}
		return true, ""
	default:
		return false, "全量期间表重命名、重建或其他 DDL 不兼容，请对受影响表重新全量"
	}
}
