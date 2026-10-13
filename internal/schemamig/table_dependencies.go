package schemamig

import (
	"slices"
	"sort"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// TableDependencies describes source names before projection. A partial history
// must never claim complete dependencies for named ALTERs.
type TableDependencies struct {
	Complete         bool
	Columns          []string
	Objects          []DDLDependency
	PartitionColumns []string
}

type DDLDependency struct {
	Kind       string
	Name       string
	Columns    []string
	Expression bool
}

func (d TableDependencies) Clone() TableDependencies {
	d.Columns = slices.Clone(d.Columns)
	d.PartitionColumns = slices.Clone(d.PartitionColumns)
	d.Objects = slices.Clone(d.Objects)
	for i := range d.Objects {
		d.Objects[i].Columns = slices.Clone(d.Objects[i].Columns)
	}
	return d
}

// CaptureTableDependencies parses a self-contained definition. Unnamed
// constraints whose server-assigned names cannot be proven remain incomplete.
func CaptureTableDependencies(ddl string) TableDependencies {
	create, err := ParseTableDefinition(ddl)
	if err != nil || create.ReferTable != nil || create.Select != nil {
		return TableDependencies{}
	}
	d := TableDependencies{Complete: true}
	for _, col := range create.Cols {
		d.Columns = append(d.Columns, col.Name.Name.O)
		for _, option := range col.Options {
			switch option.Tp {
			case ast.ColumnOptionPrimaryKey:
				d.add(DDLDependency{Kind: "index", Name: "PRIMARY", Columns: []string{col.Name.Name.O}})
			case ast.ColumnOptionUniqKey:
				d.add(DDLDependency{Kind: "index", Name: col.Name.Name.O, Columns: []string{col.Name.Name.O}})
			case ast.ColumnOptionCheck:
				d.Complete = false
			}
		}
	}
	for _, c := range create.Constraints {
		d.addConstraint(c)
	}
	if create.Partition != nil {
		d.PartitionColumns = dependencyColumns(create.Partition)
		if len(d.PartitionColumns) == 0 {
			d.Complete = false
		}
	}
	d.checkForeignIndexes()
	return d
}

// InnoDB may create an implicit supporting index. Its exact assigned name
// cannot be inferred from a CREATE/ADD FOREIGN KEY without an explicit key.
func (d *TableDependencies) checkForeignIndexes() {
	for _, foreign := range d.Objects {
		if foreign.Kind != "foreign_key" {
			continue
		}
		covered := false
		for _, index := range d.Objects {
			if index.Kind != "index" || index.Expression || len(index.Columns) < len(foreign.Columns) {
				continue
			}
			matches := true
			for i, column := range foreign.Columns {
				if !strings.EqualFold(index.Columns[i], column) {
					matches = false
					break
				}
			}
			covered = covered || matches
		}
		if !covered {
			d.Complete = false
		}
	}
}

func dependencyColumns(node ast.Node) []string {
	if node == nil {
		return nil
	}
	v := &dependencyColumnCollector{names: map[string]bool{}}
	node.Accept(v)
	names := v.names
	out := make([]string, 0, len(names))
	for name := range names {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

type dependencyColumnCollector struct{ names map[string]bool }

func (v *dependencyColumnCollector) Enter(n ast.Node) (ast.Node, bool) {
	if c, ok := n.(*ast.ColumnName); ok {
		v.names[foldKey(c.Name.O)] = true
	}
	return n, false
}
func (v *dependencyColumnCollector) Leave(n ast.Node) (ast.Node, bool) { return n, true }

func (d *TableDependencies) add(obj DDLDependency) {
	if obj.Name == "" {
		d.Complete = false
		return
	}
	for _, previous := range d.Objects {
		if previous.Kind == obj.Kind && strings.EqualFold(previous.Name, obj.Name) {
			d.Complete = false
			return
		}
	}
	d.Objects = append(d.Objects, obj)
}

func (d *TableDependencies) addConstraint(c *ast.Constraint) {
	if c == nil {
		return
	}
	obj := DDLDependency{Kind: "index", Name: c.Name}
	switch c.Tp {
	case ast.ConstraintPrimaryKey:
		obj.Name = "PRIMARY"
	case ast.ConstraintForeignKey:
		obj.Kind = "foreign_key"
	case ast.ConstraintCheck:
		obj.Kind = "check"
	case ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex, ast.ConstraintFulltext:
	default:
		d.Complete = false
		return
	}
	if obj.Kind == "check" {
		obj.Expression = true
		obj.Columns = dependencyColumns(c.Expr)
	} else {
		for _, key := range c.Keys {
			if key.Column != nil {
				obj.Columns = append(obj.Columns, key.Column.Name.O)
			} else if key.Expr != nil {
				obj.Expression = true
				obj.Columns = append(obj.Columns, dependencyColumns(key.Expr)...)
			}
		}
	}
	if obj.Name == "" && obj.Kind == "index" && len(c.Keys) > 0 && c.Keys[0].Column != nil {
		obj.Name = c.Keys[0].Column.Name.O
	}
	d.add(obj)
}

// AdvanceTableDependencies consumes one original DDL and owns its returned
// copy. Unsupported dependency mutations invalidate proof for later ALTERs.
func AdvanceTableDependencies(before TableDependencies, ddl string) TableDependencies {
	d := before.Clone()
	stmt, err := parseOneTiDBStmt(ddl)
	if err != nil {
		d.Complete = false
		return d
	}
	switch s := stmt.(type) {
	case *ast.CreateIndexStmt:
		d.addConstraint(&ast.Constraint{Tp: ast.ConstraintIndex, Name: s.IndexName, Keys: s.IndexPartSpecifications})
	case *ast.DropIndexStmt:
		d.remove("index", s.IndexName)
	case *ast.RenameTableStmt:
		// Relation identity is maintained by the owning Tracker.
	case *ast.AlterTableStmt:
		for _, spec := range s.Specs {
			switch spec.Tp {
			case ast.AlterTableAddConstraint:
				d.addConstraint(spec.Constraint)
			case ast.AlterTableDropIndex:
				d.remove("index", spec.Name)
			case ast.AlterTableDropPrimaryKey:
				d.remove("index", "PRIMARY")
			case ast.AlterTableDropForeignKey:
				d.remove("foreign_key", spec.Name)
			case ast.AlterTableDropCheck:
				name := spec.Name
				if spec.Constraint != nil {
					name = spec.Constraint.Name
				}
				d.remove("check", name)
			case ast.AlterTableRenameIndex:
				if i := d.object("index", spec.FromKey.O); i >= 0 {
					d.Objects[i].Name = spec.ToKey.O
				} else {
					d.Complete = false
				}
			case ast.AlterTableLock, ast.AlterTableAlgorithm, ast.AlterTableOption, ast.AlterTableForce,
				ast.AlterTableIndexInvisible, ast.AlterTableAlterCheck, ast.AlterTableRenameTable,
				ast.AlterTableEnableKeys, ast.AlterTableDisableKeys, ast.AlterTableAlterColumn:
			case ast.AlterTableAddColumns:
				for _, col := range spec.NewColumns {
					d.Columns = append(d.Columns, col.Name.Name.O)
					for _, option := range col.Options {
						if option.Tp == ast.ColumnOptionPrimaryKey || option.Tp == ast.ColumnOptionUniqKey || option.Tp == ast.ColumnOptionCheck {
							d.Complete = false
						}
					}
				}
				for _, c := range spec.NewConstraints {
					d.addConstraint(c)
				}
			case ast.AlterTableModifyColumn:
				// A modified inline constraint needs the server's resulting names.
				for _, col := range spec.NewColumns {
					for _, option := range col.Options {
						if option.Tp == ast.ColumnOptionPrimaryKey || option.Tp == ast.ColumnOptionUniqKey || option.Tp == ast.ColumnOptionCheck {
							d.Complete = false
						}
					}
				}
			case ast.AlterTableDropColumn:
				if spec.OldColumnName == nil {
					d.Complete = false
					continue
				}
				name := spec.OldColumnName.Name.O
				for _, obj := range d.Objects {
					for _, c := range obj.Columns {
						if strings.EqualFold(c, name) {
							// A projected composite index may never have existed on
							// the target. Removing a source column does not establish
							// that target's earlier dependency history.
							d.Complete = false
						}
					}
				}
				for _, c := range d.PartitionColumns {
					if strings.EqualFold(c, name) {
						d.Complete = false
					}
				}
				d.Columns = slices.DeleteFunc(d.Columns, func(c string) bool { return strings.EqualFold(c, name) })
				for i := range d.Objects {
					d.Objects[i].Columns = slices.DeleteFunc(d.Objects[i].Columns, func(c string) bool { return strings.EqualFold(c, name) })
				}
				d.Objects = slices.DeleteFunc(d.Objects, func(obj DDLDependency) bool { return obj.Kind == "index" && !obj.Expression && len(obj.Columns) == 0 })
			case ast.AlterTableChangeColumn, ast.AlterTableRenameColumn:
				if spec.OldColumnName == nil {
					d.Complete = false
					continue
				}
				newName := spec.NewColumnName
				if spec.Tp == ast.AlterTableChangeColumn && len(spec.NewColumns) == 1 {
					newName = spec.NewColumns[0].Name
					for _, option := range spec.NewColumns[0].Options {
						if option.Tp == ast.ColumnOptionPrimaryKey || option.Tp == ast.ColumnOptionUniqKey || option.Tp == ast.ColumnOptionCheck {
							d.Complete = false
						}
					}
				}
				if newName == nil {
					d.Complete = false
					continue
				}
				d.renameColumn(spec.OldColumnName.Name.O, newName.Name.O)
			case ast.AlterTablePartition:
				if spec.Partition == nil {
					d.Complete = false
					continue
				}
				d.PartitionColumns = dependencyColumns(spec.Partition)
				if len(d.PartitionColumns) == 0 {
					d.Complete = false
				}
			case ast.AlterTableRemovePartitioning:
				d.PartitionColumns = nil
			case ast.AlterTableAddPartitions, ast.AlterTableDropPartition, ast.AlterTableTruncatePartition,
				ast.AlterTableCoalescePartitions, ast.AlterTableReorganizePartition,
				ast.AlterTableRebuildPartition, ast.AlterTableCheckPartitions, ast.AlterTableOptimizePartition,
				ast.AlterTableRepairPartition:
			default:
				d.Complete = false
			}
		}
	default:
		d.Complete = false
	}
	d.checkForeignIndexes()
	return d
}

func (d *TableDependencies) renameColumn(old, name string) {
	rename := func(values []string) {
		for i, c := range values {
			if strings.EqualFold(c, old) {
				values[i] = name
			}
		}
	}
	rename(d.Columns)
	rename(d.PartitionColumns)
	for i := range d.Objects {
		rename(d.Objects[i].Columns)
	}
}

func (d TableDependencies) object(kind, name string) int {
	for i, obj := range d.Objects {
		if obj.Kind == kind && strings.EqualFold(obj.Name, name) {
			return i
		}
	}
	return -1
}
func (d *TableDependencies) remove(kind, name string) {
	if i := d.object(kind, name); i >= 0 {
		d.Objects = slices.Delete(d.Objects, i, i+1)
	} else {
		d.Complete = false
	}
}
