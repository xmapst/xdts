package iproto

import (
	"errors"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/xmapst/xdts/internal/schemamig"
)

func SourceDependencies(schema, table string, d schemamig.TableDependencies) *DDLTableDependencies {
	out := &DDLTableDependencies{Schema: schema, Table: table, Complete: d.Complete, Columns: slices.Clone(d.Columns), PartitionColumns: slices.Clone(d.PartitionColumns)}
	for _, obj := range d.Objects {
		out.Objects = append(out.Objects, &DDLDependency{Kind: obj.Kind, Name: obj.Name, Columns: slices.Clone(obj.Columns), Expression: obj.Expression})
	}
	return out
}

func OriginalDependencies(value *DDLTableDependencies) schemamig.TableDependencies {
	d := schemamig.TableDependencies{Complete: value.GetComplete(), Columns: slices.Clone(value.GetColumns()), PartitionColumns: slices.Clone(value.GetPartitionColumns())}
	for _, obj := range value.GetObjects() {
		d.Objects = append(d.Objects, schemamig.DDLDependency{Kind: obj.GetKind(), Name: obj.GetName(), Columns: slices.Clone(obj.GetColumns()), Expression: obj.GetExpression()})
	}
	return d
}

func ValidateDDLSourceDependencies(ddl *DDL) error {
	values := ddl.GetSourceDependencies()
	if len(values) == 0 {
		return nil
	}
	objects, known := schemamig.DDLObjects(ddl.GetSchema(), ddl.GetStatement())
	if !known || len(values) > 1 || len(objects) == 0 {
		return errors.New("iproto: original dependencies require one DDL table")
	}
	value := values[0]
	equal := strings.EqualFold
	if ddl.GetColumnPolicy().GetSourceCaseSensitive() {
		equal = func(a, b string) bool { return a == b }
	}
	if value == nil || !value.Complete || !validDependencyIdentifier(value.Schema) || !validDependencyIdentifier(value.Table) || !equal(value.Schema, objects[0].Schema) || !equal(value.Table, objects[0].Table) || len(value.Columns) == 0 || len(value.Columns) > 4096 || len(value.Objects) > 4096 || len(value.PartitionColumns) > 4096 {
		return errors.New("iproto: invalid original dependency table")
	}
	columns := map[string]bool{}
	for _, name := range value.Columns {
		key := strings.ToLower(name)
		if !validDependencyIdentifier(name) || columns[key] {
			return errors.New("iproto: invalid original dependency column")
		}
		columns[key] = true
	}
	checkColumns := func(names []string) bool {
		if len(names) > 4096 {
			return false
		}
		for _, name := range names {
			if !columns[strings.ToLower(name)] {
				return false
			}
		}
		return true
	}
	seen := map[string]bool{}
	for _, obj := range value.Objects {
		if obj == nil || (obj.Kind != "index" && obj.Kind != "foreign_key" && obj.Kind != "check") || !validDependencyIdentifier(obj.Name) || !checkColumns(obj.Columns) || (obj.Kind != "check" && len(obj.Columns) == 0 && !obj.Expression) {
			return errors.New("iproto: invalid original named dependency")
		}
		key := obj.Kind + ":" + strings.ToLower(obj.Name)
		if seen[key] {
			return errors.New("iproto: duplicate original dependency")
		}
		seen[key] = true
	}
	if !checkColumns(value.PartitionColumns) {
		return errors.New("iproto: invalid original partition dependency")
	}
	return nil
}
func validDependencyIdentifier(name string) bool {
	return name != "" && utf8.ValidString(name) && utf8.RuneCountInString(name) <= 64 && !strings.ContainsRune(name, 0)
}
