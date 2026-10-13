package iproto

import (
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/xmapst/xdts/internal/schemamig"
)

// CompileMapping creates an independent mapping from source identities. Column
// names are always case insensitive; database/table names follow source caps.
func CompileMapping(rules *MappingRules, sensitive bool) (schemamig.Mapping, error) {
	m := schemamig.Mapping{Schemas: map[string]string{}, Tables: map[schemamig.SchemaTable]string{}, Columns: map[schemamig.SchemaTable]map[string]string{}}
	errInvalid := errors.New("invalid or ambiguous mapping rules")
	valid := func(name string) bool {
		return name != "" && utf8.ValidString(name) && utf8.RuneCountInString(name) <= 64 && !strings.ContainsRune(name, 0)
	}
	fold := func(name string) string {
		if sensitive {
			return name
		}
		return strings.ToLower(name)
	}
	system := func(name string) bool {
		switch strings.ToLower(name) {
		case "mysql", "information_schema", "performance_schema", "sys", ReservedLineageSchema:
			return true
		}
		return false
	}
	schemaSources := map[string]bool{}
	for _, r := range rules.GetSchemas() {
		if r == nil || !valid(r.SourceSchema) || !valid(r.TargetSchema) || system(r.SourceSchema) || system(r.TargetSchema) {
			return m, errInvalid
		}
		key := fold(r.SourceSchema)
		if schemaSources[key] {
			return m, errInvalid
		}
		schemaSources[key] = true
		m.Schemas[r.SourceSchema] = r.TargetSchema
	}
	seenTargets := map[schemamig.SchemaTable]bool{}
	tableSources := map[schemamig.SchemaTable]bool{}
	for _, r := range rules.GetTables() {
		if r == nil || !valid(r.SourceSchema) || system(r.SourceSchema) || !valid(r.SourceTable) || !valid(r.TargetTable) {
			return m, errInvalid
		}
		key := schemamig.SchemaTable{Schema: fold(r.SourceSchema), Table: fold(r.SourceTable)}
		if tableSources[key] {
			return m, errInvalid
		}
		tableSources[key] = true
		destination, _, _ := m.TargetNames(r.SourceSchema, r.SourceTable, nil, sensitive)
		target := schemamig.SchemaTable{Schema: fold(destination), Table: fold(r.TargetTable)}
		if seenTargets[target] {
			return m, errInvalid
		}
		seenTargets[target] = true
		m.Tables[schemamig.SchemaTable{Schema: r.SourceSchema, Table: r.SourceTable}] = r.TargetTable
	}
	columnTables := map[schemamig.SchemaTable]schemamig.SchemaTable{}
	for _, r := range rules.GetColumns() {
		if r == nil || !valid(r.SourceSchema) || system(r.SourceSchema) || !valid(r.SourceTable) || !valid(r.SourceColumn) || !valid(r.TargetColumn) {
			return m, errInvalid
		}
		folded := schemamig.SchemaTable{Schema: fold(r.SourceSchema), Table: fold(r.SourceTable)}
		ref, exists := columnTables[folded]
		if !exists {
			ref = schemamig.SchemaTable{Schema: r.SourceSchema, Table: r.SourceTable}
			columnTables[folded] = ref
		} else if ref.Schema != r.SourceSchema || ref.Table != r.SourceTable {
			// A saved mapping must remain deterministic when projected to a
			// source with case sensitive database/table names.
			return m, errInvalid
		}
		if m.Columns[ref] == nil {
			m.Columns[ref] = map[string]string{}
		}
		key := strings.ToLower(r.SourceColumn)
		if _, exists := m.Columns[ref][key]; exists {
			return m, errInvalid
		}
		for _, name := range m.Columns[ref] {
			if strings.EqualFold(name, r.TargetColumn) {
				return m, errInvalid
			}
		}
		m.Columns[ref][key] = r.TargetColumn
	}
	return m, nil
}
