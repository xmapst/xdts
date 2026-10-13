package iproto

import (
	"maps"
	"slices"
	"strings"

	"github.com/xmapst/xdts/internal/schemamig"
)

type shadowMappingResolver struct {
	plan            *ShadowTablePlan
	base            schemamig.Mapping
	sourceSensitive bool
}

func (r *shadowMappingResolver) ResolveTable(schema, table string) (string, string, bool) {
	alias, found := r.plan.Match(schema, table)
	if !found || alias.Kind == ShadowTableLog {
		return "", "", false
	}
	_, originalTarget, _ := r.base.TargetNames(schema, alias.OriginalTable, nil, r.sourceSensitive)
	// Compilation validated every fixed alias and the timestamp name length.
	mapped, err := alias.TargetName(originalTarget)
	if err != nil {
		return "", "", false
	}
	return alias.OriginalTable, mapped, true
}

type shadowMappingColumns struct {
	base schemamig.IColumnFilter
	plan *ShadowTablePlan
}

func (p shadowMappingColumns) Allow(schema, table, column string) bool {
	return p.base.Allow(schema, p.plan.LogicalTable(schema, table), column)
}

func (p shadowMappingColumns) FiltersTable(schema, table string) bool {
	return p.base.FiltersTable(schema, p.plan.LogicalTable(schema, table))
}

// CompileShadowMapping owns all mutable mapping containers independently. It
// checks physical target identities before exposing an immutable resolver.
func CompileShadowMapping(rules []*ShadowTableRule, mapping schemamig.Mapping, sourceSensitive, targetSensitive bool) (schemamig.Mapping, error) {
	plan, err := CompileShadowTables(rules, sourceSensitive)
	if err != nil {
		return schemamig.Mapping{}, err
	}
	base := mapping
	base.TableAliases = nil
	base.Schemas = maps.Clone(mapping.Schemas)
	base.Tables = maps.Clone(mapping.Tables)
	base.Columns = make(map[schemamig.SchemaTable]map[string]string, len(mapping.Columns))
	for table, columns := range mapping.Columns {
		base.Columns[table] = maps.Clone(columns)
	}
	base.DropColumns = make(map[schemamig.SchemaTable][]string, len(mapping.DropColumns))
	for table, columns := range mapping.DropColumns {
		base.DropColumns[table] = slices.Clone(columns)
	}
	key := func(schema, table string) string {
		if !targetSensitive {
			schema, table = strings.ToLower(schema), strings.ToLower(table)
		}
		return schema + "\x00" + table
	}
	owners := make(map[string]string)
	add := func(schema, table, owner string) error {
		if !validShadowIdentifier(schema) || !validShadowIdentifier(table) {
			return ErrShadowTableConfig
		}
		name := key(schema, table)
		if previous, found := owners[name]; found && previous != owner {
			return ErrShadowTableConfig
		}
		owners[name] = owner
		return nil
	}
	for _, spec := range plan.specs {
		schema, table, _ := base.TargetNames(spec.schema, spec.table, nil, sourceSensitive)
		if err := add(schema, table, plan.key(spec.schema, spec.table)); err != nil {
			return schemamig.Mapping{}, err
		}
	}
	for _, alias := range plan.aliases {
		schema, table, _ := base.TargetNames(alias.Schema, alias.OriginalTable, nil, sourceSensitive)
		physical, err := alias.TargetName(table)
		if err != nil {
			return schemamig.Mapping{}, err
		}
		if err := add(schema, physical, plan.key(alias.Schema, alias.PhysicalTable)); err != nil {
			return schemamig.Mapping{}, err
		}
	}
	// A timestamped old table's suffix varies but its truncation length is
	// fixed. Check both valid UTF-8 derivation and target pattern ownership.
	timestamps := make(map[string]bool)
	for _, spec := range plan.specs {
		if spec.tool != ShadowTableTool_SHADOW_TABLE_TOOL_GHOST || spec.oldCustom {
			continue
		}
		schema, table, _ := base.TargetNames(spec.schema, spec.table, nil, sourceSensitive)
		prototype := ShadowTableAlias{Tool: spec.tool, prefix: "_", suffix: "_20261010090000_del"}
		physical, err := prototype.TargetName(table)
		if err != nil {
			return schemamig.Mapping{}, err
		}
		pattern := key(schema, physical)
		if timestamps[pattern] {
			return schemamig.Mapping{}, ErrShadowTableConfig
		}
		timestamps[pattern] = true
	}
	out := base
	if len(rules) != 0 {
		out.TableAliases = &shadowMappingResolver{plan: plan, base: base, sourceSensitive: sourceSensitive}
		if base.ColumnFilter != nil {
			out.ColumnFilter = shadowMappingColumns{base: base.ColumnFilter, plan: plan}
		}
	}
	return out, nil
}
