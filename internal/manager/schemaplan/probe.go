package schemaplan

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/rule"
	"github.com/xmapst/xdts/internal/schemamig"
)

// ProbeDefinitions converts actual node output; identity checks happen in Build.
func ProbeDefinitions(result *iproto.DefsResult) ([]Definition, error) {
	out := make([]Definition, 0, len(result.GetEntries()))
	for _, entry := range result.GetEntries() {
		kind, err := ObjectKind(entry.GetKind())
		if err != nil {
			return nil, err
		}
		out = append(out, Definition{Schema: entry.GetSchema(), Name: entry.GetObjectName(), Kind: kind, DDL: entry.GetDefinition(), ViewColumns: append([]string(nil), entry.GetViewColumns()...), TableCollationV1: entry.GetTableCollationV1()})
	}
	return out, nil
}
func ObjectKind(k iproto.SchemaObjectKind) (model.ObjectType, error) {
	switch k {
	case iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_DATABASE:
		return model.ObjectTypeSchema, nil
	case iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_TABLE:
		return model.ObjectTypeTable, nil
	case iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_VIEW:
		return model.ObjectTypeView, nil
	case iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_PROCEDURE:
		return model.ObjectTypeProcedure, nil
	case iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_FUNCTION:
		return model.ObjectTypeFunction, nil
	case iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_TRIGGER:
		return model.ObjectTypeTrigger, nil
	case iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_EVENT:
		return model.ObjectTypeEvent, nil
	case iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_FOREIGN_KEY:
		return model.ObjectTypeForeignKey, nil
	}
	return "", errors.New("unknown definition object kind")
}
func ProtocolKind(k model.ObjectType) (iproto.SchemaObjectKind, error) {
	switch k {
	case model.ObjectTypeSchema:
		return iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_DATABASE, nil
	case model.ObjectTypeTable:
		return iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_TABLE, nil
	case model.ObjectTypeView:
		return iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_VIEW, nil
	case model.ObjectTypeProcedure:
		return iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_PROCEDURE, nil
	case model.ObjectTypeFunction:
		return iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_FUNCTION, nil
	case model.ObjectTypeTrigger:
		return iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_TRIGGER, nil
	case model.ObjectTypeEvent:
		return iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_EVENT, nil
	case model.ObjectTypeForeignKey:
		return iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_FOREIGN_KEY, nil
	}
	return 0, errors.New("unknown migration object kind")
}

// SelectDefinitions evaluates the existing source object policy. Databases are
// included when a selected object requires them; trigger scope is its table.
func SelectDefinitions(all []Definition, filters []*iproto.ObjectFilterRule, sensitive bool) ([]Definition, error) {
	input := rule.RuleSet{}
	for _, f := range filters {
		entry := rule.ObjectFilterRule{SchemaPattern: f.GetSchemaPattern(), TablePattern: f.GetTablePattern()}
		switch f.GetAction() {
		case iproto.FilterAction_FILTER_ACTION_INCLUDE:
			entry.Action = rule.FilterActionInclude
		case iproto.FilterAction_FILTER_ACTION_EXCLUDE:
			entry.Action = rule.FilterActionExclude
		default:
			return nil, errors.New("invalid object filter action")
		}
		convert := func(v iproto.MatchType) (rule.MatchType, error) {
			switch v {
			case iproto.MatchType_MATCH_TYPE_UNSPECIFIED:
				return rule.MatchUnspecified, nil
			case iproto.MatchType_MATCH_TYPE_EXACT:
				return rule.MatchExact, nil
			case iproto.MatchType_MATCH_TYPE_GLOB:
				return rule.MatchGlob, nil
			case iproto.MatchType_MATCH_TYPE_REGEX:
				return rule.MatchRegex, nil
			}
			return 0, errors.New("invalid object match type")
		}
		var err error
		entry.SchemaMatch, err = convert(f.GetSchemaMatch())
		if err != nil {
			return nil, err
		}
		entry.TableMatch, err = convert(f.GetTableMatch())
		if err != nil {
			return nil, err
		}
		input.ObjectFilters = append(input.ObjectFilters, entry)
	}
	policy, err := rule.Compile(input, rule.SideSource, sensitive)
	if err != nil {
		return nil, err
	}
	schemas := map[string]bool{}
	selected := []Definition{}
	for _, def := range all {
		if def.Kind == model.ObjectTypeSchema {
			continue
		}
		name := def.Name
		if def.Kind == model.ObjectTypeTrigger {
			refs, known := schemamig.DDLObjects(def.Schema, def.DDL)
			if !known || len(refs) != 1 {
				return nil, errors.New("invalid trigger table")
			}
			name = refs[0].Table
		}
		if policy.MatchTable(def.Schema, name) {
			selected = append(selected, def)
			schemas[def.Schema] = true
		}
	}
	for _, def := range all {
		if def.Kind == model.ObjectTypeSchema && (schemas[def.Schema] || policy.MatchTable(def.Schema, "")) {
			selected = append(selected, def)
		}
	}
	return selected, nil
}

func MappingFromProtocol(rules *iproto.MappingRules) (schemamig.Mapping, error) {
	return iproto.CompileMapping(rules, false)
}
func TargetSchemas(source []Definition, mapping schemamig.Mapping, sensitive bool) []string {
	seen := map[string]bool{}
	for _, def := range source {
		schema, _, _ := mapping.TargetNames(def.Schema, def.Name, nil, sensitive)
		seen[schema] = true
	}
	out := make([]string, 0, len(seen))
	for schema := range seen {
		out = append(out, schema)
	}
	sort.Strings(out)
	return out
}

// ApplyEnvironmentChecks only blocks; it never changes a server variable.
func (p *Plan) ApplyEnvironmentChecks(target *iproto.DefsResult) {
	trusted := target.GetKeyParams()["log_bin_trust_function_creators"] == "1" || target.GetKeyParams()["log_bin"] == "0"
	super := false
	for _, grant := range target.GetGrants() {
		upper := strings.ToUpper(grant)
		if strings.Contains(upper, " ON *.* TO ") && (strings.HasPrefix(upper, "GRANT ALL PRIVILEGES ON ") || strings.Contains(upper, " SUPER,") || strings.HasPrefix(upper, "GRANT SUPER ") || strings.Contains(upper, ", SUPER ON ")) {
			super = true
		}
	}
	for i := range p.Steps {
		step := &p.Steps[i]
		if step.Kind == model.ObjectTypeFunction && step.Action == model.MigrationActionCreate && !trusted && !super {
			step.Action = model.MigrationActionBlock
			step.BlockReason = "目标启用 binlog 且创建函数条件不满足，请 DBA 配置 log_bin_trust_function_creators 或账号权限"
			p.Blocked = true
		}
	}
}

func (p Plan) Protocol(identity string) (*iproto.SchemaMigrationPlan, error) {
	if p.Blocked {
		return nil, errors.New("cannot publish a blocked plan")
	}
	out := &iproto.SchemaMigrationPlan{SchemaUuid: identity}
	for _, step := range p.Steps {
		kind, err := ProtocolKind(step.Kind)
		if err != nil {
			return nil, err
		}
		action := iproto.SchemaMigrationAction_SCHEMA_MIGRATION_ACTION_CREATE
		if step.Action == model.MigrationActionSkip {
			action = iproto.SchemaMigrationAction_SCHEMA_MIGRATION_ACTION_SKIP
		} else if step.Action != model.MigrationActionCreate {
			return nil, fmt.Errorf("unexecutable action %s", step.Action)
		}
		out.Steps = append(out.Steps, &iproto.SchemaMigrationStep{Seq: uint64(step.Seq), Kind: kind, Schema: step.Schema, ObjectName: step.Name, Action: action, Ddl: step.DDL, AfterSnapshot: step.AfterSnapshot, TableName: step.TableName, ViewPlaceholder: step.ViewPlaceholder, ViewReplacementSeq: uint64(step.ViewReplacementSeq), ViewReplacementDdl: step.ViewReplacementDDL, ViewPlaceholderSeq: uint64(step.ViewPlaceholderSeq), ViewPlaceholderDdl: step.ViewPlaceholderDDL})
	}
	return out, nil
}
