package schemaplan

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/schemamig"
)

type Definition struct {
	Schema           string           `json:"schema"`
	Name             string           `json:"name"`
	Kind             model.ObjectType `json:"kind"`
	DDL              string           `json:"ddl"`
	ViewColumns      []string         `json:"viewColumns,omitempty"`
	TableCollationV1 string           `json:"tableCollationV1,omitempty"`
}
type Request struct {
	Source              []Definition
	Target              []Definition
	Mapping             schemamig.Mapping
	Options             schemamig.Options
	TargetCaseSensitive bool
	// nil preserves the caller's explicit per-object Options. A populated map
	// contains only account existence positively verified by the target node.
	DefinerAccounts map[string]bool
}
type Step struct {
	Seq                uint32                 `json:"seq"`
	Kind               model.ObjectType       `json:"kind"`
	SourceSchema       string                 `json:"sourceSchema"`
	SourceName         string                 `json:"sourceName"`
	Schema             string                 `json:"schema"`
	Name               string                 `json:"name"`
	Action             model.MigrationAction  `json:"action"`
	SourceDDL          string                 `json:"sourceDdl"`
	DDL                string                 `json:"ddl"`
	Differences        []schemamig.Difference `json:"differences"`
	BlockReason        string                 `json:"blockReason,omitempty"`
	NeedsDelimiter     bool                   `json:"needsDelimiter"`
	AfterSnapshot      bool                   `json:"afterSnapshot"`
	TableName          string                 `json:"tableName,omitempty"`
	ViewPlaceholder    bool                   `json:"viewPlaceholder,omitempty"`
	ViewReplacementSeq uint32                 `json:"viewReplacementSeq,omitempty"`
	ViewReplacementDDL string                 `json:"viewReplacementDdl,omitempty"`
	ViewPlaceholderSeq uint32                 `json:"viewPlaceholderSeq,omitempty"`
	ViewPlaceholderDDL string                 `json:"viewPlaceholderDdl,omitempty"`
}
type Plan struct {
	Steps   []Step `json:"steps"`
	Blocked bool   `json:"blocked"`
}

func Build(req Request) (Plan, error) {
	if len(req.Source) == 0 || len(req.Source) > 4000 || len(req.Target) > 4000 {
		return Plan{}, errors.New("invalid migration object count")
	}
	source := append([]Definition(nil), req.Source...)
	sort.Slice(source, func(i, j int) bool {
		a, b := source[i], source[j]
		if rank(a.Kind) != rank(b.Kind) {
			return rank(a.Kind) < rank(b.Kind)
		}
		if a.Schema != b.Schema {
			return a.Schema < b.Schema
		}
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		return a.Kind < b.Kind
	})
	srcSeen := map[string]bool{}
	targets := map[string]Definition{}
	for _, def := range req.Target {
		if err := validDefinition(def); err != nil {
			return Plan{}, err
		}
		key := objectKey(def.Kind, def.Schema, def.Name, req.TargetCaseSensitive)
		if _, ok := targets[key]; ok {
			return Plan{}, errors.New("ambiguous target object identity")
		}
		targets[key] = def
	}
	plan := Plan{Steps: make([]Step, 0, len(source))}
	mapped := map[string][]int{}
	selectedRelations := map[string]int{}
	for _, def := range source {
		if err := validDefinition(def); err != nil {
			return Plan{}, err
		}
		srcKey := objectKey(def.Kind, def.Schema, def.Name, req.Options.SourceCaseSensitive)
		if srcSeen[srcKey] {
			return Plan{}, errors.New("duplicate source object identity")
		}
		srcSeen[srcKey] = true
		schema, name, _ := req.Mapping.TargetNames(def.Schema, def.Name, nil, req.Options.SourceCaseSensitive)
		if def.Kind == model.ObjectTypeSchema {
			name = schema
		}
		if !validName(schema) || !validName(name) {
			return Plan{}, errors.New("invalid mapped object name")
		}
		step := Step{Kind: def.Kind, SourceSchema: def.Schema, SourceName: def.Name, Schema: schema, Name: name, Action: model.MigrationActionCreate, SourceDDL: def.DDL, Differences: []schemamig.Difference{}, AfterSnapshot: def.Kind == model.ObjectTypeTrigger || def.Kind == model.ObjectTypeForeignKey || def.Kind == model.ObjectTypeEvent}
		opts := req.Options
		opts.SourceDefaultSchema = def.Schema
		if req.DefinerAccounts != nil {
			account, accountErr := schemamig.DefinitionDefiner(rewriteKind(def.Kind), def.DDL)
			if accountErr != nil {
				return Plan{}, errors.New("invalid definition definer")
			}
			opts.DefinerAccountExists = account == nil || account.CurrentUser || req.DefinerAccounts[account.Key()]
		}
		var result schemamig.Result
		var err error
		rewriteDDL := def.DDL
		if def.Kind == model.ObjectTypeTable {
			rewriteDDL, err = schemamig.WithTableCollation(def.DDL, def.TableCollationV1)
			if err != nil {
				return Plan{}, err
			}
		}
		if def.Kind == model.ObjectTypeSchema {
			var rewritten schemamig.IncrementalResult
			rewritten, err = schemamig.RewriteIncremental(schemamig.IncrementalRequest{Schema: def.Schema, DDL: def.DDL, Mapping: req.Mapping, Options: opts})
			result = rewritten.Result
		} else {
			result, err = schemamig.Rewrite(schemamig.Request{Kind: rewriteKind(def.Kind), DefaultSchema: def.Schema, DDL: rewriteDDL, Mapping: req.Mapping, Options: opts})
		}
		if err != nil {
			step.Action = model.MigrationActionBlock
			step.BlockReason = "定义类型与对象不匹配或改写参数不合法"
		} else if result.Blocked {
			step.Action = model.MigrationActionBlock
			step.BlockReason = result.Reason
		} else {
			step.DDL = result.DDL
			step.NeedsDelimiter = result.NeedsDelimiter
			if def.Kind == model.ObjectTypeTable {
				if _, parseErr := schemamig.CompareDefinitions(schemamig.ObjectTable, step.DDL, step.DDL); parseErr != nil {
					step.Action = model.MigrationActionBlock
					step.BlockReason = "映射后表定义不合法或存在重复列"
				}
				if req.Mapping.ColumnFilter != nil {
					originalKey, _, originalErr := schemamig.InitializationTableProperties(def.DDL)
					projectedKey, _, projectedErr := schemamig.InitializationTableProperties(step.DDL)
					if originalErr != nil || projectedErr != nil || originalKey && !projectedKey {
						step.Action = model.MigrationActionBlock
						step.BlockReason = "列过滤移除了完整定位键，不能安全同步目标表"
					}
				}
			}
			if actual, exists := targets[objectKey(def.Kind, schema, name, req.TargetCaseSensitive)]; exists && step.Action != model.MigrationActionBlock {
				switch def.Kind {
				case model.ObjectTypeSchema:
					step.Differences, err = schemamig.CompareDatabaseDefinitions(step.DDL, actual.DDL)
				case model.ObjectTypeTable:
					var expectedBase, actualBase string
					actualBase, err = schemamig.WithTableCollation(actual.DDL, actual.TableCollationV1)
					if err != nil {
						return Plan{}, err
					}
					expectedBase, _, err = schemamig.SplitTableForeignKeys(schema, step.DDL)
					if err == nil {
						actualBase, _, err = schemamig.SplitTableForeignKeys(schema, actualBase)
					}
					if err == nil {
						step.Differences, err = schemamig.CompareDefinitions(schemamig.ObjectTable, expectedBase, actualBase)
					}
				default:
					step.Differences, err = schemamig.CompareDefinitionsInSchema(rewriteKind(def.Kind), schema, step.DDL, actual.DDL)
				}
				if err != nil {
					step.Action = model.MigrationActionBlock
					step.BlockReason = "目标定义无法安全解析"
				} else if len(step.Differences) != 0 {
					step.Action = model.MigrationActionBlock
					step.BlockReason = "目标已存在且定义不兼容"
				} else {
					step.Action = model.MigrationActionSkip
				}
			}
		}
		idx := len(plan.Steps)
		plan.Steps = append(plan.Steps, step)
		key := objectKey(def.Kind, schema, name, req.TargetCaseSensitive)
		mapped[key] = append(mapped[key], idx)
		if def.Kind == model.ObjectTypeTable || def.Kind == model.ObjectTypeView {
			selectedRelations[relationKey(def.Schema, def.Name, req.Options.SourceCaseSensitive)] = idx
		}
	}
	for _, indices := range mapped {
		if len(indices) < 2 {
			continue
		}
		names := make([]string, 0, len(indices))
		for _, i := range indices {
			names = append(names, plan.Steps[i].SourceSchema+"."+plan.Steps[i].SourceName)
		}
		reason := "映射后对象名称碰撞: " + strings.Join(names, ", ")
		for _, i := range indices {
			plan.Steps[i].Action = model.MigrationActionBlock
			plan.Steps[i].BlockReason = reason
		}
	}
	plan.splitForeignKeys(targets, req.TargetCaseSensitive)
	dependencies := map[int][]int{}
	for i := range plan.Steps {
		step := &plan.Steps[i]
		if step.Kind != model.ObjectTypeView || step.Action == model.MigrationActionBlock {
			continue
		}
		refs, err := schemamig.ViewDependencies(step.SourceSchema, step.SourceDDL)
		if err != nil {
			step.Action = model.MigrationActionBlock
			step.BlockReason = "视图依赖无法安全解析"
			continue
		}
		for _, ref := range refs {
			idx, ok := selectedRelations[relationKey(ref.Schema, ref.Table, req.Options.SourceCaseSensitive)]
			if !ok {
				step.Action = model.MigrationActionBlock
				step.BlockReason = "视图引用了未选择的对象: " + ref.Schema + "." + ref.Table
				break
			}
			dependencies[i] = append(dependencies[i], idx)
		}
	}
	plan.orderViews(req, dependencies)
	if len(plan.Steps) > 4000 {
		return Plan{}, errors.New("migration statement count exceeds limit")
	}
	for i := range plan.Steps {
		plan.Steps[i].Seq = uint32(i + 1)
		if plan.Steps[i].Action == model.MigrationActionBlock {
			plan.Blocked = true
		}
	}
	plan.bindViewReplacements()
	return plan, nil
}

func validName(s string) bool {
	return s != "" && utf8.ValidString(s) && utf8.RuneCountInString(s) <= 64 && !strings.ContainsRune(s, 0)
}
func validDefinition(d Definition) error {
	if !d.Kind.Valid() || d.Kind == model.ObjectTypeForeignKey || !validName(d.Schema) || !validName(d.Name) || d.DDL == "" {
		return errors.New("invalid object definition")
	}
	kind, schema, name, err := schemamig.CreationIdentity(d.Schema, d.DDL)
	if err != nil || schema != d.Schema || name != d.Name || kind != rewriteKind(d.Kind) {
		return errors.New("object definition identity mismatch")
	}
	return nil
}
func objectKey(kind model.ObjectType, schema, name string, sensitive bool) string {
	namespace := string(kind)
	if kind == model.ObjectTypeTable || kind == model.ObjectTypeView {
		namespace = "relation"
	}
	if !sensitive {
		schema = strings.ToLower(schema)
	}
	if !sensitive || (kind != model.ObjectTypeTable && kind != model.ObjectTypeView && kind != model.ObjectTypeSchema) {
		name = strings.ToLower(name)
	}
	return namespace + "\x00" + schema + "\x00" + name
}
func relationKey(schema, name string, sensitive bool) string {
	return objectKey(model.ObjectTypeTable, schema, name, sensitive)
}
func rank(k model.ObjectType) int {
	switch k {
	case model.ObjectTypeSchema:
		return 0
	case model.ObjectTypeTable:
		return 1
	case model.ObjectTypeView:
		return 2
	case model.ObjectTypeProcedure, model.ObjectTypeFunction:
		return 3
	case model.ObjectTypeEvent:
		return 7
	case model.ObjectTypeForeignKey:
		return 5
	case model.ObjectTypeTrigger:
		return 6
	}
	return 7
}
func rewriteKind(k model.ObjectType) schemamig.ObjectKind {
	switch k {
	case model.ObjectTypeTable:
		return schemamig.ObjectTable
	case model.ObjectTypeView:
		return schemamig.ObjectView
	case model.ObjectTypeProcedure:
		return schemamig.ObjectProcedure
	case model.ObjectTypeFunction:
		return schemamig.ObjectFunction
	case model.ObjectTypeTrigger:
		return schemamig.ObjectTrigger
	case model.ObjectTypeEvent:
		return schemamig.ObjectEvent
	}
	return schemamig.ObjectUnknown
}

// ExportDDL refuses blocked plans. It only exports executable create steps;
// skipped objects cannot accidentally recreate data or reset event state.
func (p Plan) ExportDDL() (string, error) {
	if p.Blocked {
		return "", errors.New("migration plan blocked")
	}
	var out strings.Builder
	for _, step := range p.Steps {
		if step.Action == model.MigrationActionBlock || step.Action != model.MigrationActionCreate && step.Action != model.MigrationActionSkip {
			return "", errors.New("migration plan contains an unexecutable action")
		}
		if step.Action != model.MigrationActionCreate {
			continue
		}
		if step.DDL == "" {
			return "", errors.New("migration statement missing")
		}
		fmt.Fprintf(&out, "-- step %d (%s)\n", step.Seq, step.Kind)
		if step.AfterSnapshot {
			out.WriteString("-- Execute after snapshot convergence.\n")
		}
		if step.NeedsDelimiter {
			out.WriteString(schemamig.WrapDelimiter(step.DDL))
		} else {
			out.WriteString(strings.TrimRight(step.DDL, " \t\r\n;"))
			out.WriteString(";\n")
		}
		out.WriteByte('\n')
	}
	return out.String(), nil
}
