package schemaplan

import (
	"sort"
	"strings"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/schemamig"
)

func (p *Plan) splitForeignKeys(targets map[string]Definition, sensitive bool) {
	selected := map[string]int{}
	for i, s := range p.Steps {
		if s.Kind == model.ObjectTypeTable {
			selected[relationKey(s.Schema, s.Name, sensitive)] = i
		}
	}
	deferred := []Step{}
	symbols := map[string][]int{}
	targetSymbols := map[string]string{}
	for _, target := range targets {
		if target.Kind != model.ObjectTypeTable {
			continue
		}
		_, keys, err := schemamig.SplitTableForeignKeys(target.Schema, target.DDL)
		if err != nil {
			continue // Selected target definitions are checked below.
		}
		for _, key := range keys {
			targetSymbols[objectKey(model.ObjectTypeForeignKey, key.Schema, key.Name, sensitive)] = relationKey(target.Schema, target.Name, sensitive)
		}
	}
	for i := range p.Steps {
		s := &p.Steps[i]
		if s.Kind != model.ObjectTypeTable || s.Action == model.MigrationActionBlock {
			continue
		}
		base, keys, err := schemamig.SplitTableForeignKeys(s.Schema, s.DDL)
		if err != nil {
			s.Action = model.MigrationActionBlock
			s.BlockReason = "无法安全拆分外键定义"
			continue
		}
		original := s.DDL
		s.DDL = base
		actualKeys := map[string]schemamig.ForeignKey{}
		if target, ok := targets[objectKey(model.ObjectTypeTable, s.Schema, s.Name, sensitive)]; ok {
			_, all, err := schemamig.SplitTableForeignKeys(s.Schema, target.DDL)
			if err != nil {
				s.Action = model.MigrationActionBlock
				s.BlockReason = "目标外键定义无法安全解析"
				continue
			}
			for _, k := range all {
				actualKeys[strings.ToLower(k.Name)] = k
			}
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i].Name < keys[j].Name })
		for _, k := range keys {
			step := Step{Kind: model.ObjectTypeForeignKey, SourceSchema: s.SourceSchema, SourceName: k.Name, Schema: s.Schema, Name: k.Name, TableName: s.Name, Action: model.MigrationActionCreate, SourceDDL: s.SourceDDL, DDL: k.DDL, AfterSnapshot: true, Differences: []schemamig.Difference{}}
			ref, ok := selected[relationKey(k.Reference.Schema, k.Reference.Table, sensitive)]
			if !ok || p.Steps[ref].Action == model.MigrationActionBlock {
				step.Action = model.MigrationActionBlock
				step.BlockReason = "外键引用了未选择或被阻断的表: " + k.Reference.Schema + "." + k.Reference.Table
			}
			if actual, ok := actualKeys[strings.ToLower(k.Name)]; ok {
				delete(actualKeys, strings.ToLower(k.Name))
				step.Differences = schemamig.CompareForeignKeys(k, actual)
				if len(step.Differences) != 0 {
					step.Action = model.MigrationActionBlock
					step.BlockReason = "目标同名外键定义不兼容"
				} else if step.Action != model.MigrationActionBlock {
					step.Action = model.MigrationActionSkip
				}
			}
			key := objectKey(model.ObjectTypeForeignKey, step.Schema, step.Name, sensitive)
			if owner, exists := targetSymbols[key]; exists && owner != relationKey(step.Schema, step.TableName, sensitive) {
				step.Action = model.MigrationActionBlock
				step.BlockReason = "目标库同名外键已属于另一张表"
			}
			symbols[key] = append(symbols[key], len(deferred))
			deferred = append(deferred, step)
		}
		if len(actualKeys) > 0 {
			s.Action = model.MigrationActionBlock
			s.BlockReason = "目标存在计划之外的外键约束"
			s.DDL = original
			for _, k := range actualKeys {
				s.Differences = append(s.Differences, schemamig.Difference{Path: "foreign_keys." + k.Name, Expected: "absent", Actual: k.DDL})
			}
			sort.Slice(s.Differences, func(i, j int) bool { return s.Differences[i].Path < s.Differences[j].Path })
		}
	}
	for _, indices := range symbols {
		if len(indices) > 1 {
			for _, i := range indices {
				deferred[i].Action = model.MigrationActionBlock
				deferred[i].BlockReason = "同一目标库内外键约束名称碰撞"
			}
		}
	}
	// A later table may have become blocked when comparing its existing keys.
	for i := range deferred {
		s := &deferred[i]
		fk, err := schemamig.ForeignKeyIdentity(s.Schema, s.DDL)
		own := selected[relationKey(s.Schema, s.TableName, sensitive)]
		ref, ok := selected[relationKey(fk.Reference.Schema, fk.Reference.Table, sensitive)]
		if err != nil || !ok || p.Steps[own].Action == model.MigrationActionBlock || p.Steps[ref].Action == model.MigrationActionBlock {
			s.Action = model.MigrationActionBlock
			s.BlockReason = "外键所属或引用表被阻断"
		}
	}
	// Preserve existing relation indices for the view dependency graph; phases
	// are selected by the runner and exported in execution order.
	p.Steps = append(p.Steps, deferred...)
}
