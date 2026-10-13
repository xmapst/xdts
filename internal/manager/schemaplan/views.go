package schemaplan

import (
	"slices"
	"sort"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/schemamig"
)

func (p *Plan) orderViews(req Request, dependencies map[int][]int) {
	columns := map[string][]string{}
	functions := map[string]int{}
	for _, def := range req.Source {
		if def.Kind == model.ObjectTypeView {
			columns[relationKey(def.Schema, def.Name, req.Options.SourceCaseSensitive)] = def.ViewColumns
		}
	}
	for i, s := range p.Steps {
		if s.Kind == model.ObjectTypeFunction {
			functions[objectKey(s.Kind, s.SourceSchema, s.SourceName, req.Options.SourceCaseSensitive)] = i
		}
	}
	placeholder := map[int]bool{}
	for i := range p.Steps {
		s := &p.Steps[i]
		if s.Kind != model.ObjectTypeView || s.Action == model.MigrationActionBlock {
			continue
		}
		refs, err := schemamig.ViewFunctionReferences(s.SourceSchema, s.SourceDDL)
		if err != nil {
			s.Action, s.BlockReason = model.MigrationActionBlock, "视图函数依赖无法安全解析"
			continue
		}
		for _, ref := range refs {
			if dep, ok := functions[objectKey(model.ObjectTypeFunction, ref.Schema, ref.Table, req.Options.SourceCaseSensitive)]; ok {
				switch p.Steps[dep].Action {
				case model.MigrationActionBlock:
					s.Action, s.BlockReason = model.MigrationActionBlock, "视图依赖的存储函数被阻断"
				case model.MigrationActionCreate:
					placeholder[i] = true
				}
			} else if ref.Qualified {
				s.Action, s.BlockReason = model.MigrationActionBlock, "视图引用未选择的存储函数: "+ref.Schema+"."+ref.Table
			}
		}
	}
	state := make([]uint8, len(p.Steps))
	var stack, ordered []int
	var visit func(int)
	visit = func(i int) {
		if state[i] == 2 {
			return
		}
		if state[i] == 1 {
			for _, s := range slices.Backward(stack) {
				placeholder[s] = true
				if s == i {
					break
				}
			}
			return
		}
		state[i] = 1
		stack = append(stack, i)
		for _, dep := range dependencies[i] {
			visit(dep)
		}
		stack = stack[:len(stack)-1]
		state[i] = 2
		ordered = append(ordered, i)
	}
	for i := range p.Steps {
		visit(i)
	}
	// A view built against temporary output also needs a final replacement.
	for changed := true; changed; {
		changed = false
		for i, deps := range dependencies {
			for _, dep := range deps {
				if p.Steps[dep].Action == model.MigrationActionBlock && p.Steps[i].Action != model.MigrationActionBlock {
					p.Steps[i].Action, p.Steps[i].BlockReason = model.MigrationActionBlock, "视图依赖对象被阻断: "+p.Steps[dep].SourceSchema+"."+p.Steps[dep].SourceName
					changed = true
				}
				if placeholder[dep] && !placeholder[i] {
					placeholder[i], changed = true, true
				}
			}
		}
	}
	var initial, replacements []Step
	for _, i := range ordered {
		s := p.Steps[i]
		if placeholder[i] && s.Kind == model.ObjectTypeView && s.Action == model.MigrationActionCreate {
			temporary, final, err := schemamig.ViewPlaceholder(s.DDL, columns[relationKey(s.SourceSchema, s.SourceName, req.Options.SourceCaseSensitive)])
			if err != nil {
				s.Action, s.BlockReason = model.MigrationActionBlock, "视图占位缺少有效的源端实际列名"
			} else {
				replace := s
				replace.DDL = final
				s.DDL, s.ViewPlaceholder, s.ViewReplacementDDL = temporary, true, final
				replacements = append(replacements, replace)
			}
		}
		initial = append(initial, s)
	}
	// Placeholder construction can itself block: propagate that to consumers.
	blocked := map[string]bool{}
	for _, s := range initial {
		if s.Action == model.MigrationActionBlock && (s.Kind == model.ObjectTypeView || s.Kind == model.ObjectTypeTable) {
			blocked[relationKey(s.SourceSchema, s.SourceName, req.Options.SourceCaseSensitive)] = true
		}
	}
	for changed := true; changed; {
		changed = false
		for i := range initial {
			s := &initial[i]
			if s.Kind != model.ObjectTypeView || s.Action == model.MigrationActionBlock {
				continue
			}
			refs, _ := schemamig.ViewDependencies(s.SourceSchema, s.SourceDDL)
			for _, ref := range refs {
				if blocked[relationKey(ref.Schema, ref.Table, req.Options.SourceCaseSensitive)] {
					s.Action, s.BlockReason = model.MigrationActionBlock, "视图依赖对象被阻断"
					blocked[relationKey(s.SourceSchema, s.SourceName, req.Options.SourceCaseSensitive)] = true
					changed = true
				}
			}
		}
	}
	sort.SliceStable(initial, func(i, j int) bool { return rank(initial[i].Kind) < rank(initial[j].Kind) })
	// Final views follow routines and precede deferred constraints/triggers.
	cut := len(initial)
	for i, s := range initial {
		if rank(s.Kind) > 3 {
			cut = i
			break
		}
	}
	p.Steps = append(append(initial[:cut:cut], replacements...), initial[cut:]...)
}

func (p *Plan) bindViewReplacements() {
	for i := range p.Steps {
		s := &p.Steps[i]
		if !s.ViewPlaceholder {
			continue
		}
		for j := i + 1; j < len(p.Steps); j++ {
			final := &p.Steps[j]
			if final.Kind == model.ObjectTypeView && !final.ViewPlaceholder && final.Schema == s.Schema && final.Name == s.Name && final.DDL == s.ViewReplacementDDL {
				s.ViewReplacementSeq = final.Seq
				final.ViewPlaceholderSeq, final.ViewPlaceholderDDL = s.Seq, s.DDL
				break
			}
		}
	}
}
