package iproto

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"
)

var ErrShadowTableConfig = errors.New("iproto: invalid or ambiguous shadow table rules")

type ShadowTableKind uint8

const (
	ShadowTableGhost ShadowTableKind = iota + 1
	ShadowTableOld
	ShadowTableLog
)

// ShadowTableAlias carries values only; it never exposes a compiled plan's maps.
type ShadowTableAlias struct {
	Schema        string
	OriginalTable string
	PhysicalTable string
	Tool          ShadowTableTool
	Kind          ShadowTableKind
	prefix        string
	suffix        string
	literal       bool
}

// TargetName uses the original table's mapped identity and the same naming
// operation as the source tool. The caller must check target-name collisions.
func (a ShadowTableAlias) TargetName(originalTarget string) (string, error) {
	if !validShadowIdentifier(originalTarget) || (a.Tool != ShadowTableTool_SHADOW_TABLE_TOOL_GHOST && a.Tool != ShadowTableTool_SHADOW_TABLE_TOOL_PT_OSC) {
		return "", ErrShadowTableConfig
	}
	if a.literal {
		if !validShadowIdentifier(a.PhysicalTable) {
			return "", ErrShadowTableConfig
		}
		return a.PhysicalTable, nil
	}
	if a.prefix == "" || a.suffix == "" {
		return "", ErrShadowTableConfig
	}
	name := a.prefix + originalTarget + a.suffix
	if a.Tool == ShadowTableTool_SHADOW_TABLE_TOOL_GHOST {
		limit := 64 - len(a.prefix) - len(a.suffix)
		if len(originalTarget) > limit {
			name = a.prefix + originalTarget[:limit] + a.suffix
		}
	} else if len(name) > 64 {
		name = name[:64]
	}
	if !validShadowIdentifier(name) {
		return "", ErrShadowTableConfig
	}
	return name, nil
}

type shadowTableSpec struct {
	tool          ShadowTableTool
	schema, table string
	oldCustom     bool
}

// ShadowTablePlan is immutable after compilation. Each caller compiles its own
// copy so source and target runtime owners do not share mutable protocol input.
type ShadowTablePlan struct {
	sensitive bool
	specs     []shadowTableSpec
	aliases   map[string]ShadowTableAlias
}

func validShadowIdentifier(value string) bool {
	return value != "" && utf8.ValidString(value) && utf8.RuneCountInString(value) <= 64 && !strings.ContainsAny(value, "\x00\r\n")
}

func (p *ShadowTablePlan) key(schema, table string) string {
	if !p.sensitive {
		schema, table = strings.ToLower(schema), strings.ToLower(table)
	}
	return schema + "\x00" + table
}

func CompileShadowTables(rules []*ShadowTableRule, sensitive bool) (*ShadowTablePlan, error) {
	if len(rules) > 128 {
		return nil, ErrShadowTableConfig
	}
	p := &ShadowTablePlan{sensitive: sensitive, aliases: make(map[string]ShadowTableAlias)}
	originals := make(map[string]bool)
	timestamps := make(map[string]bool)
	for _, item := range rules {
		if item == nil || !validShadowIdentifier(item.GetSchema()) || !validShadowIdentifier(item.GetTable()) || item.GetGhostTable() != "" && !validShadowIdentifier(item.GetGhostTable()) || item.GetOldTable() != "" && !validShadowIdentifier(item.GetOldTable()) || (item.GetTool() != ShadowTableTool_SHADOW_TABLE_TOOL_GHOST && item.GetTool() != ShadowTableTool_SHADOW_TABLE_TOOL_PT_OSC) {
			return nil, ErrShadowTableConfig
		}
		spec := shadowTableSpec{tool: item.GetTool(), schema: item.GetSchema(), table: item.GetTable(), oldCustom: item.GetOldTable() != ""}
		key := p.key(spec.schema, spec.table)
		if originals[key] {
			return nil, ErrShadowTableConfig
		}
		originals[key] = true
		p.specs = append(p.specs, spec)
		add := func(prefix, suffix string, kind ShadowTableKind) error {
			alias := ShadowTableAlias{Schema: spec.schema, OriginalTable: spec.table, Tool: spec.tool, Kind: kind, prefix: prefix, suffix: suffix}
			name, err := alias.TargetName(spec.table)
			if err != nil {
				return err
			}
			alias.PhysicalTable = name
			key := p.key(spec.schema, name)
			if _, exists := p.aliases[key]; exists {
				return ErrShadowTableConfig
			}
			p.aliases[key] = alias
			return nil
		}
		addLiteral := func(name string, kind ShadowTableKind) error {
			if name == "" {
				return nil
			}
			key := p.key(spec.schema, name)
			if _, exists := p.aliases[key]; exists {
				return ErrShadowTableConfig
			}
			p.aliases[key] = ShadowTableAlias{Schema: spec.schema, OriginalTable: spec.table, PhysicalTable: name, Tool: spec.tool, Kind: kind, literal: true}
			return nil
		}
		switch spec.tool {
		case ShadowTableTool_SHADOW_TABLE_TOOL_GHOST:
			for _, entry := range []struct {
				suffix string
				kind   ShadowTableKind
			}{{"_gho", ShadowTableGhost}, {"_del", ShadowTableOld}, {"_ghc", ShadowTableLog}, {"_ghk", ShadowTableLog}} {
				if entry.kind == ShadowTableGhost && item.GetGhostTable() != "" || entry.kind == ShadowTableOld && item.GetOldTable() != "" {
					continue
				}
				if err := add("_", entry.suffix, entry.kind); err != nil {
					return nil, err
				}
			}
			if !spec.oldCustom {
				// Timestamped old-table names reserve 20 bytes around the base.
				base := spec.table
				if len(base) > 44 {
					base = base[:44]
				}
				key := p.key(spec.schema, "_"+base+"_")
				if timestamps[key] {
					return nil, ErrShadowTableConfig
				}
				timestamps[key] = true
			}
		case ShadowTableTool_SHADOW_TABLE_TOOL_PT_OSC:
			for underscores := 1; underscores <= 10; underscores++ {
				for _, entry := range []struct {
					suffix string
					kind   ShadowTableKind
				}{{"_new", ShadowTableGhost}, {"_old", ShadowTableOld}} {
					if entry.kind == ShadowTableGhost && item.GetGhostTable() != "" || entry.kind == ShadowTableOld && item.GetOldTable() != "" {
						continue
					}
					if err := add(strings.Repeat("_", underscores), entry.suffix, entry.kind); err != nil {
						return nil, err
					}
				}
			}
		}
		if err := addLiteral(item.GetGhostTable(), ShadowTableGhost); err != nil {
			return nil, err
		}
		if err := addLiteral(item.GetOldTable(), ShadowTableOld); err != nil {
			return nil, err
		}
	}
	for key, alias := range p.aliases {
		if originals[key] {
			return nil, ErrShadowTableConfig
		}
		// A declared original/alias may coincide with another rule's dynamic
		// timestamped old table even when their ordinary ghost names differ.
		if _, matched := p.timestampAlias(alias.Schema, alias.PhysicalTable); matched {
			return nil, ErrShadowTableConfig
		}
	}
	for _, spec := range p.specs {
		if _, matched := p.timestampAlias(spec.schema, spec.table); matched {
			return nil, ErrShadowTableConfig
		}
	}
	return p, nil
}

func (p *ShadowTablePlan) timestampAlias(schema, table string) (ShadowTableAlias, bool) {
	for _, spec := range p.specs {
		if spec.tool != ShadowTableTool_SHADOW_TABLE_TOOL_GHOST || spec.oldCustom || p.key(spec.schema, "") != p.key(schema, "") {
			continue
		}
		base := spec.table
		if len(base) > 44 {
			base = base[:44]
		}
		prefix := "_" + base + "_"
		if len(table) != len(prefix)+18 || p.key("", table[:len(prefix)]) != p.key("", prefix) || p.key("", table[len(table)-4:]) != p.key("", "_del") {
			continue
		}
		stamp := table[len(prefix) : len(table)-4]
		if _, err := time.Parse("20060102150405", stamp); err != nil {
			continue
		}
		return ShadowTableAlias{Schema: spec.schema, OriginalTable: spec.table, PhysicalTable: table, Tool: spec.tool, Kind: ShadowTableOld, prefix: "_", suffix: "_" + stamp + "_del"}, true
	}
	return ShadowTableAlias{}, false
}

func (p *ShadowTablePlan) Match(schema, table string) (ShadowTableAlias, bool) {
	if p == nil {
		return ShadowTableAlias{}, false
	}
	if alias, ok := p.aliases[p.key(schema, table)]; ok {
		alias.PhysicalTable = table
		return alias, true
	}
	return p.timestampAlias(schema, table)
}

func (p *ShadowTablePlan) LogicalTable(schema, table string) string {
	if alias, ok := p.Match(schema, table); ok {
		return alias.OriginalTable
	}
	return table
}

func CloneShadowTables(rules []*ShadowTableRule) []*ShadowTableRule {
	if rules == nil {
		return nil
	}
	out := make([]*ShadowTableRule, len(rules))
	for i, item := range rules {
		if item != nil {
			out[i] = proto.Clone(item).(*ShadowTableRule)
		}
	}
	return out
}

func CloneShadowTablePolicy(policy *ShadowTablePolicy) *ShadowTablePolicy {
	if policy == nil {
		return nil
	}
	return proto.Clone(policy).(*ShadowTablePolicy)
}

func ValidateShadowTablePolicy(policy *ShadowTablePolicy) error {
	if policy == nil {
		return nil
	}
	_, err := CompileShadowTables(policy.GetRules(), policy.GetSourceCaseSensitive())
	return err
}
