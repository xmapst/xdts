package iproto

import "github.com/xmapst/xdts/internal/rule"

// ShadowSourceRules evaluates temporary objects against their original table's
// rules. Physical object names stay on the row/DDL message and schema history.
// Both plans are immutable and constructed for this source runtime owner.
type ShadowSourceRules struct {
	base    *rule.Plan
	shadows *ShadowTablePlan
}

func NewShadowSourceRules(base *rule.Plan, shadows *ShadowTablePlan) (*ShadowSourceRules, error) {
	if base == nil {
		return nil, ErrShadowTableConfig
	}
	return &ShadowSourceRules{base: base, shadows: shadows}, nil
}

func (p *ShadowSourceRules) MatchTable(schema, table string) bool {
	if alias, found := p.shadows.Match(schema, table); found && alias.Kind == ShadowTableLog {
		return false
	}
	return p.base.MatchTable(schema, p.shadows.LogicalTable(schema, table))
}

func (p *ShadowSourceRules) Allow(schema, table, column string) bool {
	return p.base.Allow(schema, p.shadows.LogicalTable(schema, table), column)
}

func (p *ShadowSourceRules) FiltersTable(schema, table string) bool {
	return p.base.FiltersTable(schema, p.shadows.LogicalTable(schema, table))
}

func (p *ShadowSourceRules) HasRowFilters(schema, table string) bool {
	return p.base.HasRowFilters(schema, p.shadows.LogicalTable(schema, table))
}

func (p *ShadowSourceRules) RowFilterColumns(schema, table string) []string {
	return p.base.RowFilterColumns(schema, p.shadows.LogicalTable(schema, table))
}

func (p *ShadowSourceRules) MatchRow(schema, table string, columns []string, values []any) (bool, error) {
	return p.base.MatchRow(schema, p.shadows.LogicalTable(schema, table), columns, values)
}

func (p *ShadowSourceRules) AllowDDL() bool    { return p.base.AllowDDL() }
func (p *ShadowSourceRules) AllowInsert() bool { return p.base.AllowInsert() }
func (p *ShadowSourceRules) AllowUpdate() bool { return p.base.AllowUpdate() }
func (p *ShadowSourceRules) AllowDelete() bool { return p.base.AllowDelete() }
