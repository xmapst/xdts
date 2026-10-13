package rule

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

type compiledColumnRule struct {
	action  FilterAction
	schema  exactMatcher
	table   exactMatcher
	columns []exactMatcher
}

func compileColumnFilters(rules []ColumnFilterRule, sensitive bool) ([]compiledColumnRule, []error) {
	var errs []error
	out := make([]compiledColumnRule, 0, len(rules))
	valid := func(name string) bool {
		return name != "" && utf8.ValidString(name) && utf8.RuneCountInString(name) <= 64 && !strings.ContainsRune(name, 0)
	}
	for i, r := range rules {
		if r.Action != FilterActionInclude && r.Action != FilterActionExclude {
			errs = append(errs, fmt.Errorf("rule: column_filters[%d]: invalid action", i))
		}
		if !valid(r.Schema) || !valid(r.Table) {
			errs = append(errs, fmt.Errorf("rule: column_filters[%d]: invalid exact schema/table", i))
		}
		compiled := compiledColumnRule{action: r.Action, schema: exactMatcher{pattern: r.Schema, caseSensitive: sensitive}, table: exactMatcher{pattern: r.Table, caseSensitive: sensitive}}
		for j, name := range r.Columns {
			if !valid(name) {
				errs = append(errs, fmt.Errorf("rule: column_filters[%d].columns[%d]: invalid column", i, j))
			}
			compiled.columns = append(compiled.columns, exactMatcher{pattern: name})
		}
		out = append(out, compiled)
	}
	return out, errs
}
