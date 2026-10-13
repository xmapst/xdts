package iproto

import (
	"fmt"
	"strings"
)

// ValidateConflictPolicy checks published names without touching external state.
func ValidateConflictPolicy(policy *ConflictPolicy) error {
	switch policy.GetMode() {
	case ConflictMode_CONFLICT_MODE_UNSPECIFIED, ConflictMode_CONFLICT_MODE_SOURCE_WINS, ConflictMode_CONFLICT_MODE_LWW, ConflictMode_CONFLICT_MODE_STOP:
	default:
		return fmt.Errorf("iproto: unknown conflict policy")
	}
	if len(policy.GetLwwTables()) > 4000 {
		return fmt.Errorf("iproto: too many LWW tables")
	}
	seen := make(map[string]bool)
	for _, table := range policy.GetLwwTables() {
		if table == nil || strings.TrimSpace(table.GetSchema()) == "" || strings.TrimSpace(table.GetTable()) == "" || strings.TrimSpace(table.GetTimestampColumn()) == "" || len(table.GetSchema()) > 128 || len(table.GetTable()) > 128 || len(table.GetTimestampColumn()) > 64 {
			return fmt.Errorf("iproto: LWW table requires schema, table and timestamp column")
		}
		key := strings.ToLower(table.GetSchema() + "\x00" + table.GetTable())
		if seen[key] {
			return fmt.Errorf("iproto: duplicate LWW table")
		}
		seen[key] = true
	}
	if policy.GetMode() != ConflictMode_CONFLICT_MODE_LWW && len(policy.GetLwwTables()) > 0 {
		return fmt.Errorf("iproto: LWW tables require LWW mode")
	}
	if len(policy.GetMaskRules()) > 4000 {
		return fmt.Errorf("iproto: too many conflict mask rules")
	}
	masked := make(map[string]bool)
	for _, rule := range policy.GetMaskRules() {
		if rule == nil || strings.TrimSpace(rule.GetSchema()) == "" || strings.TrimSpace(rule.GetTable()) == "" || len(rule.GetSchema()) > 128 || len(rule.GetTable()) > 128 || len(rule.GetColumns()) == 0 || len(rule.GetColumns()) > 4096 {
			return fmt.Errorf("iproto: conflict mask rule requires schema, table and columns")
		}
		key := strings.ToLower(rule.GetSchema() + "\x00" + rule.GetTable())
		if masked[key] {
			return fmt.Errorf("iproto: duplicate conflict mask table")
		}
		masked[key] = true
		columns := make(map[string]bool)
		for _, column := range rule.GetColumns() {
			key := strings.ToLower(column)
			if strings.TrimSpace(column) == "" || len(column) > 64 || columns[key] {
				return fmt.Errorf("iproto: conflict mask columns must be nonempty and unique")
			}
			columns[key] = true
		}
	}
	return nil
}
