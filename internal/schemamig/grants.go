package schemamig

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// GrantsPrivilege proves direct privileges at global or exact table scope. Empty
// schema/table require a global grant. ALL never includes dynamic privileges.
// Roles, column grants and partial revokes cannot prove complete access.
func GrantsPrivilege(grants []string, schema, table, privilege string) bool {
	static := false
	for _, name := range mysql.Priv2Str {
		if strings.EqualFold(name, privilege) {
			static = true
			break
		}
	}
	allowed := false
	for _, grant := range grants {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(grant)), "REVOKE ") {
			return false
		}
		n, err := parseOneTiDBStmt(grant)
		if err != nil {
			continue
		}
		g, ok := n.(*ast.GrantStmt)
		if !ok || g.Level == nil {
			continue
		}
		switch g.Level.Level {
		case ast.GrantLevelGlobal:
		case ast.GrantLevelDB:
			if schema == "" || g.Level.DBName != schema {
				continue
			}
		case ast.GrantLevelTable:
			if schema == "" || table == "" || g.Level.DBName != schema || g.Level.TableName != table {
				continue
			}
		default:
			continue
		}
		for _, p := range g.Privs {
			if len(p.Cols) != 0 {
				continue
			}
			if (static && p.Priv == mysql.AllPriv) || strings.EqualFold(mysql.Priv2Str[p.Priv], privilege) || (p.Priv == mysql.ExtendedPriv && g.Level.Level == ast.GrantLevelGlobal && strings.EqualFold(p.Name, privilege)) {
				allowed = true
			}
		}
	}
	return allowed
}

// GrantsSchemaExecute accepts parsed direct global/database privileges. Role
// grants and patterns cannot prove access and therefore return false.
func GrantsSchemaExecute(grants []string, schema string) bool {
	for _, grant := range grants {
		n, err := parseOneTiDBStmt(grant)
		if err != nil {
			continue
		}
		g, ok := n.(*ast.GrantStmt)
		if !ok || g.Level == nil {
			continue
		}
		if g.Level.Level != ast.GrantLevelGlobal && (g.Level.Level != ast.GrantLevelDB || g.Level.DBName != schema) {
			continue
		}
		for _, p := range g.Privs {
			if p.Priv == mysql.AllPriv || p.Priv == mysql.ExecutePriv {
				return true
			}
		}
	}
	return false
}

// GrantsTableTrigger requires direct privileges proving complete trigger
// visibility. Role grants and partial revokes cannot establish that proof.
func GrantsTableTrigger(grants []string, schema, table string) bool {
	allowed := false
	for _, grant := range grants {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(grant)), "REVOKE ") {
			return false
		}
		n, err := parseOneTiDBStmt(grant)
		if err != nil {
			continue
		}
		g, ok := n.(*ast.GrantStmt)
		if !ok || g.Level == nil {
			continue
		}
		switch g.Level.Level {
		case ast.GrantLevelGlobal:
		case ast.GrantLevelDB:
			if g.Level.DBName != schema {
				continue
			}
		case ast.GrantLevelTable:
			if g.Level.DBName != schema || g.Level.TableName != table {
				continue
			}
		default:
			continue
		}
		for _, privilege := range g.Privs {
			if len(privilege.Cols) == 0 && (privilege.Priv == mysql.AllPriv || privilege.Priv == mysql.TriggerPriv) {
				allowed = true
			}
		}
	}
	return allowed
}
