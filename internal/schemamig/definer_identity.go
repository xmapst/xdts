package schemamig

import (
	"fmt"
	"strings"
	"unicode/utf8"

	mysql "github.com/bytebase/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// DefinerAccount contains only the exact MySQL account identity, no credentials.
type DefinerAccount struct {
	User        string `json:"user"`
	Host        string `json:"host"`
	CurrentUser bool   `json:"currentUser,omitempty"`
}

func (a DefinerAccount) Key() string { return a.User + "\x00" + a.Host }
func (a DefinerAccount) SQL() string {
	if a.CurrentUser {
		return "CURRENT_USER"
	}
	return quoteMySQLIdent(a.User) + "@" + quoteMySQLIdent(a.Host)
}
func ParseDefinerAccount(text string) (DefinerAccount, error) {
	a, err := resolveDefinerFallback(text)
	if err != nil {
		return DefinerAccount{}, err
	}
	out := DefinerAccount{User: a.user, Host: a.host, CurrentUser: a.currentUser}
	if !out.CurrentUser && (out.User == "" || out.Host == "" || !utf8.ValidString(out.User) || !utf8.ValidString(out.Host) || utf8.RuneCountInString(out.User) > 32 || len(out.Host) > 255 || strings.ContainsRune(out.User, 0) || strings.ContainsRune(out.Host, 0)) {
		return DefinerAccount{}, fmt.Errorf("invalid definer account identity")
	}
	return out, nil
}

// DefinitionDefiner reads a parsed CREATE or ALTER EVENT clause. Body text and comments
// never become account lookup inputs.
func DefinitionDefiner(kind ObjectKind, ddl string) (*DefinerAccount, error) {
	if kind == ObjectTable || kind == ObjectUnknown {
		return nil, nil
	}
	if kind == ObjectView {
		n, err := parseOneTiDBStmt(ddl)
		if err != nil {
			return nil, err
		}
		v, ok := n.(*ast.CreateViewStmt)
		if !ok {
			return nil, ErrDDLKindMismatch
		}
		if v.Definer == nil {
			return nil, nil
		}
		a := DefinerAccount{User: v.Definer.Username, Host: v.Definer.Hostname, CurrentUser: v.Definer.CurrentUser}
		return &a, nil
	}
	pr, err := parseRoutine(ddl)
	if err != nil {
		return nil, err
	}
	if len(pr.tree.AllQuery()) != 1 {
		return nil, ErrDDLKindMismatch
	}
	var clause mysql.IDefinerClauseContext
	switch kind {
	case ObjectProcedure:
		if s := findCreateProcedure(pr.tree); s != nil {
			clause = s.DefinerClause()
		}
	case ObjectFunction:
		if s := findCreateFunction(pr.tree); s != nil {
			clause = s.DefinerClause()
		}
	case ObjectTrigger:
		if s := findCreateTrigger(pr.tree); s != nil {
			clause = s.DefinerClause()
		}
	case ObjectEvent:
		if s := findCreateEvent(pr.tree); s != nil {
			clause = s.DefinerClause()
		} else if s := findRoutineAlter(pr.tree).event; s != nil {
			clause = s.DefinerClause()
		}
	default:
		return nil, ErrDDLKindMismatch
	}
	if clause == nil {
		return nil, nil
	}
	text := strings.TrimSpace(pr.stream.GetTextFromRuleContext(clause.User()))
	// User strings in SQL may be quoted with single quotes. Parse the identity
	// through the view AST, which shares MySQL's DEFINER account grammar.
	n, err := parseOneTiDBStmt("CREATE DEFINER=" + text + " VIEW __definer_identity AS SELECT 1")
	if err != nil {
		return nil, err
	}
	v, ok := n.(*ast.CreateViewStmt)
	if !ok || v.Definer == nil {
		return nil, ErrDDLKindMismatch
	}
	a := DefinerAccount{User: v.Definer.Username, Host: v.Definer.Hostname, CurrentUser: v.Definer.CurrentUser}
	return &a, nil
}

// AlterEventDefiner reads only the optional account in a parsed ALTER EVENT.
func AlterEventDefiner(ddl string) (*DefinerAccount, error) {
	parsed, err := parseRoutine(ddl)
	if err != nil || len(parsed.tree.AllQuery()) != 1 {
		return nil, nil // The incremental rewriter reports unsupported SQL.
	}
	f := findRoutineAlter(parsed.tree)
	if f.count != 1 || f.event == nil {
		return nil, nil
	}
	return DefinitionDefiner(ObjectEvent, ddl)
}
