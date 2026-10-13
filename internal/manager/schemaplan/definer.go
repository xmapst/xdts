package schemaplan

import (
	"errors"
	"slices"
	"sort"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/schemamig"
)

func DefinitionAccounts(source []Definition, fallback string) ([]*iproto.DefinerAccount, error) {
	accounts := map[string]schemamig.DefinerAccount{}
	for _, def := range source {
		a, err := schemamig.DefinitionDefiner(rewriteKind(def.Kind), def.DDL)
		if err != nil {
			return nil, err
		}
		if a != nil && !a.CurrentUser {
			accounts[a.Key()] = *a
		}
	}
	if fallback != "" {
		a, err := schemamig.ParseDefinerAccount(fallback)
		if err != nil {
			return nil, err
		}
		if !a.CurrentUser {
			accounts[a.Key()] = a
		}
	}
	if len(accounts) > 100 {
		return nil, errors.New("too many definer accounts")
	}
	keys := make([]string, 0, len(accounts))
	for k := range accounts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]*iproto.DefinerAccount, 0, len(keys))
	for _, k := range keys {
		a := accounts[k]
		out = append(out, &iproto.DefinerAccount{User: a.User, Host: a.Host})
	}
	return out, nil
}

func VerifiedDefinerAccounts(result *iproto.DefsResult, requested []*iproto.DefinerAccount) (map[string]bool, error) {
	allowed := map[string]bool{}
	for _, a := range requested {
		allowed[a.GetUser()+"\x00"+a.GetHost()] = true
	}
	out := map[string]bool{}
	for _, r := range result.GetAccounts() {
		key := r.GetAccount().GetUser() + "\x00" + r.GetAccount().GetHost()
		if !allowed[key] {
			return nil, errors.New("unexpected account verification result")
		}
		if _, exists := out[key]; exists {
			return nil, errors.New("duplicate account verification result")
		}
		out[key] = r.GetVerified() && r.GetExists()
	}
	return out, nil
}

// ApplyDefinerExecuteChecks fails closed when routine security cannot be
// established from the selected account's actual direct grants.
func (p *Plan) ApplyDefinerExecuteChecks(target *iproto.DefsResult) {
	accounts := map[string]*iproto.DefinerAccountResult{}
	for _, r := range target.GetAccounts() {
		accounts[r.GetAccount().GetUser()+"\x00"+r.GetAccount().GetHost()] = r
	}
	for i := range p.Steps {
		s := &p.Steps[i]
		if (s.Kind != model.ObjectTypeProcedure && s.Kind != model.ObjectTypeFunction) || s.Action == model.MigrationActionBlock {
			continue
		}
		a, err := schemamig.DefinitionDefiner(rewriteKind(s.Kind), s.DDL)
		allowed := false
		if err == nil {
			if a == nil || a.CurrentUser || a.User+"@"+a.Host == target.GetCurrentUser() {
				allowed = schemamig.GrantsSchemaExecute(target.GetGrants(), s.Schema)
			} else if r := accounts[a.Key()]; r != nil && r.GetVerified() && r.GetExists() && r.GetGrantsVerified() {
				if slices.Contains(r.GetExecuteSchemas(), s.Schema) {
					allowed = true
				}
			}
		}
		if !allowed {
			s.Action = model.MigrationActionBlock
			s.BlockReason = "无法确认目标 DEFINER 账号拥有存储程序 EXECUTE 权限，请 DBA 授权后重试"
			p.Blocked = true
		}
	}
}
