// Package auth 承载 Manager 的会话、角色与用户存储的框架无关逻辑。
package auth

import (
	"errors"
	"fmt"
)

// Role 是用户角色。需求 MGR-USR-05 明确「首版提供「管理员」「只读」两个
// 角色」，P0 阶段只有这两个取值——不要凭印象加第三个角色（如
// "operator"），也不要在没有 MGR-USR-07 细粒度 RBAC 落地前扩展枚举。
type Role string

const (
	// RoleAdmin 是管理员角色：可执行全部操作。
	RoleAdmin Role = "admin"
	// RoleReadOnly 是只读角色：只能读，不能写；也不能访问返回凭据/注册码
	// 明文的接口（需求 MGR-USR-08）。
	RoleReadOnly Role = "readonly"
)

// Valid 报告 r 是否是已知角色。
func (r Role) Valid() bool {
	switch r {
	case RoleAdmin, RoleReadOnly:
		return true
	default:
		return false
	}
}

// DenyExternalRole 是 default_external_role 配置项的特殊取值：未匹配任何
// 组/声明映射的外部用户直接拒绝登录，而不是给一个默认角色
// （需求 MGR-USR-05：「未匹配的外部用户默认只读或拒绝登录（可配）」）。
const DenyExternalRole = "deny"

// ErrExternalUserDenied 表示外部用户（LDAP/OIDC）未命中角色映射，且
// default_external_role 配置为 "deny"。
var ErrExternalUserDenied = errors.New("auth: 外部用户未匹配任何角色映射，按配置拒绝登录")

// MapExternalRole 把外部身份标识（LDAP 组 DN 列表 / OIDC 声明值列表）按配置
// 的映射表解析为角色。
//
// identifiers 里第一个在 mapping 中命中的条目生效；都未命中时按
// defaultExternalRole 处理：值为 "deny" 时拒绝登录（返回
// ErrExternalUserDenied），否则该值本身必须是一个合法角色。
func MapExternalRole(identifiers []string, mapping map[string]string, defaultExternalRole string) (Role, error) {
	for _, id := range identifiers {
		roleStr, ok := mapping[id]
		if !ok {
			continue
		}
		role := Role(roleStr)
		if !role.Valid() {
			return "", fmt.Errorf("auth: 角色映射把 %q 配置成未知角色 %q", id, roleStr)
		}
		return role, nil
	}

	if defaultExternalRole == DenyExternalRole {
		return "", ErrExternalUserDenied
	}
	role := Role(defaultExternalRole)
	if !role.Valid() {
		return "", fmt.Errorf("auth: default_external_role 配置了未知角色 %q", defaultExternalRole)
	}
	return role, nil
}
