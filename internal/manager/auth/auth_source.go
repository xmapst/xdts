package auth

// AuthSource 标识一个会话是通过哪种方式建立的。
type AuthSource string

const (
	AuthSourceLocal AuthSource = "local"
	AuthSourceLDAP  AuthSource = "ldap"
	AuthSourceOIDC  AuthSource = "oidc"
)

// Valid 报告 s 是否是已知的登录来源。
func (s AuthSource) Valid() bool {
	switch s {
	case AuthSourceLocal, AuthSourceLDAP, AuthSourceOIDC:
		return true
	default:
		return false
	}
}
