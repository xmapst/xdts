package auth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-ldap/ldap/v3"
)

// LDAPConfig 不包含 HTTP 或数据库依赖。Timeout 覆盖整个认证过程。
type LDAPConfig struct {
	URL, BindDN, BindPassword, UserBaseDN, UserFilter, CAFile string
	Timeout                                                   time.Duration
	GroupRoleMapping                                          map[string]string
	DefaultExternalRole                                       string
}

type LDAPIdentity struct {
	ExternalID string
	Username   string
	Role       Role
}

// ErrLDAPCredentials 故意不携带目录应答、密码或底层错误文本。
var ErrLDAPCredentials = errors.New("auth: LDAP 认证失败")

func ValidateLDAPConfig(c LDAPConfig) error {
	u, err := url.Parse(c.URL)
	if err != nil || u == nil || (u.Scheme != "ldap" && u.Scheme != "ldaps") || u.Hostname() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("auth: LDAP URL 必须为 ldap://host[:port] 或 ldaps://host[:port]")
	}
	if p := u.Port(); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 65535 {
			return errors.New("auth: LDAP 端口无效")
		}
	}
	for _, dn := range []string{c.BindDN, c.UserBaseDN} {
		if strings.TrimSpace(dn) == "" {
			return errors.New("auth: LDAP bind_dn/user_base_dn 不能为空")
		}
		if _, err := ldap.ParseDN(dn); err != nil {
			return errors.New("auth: LDAP bind_dn/user_base_dn 必须为有效 DN")
		}
	}
	if c.BindPassword == "" {
		return errors.New("auth: LDAP 服务账号密码不能为空")
	}
	if strings.Count(c.UserFilter, "%s") != 1 {
		return errors.New("auth: LDAP user_filter 必须恰含一个 %s")
	}
	if _, err := ldap.CompileFilter(strings.Replace(c.UserFilter, "%s", "validation", 1)); err != nil {
		return errors.New("auth: LDAP user_filter 格式无效")
	}
	if c.Timeout <= 0 || c.Timeout > time.Minute {
		return errors.New("auth: LDAP timeout 必须大于 0 且不超过 1m")
	}
	if c.DefaultExternalRole != DenyExternalRole && !Role(c.DefaultExternalRole).Valid() {
		return errors.New("auth: LDAP 默认角色无效")
	}
	if len(c.GroupRoleMapping) > 128 {
		return errors.New("auth: LDAP 组映射不能超过 128 项")
	}
	for dn, role := range c.GroupRoleMapping {
		if strings.TrimSpace(dn) == "" {
			return errors.New("auth: LDAP 组映射 DN 不能为空")
		}
		if _, err := ldap.ParseDN(dn); err != nil {
			return errors.New("auth: LDAP 组映射 DN 无效")
		}
		if !Role(role).Valid() {
			return errors.New("auth: LDAP 映射角色无效")
		}
	}
	return nil
}

// LDAPAuthenticator 的配置和 TLS 信任在构造时准备好，此后只读；每次认证
// 独立持有连接，不在并发请求之间共享已绑定的目录会话。
type LDAPAuthenticator struct {
	config    LDAPConfig
	address   string
	directTLS bool
	tlsConfig *tls.Config
	groups    []string
}

func NewLDAPAuthenticator(c LDAPConfig) (*LDAPAuthenticator, error) {
	if err := ValidateLDAPConfig(c); err != nil {
		return nil, err
	}
	u, _ := url.Parse(c.URL)
	port := u.Port()
	if port == "" {
		if u.Scheme == "ldaps" {
			port = "636"
		} else {
			port = "389"
		}
	}
	var roots *x509.CertPool
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, errors.New("auth: 无法读取 LDAP CA 文件")
		}
		roots, err = x509.SystemCertPool()
		if err != nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("auth: LDAP CA 文件不包含有效证书")
		}
	}
	copyMapping := make(map[string]string, len(c.GroupRoleMapping))
	groups := make([]string, 0, len(c.GroupRoleMapping))
	for dn, role := range c.GroupRoleMapping {
		copyMapping[dn] = role
		groups = append(groups, dn)
	}
	c.GroupRoleMapping = copyMapping
	slices.Sort(groups)
	return &LDAPAuthenticator{config: c, address: net.JoinHostPort(u.Hostname(), port), directTLS: u.Scheme == "ldaps", tlsConfig: &tls.Config{MinVersion: tls.VersionTLS12, ServerName: u.Hostname(), RootCAs: roots}, groups: groups}, nil
}

func (a *LDAPAuthenticator) Authenticate(ctx context.Context, username, password string) (LDAPIdentity, error) {
	if username == "" || password == "" || !utf8.ValidString(username) || strings.ContainsRune(username, 0) || utf8.RuneCountInString(username) > 128 || len(password) > 4096 {
		return LDAPIdentity{}, ErrLDAPCredentials
	}
	ctx, cancel := context.WithTimeout(ctx, a.config.Timeout)
	defer cancel()
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", a.address)
	if err != nil {
		return LDAPIdentity{}, ErrLDAPCredentials
	}
	defer func(cleanup func() error) { _ = cleanup() }(raw.Close)
	deadline, _ := ctx.Deadline()
	if err := raw.SetDeadline(deadline); err != nil {
		return LDAPIdentity{}, ErrLDAPCredentials
	}
	// Close 原始连接即可中断 TLS 和 LDAP 读取；避免与 StartTLS 修改内部
	// Conn.conn 并发调用 LDAP Close。
	stop := context.AfterFunc(ctx, func() { _ = raw.Close() })
	defer stop()
	var wire = raw
	if a.directTLS {
		secured := tls.Client(raw, a.tlsConfig.Clone())
		if err := secured.HandshakeContext(ctx); err != nil {
			return LDAPIdentity{}, ErrLDAPCredentials
		}
		wire = secured
	}
	conn := ldap.NewConn(wire, a.directTLS)
	conn.SetTimeout(time.Until(deadline))
	conn.Start()
	defer func(cleanup func() error) { _ = cleanup() }(conn.Close)
	if !a.directTLS {
		if err := conn.StartTLS(a.tlsConfig.Clone()); err != nil {
			return LDAPIdentity{}, ErrLDAPCredentials
		}
	}
	if err := conn.Bind(a.config.BindDN, a.config.BindPassword); err != nil {
		return LDAPIdentity{}, ErrLDAPCredentials
	}
	filter := strings.Replace(a.config.UserFilter, "%s", ldap.EscapeFilter(username), 1)
	result, err := conn.Search(ldap.NewSearchRequest(a.config.UserBaseDN, ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 2, 0, false, filter, []string{"uid", "sAMAccountName", "memberOf"}, nil))
	if err != nil || len(result.Entries) != 1 || len(result.Referrals) != 0 {
		return LDAPIdentity{}, ErrLDAPCredentials
	}
	entry := result.Entries[0]
	userDN, err := ldap.ParseDN(entry.DN)
	if err != nil || entry.DN == "" || utf8.RuneCountInString(entry.DN) > 255 {
		return LDAPIdentity{}, ErrLDAPCredentials
	}
	canonical := entry.GetEqualFoldAttributeValue("uid")
	if canonical == "" {
		canonical = entry.GetEqualFoldAttributeValue("sAMAccountName")
	}
	if canonical == "" {
		canonical = username
	}
	if canonical == "" || !utf8.ValidString(canonical) || utf8.RuneCountInString(canonical) > 128 || strings.ContainsRune(canonical, 0) {
		return LDAPIdentity{}, ErrLDAPCredentials
	}
	if err := conn.Bind(entry.DN, password); err != nil {
		return LDAPIdentity{}, ErrLDAPCredentials
	}
	// 用户绑定成功之后才做组映射；组搜索仍使用服务身份，避免用户读权限
	// 差异影响角色判定。
	if err := conn.Bind(a.config.BindDN, a.config.BindPassword); err != nil {
		return LDAPIdentity{}, ErrLDAPCredentials
	}
	var memberships []string
	for _, group := range a.groups {
		groupDN, _ := ldap.ParseDN(group)
		member := false
		for _, dn := range entry.GetEqualFoldAttributeValues("memberOf") {
			parsed, err := ldap.ParseDN(dn)
			if err == nil && groupDN.EqualFold(parsed) {
				member = true
				break
			}
		}
		if !member {
			membership := fmt.Sprintf("(|(member=%s)(uniqueMember=%s)(memberUid=%s))", ldap.EscapeFilter(userDN.String()), ldap.EscapeFilter(userDN.String()), ldap.EscapeFilter(canonical))
			found, err := conn.Search(ldap.NewSearchRequest(group, ldap.ScopeBaseObject, ldap.NeverDerefAliases, 1, 0, false, membership, []string{"1.1"}, nil))
			if err != nil && !ldap.IsErrorWithCode(err, ldap.LDAPResultNoSuchObject) {
				return LDAPIdentity{}, ErrLDAPCredentials
			}
			member = err == nil && len(found.Entries) == 1 && len(found.Referrals) == 0
		}
		if member {
			memberships = append(memberships, group)
		}
	}
	role, err := MapExternalRole(memberships, a.config.GroupRoleMapping, a.config.DefaultExternalRole)
	if err != nil || ctx.Err() != nil {
		return LDAPIdentity{}, ErrLDAPCredentials
	}
	return LDAPIdentity{ExternalID: entry.DN, Username: canonical, Role: role}, nil
}
