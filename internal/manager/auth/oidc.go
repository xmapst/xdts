package auth

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

type OIDCConfig struct {
	Issuer, ClientID, ClientSecret, RedirectURL, RoleClaim, CAFile string
	Scopes                                                         []string
	ClaimRoleMapping                                               map[string]string
	DefaultExternalRole                                            string
	Timeout, StateTTL                                              time.Duration
}

type OIDCFlow struct{ State, Nonce, Verifier, BrowserToken string }
type OIDCIdentity struct {
	ExternalID, Username string
	Role                 Role
}

var ErrOIDCCredentials = errors.New("auth: OIDC 认证失败")

func validOIDCURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User != nil || u.Fragment != "" || u.RawQuery != "" {
		return false
	}
	if u.Scheme == "https" {
		return true
	}
	ip := net.ParseIP(u.Hostname())
	return u.Scheme == "http" && (u.Hostname() == "localhost" || ip != nil && ip.IsLoopback())
}

func ValidateOIDCConfig(c OIDCConfig) error {
	if !validOIDCURL(c.Issuer) || !validOIDCURL(c.RedirectURL) {
		return errors.New("auth: OIDC issuer/redirect_url 必须 HTTPS（本地 loopback 可 HTTP），无凭据或查询参数")
	}
	u, _ := url.Parse(c.RedirectURL)
	if u.Path != "/api/v1/auth/oidc/callback" {
		return errors.New("auth: OIDC redirect_url 路径必须 /api/v1/auth/oidc/callback")
	}
	if c.ClientID == "" || c.ClientSecret == "" || c.RoleClaim == "" {
		return errors.New("auth: OIDC client_id/client_secret/role_claim 不能为空")
	}
	if !slices.Contains(c.Scopes, "openid") {
		return errors.New("auth: OIDC scopes 必须包含 openid")
	}
	if c.Timeout <= 0 || c.Timeout > time.Minute || c.StateTTL <= 0 || c.StateTTL > 15*time.Minute {
		return errors.New("auth: OIDC timeout 范围 (0,1m]，state_ttl 范围 (0,15m]")
	}
	if c.DefaultExternalRole != DenyExternalRole && !Role(c.DefaultExternalRole).Valid() {
		return errors.New("auth: OIDC 默认角色无效")
	}
	for _, role := range c.ClaimRoleMapping {
		if !Role(role).Valid() {
			return errors.New("auth: OIDC 声明映射角色无效")
		}
	}
	return nil
}

// oidcTransport 验证 discovery/JWKS/token 的地址与应答大小。所有请求都
// 使用相同 TLS 信任，不允许远端元数据把令牌交换降级到公网 HTTP。
type oidcTransport struct{ inner *http.Transport }

func (t *oidcTransport) CloseIdleConnections() { t.inner.CloseIdleConnections() }

func (t *oidcTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	address := *r.URL
	address.RawQuery = ""
	if !validOIDCURL(address.String()) {
		return nil, errors.New("auth: OIDC 远端地址不安全")
	}
	response, err := t.inner.RoundTrip(r)
	if err != nil {
		return nil, errors.New("auth: OIDC 网络请求失败")
	}
	defer func(cleanup func() error) { _ = cleanup() }(response.Body.Close)
	data, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return nil, errors.New("auth: OIDC 应答无效或过大")
	}
	response.Body = io.NopCloser(bytes.NewReader(data))
	return response, nil
}

type OIDCAuthenticator struct {
	config     OIDCConfig
	client     *http.Client
	oauth      oauth2.Config
	verifier   *oidc.IDTokenVerifier
	configHash string
}

func NewOIDCAuthenticator(ctx context.Context, c OIDCConfig) (*OIDCAuthenticator, error) {
	if err := ValidateOIDCConfig(c); err != nil {
		return nil, err
	}
	var roots *x509.CertPool
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, errors.New("auth: 无法读取 OIDC CA")
		}
		roots, err = x509.SystemCertPool()
		if err != nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("auth: OIDC CA 无有效证书")
		}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	client := &http.Client{Transport: &oidcTransport{inner: transport}, Timeout: c.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error {
		return errors.New("auth: OIDC 后端请求不允许重定向")
	}}
	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()
	provider, err := oidc.NewProvider(oidc.ClientContext(ctx, client), c.Issuer)
	if err != nil {
		transport.CloseIdleConnections()
		return nil, errors.New("auth: OIDC discovery 失败")
	}
	var discovered struct {
		JWKSURL     string   `json:"jwks_uri"`
		SigningAlgs []string `json:"id_token_signing_alg_values_supported"`
	}
	if err := provider.Claims(&discovered); err != nil || !validOIDCURL(provider.Endpoint().AuthURL) || !validOIDCURL(provider.Endpoint().TokenURL) || !validOIDCURL(discovered.JWKSURL) {
		transport.CloseIdleConnections()
		return nil, errors.New("auth: OIDC discovery 端点无效")
	}
	allowed := []string{"RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512", "EdDSA"}
	var algorithms []string
	for _, alg := range discovered.SigningAlgs {
		if slices.Contains(allowed, alg) {
			algorithms = append(algorithms, alg)
		}
	}
	if len(algorithms) == 0 {
		transport.CloseIdleConnections()
		return nil, errors.New("auth: OIDC 无支持的安全签名算法")
	}
	c.Scopes = slices.Clone(c.Scopes)
	mapping := make(map[string]string, len(c.ClaimRoleMapping))
	maps.Copy(mapping, c.ClaimRoleMapping)
	c.ClaimRoleMapping = mapping
	raw, err := json.Marshal(c)
	if err != nil {
		return nil, errors.New("auth: OIDC 配置摘要失败")
	}
	sum := sha256.Sum256(raw)
	endpoint := provider.Endpoint()
	return &OIDCAuthenticator{config: c, client: client, configHash: hex.EncodeToString(sum[:]), oauth: oauth2.Config{ClientID: c.ClientID, ClientSecret: c.ClientSecret, RedirectURL: c.RedirectURL, Scopes: c.Scopes, Endpoint: endpoint}, verifier: provider.VerifierContext(oidc.ClientContext(context.Background(), client), &oidc.Config{ClientID: c.ClientID, SupportedSigningAlgs: algorithms})}, nil
}

func (a *OIDCAuthenticator) NewFlow() (OIDCFlow, error) {
	values := make([]string, 4)
	for i := range values {
		buffer := make([]byte, 32)
		if _, err := rand.Read(buffer); err != nil {
			return OIDCFlow{}, errors.New("auth: OIDC 随机值生成失败")
		}
		values[i] = base64.RawURLEncoding.EncodeToString(buffer)
	}
	return OIDCFlow{State: values[0], Nonce: values[1], Verifier: values[2], BrowserToken: values[3]}, nil
}
func (a *OIDCAuthenticator) ConfigHash() string { return a.configHash }

func (a *OIDCAuthenticator) Close()                  { a.client.CloseIdleConnections() }
func (a *OIDCAuthenticator) StateTTL() time.Duration { return a.config.StateTTL }
func (a *OIDCAuthenticator) AuthorizationURL(flow OIDCFlow) string {
	return a.oauth.AuthCodeURL(flow.State, oidc.Nonce(flow.Nonce), oauth2.S256ChallengeOption(flow.Verifier))
}

func OIDCExternalID(issuer, subject string) string {
	sum := sha256.Sum256([]byte(issuer + "\x00" + subject))
	return "oidc:v1:" + hex.EncodeToString(sum[:])
}

func (a *OIDCAuthenticator) ExchangeAndVerify(ctx context.Context, code, nonce, verifier string) (OIDCIdentity, error) {
	if code == "" || nonce == "" || verifier == "" || len(code) > 8192 {
		return OIDCIdentity{}, ErrOIDCCredentials
	}
	ctx, cancel := context.WithTimeout(ctx, a.config.Timeout)
	defer cancel()
	ctx = oidc.ClientContext(ctx, a.client)
	tokens, err := a.oauth.Exchange(ctx, code, oauth2.VerifierOption(verifier))
	if err != nil {
		return OIDCIdentity{}, ErrOIDCCredentials
	}
	raw, ok := tokens.Extra("id_token").(string)
	if !ok || len(raw) > 1<<20 {
		return OIDCIdentity{}, ErrOIDCCredentials
	}
	id, err := a.verifier.Verify(ctx, raw)
	if err != nil || id.Issuer != a.config.Issuer || id.Subject == "" || !utf8.ValidString(id.Subject) || strings.ContainsRune(id.Subject, 0) || id.IssuedAt.IsZero() || id.IssuedAt.After(time.Now().Add(5*time.Minute)) || subtle.ConstantTimeCompare([]byte(id.Nonce), []byte(nonce)) != 1 {
		return OIDCIdentity{}, ErrOIDCCredentials
	}
	var claims map[string]json.RawMessage
	if err := id.Claims(&claims); err != nil {
		return OIDCIdentity{}, ErrOIDCCredentials
	}
	var azp string
	if value, ok := claims["azp"]; ok {
		if json.Unmarshal(value, &azp) != nil || azp != a.config.ClientID {
			return OIDCIdentity{}, ErrOIDCCredentials
		}
	} else if len(id.Audience) > 1 {
		return OIDCIdentity{}, ErrOIDCCredentials
	}
	if id.AccessTokenHash != "" && id.VerifyAccessToken(tokens.AccessToken) != nil {
		return OIDCIdentity{}, ErrOIDCCredentials
	}
	username := id.Subject
	if raw, ok := claims["preferred_username"]; ok {
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, &username) != nil {
			return OIDCIdentity{}, ErrOIDCCredentials
		}
	}
	if username == "" || !utf8.ValidString(username) || utf8.RuneCountInString(username) > 128 || strings.ContainsRune(username, 0) {
		return OIDCIdentity{}, ErrOIDCCredentials
	}
	var identifiers []string
	if value, ok := claims[a.config.RoleClaim]; ok {
		var single string
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return OIDCIdentity{}, ErrOIDCCredentials
		}
		if json.Unmarshal(value, &single) == nil {
			identifiers = []string{single}
		} else {
			var entries []json.RawMessage
			if json.Unmarshal(value, &entries) != nil || entries == nil {
				return OIDCIdentity{}, ErrOIDCCredentials
			}
			for _, entry := range entries {
				var name string
				if bytes.Equal(bytes.TrimSpace(entry), []byte("null")) || json.Unmarshal(entry, &name) != nil {
					return OIDCIdentity{}, ErrOIDCCredentials
				}
				identifiers = append(identifiers, name)
			}
		}
	}
	role, err := MapExternalRole(identifiers, a.config.ClaimRoleMapping, a.config.DefaultExternalRole)
	if err != nil || ctx.Err() != nil {
		return OIDCIdentity{}, ErrOIDCCredentials
	}
	return OIDCIdentity{ExternalID: OIDCExternalID(a.config.Issuer, id.Subject), Username: username, Role: role}, nil
}
