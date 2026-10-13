package http

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// HeaderCSRF 和 CookieCSRF 定义双提交 CSRF Cookie 与请求头，
// 对需要保护的非幂等请求校验两者一致。
const (
	HeaderCSRF = "XDTS-CSRF"
	CookieCSRF = "XDTS-CSRF"
)

// LoginPath 对应的 POST /auth/login 由本地/LDAP（internal/manager/http/auth_local.go）实现；
// OIDCStartPath/OIDCCallbackPath 由 internal/manager/http/auth_oidc.go 实现。LDAP 不单独开路由，前端共用同一张登录
// 表单向 LoginPath 提交（web/src/api/auth.ts 顶部注释：「本地账号 / LDAP
// （共用同一登录表单，由后端按配置路由）/ OIDC」），LDAP 校验已接入
// internal/manager/http/auth_local.go 的 handleLogin。
const (
	LoginPath        = "/api/v1/auth/login"
	OIDCStartPath    = "/api/v1/auth/oidc/start"
	OIDCCallbackPath = "/api/v1/auth/oidc/callback"
)

// idempotentMethods 是不需要 CSRF 校验的方法集合。
var idempotentMethods = map[string]bool{
	http.MethodGet:     true,
	http.MethodHead:    true,
	http.MethodOptions: true,
	http.MethodTrace:   true,
}

// CSRF 实现双提交 Cookie 校验：非幂等方法必须同时携带 XDTS-CSRF 请求头，
// 且与同名 Cookie 的值一致，否则拒绝。
//
// exempt 是显式豁免的路径集合（按 c.FullPath() 匹配，即路由模板而非带参数
// 的实际路径）。目前唯一已知需要豁免的是 LoginPath：建立会话前不存在
// CSRF Cookie。其余写接口一律不豁免。
func CSRF(exempt ...string) gin.HandlerFunc {
	exemptSet := make(map[string]bool, len(exempt))
	for _, p := range exempt {
		exemptSet[p] = true
	}
	return func(c *gin.Context) {
		if idempotentMethods[c.Request.Method] {
			c.Next()
			return
		}
		if exemptSet[c.FullPath()] {
			c.Next()
			return
		}

		header := c.GetHeader(HeaderCSRF)
		cookie, err := c.Cookie(CookieCSRF)
		if header == "" || err != nil || cookie == "" || header != cookie {
			respondError(c, http.StatusForbidden, "csrf_invalid", "CSRF 校验失败", nil)
			c.Abort()
			return
		}
		c.Next()
	}
}

// NewCSRFToken 生成一个新的 CSRF 令牌（32 字节随机数，十六进制编码）。
func NewCSRFToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("http: 生成 CSRF 令牌失败: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// IssueCSRFCookie 生成一个新的 CSRF 令牌并写入 Cookie，返回令牌明文。
//
// 轮换策略：调用方应当只在签发新会话（登录成功）时调用一次；不要在每次
// 请求时都轮换。真正的登录/续期 handler 属于另外的任务，这里只提供签发
// 这个原子操作。
func IssueCSRFCookie(c *gin.Context, maxAge time.Duration, secure bool) (string, error) {
	token, err := NewCSRFToken()
	if err != nil {
		return "", err
	}
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(CookieCSRF, token, int(maxAge.Seconds()), "/", "", secure, false)
	return token, nil
}

// ClearCSRFCookie 清除 CSRF Cookie（登出时应该和 ClearSessionCookie 一起
// 调用）。
func ClearCSRFCookie(c *gin.Context, secure bool) {
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(CookieCSRF, "", -1, "/", "", secure, false)
}
