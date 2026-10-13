package http

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/xmapst/xdts/internal/manager/auth"
)

// SessionCookieName 是会话令牌所在的 Cookie 名（HttpOnly +
// Secure + SameSite）。
const SessionCookieName = "XDTS-SESSION"

const (
	// ctxKeyUsername 是访问日志读取「当前用户」使用的 context key
	// （internal/manager/http/access_log.go）。
	ctxKeyUsername           = "xdts.username"
	ctxKeyUserID             = "xdts.user_id"
	ctxKeyRole               = "xdts.role"
	ctxKeyAuthSource         = "xdts.auth_source"
	ctxKeyMustChangePassword = "xdts.must_change_password"
)

// sessionAuthExemptPaths is the common source of unauthenticated route templates.
func sessionAuthExemptPaths() []string {
	return []string{LoginPath, OIDCStartPath, OIDCCallbackPath, MeAuthPath}
}

// exempt matches registered Gin route templates, not user-supplied URL paths.
func SessionAuth(sessions auth.ISessionStore, users auth.IUserStore, exempt ...string) gin.HandlerFunc {
	exemptSet := make(map[string]bool, len(exempt))
	for _, p := range exempt {
		exemptSet[p] = true
	}
	return func(c *gin.Context) {
		if exemptSet[c.FullPath()] {
			c.Next()
			return
		}

		user, sess, ok := resolveAuthenticatedUser(c, sessions, users)
		if !ok {
			unauthenticated(c)
			return
		}

		c.Set(ctxKeyUsername, user.Username)
		c.Set(ctxKeyUserID, user.ID)
		c.Set(ctxKeyRole, user.Role)
		c.Set(ctxKeyAuthSource, sess.AuthSource)
		c.Set(ctxKeyMustChangePassword, user.MustChangePassword)
		c.Next()
	}
}

// RequirePasswordChange 把首次改密要求作为服务端业务接口的闸门。
func RequirePasswordChange() gin.HandlerFunc {
	return func(c *gin.Context) {
		mustChange, _ := c.Get(ctxKeyMustChangePassword)
		if mustChange != true {
			c.Next()
			return
		}
		switch c.FullPath() {
		case "/api/v1/auth/me", "/api/v1/auth/logout", "/api/v1/auth/password":
			c.Next()
		default:
			respondError(c, http.StatusForbidden, "password_change_required", "请先修改初始密码", nil)
			c.Abort()
		}
	}
}

// resolveAuthenticatedUser 尝试从请求的会话 Cookie 解析出「当前」用户与
// 对应的会话记录，校验规则和 SessionAuth 完全一致：令牌存在且未过期
// （ISessionStore.Verify）、账号未被禁用（IUserStore 里的「当前」状态，不
// 信任签发时的角色快照）。
//
// 返回 Session 而不只是 User：AuthSource（这次会话是本地/LDAP/OIDC 哪种
// 方式登录建立的）记在 Session 上而不是 User 上（同一个账号未来可能既能
// 本地登录也能 LDAP 登录），`/auth/me` 需要这个值。
//
// 之所以从 SessionAuth 里抽出来：GET /auth/me 需要在未登录时也返回 200
// （只是 authenticated:false），不能像 SessionAuth 一样直接 401+Abort，
// 因此不能直接复用中间件本身，但校验逻辑必须和 SessionAuth 完全一致——
// 抽成共享函数，避免两处实现走样（比如一处忘记检查 Disabled）。
func resolveAuthenticatedUser(c *gin.Context, sessions auth.ISessionStore, users auth.IUserStore) (auth.User, auth.Session, bool) {
	token, err := c.Cookie(SessionCookieName)
	if err != nil || token == "" {
		return auth.User{}, auth.Session{}, false
	}

	sess, err := sessions.Verify(c.Request.Context(), token)
	if err != nil {
		return auth.User{}, auth.Session{}, false
	}

	user, err := users.GetByID(c.Request.Context(), sess.UserID)
	if err != nil || user.Disabled {
		return auth.User{}, auth.Session{}, false
	}
	return user, sess, true
}

func unauthenticated(c *gin.Context) {
	respondError(c, http.StatusUnauthorized, "unauthenticated", "未登录或会话已过期", nil)
	c.Abort()
}

// RoleFromContext 取出当前请求已认证用户的角色；未认证时返回零值。
func RoleFromContext(c *gin.Context) (auth.Role, bool) {
	v, ok := c.Get(ctxKeyRole)
	if !ok {
		return "", false
	}
	role, ok := v.(auth.Role)
	return role, ok
}

// UserIDFromContext 取出当前请求已认证用户的 ID；未认证时返回空字符串。
func UserIDFromContext(c *gin.Context) string {
	v, ok := c.Get(ctxKeyUserID)
	if !ok {
		return ""
	}
	id, _ := v.(string)
	return id
}

// UsernameFromContext 取出当前请求已认证用户的用户名；未认证时返回空
// 字符串。供审计记录（AuditEvent.Username）等需要"可读身份"而不只是
// ID 的场景使用，避免每个写操作 handler 各自重复
// `c.Get(ctxKeyUsername)` + 类型断言这段样板。
func UsernameFromContext(c *gin.Context) string {
	v, ok := c.Get(ctxKeyUsername)
	if !ok {
		return ""
	}
	name, _ := v.(string)
	return name
}

// AuthSourceFromContext 取出当前会话的登录来源（本地/LDAP/OIDC）；未认证
// 时返回零值。
func AuthSourceFromContext(c *gin.Context) (auth.AuthSource, bool) {
	v, ok := c.Get(ctxKeyAuthSource)
	if !ok {
		return "", false
	}
	src, ok := v.(auth.AuthSource)
	return src, ok
}

// SameSite 用 Lax 而不是 Strict：OIDC 授权码回调（GET
// /api/v1/auth/oidc/callback）是一次跨站导航，Strict Cookie 在这种场景下
// 不会被浏览器带上，会打断回调流程；Lax 仍然能挡住跨站的非幂等请求，安全
// 属性上足够。
func IssueSessionCookie(c *gin.Context, token string, maxAge time.Duration, secure bool) {
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(SessionCookieName, token, int(maxAge.Seconds()), "/", "", secure, true)
}

// ClearSessionCookie 清除会话 Cookie（登出时使用）。
func ClearSessionCookie(c *gin.Context, secure bool) {
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(SessionCookieName, "", -1, "/", "", secure, true)
}
