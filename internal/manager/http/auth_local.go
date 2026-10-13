package http

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/xmapst/xdts/internal/manager/auth"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// 和 internal/manager/http/csrf.go 里的 LoginPath/OIDCStartPath/OIDCCallbackPath 不放在一起：
// 那三个是「建立会话之前」必须豁免的端点（不豁免会形成登录死锁），
// /auth/me 的豁免原因不同——它是「未登录也要返回 200」的查询接口，本身
// 没有 CSRF 问题（GET 是幂等方法，天然豁免 CSRF），只需要豁免
// SessionAuth，由 handler 自己判断有没有会话。
const MeAuthPath = "/api/v1/auth/me"

// dummyPasswordForTimingProbe 是反时序侧信道探测使用的固定明文；具体取值
// 不重要（只要求任何真实密码都不会恰好等于它），它只用来在用户不存在的
// 分支里也跑一遍和真实分支同样代价的 argon2 计算。
const dummyPasswordForTimingProbe = "xdts-timing-probe-never-a-real-password"

// LoginMethods 描述当前启用的登录方式，供 GET /auth/me 告诉前端登录页该
// 展示哪些登录入口。
type LoginMethods struct {
	Local bool `json:"local"`
	LDAP  bool `json:"ldap"`
	OIDC  bool `json:"oidc"`
}

// implementedLoginMethods 是本二进制真正挂了登录路由的方式集合——
// 注意它回答的问题与 Options.AuthMethods 不同：后者是"运维在配置里启用了
// 什么"，它是"这个二进制能做什么"。
//
// 因此 registerAuthRoutes 的每一个挂载分支都读本变量（见该函数），
// cmd/manager 的启动检查通过 UnimplementedEnabledMethods 读同一个它。
// 新增一种登录方式时：把路由写进对应的 if 分支里，并把这里改成 true。
// 只做前一半，路由挂不上；只做后一半，启动检查会放行一个没有路由的方式。
// 两者都会立刻可见，这正是同源的目的。
var implementedLoginMethods = LoginMethods{
	// POST /auth/login + handleLogin（本文件）。
	Local: true,
	// LDAP 与本地共用 POST /auth/login。
	LDAP: true,
	// 授权码 + PKCE 的 start/callback 路由。
	OIDC: true,
}

// UnimplementedEnabledMethods 返回 enabled 里"启用了但本二进制没有实现"的
// 方式名（稳定顺序：local, ldap, oidc）。
func UnimplementedEnabledMethods(enabled LoginMethods) []string {
	var out []string
	if enabled.Local && !implementedLoginMethods.Local {
		out = append(out, "local")
	}
	if enabled.LDAP && !implementedLoginMethods.LDAP {
		out = append(out, "ldap")
	}
	if enabled.OIDC && !implementedLoginMethods.OIDC {
		out = append(out, "oidc")
	}
	return out
}

// AnyEnabledMethodImplemented 报告 enabled 里是否至少有一种登录方式
// 本二进制真的实现了。
func AnyEnabledMethodImplemented(enabled LoginMethods) bool {
	return (enabled.Local && implementedLoginMethods.Local) ||
		(enabled.LDAP && implementedLoginMethods.LDAP) ||
		(enabled.OIDC && implementedLoginMethods.OIDC)
}

// AuthSource 来自当前会话（auth.Session.AuthSource），不是用户的固定属性
// ——同一个账号未来可能既能本地登录也能 LDAP 登录，「这次是怎么登进来的」
// 只有会话知道。本地登录 handler 签发会话时固定传 auth.AuthSourceLocal，
// `/auth/me` 从 resolveAuthenticatedUser 返回的 Session 里读真实值。
type currentUserView struct {
	ID                 string          `json:"id"`
	Username           string          `json:"username"`
	DisplayName        string          `json:"displayName"`
	Role               auth.Role       `json:"role"`
	AuthSource         auth.AuthSource `json:"authSource"`
	MustChangePassword bool            `json:"mustChangePassword"`
}

type loginRequest struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

type loginResponse struct {
	User currentUserView `json:"user"`
}

type meResponse struct {
	Authenticated bool             `json:"authenticated"`
	User          *currentUserView `json:"user,omitempty"`
	Methods       LoginMethods     `json:"methods"`
}

// localAuthHandler 持有本地登录/登出/查询当前用户三个 handler 共享的依赖。
// 不导出：只在 New() 里构造并挂路由，其它代码不应该越过路由直接调用它的
// 方法。
type localAuthHandler struct {
	db            *gorm.DB
	sessions      auth.ISessionStore
	users         auth.IUserStore
	localUsers    auth.ILocalUserStore
	argon2        auth.Argon2Params
	dummyHash     string
	sessionTTL    time.Duration
	cookieSecure  bool
	methods       LoginMethods
	audit         IAuditRecorder
	lockouts      auth.ILockoutStore
	lockoutPolicy auth.LockoutPolicy
	ldap          *auth.LDAPAuthenticator
}

// defaultSessionTTL 是 Options.SessionTTL 未显式设置（<= 0）时的兜底值，
// 和 config 包 auth.session_ttl 的默认值保持一致（internal/manager/config/load.go 的
// applyDefaults）。只是防御性兜底：正常启动路径下 cmd/manager/main.go 总是
// 会把 cfg.Auth.SessionTTL 传进来。
const defaultSessionTTL = 12 * time.Hour

// newLocalAuthHandler 构造 localAuthHandler。dummyHash 在这里只计算一次
// （而不是每次登录请求都算一次）：它的作用只是让「用户不存在」分支和
// 「用户存在」分支跑同样代价的 argon2 计算，不需要每次都是新的随机值。
func newLocalAuthHandler(sessions auth.ISessionStore, users auth.IUserStore, localUsers auth.ILocalUserStore, opts Options, audit IAuditRecorder, lockouts auth.ILockoutStore) (*localAuthHandler, error) {
	params := opts.Argon2Params
	if params == (auth.Argon2Params{}) {
		params = auth.DefaultArgon2Params()
	}
	dummyHash, err := auth.HashPassword(dummyPasswordForTimingProbe, params)
	if err != nil {
		return nil, fmt.Errorf("http: 初始化本地登录反时序探测哈希失败: %w", err)
	}

	ttl := opts.SessionTTL
	if ttl <= 0 {
		ttl = defaultSessionTTL
	}

	lockoutPolicy := opts.LockoutPolicy
	if lockoutPolicy == (auth.LockoutPolicy{}) {
		lockoutPolicy = auth.DefaultLockoutPolicy()
	}

	var ldapAuth *auth.LDAPAuthenticator
	if opts.AuthMethods.LDAP {
		if opts.DB == nil {
			return nil, errors.New("http: LDAP 登录需要管理库")
		}
		ldapAuth, err = auth.NewLDAPAuthenticator(opts.LDAPConfig)
		if err != nil {
			return nil, err
		}
	}
	return &localAuthHandler{
		ldap:          ldapAuth,
		db:            opts.DB,
		sessions:      sessions,
		users:         users,
		localUsers:    localUsers,
		argon2:        params,
		dummyHash:     dummyHash,
		sessionTTL:    ttl,
		cookieSecure:  opts.CookieSecure,
		methods:       opts.AuthMethods,
		audit:         audit,
		lockouts:      lockouts,
		lockoutPolicy: lockoutPolicy,
	}, nil
}

// handleLogin 处理 POST /api/v1/auth/login。
//
// 本地账号不存在时按配置尝试 LDAP；同名本地账号不能由目录口令接管。
// 两种来源共用会话、CSRF、失败计数和审计。
func (h *localAuthHandler) handleLogin(c *gin.Context) {
	if !h.methods.Local && !h.methods.LDAP {
		Fail(c, http.StatusForbidden, "local_auth_disabled", "本地账号登录未启用")
		return
	}

	var req loginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "用户名和密码不能为空")
		return
	}

	ctx := c.Request.Context()
	clientIP := c.ClientIP()
	userAgent := c.Request.UserAgent()
	now := time.Now()

	// 口令校验之前先查两个维度是否已被锁定（需求 MGR-USR-01a：按用户名 +
	// 客户端 IP 独立计数，见 auth.LockoutScope 的注释）。这里只查询、不
	// 提前 return——是否锁定要等到和口令校验结果合并之后再统一判定和响应
	// （安全要求 2），否则「已锁定」分支会比「未锁定但密码错」分支更快
	// 返回，等于用响应耗时把锁定状态泄漏出去。
	usernameLocked, err := h.lockouts.Check(ctx, auth.LockoutScopeUsername, req.Username, now)
	if err != nil {
		FailInternal(c, err, "lockout_check_failed")
		return
	}
	ipLocked, err := h.lockouts.Check(ctx, auth.LockoutScopeClientIP, clientIP, now)
	if err != nil {
		FailInternal(c, err, "lockout_check_failed")
		return
	}

	cred, lookupErr := h.localUsers.GetByUsername(ctx, req.Username)
	hashToVerify := h.dummyHash
	if lookupErr == nil {
		hashToVerify = cred.PasswordHash
	}

	// 无论前面判定如何都要跑这一次 argon2 校验（安全要求 2）。
	matched, verifyErr := auth.VerifyPassword(req.Password, hashToVerify)

	authSource := auth.AuthSourceLocal
	valid := h.methods.Local && !usernameLocked && !ipLocked && lookupErr == nil && verifyErr == nil && matched && !cred.Disabled
	if !usernameLocked && !ipLocked && errors.Is(lookupErr, auth.ErrLocalUserNotFound) && h.ldap != nil {
		identity, ldapErr := h.ldap.Authenticate(ctx, req.Username, req.Password)
		if ldapErr == nil {
			user, provisionErr := repo.UpsertExternalUser(ctx, h.db, model.AuthSourceLDAP, identity.ExternalID, identity.Username, model.UserRole(identity.Role), now)
			if provisionErr == nil {
				cred = auth.LocalCredential{UserID: user.ID.String(), Username: user.Username, Role: auth.Role(user.Role)}
				valid, authSource = true, auth.AuthSourceLDAP
			} else if !errors.Is(provisionErr, repo.ErrExternalIdentityDenied) {
				FailInternal(c, provisionErr, "external_user_store_failed")
				return
			}
		}
	}
	if !valid {
		// reason 只用于服务端日志/审计，定位「是不是在被撞库/密码喷洒」；
		// 绝不能通过 HTTP 响应回显（安全要求 1）。
		reason := "密码错误"
		switch {
		case usernameLocked && ipLocked:
			reason = "账号与客户端 IP 均已被锁定"
		case usernameLocked:
			reason = "账号已被锁定"
		case ipLocked:
			reason = "客户端 IP 已被锁定"
		case lookupErr != nil:
			reason = "用户不存在"
		case cred.Disabled:
			reason = "账号已被禁用"
		case verifyErr != nil:
			reason = "密码哈希记录格式异常"
		}
		slog.LogAttrs(ctx, slog.LevelWarn, "login failed",
			slog.String("username", req.Username),
			slog.String("client_ip", clientIP),
		)
		h.audit.Record(ctx, AuditEvent{
			Username:   req.Username,
			Action:     model.AuditActionLoginFailure,
			TargetType: model.AuditTargetTypeSession,
			Success:    false,
			Reason:     reason,
			ClientIP:   clientIP,
			UserAgent:  userAgent,
		})
		// Do not count another failure in an already locked dimension.
		if !usernameLocked && !ipLocked {
			if _, err := h.lockouts.RecordFailure(ctx, auth.LockoutScopeUsername, req.Username, now, h.lockoutPolicy); err != nil {
				slog.LogAttrs(ctx, slog.LevelError, "lockout counter persistence failed",
					slog.String("scope", string(auth.LockoutScopeUsername)),
					slog.Any("error", err),
				)
			}
			if _, err := h.lockouts.RecordFailure(ctx, auth.LockoutScopeClientIP, clientIP, now, h.lockoutPolicy); err != nil {
				slog.LogAttrs(ctx, slog.LevelError, "lockout counter persistence failed",
					slog.String("scope", string(auth.LockoutScopeClientIP)),
					slog.Any("error", err),
				)
			}
		}
		Fail(c, http.StatusUnauthorized, "invalid_credentials", "用户名或密码错误")
		return
	}

	// 登录成功：清零两个维度的失败计数，不让之前的失败尝试继续累积。
	if err := h.lockouts.Reset(ctx, auth.LockoutScopeUsername, req.Username); err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "lockout counter reset failed",
			slog.String("scope", string(auth.LockoutScopeUsername)),
			slog.Any("error", err),
		)
	}
	if err := h.lockouts.Reset(ctx, auth.LockoutScopeClientIP, clientIP); err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "lockout counter reset failed",
			slog.String("scope", string(auth.LockoutScopeClientIP)),
			slog.Any("error", err),
		)
	}

	token, _, err := h.sessions.Issue(ctx, cred.UserID, cred.Username, cred.Role, authSource, clientIP, userAgent, h.sessionTTL)
	if err != nil {
		FailInternal(c, err, "session_issue_failed")
		return
	}
	if _, err := IssueCSRFCookie(c, h.sessionTTL, h.cookieSecure); err != nil {
		FailInternal(c, err, "csrf_issue_failed")
		return
	}
	IssueSessionCookie(c, token, h.sessionTTL, h.cookieSecure)

	h.audit.Record(ctx, AuditEvent{
		UserID:     cred.UserID,
		Username:   cred.Username,
		Action:     model.AuditActionLogin,
		TargetType: model.AuditTargetTypeSession,
		TargetID:   cred.UserID,
		Success:    true,
		ClientIP:   clientIP,
		UserAgent:  userAgent,
	})

	OK(c, loginResponse{User: currentUserView{
		ID:                 cred.UserID,
		Username:           cred.Username,
		DisplayName:        cred.Username,
		Role:               cred.Role,
		AuthSource:         authSource,
		MustChangePassword: cred.MustChangePassword,
	}})
}

// handleLogout 处理 POST /api/v1/auth/logout（需求 MGR-USR-04）。
//
// 这个端点没有豁免 SessionAuth：能走到这里说明中间件已经校验过会话合法
// 并把 user_id/username 写进了 context，这里只需要撤销令牌、清 Cookie。
func (h *localAuthHandler) handleLogout(c *gin.Context) {
	ctx := c.Request.Context()
	clientIP := c.ClientIP()
	userID := UserIDFromContext(c)
	username, _ := c.Get(ctxKeyUsername)
	usernameStr, _ := username.(string)

	if token, err := c.Cookie(SessionCookieName); err == nil && token != "" {
		if revokeErr := h.sessions.Revoke(ctx, token); revokeErr != nil {
			FailInternal(c, revokeErr, "session_revoke_failed")
			return
		}
	}

	ClearSessionCookie(c, h.cookieSecure)
	ClearCSRFCookie(c, h.cookieSecure)

	h.audit.Record(ctx, AuditEvent{
		UserID:     userID,
		Username:   usernameStr,
		Action:     model.AuditActionLogout,
		TargetType: model.AuditTargetTypeSession,
		TargetID:   userID,
		Success:    true,
		ClientIP:   clientIP,
		UserAgent:  c.Request.UserAgent(),
	})

	OK(c, gin.H{})
}

// handleMe 处理 GET /api/v1/auth/me。这个路径豁免了 SessionAuth
// （internal/manager/http/server.go），所以未登录也要走到这里、返回 200 而不是 401——契约细节
// 见 LoginMethods 和 meResponse 上面的注释。
func (h *localAuthHandler) handleMe(c *gin.Context) {
	user, sess, ok := resolveAuthenticatedUser(c, h.sessions, h.users)
	if !ok {
		OK(c, meResponse{Authenticated: false, Methods: h.methods})
		return
	}

	OK(c, meResponse{
		Authenticated: true,
		Methods:       h.methods,
		User: &currentUserView{
			ID:                 user.ID,
			Username:           user.Username,
			DisplayName:        user.Username,
			Role:               user.Role,
			AuthSource:         sess.AuthSource,
			MustChangePassword: user.MustChangePassword,
		},
	})
}

func (h *localAuthHandler) handleChangePassword(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	if source, _ := c.Get(ctxKeyAuthSource); source != auth.AuthSourceLocal {
		Fail(c, http.StatusForbidden, "local_account_required", "仅本地账号可修改密码")
		return
	}
	var req struct {
		OldPassword string `json:"oldPassword"`
		NewPassword string `json:"newPassword"`
	}
	if c.ShouldBindJSON(&req) != nil || !validPassword(req.OldPassword) || !validPassword(req.NewPassword) {
		Fail(c, http.StatusBadRequest, "invalid_password", "密码不能为空且不能超过 1024 字节")
		return
	}
	if req.OldPassword == req.NewPassword {
		Fail(c, http.StatusBadRequest, "password_unchanged", "新密码不能与旧密码相同")
		return
	}
	uid, err := uuidx.Parse(UserIDFromContext(c))
	if err != nil {
		unauthenticated(c)
		return
	}
	newHash, err := auth.HashPassword(req.NewPassword, h.argon2)
	if err != nil {
		FailInternal(c, err, "hash_password_failed")
		return
	}
	var oldPasswordInvalid bool
	err = h.db.WithContext(c.Request.Context()).Transaction(func(tx *gorm.DB) error {
		var user model.User
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&user, "id = ?", uid).Error; err != nil {
			return err
		}
		if user.AuthSource != model.AuthSourceLocal {
			return fmt.Errorf("password change: account is not local")
		}
		matched, err := auth.VerifyPassword(req.OldPassword, user.PasswordHash)
		if err != nil || !matched {
			oldPasswordInvalid = true
			return nil
		}
		if err := repo.SetUserPassword(c.Request.Context(), tx, uid, newHash); err != nil {
			return err
		}
		return repo.RevokeAllUserSessionsForUser(c.Request.Context(), tx, uid)
	})
	if err != nil {
		FailInternal(c, err, "change_password_failed")
		return
	}
	if oldPasswordInvalid {
		Fail(c, http.StatusBadRequest, "old_password_invalid", "旧密码错误")
		return
	}
	ClearSessionCookie(c, h.cookieSecure)
	ClearCSRFCookie(c, h.cookieSecure)
	h.audit.Record(c.Request.Context(), AuditEvent{
		UserID: uid.String(), Username: UsernameFromContext(c),
		Action: model.AuditActionUserPasswordChange, TargetType: model.AuditTargetTypeUser,
		TargetID: uid.String(), Success: true, ClientIP: c.ClientIP(), UserAgent: c.Request.UserAgent(),
	})
	c.Status(http.StatusNoContent)
}
