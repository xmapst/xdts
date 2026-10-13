package http

import (
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/xmapst/xdts/internal/manager/auth"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
)

const oidcBrowserCookie = "XDTS-OIDC"

type oidcAuthHandler struct {
	provider   *auth.OIDCAuthenticator
	login      *localAuthHandler
	configured bool
}

func (h *oidcAuthHandler) start(c *gin.Context) {
	if !h.configured {
		Fail(c, http.StatusForbidden, "oidc_auth_disabled", "OIDC 登录未启用")
		return
	}
	flow, err := h.provider.NewFlow()
	if err != nil {
		FailInternal(c, err, "oidc_flow_failed")
		return
	}
	now := time.Now()
	record := model.OIDCLoginState{StateHash: repo.HashOIDCValue(flow.State), BrowserHash: repo.HashOIDCValue(flow.BrowserToken), Nonce: flow.Nonce, Verifier: flow.Verifier, ConfigHash: h.provider.ConfigHash(), ExpiresAt: now.Add(h.provider.StateTTL())}
	if err := repo.CreateOIDCLoginState(c.Request.Context(), h.login.db, record, now); err != nil {
		FailInternal(c, err, "oidc_state_store_failed")
		return
	}
	h.setBrowserCookie(c, flow.BrowserToken, int(h.provider.StateTTL().Seconds()))
	c.Header("Cache-Control", "no-store")
	c.Header("Referrer-Policy", "no-referrer")
	c.Redirect(http.StatusFound, h.provider.AuthorizationURL(flow))
}

func (h *oidcAuthHandler) setBrowserCookie(c *gin.Context, value string, seconds int) {
	http.SetCookie(c.Writer, &http.Cookie{Name: oidcBrowserCookie, Value: value, Path: OIDCCallbackPath, HttpOnly: true, Secure: h.login.cookieSecure, SameSite: http.SameSiteLaxMode, MaxAge: seconds})
}

func (h *oidcAuthHandler) failure(c *gin.Context) {
	h.login.audit.Record(c.Request.Context(), AuditEvent{Action: model.AuditActionLoginFailure, TargetType: model.AuditTargetTypeSession, Success: false, Reason: "OIDC 认证失败", ClientIP: c.ClientIP(), UserAgent: c.Request.UserAgent()})
	Fail(c, http.StatusUnauthorized, "invalid_credentials", "登录认证失败")
}

func (h *oidcAuthHandler) callback(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.Header("Referrer-Policy", "no-referrer")
	if !h.configured {
		Fail(c, http.StatusForbidden, "oidc_auth_disabled", "OIDC 登录未启用")
		return
	}
	query := c.Request.URL.Query()
	if len(query["state"]) != 1 || len(query["code"]) > 1 || len(query["error"]) > 1 {
		h.failure(c)
		return
	}
	browser, err := c.Cookie(oidcBrowserCookie)
	if err != nil {
		h.failure(c)
		return
	}
	record, err := repo.ConsumeOIDCLoginState(c.Request.Context(), h.login.db, query.Get("state"), browser, h.provider.ConfigHash(), time.Now())
	if err != nil {
		if errors.Is(err, repo.ErrOIDCStateInvalid) {
			h.failure(c)
		} else {
			FailInternal(c, err, "oidc_state_consume_failed")
		}
		return
	}
	h.setBrowserCookie(c, "", -1)
	if query.Get("error") != "" || len(query["code"]) != 1 {
		h.failure(c)
		return
	}
	identity, err := h.provider.ExchangeAndVerify(c.Request.Context(), query.Get("code"), record.Nonce, record.Verifier)
	if err != nil {
		h.failure(c)
		return
	}
	user, err := repo.UpsertExternalUser(c.Request.Context(), h.login.db, model.AuthSourceOIDC, identity.ExternalID, identity.Username, model.UserRole(identity.Role), time.Now())
	if err != nil {
		if errors.Is(err, repo.ErrExternalIdentityDenied) {
			h.failure(c)
		} else {
			FailInternal(c, err, "external_user_store_failed")
		}
		return
	}
	token, _, err := h.login.sessions.Issue(c.Request.Context(), user.ID.String(), user.Username, identity.Role, auth.AuthSourceOIDC, c.ClientIP(), c.Request.UserAgent(), h.login.sessionTTL)
	if err != nil {
		FailInternal(c, err, "session_issue_failed")
		return
	}
	if _, err := IssueCSRFCookie(c, h.login.sessionTTL, h.login.cookieSecure); err != nil {
		FailInternal(c, err, "csrf_issue_failed")
		return
	}
	IssueSessionCookie(c, token, h.login.sessionTTL, h.login.cookieSecure)
	h.login.audit.Record(c.Request.Context(), AuditEvent{UserID: user.ID.String(), Username: user.Username, Action: model.AuditActionLogin, TargetType: model.AuditTargetTypeSession, TargetID: user.ID.String(), Success: true, ClientIP: c.ClientIP(), UserAgent: c.Request.UserAgent()})
	c.Redirect(http.StatusFound, "/")
}
