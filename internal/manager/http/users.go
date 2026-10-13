package http

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/auth"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

type userHandler struct {
	db      *gorm.DB
	audit   IAuditRecorder
	argon2  auth.Argon2Params
	lockout auth.LockoutPolicy
}

type userSummary struct {
	ID          string           `json:"id"`
	Username    string           `json:"username"`
	DisplayName string           `json:"displayName"`
	Role        model.UserRole   `json:"role"`
	Status      model.UserStatus `json:"status"`
	AuthSource  model.AuthSource `json:"authSource"`
	LastLoginAt *time.Time       `json:"lastLoginAt"`
	CreatedAt   time.Time        `json:"createdAt"`
}

type userDetail struct {
	userSummary
	ExternalID   string     `json:"externalId"`
	UpdatedAt    time.Time  `json:"updatedAt"`
	LockedUntil  *time.Time `json:"lockedUntil"`
	FailureCount int        `json:"failureCount"`
}

func registerUserRoutes(api *gin.RouterGroup, db *gorm.DB, audit IAuditRecorder, argon2 auth.Argon2Params, lockout auth.LockoutPolicy) {
	if argon2 == (auth.Argon2Params{}) {
		argon2 = auth.DefaultArgon2Params()
	}
	if lockout == (auth.LockoutPolicy{}) {
		lockout = auth.DefaultLockoutPolicy()
	}
	h := &userHandler{db: db, audit: audit, argon2: argon2, lockout: lockout}
	api.GET("/users", h.list)
	api.GET("/users/:id", h.get)
	api.POST("/users", h.create)
	api.PATCH("/users/:id", h.update)
	api.PUT("/users/:id/password", h.resetPassword)
	api.PUT("/users/:id/status", h.setStatus)
	api.POST("/users/:id/unlock", h.unlock)
	api.POST("/users/:id/sessions/revoke-all", h.revokeSessions)
	api.DELETE("/users/:id", h.delete)
	api.GET("/users/:id/scopes", RequireRole(auth.RoleAdmin), h.listScopes)
	api.PUT("/users/:id/scopes", RequireRole(auth.RoleAdmin), h.replaceScopes)
}

type userScopeView struct {
	ID         string           `json:"id"`
	Scope      model.GrantScope `json:"scope"`
	ResourceID string           `json:"resourceId"`
	CanWrite   bool             `json:"canWrite"`
}

type replaceScopesRequest struct {
	Restricted *bool `json:"restricted" binding:"required"`
	Scopes     []struct {
		Scope      model.GrantScope `json:"scope"`
		ResourceID string           `json:"resourceId"`
		CanWrite   bool             `json:"canWrite"`
	} `json:"scopes"`
}

func (h *userHandler) listScopes(c *gin.Context) {
	u, ok := h.load(c)
	if !ok {
		return
	}
	restricted, err := repo.UserScopeRestricted(c.Request.Context(), h.db, u.ID)
	if err != nil {
		FailInternal(c, err, "read_user_scope_failed")
		return
	}
	grants, err := repo.ListUserGrants(c.Request.Context(), h.db, u.ID)
	if err != nil {
		FailInternal(c, err, "list_user_scopes_failed")
		return
	}
	out := make([]userScopeView, 0, len(grants))
	for _, g := range grants {
		out = append(out, userScopeView{ID: g.ID.String(), Scope: g.Scope, ResourceID: g.ResourceID.String(), CanWrite: false})
	}
	OK(c, gin.H{"restricted": restricted, "items": out})
}

func (h *userHandler) replaceScopes(c *gin.Context) {
	u, ok := h.load(c)
	if !ok {
		return
	}
	var req replaceScopesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "请求体格式不合法")
		return
	}
	actor, err := uuidx.Parse(UserIDFromContext(c))
	if err != nil {
		Fail(c, http.StatusForbidden, "forbidden", "没有权限执行此操作")
		return
	}
	grants := make([]model.UserGrant, 0, len(req.Scopes))
	seen := make(map[string]struct{}, len(req.Scopes))
	for _, item := range req.Scopes {
		resource, parseErr := uuidx.Parse(item.ResourceID)
		if parseErr != nil || !item.Scope.Valid() {
			Fail(c, http.StatusBadRequest, "invalid_scope", "scope 或 resourceId 不合法")
			return
		}
		key := string(item.Scope) + ":" + resource.String()
		if _, exists := seen[key]; exists {
			Fail(c, http.StatusBadRequest, "duplicate_scope", "scope 授权不能重复")
			return
		}
		seen[key] = struct{}{}

		grants = append(grants, model.UserGrant{ResourceID: resource, Scope: item.Scope, CanWrite: item.CanWrite})
	}
	if err := repo.ReplaceUserScopes(c.Request.Context(), h.db, u.ID, actor, *req.Restricted, grants); err != nil {
		if errors.Is(err, repo.ErrInvalidUserScope) {
			Fail(c, http.StatusBadRequest, "invalid_scope", "授权范围不合法")
			return
		}
		FailInternal(c, err, "replace_user_scopes_failed")
		return
	}
	event := baseAuditEvent(c)
	event.Action = model.AuditActionUserGrantChange
	event.TargetType = model.AuditTargetTypeUser
	event.TargetID = u.ID.String()
	event.Changes, _ = json.Marshal(map[string]any{"scope_count": len(grants), "restricted": *req.Restricted, "scopes": req.Scopes})
	h.audit.Record(c.Request.Context(), event)
	h.listScopes(c)
}

func toUserSummary(u model.User) userSummary {
	status := u.Status
	if status == model.UserStatusLocked {
		status = model.UserStatusDisabled
	}
	return userSummary{ID: u.ID.String(), Username: u.Username, DisplayName: u.Username,
		Role: u.Role, Status: status, AuthSource: u.AuthSource,
		LastLoginAt: u.LastLoginAt, CreatedAt: u.CreatedAt}
}

func (h *userHandler) detail(c *gin.Context, u model.User) (userDetail, error) {
	view := userDetail{userSummary: toUserSummary(u), ExternalID: u.ExternalID, UpdatedAt: u.UpdatedAt}
	view.Status = u.Status
	lockout, err := repo.GetLoginLockout(c.Request.Context(), h.db, model.LoginLockoutScopeUsername, u.Username)
	if err != nil {
		return userDetail{}, err
	}
	if lockout != nil {
		if time.Since(lockout.WindowStartedAt) < h.lockout.Window {
			view.FailureCount = lockout.FailureCount
		}
		if lockout.LockedUntil != nil && lockout.LockedUntil.After(time.Now()) {
			view.LockedUntil = lockout.LockedUntil
			if u.Status == model.UserStatusActive {
				view.Status = model.UserStatusLocked
			}
		}
	}
	return view, nil
}

func (h *userHandler) load(c *gin.Context) (*model.User, bool) {
	if !requireDB(c, h.db) {
		return nil, false
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return nil, false
	}
	u, err := repo.GetUser(c.Request.Context(), h.db, id)
	if errors.Is(err, repo.ErrUserNotFound) {
		Fail(c, http.StatusNotFound, "not_found", "用户不存在")
		return nil, false
	}
	if err != nil {
		FailInternal(c, err, "get_user_failed")
		return nil, false
	}
	return u, true
}

func (h *userHandler) respondDetail(c *gin.Context, u model.User, status int) {
	fresh, err := repo.GetUser(c.Request.Context(), h.db, u.ID)
	if err != nil {
		FailInternal(c, err, "get_user_failed")
		return
	}
	view, err := h.detail(c, *fresh)
	if err != nil {
		FailInternal(c, err, "get_user_lockout_failed")
		return
	}
	OKStatus(c, status, view)
}

func validUsername(name string) bool {
	return name != "" && utf8.RuneCountInString(name) <= 128
}

func validPassword(password string) bool {
	return password != "" && len(password) <= 1024
}

func (h *userHandler) auditChange(c *gin.Context, action model.AuditAction, target model.AuditTargetType, id uuidx.UUID, changes any) {
	event := baseAuditEvent(c)
	event.Action, event.TargetType, event.TargetID = action, target, id.String()
	if changes != nil {
		event.Changes, _ = json.Marshal(changes)
	}
	h.audit.Record(c.Request.Context(), event)
}

func (h *userHandler) list(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	users, err := repo.ListUsers(c.Request.Context(), h.db)
	if err != nil {
		FailInternal(c, err, "list_users_failed")
		return
	}
	keyword := strings.ToLower(strings.TrimSpace(c.Query("keyword")))
	role, source, status := c.Query("role"), c.Query("authSource"), c.Query("status")
	views := make([]userSummary, 0, len(users))
	for _, u := range users {
		view := toUserSummary(u)
		if keyword != "" && !strings.Contains(strings.ToLower(u.Username), keyword) {
			continue
		}
		if model.UserRole(role).Valid() && role != string(u.Role) {
			continue
		}
		if model.AuthSource(source).Valid() && source != string(u.AuthSource) {
			continue
		}
		if (status == "active" || status == "disabled") && status != string(view.Status) {
			continue
		}
		views = append(views, view)
	}
	page := parsePositiveIntOrDefault(c.Query("page"), 1)
	pageSize := min(parsePositiveIntOrDefault(c.Query("pageSize"), defaultListPageSize), maxListPageSize)
	start := len(views)
	if page-1 <= len(views)/pageSize {
		start = min((page-1)*pageSize, len(views))
	}
	end := min(start+pageSize, len(views))
	OK(c, paginated[userSummary]{Items: views[start:end], Total: len(views), Page: page, PageSize: pageSize})
}

func (h *userHandler) get(c *gin.Context) {
	u, ok := h.load(c)
	if !ok {
		return
	}
	h.respondDetail(c, *u, http.StatusOK)
}

func (h *userHandler) create(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	var req struct {
		Username   string          `json:"username"`
		Password   string          `json:"password"`
		Role       string          `json:"role"`
		AuthSource json.RawMessage `json:"authSource"`
	}
	if c.ShouldBindJSON(&req) != nil || !validUsername(strings.TrimSpace(req.Username)) || req.Role == "" || len(req.AuthSource) > 0 {
		Fail(c, http.StatusBadRequest, "invalid_request", "请求参数不合法")
		return
	}
	if !model.UserRole(req.Role).Valid() {
		Fail(c, http.StatusBadRequest, "invalid_role", "用户角色不合法")
		return
	}
	if !validPassword(req.Password) {
		Fail(c, http.StatusBadRequest, "invalid_password", "密码不能为空且不能超过 1024 字节")
		return
	}
	hash, err := auth.HashPassword(req.Password, h.argon2)
	if err != nil {
		FailInternal(c, err, "hash_password_failed")
		return
	}
	u := &model.User{Username: strings.TrimSpace(req.Username), PasswordHash: hash,
		AuthSource: model.AuthSourceLocal, Role: model.UserRole(req.Role), Status: model.UserStatusActive}
	if err := repo.CreateUser(c.Request.Context(), h.db, u); err != nil {
		if errors.Is(err, repo.ErrUsernameConflict) {
			Fail(c, http.StatusConflict, "username_conflict", "用户名已存在")
			return
		}
		FailInternal(c, err, "create_user_failed")
		return
	}
	h.auditChange(c, model.AuditActionUserCreate, model.AuditTargetTypeUser, u.ID, nil)
	h.respondDetail(c, *u, http.StatusCreated)
}

func (h *userHandler) update(c *gin.Context) {
	u, ok := h.load(c)
	if !ok {
		return
	}
	var req struct {
		Username *string `json:"username"`
		Role     *string `json:"role"`
	}
	if c.ShouldBindJSON(&req) != nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "请求参数不合法")
		return
	}
	if req.Username != nil && u.AuthSource != model.AuthSourceLocal {
		Fail(c, http.StatusBadRequest, "external_user_field_not_editable", "外部用户不能修改用户名")
		return
	}
	before := map[string]string{}
	after := map[string]string{}
	fields := []string{}
	if req.Username != nil {
		name := strings.TrimSpace(*req.Username)
		if !validUsername(name) {
			Fail(c, http.StatusBadRequest, "invalid_request", "用户名不能为空且不能超过 128 字符")
			return
		}
		if name != u.Username {
			before["username"] = u.Username
			after["username"] = name
			fields = append(fields, "username")
			u.Username = name
		}
	}
	if req.Role != nil {
		role := model.UserRole(*req.Role)
		if !role.Valid() {
			Fail(c, http.StatusBadRequest, "invalid_role", "用户角色不合法")
			return
		}
		if u.ID.String() == UserIDFromContext(c) && u.Role == model.UserRoleAdmin && role == model.UserRoleReadOnly {
			Fail(c, http.StatusConflict, "cannot_demote_self", "不能降低自己的管理员角色")
			return
		}
		if role != u.Role {
			before["role"] = string(u.Role)
			after["role"] = string(role)
			fields = append(fields, "role")
			u.Role = role
		}
	}
	if len(fields) > 0 {
		if err := repo.UpdateUser(c.Request.Context(), h.db, u); err != nil {
			if errors.Is(err, repo.ErrUsernameConflict) {
				Fail(c, http.StatusConflict, "username_conflict", "用户名已存在")
				return
			}
			FailInternal(c, err, "update_user_failed")
			return
		}
		h.auditChange(c, model.AuditActionUserUpdate, model.AuditTargetTypeUser, u.ID,
			gin.H{"fields": fields, "before": before, "after": after})
	}
	h.respondDetail(c, *u, http.StatusOK)
}

func (h *userHandler) resetPassword(c *gin.Context) {
	u, ok := h.load(c)
	if !ok {
		return
	}
	if u.AuthSource != model.AuthSourceLocal {
		Fail(c, http.StatusBadRequest, "external_user_field_not_editable", "外部用户密码不能在此重置")
		return
	}
	var req struct {
		Password string `json:"password"`
	}
	if c.ShouldBindJSON(&req) != nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "请求参数不合法")
		return
	}
	if !validPassword(req.Password) {
		Fail(c, http.StatusBadRequest, "invalid_password", "密码不能为空且不能超过 1024 字节")
		return
	}
	hash, err := auth.HashPassword(req.Password, h.argon2)
	if err != nil {
		FailInternal(c, err, "hash_password_failed")
		return
	}
	err = h.db.WithContext(c.Request.Context()).Transaction(func(tx *gorm.DB) error {
		if err := repo.SetUserPassword(c.Request.Context(), tx, u.ID, hash); err != nil {
			return err
		}
		return repo.RevokeAllUserSessionsForUser(c.Request.Context(), tx, u.ID)
	})
	if err != nil {
		FailInternal(c, err, "reset_password_failed")
		return
	}
	h.auditChange(c, model.AuditActionUserPasswordReset, model.AuditTargetTypeUser, u.ID, nil)
	c.Status(http.StatusNoContent)
}

func (h *userHandler) setStatus(c *gin.Context) {
	u, ok := h.load(c)
	if !ok {
		return
	}
	var req struct {
		Status string `json:"status"`
	}
	if c.ShouldBindJSON(&req) != nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "请求参数不合法")
		return
	}
	if req.Status == "locked" {
		Fail(c, http.StatusBadRequest, "status_locked_not_settable", "不能手动设置锁定状态")
		return
	}
	if req.Status != "active" && req.Status != "disabled" {
		Fail(c, http.StatusBadRequest, "invalid_status", "用户状态不合法")
		return
	}
	status := model.UserStatus(req.Status)
	if u.ID.String() == UserIDFromContext(c) && status == model.UserStatusDisabled {
		Fail(c, http.StatusConflict, "cannot_disable_self", "不能禁用自己的账号")
		return
	}
	if status != u.Status {
		old := u.Status
		err := h.db.WithContext(c.Request.Context()).Transaction(func(tx *gorm.DB) error {
			if err := repo.SetUserStatus(c.Request.Context(), tx, u.ID, status); err != nil {
				return err
			}
			if status == model.UserStatusDisabled {
				return repo.RevokeAllUserSessionsForUser(c.Request.Context(), tx, u.ID)
			}
			return nil
		})
		if err != nil {
			FailInternal(c, err, "set_user_status_failed")
			return
		}
		u.Status = status
		h.auditChange(c, model.AuditActionUserStatusChange, model.AuditTargetTypeUser, u.ID,
			gin.H{"from": old, "to": status})
	}
	h.respondDetail(c, *u, http.StatusOK)
}

func (h *userHandler) unlock(c *gin.Context) {
	u, ok := h.load(c)
	if !ok {
		return
	}
	if err := repo.ResetLoginLockout(c.Request.Context(), h.db, model.LoginLockoutScopeUsername, u.Username, time.Now()); err != nil {
		FailInternal(c, err, "unlock_user_failed")
		return
	}
	h.auditChange(c, model.AuditActionUserUnlock, model.AuditTargetTypeUser, u.ID, nil)
	h.respondDetail(c, *u, http.StatusOK)
}

func (h *userHandler) revokeSessions(c *gin.Context) {
	u, ok := h.load(c)
	if !ok {
		return
	}
	count, err := repo.RevokeAllUserSessionsForUserCount(c.Request.Context(), h.db, u.ID)
	if err != nil {
		FailInternal(c, err, "revoke_user_sessions_failed")
		return
	}
	h.auditChange(c, model.AuditActionUserSessionsRevoke, model.AuditTargetTypeSession, u.ID, nil)
	OK(c, gin.H{"revokedCount": count})
}

func (h *userHandler) delete(c *gin.Context) {
	u, ok := h.load(c)
	if !ok {
		return
	}
	if u.ID.String() == UserIDFromContext(c) {
		Fail(c, http.StatusConflict, "cannot_delete_self", "不能删除自己的账号")
		return
	}
	err := h.db.WithContext(c.Request.Context()).Transaction(func(tx *gorm.DB) error {
		if err := repo.DeleteUser(c.Request.Context(), tx, u.ID); err != nil {
			return err
		}
		return repo.RevokeAllUserSessionsForUser(c.Request.Context(), tx, u.ID)
	})
	if err != nil {
		FailInternal(c, err, "delete_user_failed")
		return
	}
	h.auditChange(c, model.AuditActionUserDelete, model.AuditTargetTypeUser, u.ID, nil)
	c.Status(http.StatusNoContent)
}
