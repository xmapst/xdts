package http

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/auth"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// RequirePipelineScope enforces center/pipeline grants for non-admin users.
// It is deliberately attached to resource routes, after SessionAuth and the
// global readonly gate have populated the current user context.
func RequirePipelineScope(db *gorm.DB, write bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		role, ok := RoleFromContext(c)
		if ok && role == auth.RoleAdmin {
			c.Next()
			return
		}
		if !ok || role != auth.RoleReadOnly || write {
			respondError(c, http.StatusForbidden, "forbidden", "没有权限执行此操作", nil)
			c.Abort()
			return
		}
		if db == nil {
			respondError(c, http.StatusInternalServerError, "database_unavailable", "管理库不可用", nil)
			c.Abort()
			return
		}
		userID, err := uuidx.Parse(UserIDFromContext(c))
		if err != nil {
			respondError(c, http.StatusForbidden, "forbidden", "没有权限执行此操作", nil)
			c.Abort()
			return
		}
		pipelineID, err := uuidx.Parse(c.Param("id"))
		if err != nil {
			respondError(c, http.StatusBadRequest, "invalid_id", "id 格式不合法", nil)
			c.Abort()
			return
		}
		allowed, err := repo.UserCanAccessPipeline(c.Request.Context(), db, userID, pipelineID, write)
		if err != nil {
			FailInternal(c, err, "check_pipeline_scope_failed")
			c.Abort()
			return
		}
		if !allowed {
			respondError(c, http.StatusForbidden, "forbidden", "没有权限访问此 pipeline", nil)
			c.Abort()
			return
		}
		c.Next()
	}
}

// ResourceScopeGate attaches the current user to resource reads and rejects
// global configuration routes whose responses cannot be scoped to resources.
func ResourceScopeGate(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		role, ok := RoleFromContext(c)
		if !ok || role == auth.RoleAdmin || strings.HasPrefix(c.FullPath(), "/api/v1/auth/") || db == nil {
			c.Next()
			return
		}
		userID, err := uuidx.Parse(UserIDFromContext(c))
		if err != nil {
			respondError(c, http.StatusForbidden, "forbidden", "没有权限访问此资源", nil)
			c.Abort()
			return
		}
		c.Request = c.Request.WithContext(repo.WithUserReadScope(c.Request.Context(), userID))
		if c.Request.Method == http.MethodGet && !validateCenterMetadataRead(c, db) {
			return
		}
		restricted, err := repo.UserScopeRestricted(c.Request.Context(), db, userID)
		if err != nil {
			FailInternal(c, err, "read_user_scope_failed")
			c.Abort()
			return
		}
		if !restricted {
			c.Next()
			return
		}
		if c.Request.Method == http.MethodGet && resourceReadRoute(c.FullPath()) {
			c.Next()
			return
		}
		switch {
		case c.FullPath() == "/api/v1/pipelines" && c.Request.Method == http.MethodGet:
			c.Next()
		case strings.HasPrefix(c.FullPath(), "/api/v1/pipelines/:id/"), c.FullPath() == "/api/v1/pipelines/:id", c.FullPath() == "/api/v1/metrics/pipelines/:id":
			RequirePipelineScope(db, false)(c)
		default:
			respondError(c, http.StatusForbidden, "scope_filter_unavailable", "此接口暂不支持资源范围访问", nil)
			c.Abort()
		}
	}
}

func resourceReadRoute(path string) bool {
	switch path {
	case "/api/v1/centers", "/api/v1/centers/:id", "/api/v1/centers/:id/origin-uuids",
		"/api/v1/nodes", "/api/v1/nodes/:id", "/api/v1/endpoints", "/api/v1/endpoints/:id",
		"/api/v1/endpoints/:id/catalog", "/api/v1/endpoints/:id/definitions",
		"/api/v1/strategies", "/api/v1/strategies/:id", "/api/v1/strategies/:id/versions",
		"/api/v1/strategies/:id/versions/:version/diff",
		"/api/v1/logs", "/api/v1/events", "/api/v1/conflicts", "/api/v1/alerts", "/api/v1/dashboard":
		return true
	}
	return false
}

func validateCenterMetadataRead(c *gin.Context, db *gorm.DB) bool {
	path := c.FullPath()
	centerResource := strings.HasPrefix(path, "/api/v1/centers/:id")
	endpointCatalog := path == "/api/v1/endpoints/:id/catalog" || path == "/api/v1/endpoints/:id/definitions"
	if !centerResource && !endpointCatalog {
		return true
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		c.Abort()
		return false
	}
	if endpointCatalog {
		endpoint, err := repo.GetEndpoint(c.Request.Context(), db, id)
		if err != nil {
			// Do not disclose whether an out-of-scope endpoint exists.
			Fail(c, http.StatusForbidden, "forbidden", "没有权限访问此资源")
			c.Abort()
			return false
		}
		id = endpoint.CenterID
	}
	allowed, err := repo.UserHasFullCenterScope(c.Request.Context(), db, id)
	if err != nil {
		FailInternal(c, err, "check_center_scope_failed")
		c.Abort()
		return false
	}
	if !allowed {
		Fail(c, http.StatusForbidden, "forbidden", "没有权限访问此中心的完整资源")
		c.Abort()
		return false
	}
	return true
}
