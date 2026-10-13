package http

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/xmapst/xdts/internal/manager/auth"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

type settingsHandler struct{ db *gorm.DB }

func registerSettingsRoutes(api *gin.RouterGroup, db *gorm.DB) {
	h := &settingsHandler{db: db}
	api.GET("/settings", RequireRole(auth.RoleAdmin), h.get)
	api.PUT("/settings", RequireRole(auth.RoleAdmin), h.update)
}

func (h *settingsHandler) get(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	snapshot, err := repo.ReadRuntimeSettings(ctx, h.db)
	if err != nil {
		FailInternal(c, err, "read_settings_failed")
		return
	}
	OK(c, snapshot)
}

func (h *settingsHandler) update(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	var request repo.RuntimeSettingsSnapshot
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, 4097))
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	decodeErr := decoder.Decode(&request)
	var trailing any
	if err != nil || len(body) > 4096 || decodeErr != nil || decoder.Decode(&trailing) != io.EOF || request.Validate() != nil {
		Fail(c, http.StatusBadRequest, "invalid_settings", "设置参数不合法")
		return
	}
	user, err := uuidx.Parse(UserIDFromContext(c))
	if err != nil || user.IsZero() {
		Fail(c, http.StatusUnauthorized, "unauthorized", "请先登录")
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	var result repo.RuntimeSettingsSnapshot
	err = h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now := time.Now()
		var err error
		var changed bool
		result, changed, err = repo.UpdateRuntimeSettings(ctx, tx, request.RuntimeSettings, request.Version, user, now)
		if err != nil || !changed {
			return err
		}
		changes, err := json.Marshal(result.RuntimeSettings)
		if err != nil {
			return err
		}
		detail, err := json.Marshal(auditDetail{Username: UsernameFromContext(c), Success: true, Changes: changes})
		if err != nil {
			return err
		}
		return repo.CreateAuditLog(ctx, tx, &model.AuditLog{UserID: user, Action: model.AuditActionSettingsChange, TargetType: model.AuditTargetTypeSetting, Detail: model.JSON(detail)}, now)
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	switch {
	case errors.Is(err, repo.ErrRuntimeSettingsVersion):
		Fail(c, http.StatusConflict, "settings_version_conflict", "设置已被更新，请刷新后重试")
	case errors.Is(err, repo.ErrRuntimeSettingsInvalid):
		Fail(c, http.StatusBadRequest, "invalid_settings", "设置参数不合法")
	case err != nil:
		FailInternal(c, err, "update_settings_failed")
	default:
		OK(c, result)
	}
}
