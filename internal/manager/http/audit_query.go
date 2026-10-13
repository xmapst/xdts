package http

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/xmapst/xdts/internal/manager/auth"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/redact"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

type auditLogView struct {
	ID         string                `json:"id"`
	Ts         time.Time             `json:"ts"`
	UserID     *string               `json:"userId"`
	Actor      string                `json:"actor"`
	Action     model.AuditAction     `json:"action"`
	TargetType model.AuditTargetType `json:"targetType"`
	TargetID   *string               `json:"targetId"`
	Success    *bool                 `json:"success"`
	Detail     map[string]any        `json:"detail"`
}

type auditQueryHandler struct{ db *gorm.DB }

func registerAuditQueryRoutes(api *gin.RouterGroup, db *gorm.DB) {
	h := &auditQueryHandler{db: db}
	api.GET("/audit-logs", RequireRole(auth.RoleAdmin), h.list)
}

func (h *auditQueryHandler) list(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	q, page, pageSize, err := parseAuditQuery(c)
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_audit_query", "审计筛选或分页参数不合法")
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	var items []model.AuditLog
	var total int64
	err = h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		items, err = repo.ListAuditLogs(ctx, tx, q)
		if err != nil {
			return err
		}
		total, err = repo.CountAuditLogs(ctx, tx, q)
		return err
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		FailInternal(c, err, "audit_query_failed")
		return
	}
	views := make([]auditLogView, 0, len(items))
	for _, item := range items {
		view, err := toAuditLogView(item)
		if err != nil {
			FailInternal(c, err, "invalid_stored_audit_detail")
			return
		}
		views = append(views, view)
	}
	OK(c, paginated[auditLogView]{Items: views, Total: int(total), Page: page, PageSize: pageSize})
}

func parseAuditQuery(c *gin.Context) (repo.AuditLogQuery, int, int, error) {
	q := repo.AuditLogQuery{}
	invalid := errors.New("invalid audit query")
	page, size := 1, defaultListPageSize
	for _, field := range []struct {
		key string
		out *int
		max int
	}{{"page", &page, 1000001}, {"pageSize", &size, maxListPageSize}} {
		if raw := c.Query(field.key); raw != "" {
			v, err := strconv.Atoi(raw)
			if err != nil || v < 1 || v > field.max {
				return q, 0, 0, invalid
			}
			*field.out = v
		}
	}
	q.Offset, q.Limit = (page-1)*size, size
	if q.Offset > 1000000 {
		return q, 0, 0, invalid
	}
	for _, field := range []struct {
		key string
		out *uuidx.UUID
	}{{"userId", &q.UserID}, {"targetId", &q.TargetID}} {
		if raw := c.Query(field.key); raw != "" {
			id, err := uuidx.Parse(raw)
			if err != nil || id.IsZero() {
				return q, 0, 0, invalid
			}
			*field.out = id
		}
	}
	q.Actor = strings.TrimSpace(c.Query("actor"))
	if !utf8.ValidString(q.Actor) || len([]rune(q.Actor)) > 128 {
		return q, 0, 0, invalid
	}
	q.Action = model.AuditAction(c.Query("action"))
	q.TargetType = model.AuditTargetType(c.Query("targetType"))
	if q.Action != "" && !q.Action.Valid() || q.TargetType != "" && !q.TargetType.Valid() {
		return q, 0, 0, invalid
	}
	for _, field := range []struct {
		key string
		out *time.Time
	}{{"since", &q.Since}, {"until", &q.Until}} {
		if raw := c.Query(field.key); raw != "" {
			v, err := time.Parse(time.RFC3339Nano, raw)
			if err != nil || v.Year() < 1000 || v.Year() > 9999 {
				return q, 0, 0, invalid
			}
			*field.out = v
		}
	}
	if !q.Until.IsZero() && q.Until.Before(q.Since) {
		return q, 0, 0, invalid
	}
	return q, page, size, nil
}

func toAuditLogView(a model.AuditLog) (auditLogView, error) {
	view := auditLogView{ID: a.ID.String(), Ts: a.Ts, UserID: alertOptionalID(a.UserID), Action: a.Action, TargetType: a.TargetType, TargetID: alertOptionalID(a.TargetID), Detail: map[string]any{}}
	if a.UserID == auditUnknownUserID {
		view.UserID = nil
	}
	if len(a.Detail) > 1<<20 {
		view.Detail["truncated"] = true
		return view, nil
	}
	if len(a.Detail) != 0 && !bytes.Equal(bytes.TrimSpace(a.Detail), []byte("null")) {
		decoder := json.NewDecoder(bytes.NewReader(a.Detail))
		decoder.UseNumber()
		if err := decoder.Decode(&view.Detail); err != nil {
			return auditLogView{}, errors.New("invalid audit detail object")
		}
	}
	view.Actor, _ = view.Detail["username"].(string)
	if value, ok := view.Detail["success"].(bool); ok {
		view.Success = &value
	}
	view.Detail = redactAuditValue(view.Detail, 0).(map[string]any)
	return view, nil
}

// Keys identify stored secrets; free text uses the existing DSN/URL redactor.
// The depth and input size limits bound work on stored detail objects.
func redactAuditValue(value any, depth int) any {
	if depth > 64 {
		return "***"
	}
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, child := range v {
			normalized := strings.ToLower(strings.NewReplacer("_", "", "-", "", ".", "").Replace(key))
			if strings.Contains(normalized, "password") || strings.Contains(normalized, "token") || strings.Contains(normalized, "secret") || strings.Contains(normalized, "credential") || normalized == "webhookurl" || normalized == "authorization" || normalized == "privatekey" || normalized == "registercode" {
				out[key] = "***"
			} else {
				out[key] = redactAuditValue(child, depth+1)
			}
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, child := range v {
			out[i] = redactAuditValue(child, depth+1)
		}
		return out
	case string:
		return redact.PotentialCredentials(v)
	default:
		return v
	}
}
