package http

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/xmapst/xdts/internal/manager/auth"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

type alertRuleView struct {
	ID                string               `json:"id"`
	Name              string               `json:"name"`
	Dimension         model.AlertDimension `json:"dimension"`
	Enabled           bool                 `json:"enabled"`
	Threshold         model.AlertThreshold `json:"threshold"`
	WebhookConfigured bool                 `json:"webhookConfigured"`
	SuppressSeconds   int64                `json:"suppressSeconds"`
	Version           int64                `json:"version"`
	CreatedAt         time.Time            `json:"createdAt"`
}

type alertView struct {
	ID         string               `json:"id"`
	RuleID     *string              `json:"ruleId"`
	Dimension  model.AlertDimension `json:"dimension"`
	Level      model.EventLevel     `json:"level"`
	Status     model.AlertStatus    `json:"status"`
	PipelineID *string              `json:"pipelineId"`
	NodeID     *string              `json:"nodeId"`
	Detail     json.RawMessage      `json:"detail"`
	FiredAt    time.Time            `json:"firedAt"`
	HitCount   int64                `json:"hitCount"`
	LastSeenAt *time.Time           `json:"lastSeenAt"`
	ResolvedAt *time.Time           `json:"resolvedAt"`
	NotifiedAt *time.Time           `json:"notifiedAt"`
}

type alertRuleRequest struct {
	Name            string               `json:"name" binding:"required"`
	Dimension       model.AlertDimension `json:"dimension" binding:"required"`
	Enabled         *bool                `json:"enabled"`
	Threshold       json.RawMessage      `json:"threshold"`
	WebhookURL      *string              `json:"webhookUrl"`
	SuppressSeconds int64                `json:"suppressSeconds"`
	Version         int64                `json:"version"`
}

type alertHandler struct {
	db    *gorm.DB
	audit IAuditRecorder
}

func registerAlertRoutes(api *gin.RouterGroup, db *gorm.DB, audit IAuditRecorder) {
	h := &alertHandler{db: db, audit: audit}
	api.GET("/alert-rules", h.rules)
	api.POST("/alert-rules", RequireRole(auth.RoleAdmin), h.save)
	api.PUT("/alert-rules/:id", RequireRole(auth.RoleAdmin), h.save)
	api.DELETE("/alert-rules/:id", RequireRole(auth.RoleAdmin), h.delete)
	api.GET("/alerts", h.alerts)
}

func toAlertRuleView(r model.AlertRule) (alertRuleView, error) {
	t, err := model.ParseAlertThreshold(r.Threshold)
	return alertRuleView{ID: r.ID.String(), Name: r.Name, Dimension: r.Dimension, Enabled: r.Enabled, Threshold: t, WebhookConfigured: r.WebhookURL != "", SuppressSeconds: r.SuppressSeconds, Version: r.LockVersion, CreatedAt: r.CreatedAt}, err
}

func alertOptionalID(id uuidx.UUID) *string {
	if id.IsZero() {
		return nil
	}
	s := id.String()
	return &s
}

func (h *alertHandler) rules(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	p, n := alertListPage(c)
	rules, total, err := repo.ListAlertRules(c.Request.Context(), h.db, p, n)
	if err != nil {
		FailInternal(c, err, "list_alert_rules_failed")
		return
	}
	items := make([]alertRuleView, 0, len(rules))
	for _, r := range rules {
		v, err := toAlertRuleView(r)
		if err != nil {
			FailInternal(c, err, "invalid_stored_alert_rule")
			return
		}
		items = append(items, v)
	}
	OK(c, paginated[alertRuleView]{Items: items, Total: int(total), Page: p, PageSize: n})
}

func alertListPage(c *gin.Context) (int, int) {
	p := parsePositiveIntOrDefault(c.Query("page"), 1)
	n := min(parsePositiveIntOrDefault(c.Query("pageSize"), defaultListPageSize), maxListPageSize)
	if p > 1000000 {
		p = 1000000
	}
	return p, n
}

func (h *alertHandler) save(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	var req alertRuleRequest
	if c.ShouldBindJSON(&req) != nil {
		Fail(c, 400, "invalid_request", "请求参数不合法")
		return
	}
	rule := model.AlertRule{Enabled: true}
	updating := c.Param("id") != ""
	if updating {
		id, err := uuidx.Parse(c.Param("id"))
		if err != nil || id.IsZero() {
			Fail(c, 400, "invalid_id", "id 格式不合法")
			return
		}
		r, err := repo.GetAlertRule(c.Request.Context(), h.db, id)
		if err != nil {
			h.fail(c, err)
			return
		}
		rule = *r
		if req.Version != rule.LockVersion {
			h.fail(c, repo.ErrAlertRuleVersionConflict)
			return
		}
	} else {
		user, err := uuidx.Parse(UserIDFromContext(c))
		if err != nil || user.IsZero() {
			Fail(c, 401, "unauthorized", "请重新登录")
			return
		}
		rule.CreatedBy = user
	}
	rule.Name = strings.TrimSpace(req.Name)
	rule.Dimension = req.Dimension
	rule.Threshold = append(model.JSON(nil), req.Threshold...)
	rule.SuppressSeconds = req.SuppressSeconds
	if req.Enabled != nil {
		rule.Enabled = *req.Enabled
	}
	if req.WebhookURL != nil {
		rule.WebhookURL = strings.TrimSpace(*req.WebhookURL)
	}
	if err := model.ValidateAlertRuleConfiguration(rule); err != nil {
		Fail(c, 400, "invalid_alert_rule", "告警规则参数不合法")
		return
	}
	var err error
	if updating {
		err = repo.UpdateAlertRule(c.Request.Context(), h.db, rule)
		rule.LockVersion++
	} else {
		err = repo.CreateAlertRule(c.Request.Context(), h.db, &rule)
	}
	if err != nil {
		h.fail(c, err)
		return
	}
	event := baseAuditEvent(c)
	event.Action = model.AuditActionAlertRuleChange
	event.TargetType = model.AuditTargetTypeAlertRule
	event.TargetID = rule.ID.String()
	h.audit.Record(c.Request.Context(), event)
	v, err := toAlertRuleView(rule)
	if err != nil {
		FailInternal(c, err, "invalid_stored_alert_rule")
		return
	}
	OK(c, v)
}

func (h *alertHandler) delete(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil || id.IsZero() {
		Fail(c, 400, "invalid_id", "id 格式不合法")
		return
	}
	var req struct {
		Version int64 `json:"version" binding:"required"`
	}
	if c.ShouldBindJSON(&req) != nil {
		Fail(c, 400, "invalid_request", "请求参数不合法")
		return
	}
	if _, err := repo.GetAlertRule(c.Request.Context(), h.db, id); err != nil {
		h.fail(c, err)
		return
	}
	if err := repo.DeleteAlertRule(c.Request.Context(), h.db, id, req.Version, time.Now()); err != nil {
		h.fail(c, err)
		return
	}
	event := baseAuditEvent(c)
	event.Action = model.AuditActionAlertRuleDelete
	event.TargetType = model.AuditTargetTypeAlertRule
	event.TargetID = id.String()
	h.audit.Record(c.Request.Context(), event)
	OK(c, struct{}{})
}

func (h *alertHandler) fail(c *gin.Context, err error) {
	switch {
	case errors.Is(err, repo.ErrAlertRuleNotFound):
		Fail(c, 404, "not_found", "告警规则不存在")
	case errors.Is(err, repo.ErrAlertRuleNameConflict):
		Fail(c, 409, "alert_rule_name_conflict", "告警规则名称已存在")
	case errors.Is(err, repo.ErrAlertRuleVersionConflict):
		Fail(c, 409, "alert_rule_version_conflict", "规则已修改，请刷新后重试")
	default:
		FailInternal(c, err, "alert_rule_failed")
	}
}

func (h *alertHandler) alerts(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	f := repo.AlertFilter{}
	if s := c.Query("ruleId"); s != "" {
		id, err := uuidx.Parse(s)
		if err != nil || id.IsZero() {
			Fail(c, 400, "invalid_filter", "筛选参数不合法")
			return
		}
		f.RuleID = &id
	}
	if s := c.Query("level"); s != "" {
		v := model.EventLevel(s)
		if !v.Valid() {
			Fail(c, 400, "invalid_filter", "筛选参数不合法")
			return
		}
		f.Level = &v
	}
	if s := c.Query("status"); s != "" {
		v := model.AlertStatus(s)
		if !v.Valid() {
			Fail(c, 400, "invalid_filter", "筛选参数不合法")
			return
		}
		f.Status = &v
	}
	for _, field := range []struct {
		key string
		out **time.Time
	}{{"since", &f.Since}, {"until", &f.Until}} {
		if s := c.Query(field.key); s != "" {
			v, err := time.Parse(time.RFC3339Nano, s)
			if err != nil || v.Year() < 1000 || v.Year() > 9999 {
				Fail(c, 400, "invalid_filter", "时间范围不合法")
				return
			}
			*field.out = &v
		}
	}
	if f.Since != nil && f.Until != nil && f.Until.Before(*f.Since) {
		Fail(c, 400, "invalid_filter", "时间范围不合法")
		return
	}
	p, n := alertListPage(c)
	entries, total, err := repo.ListAlerts(c.Request.Context(), h.db, f, p, n)
	if err != nil {
		FailInternal(c, err, "list_alerts_failed")
		return
	}
	items := make([]alertView, 0, len(entries))
	for _, a := range entries {
		detail := json.RawMessage(a.Detail)
		if len(detail) == 0 {
			detail = json.RawMessage(`{}`)
		}
		items = append(items, alertView{ID: a.ID.String(), RuleID: alertOptionalID(a.RuleID), Dimension: a.Dimension, Level: a.Level, Status: a.Status, PipelineID: alertOptionalID(a.PipelineID), NodeID: alertOptionalID(a.NodeID), Detail: detail, FiredAt: a.FiredAt, HitCount: a.HitCount, LastSeenAt: a.LastSeenAt, ResolvedAt: a.ResolvedAt, NotifiedAt: a.NotifiedAt})
	}
	OK(c, paginated[alertView]{Items: items, Total: int(total), Page: p, PageSize: n})
}
