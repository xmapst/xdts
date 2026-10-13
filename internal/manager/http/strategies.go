// Strategy HTTP handlers manage strategy definitions, immutable versions and usage.
package http

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/pipelinespec"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// strategyView 对齐 web/src/api/strategies.ts 的 StrategySummary。
type strategyView struct {
	ID             string    `json:"id"`
	Name           string    `json:"name"`
	Description    string    `json:"description"`
	CurrentVersion uint32    `json:"currentVersion"`
	CreatedAt      time.Time `json:"createdAt"`
	UpdatedAt      time.Time `json:"updatedAt"`
}

func toStrategyView(s model.Strategy) strategyView {
	return strategyView{
		ID:             s.ID.String(),
		Name:           s.Name,
		Description:    s.Description,
		CurrentVersion: s.CurrentVersion,
		CreatedAt:      s.CreatedAt,
		UpdatedAt:      s.UpdatedAt,
	}
}

// strategyVersionView 对齐 StrategyVersionSummary。Rules 原样透传（不解析
// 语义，见文件头注释），用 json.RawMessage 而不是 model.JSON 类型，避免
// http 包对外暴露 model.JSON 这个仓储层类型的编解码细节。
type strategyVersionView struct {
	ID         string          `json:"id"`
	StrategyID string          `json:"strategyId"`
	Version    uint32          `json:"version"`
	Rules      json.RawMessage `json:"rules"`
	CreatedBy  string          `json:"createdBy"`
	CreatedAt  time.Time       `json:"createdAt"`
}

func toStrategyVersionView(v model.StrategyVersion) strategyVersionView {
	return strategyVersionView{
		ID:         v.ID.String(),
		StrategyID: v.StrategyID.String(),
		Version:    v.Version,
		Rules:      json.RawMessage(v.Rules),
		CreatedBy:  v.CreatedBy.String(),
		CreatedAt:  v.CreatedAt,
	}
}

// strategyDiffChangeType 对齐 StrategyDiffChangeType。
type strategyDiffChangeType string

const (
	strategyDiffAdded   strategyDiffChangeType = "added"
	strategyDiffRemoved strategyDiffChangeType = "removed"
	strategyDiffChanged strategyDiffChangeType = "changed"
)

// strategyDiffEntry 对齐 StrategyDiffEntry。OldValue/NewValue 故意不加
// omitempty：前端类型是 `oldValue: unknown`（必定存在的字段，不是可选
// 字段），added/removed 场景下取 nil，要序列化成 JSON null，而不是让字段
// 整个消失。
type strategyDiffEntry struct {
	Path       string                 `json:"path"`
	ChangeType strategyDiffChangeType `json:"changeType"`
	OldValue   any                    `json:"oldValue"`
	NewValue   any                    `json:"newValue"`
}

// strategyCreateRequest 对齐 StrategyInput。创建只建空壳，不接受
// rules——repo.CreateStrategy 本就不接受，第一个版本要紧接着单独调用
// POST /strategies/:id/versions 创建（repo 层既有实现如此，不是这里的
// 设计选择）。
type strategyCreateRequest struct {
	Name        string `json:"name" binding:"required"`
	Description string `json:"description"`
}

// strategyUpdateRequest 对齐 StrategyUpdateInput（Partial<StrategyInput>）：
// 两个字段都可选，PATCH 语义，用指针区分“未提供”与“显式设为空字符串”。
type strategyUpdateRequest struct {
	Name        *string `json:"name"`
	Description *string `json:"description"`
}

// strategyVersionCreateRequest 对齐 createStrategyVersion(id, rules) 的
// 请求体 `{ rules }`。
type strategyVersionCreateRequest struct {
	Rules json.RawMessage `json:"rules" binding:"required"`
}

type strategyHandler struct {
	db    *gorm.DB
	audit IAuditRecorder
}

// 策略 JSON 通过 pipelinespec.ParseStrategyRules 解析和校验。
func registerStrategyRoutes(api *gin.RouterGroup, db *gorm.DB, audit IAuditRecorder) {
	h := &strategyHandler{db: db, audit: audit}
	api.GET("/strategies", h.list)
	api.POST("/strategies", h.create)
	api.GET("/strategies/:id", h.get)
	api.PATCH("/strategies/:id", h.update)
	api.DELETE("/strategies/:id", h.delete)
	api.GET("/strategies/:id/versions", h.listVersions)
	api.POST("/strategies/:id/versions", h.createVersion)
	api.DELETE("/strategies/:id/versions/:version", h.deleteVersion)
	api.GET("/strategies/:id/versions/:version/diff", h.diffVersions)
}

// validateStrategyRules 校验 rules 是“合法 JSON 且非 null/非空对象”——不
// 解析其业务语义（MGR-STR-02，见文件头注释）。json.RawMessage 字段配合
// binding:"required" 只能挡住“完全没传这个 key”，挡不住显式传
// `{"rules": null}`（4 字节的合法 JSON，长度非零），必须在这里单独校验。
func validateStrategyRules(raw json.RawMessage) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return errors.New("rules 不能为空")
	}
	var v any
	if err := json.Unmarshal(trimmed, &v); err != nil {
		return fmt.Errorf("rules 不是合法 JSON: %w", err)
	}
	obj, ok := v.(map[string]any)
	if !ok || len(obj) == 0 {
		return errors.New("rules 必须是非空 JSON 对象")
	}
	return nil
}

// parseStrategyVersionParam 解析路径/查询参数里的版本号——版本号从 1 开始
// 自增（repo.CreateStrategyVersion），落库列是无符号整数。
func parseStrategyVersionParam(raw string) (uint32, error) {
	v, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(v), nil
}

func (h *strategyHandler) list(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	strategies, err := repo.ListStrategies(c.Request.Context(), h.db)
	if err != nil {
		FailInternal(c, err, "list_strategies_failed")
		return
	}
	views := make([]strategyView, 0, len(strategies))
	for _, s := range strategies {
		views = append(views, toStrategyView(s))
	}
	OK(c, views)
}

func (h *strategyHandler) get(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	s, err := repo.GetStrategy(c.Request.Context(), h.db, id)
	if err != nil {
		if errors.Is(err, repo.ErrStrategyNotFound) {
			Fail(c, http.StatusNotFound, "not_found", "策略不存在")
			return
		}
		FailInternal(c, err, "get_strategy_failed")
		return
	}
	OK(c, toStrategyView(*s))
}

func (h *strategyHandler) create(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	var req strategyCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "请求参数不合法")
		return
	}

	s := &model.Strategy{Name: req.Name, Description: req.Description}
	if err := repo.CreateStrategy(c.Request.Context(), h.db, s); err != nil {
		if errors.Is(err, repo.ErrStrategyNameConflict) {
			Fail(c, http.StatusConflict, "strategy_name_conflict", "策略名称已存在")
			return
		}
		FailInternal(c, err, "create_strategy_failed")
		return
	}

	event := baseAuditEvent(c)
	event.Action = model.AuditActionStrategyChange
	event.TargetType = model.AuditTargetTypeStrategy
	event.TargetID = s.ID.String()
	h.audit.Record(c.Request.Context(), event)

	OK(c, toStrategyView(*s))
}

func (h *strategyHandler) update(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	var req strategyUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "请求参数不合法")
		return
	}

	ctx := c.Request.Context()
	current, err := repo.GetStrategy(ctx, h.db, id)
	if err != nil {
		if errors.Is(err, repo.ErrStrategyNotFound) {
			Fail(c, http.StatusNotFound, "not_found", "策略不存在")
			return
		}
		FailInternal(c, err, "get_strategy_failed")
		return
	}

	name := current.Name
	if req.Name != nil {
		name = *req.Name
	}
	description := current.Description
	if req.Description != nil {
		description = *req.Description
	}

	if err := repo.UpdateStrategyMeta(ctx, h.db, id, name, description); err != nil {
		switch {
		case errors.Is(err, repo.ErrStrategyNameConflict):
			Fail(c, http.StatusConflict, "strategy_name_conflict", "策略名称已存在")
		case errors.Is(err, repo.ErrStrategyNotFound):
			Fail(c, http.StatusNotFound, "not_found", "策略不存在")
		default:
			FailInternal(c, err, "update_strategy_failed")
		}
		return
	}

	// 重新查询而不是就地改写 current 后直接返回：UpdateStrategyMeta 用的是
	// map 形式的 Updates，updated_at 是否被 gorm 自动写回不应该由调用方猜测，
	// 直接读一次库里的权威状态最省心。
	updated, err := repo.GetStrategy(ctx, h.db, id)
	if err != nil {
		FailInternal(c, err, "get_strategy_failed")
		return
	}

	event := baseAuditEvent(c)
	event.Action = model.AuditActionStrategyChange
	event.TargetType = model.AuditTargetTypeStrategy
	event.TargetID = id.String()
	h.audit.Record(ctx, event)

	OK(c, toStrategyView(*updated))
}

// delete 处理 DELETE /strategies/:id。repo.DeleteStrategy 已经做了“无
// pipeline 引用”校验，这里只把 ErrStrategyHasDependents 转成 409，人类
// 可读的阻断原因（含引用数量）直接透传 err.Error()，与 internal/manager/http/centers.go 的
// delete 是同一形状。
func (h *strategyHandler) delete(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	if err := repo.DeleteStrategy(c.Request.Context(), h.db, id); err != nil {
		switch {
		case errors.Is(err, repo.ErrStrategyNotFound):
			Fail(c, http.StatusNotFound, "not_found", "策略不存在")
		case errors.Is(err, repo.ErrStrategyHasDependents):
			Fail(c, http.StatusConflict, "strategy_has_dependents", err.Error())
		default:
			FailInternal(c, err, "delete_strategy_failed")
		}
		return
	}

	event := baseAuditEvent(c)
	event.Action = model.AuditActionStrategyDelete
	event.TargetType = model.AuditTargetTypeStrategy
	event.TargetID = id.String()
	h.audit.Record(c.Request.Context(), event)

	OK(c, gin.H{})
}

func (h *strategyHandler) listVersions(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	if _, err := repo.GetStrategy(c.Request.Context(), h.db, id); err != nil {
		if errors.Is(err, repo.ErrStrategyNotFound) {
			Fail(c, http.StatusNotFound, "not_found", "策略不存在")
			return
		}
		FailInternal(c, err, "get_strategy_failed")
		return
	}
	svs, err := repo.ListStrategyVersions(c.Request.Context(), h.db, id)
	if err != nil {
		FailInternal(c, err, "list_strategy_versions_failed")
		return
	}
	views := make([]strategyVersionView, 0, len(svs))
	for _, v := range svs {
		views = append(views, toStrategyVersionView(v))
	}
	OK(c, views)
}

// createVersion 处理 POST /strategies/:id/versions——这就是“编辑”
// （MGR-STR-01：版本不可变，修改即生成新版本）。版本号与 current_version
// 推进都由 repo.CreateStrategyVersion 在同一事务里完成。
func (h *strategyHandler) createVersion(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	var req strategyVersionCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "请求参数不合法")
		return
	}
	canonicalRules, err := canonicalPipelineRules(req.Rules)
	if err != nil {
		writePipelineValidationError(c, err)
		return
	}

	createdBy, err := uuidx.Parse(UserIDFromContext(c))
	if err != nil {
		FailInternal(c, err, "invalid_session_user_id")
		return
	}

	ctx := c.Request.Context()
	sv, err := repo.CreateStrategyVersion(ctx, h.db, id, model.JSON(canonicalRules), createdBy)
	if err != nil {
		if errors.Is(err, repo.ErrStrategyNotFound) {
			Fail(c, http.StatusNotFound, "not_found", "策略不存在")
			return
		}
		FailInternal(c, err, "create_strategy_version_failed")
		return
	}

	event := baseAuditEvent(c)
	event.Action = model.AuditActionStrategyChange
	event.TargetType = model.AuditTargetTypeStrategy
	event.TargetID = id.String()
	h.audit.Record(ctx, event)

	OK(c, toStrategyVersionView(*sv))
}

// deleteVersion 处理 DELETE /strategies/:id/versions/:version（需求
// MGR-STR-04）。repo.DeleteStrategyVersion 已经做了两条互相独立的拒绝
// 检查（是否当前版本 / 是否被 pipeline 引用），这里只做 errors.Is 映射，
// 且两者不复用同一个错误码——前端要能分别提示“先发布新版本”与“先解除
// 引用”，这是两种完全不同的用户动作。
func (h *strategyHandler) deleteVersion(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	version, err := parseStrategyVersionParam(c.Param("version"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_version", "version 格式不合法")
		return
	}

	if err := repo.DeleteStrategyVersion(c.Request.Context(), h.db, id, version); err != nil {
		switch {
		case errors.Is(err, repo.ErrStrategyNotFound):
			Fail(c, http.StatusNotFound, "not_found", "策略不存在")
		case errors.Is(err, repo.ErrStrategyVersionNotFound):
			Fail(c, http.StatusNotFound, "version_not_found", "策略版本不存在")
		case errors.Is(err, repo.ErrStrategyVersionIsCurrent):
			Fail(c, http.StatusConflict, "version_is_current", "该版本是当前生效版本，请先发布新版本后再删除")
		case errors.Is(err, repo.ErrStrategyVersionHasDependents):
			Fail(c, http.StatusConflict, "version_has_dependents", err.Error())
		default:
			FailInternal(c, err, "delete_strategy_version_failed")
		}
		return
	}

	// 同 delete()：用 AuditActionStrategyVersionDelete，理由完全一致
	// （删除不可逆、审计记录不可回填，需要能单独筛选）。
	event := baseAuditEvent(c)
	event.Action = model.AuditActionStrategyVersionDelete
	event.TargetType = model.AuditTargetTypeStrategy
	event.TargetID = id.String()
	h.audit.Record(c.Request.Context(), event)

	OK(c, gin.H{})
}

func (h *strategyHandler) diffVersions(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	version, err := parseStrategyVersionParam(c.Param("version"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_version", "version 格式不合法")
		return
	}

	ctx := c.Request.Context()
	targetVersion, err := repo.GetStrategyVersion(ctx, h.db, id, version)
	if err != nil {
		if errors.Is(err, repo.ErrStrategyVersionNotFound) {
			Fail(c, http.StatusNotFound, "version_not_found", "对比的版本不存在")
			return
		}
		FailInternal(c, err, "get_strategy_version_failed")
		return
	}

	base := version - 1
	if raw := c.Query("base"); raw != "" {
		parsedBase, err := parseStrategyVersionParam(raw)
		if err != nil {
			Fail(c, http.StatusBadRequest, "invalid_base", "base 格式不合法")
			return
		}
		base = parsedBase
	}
	baseVersion, err := repo.GetStrategyVersion(ctx, h.db, id, base)
	if err != nil {
		if errors.Is(err, repo.ErrStrategyVersionNotFound) {
			Fail(c, http.StatusNotFound, "version_not_found", "对比的版本不存在")
			return
		}
		FailInternal(c, err, "get_strategy_version_failed")
		return
	}

	var oldRules, newRules any
	if err := json.Unmarshal(baseVersion.Rules, &oldRules); err != nil {
		FailInternal(c, err, "parse_base_rules_failed")
		return
	}
	if err := json.Unmarshal(targetVersion.Rules, &newRules); err != nil {
		FailInternal(c, err, "parse_target_rules_failed")
		return
	}

	OK(c, diffStrategyRules(oldRules, newRules, ""))
}

// 规则：
//   - 双方都是 JSON 对象（map[string]any，显式排除数组）时逐 key 递归展开
//     "a.b.c" 路径；只在一侧出现的 key 记为 added/removed；两侧都是对象的
//     字段递归；否则按值整体比较，不等则记为 changed。
//   - 数组与标量一律整体按值比较，不做元素级 diff（含顺序变化——两个内容
//     相同但顺序不同的数组视为不相等）。
//   - 双方都不是对象（例如直接比较两个标量）时，如果不相等，用 "(root)"
//     作为 path。
func diffStrategyRules(oldValue, newValue any, basePath string) []strategyDiffEntry {
	entries := make([]strategyDiffEntry, 0)

	oldObj, oldIsObj := oldValue.(map[string]any)
	newObj, newIsObj := newValue.(map[string]any)

	if oldIsObj && newIsObj {
		seen := make(map[string]struct{}, len(oldObj)+len(newObj))
		for k := range oldObj {
			seen[k] = struct{}{}
		}
		for k := range newObj {
			seen[k] = struct{}{}
		}
		for k := range seen {
			path := k
			if basePath != "" {
				path = basePath + "." + k
			}
			oldChild, hasOld := oldObj[k]
			newChild, hasNew := newObj[k]
			switch {
			case !hasOld:
				entries = append(entries, strategyDiffEntry{Path: path, ChangeType: strategyDiffAdded, OldValue: nil, NewValue: newChild})
			case !hasNew:
				entries = append(entries, strategyDiffEntry{Path: path, ChangeType: strategyDiffRemoved, OldValue: oldChild, NewValue: nil})
			default:
				oldChildObj, oldChildIsObj := oldChild.(map[string]any)
				newChildObj, newChildIsObj := newChild.(map[string]any)
				if oldChildIsObj && newChildIsObj {
					entries = append(entries, diffStrategyRules(oldChildObj, newChildObj, path)...)
				} else if !reflect.DeepEqual(oldChild, newChild) {
					entries = append(entries, strategyDiffEntry{Path: path, ChangeType: strategyDiffChanged, OldValue: oldChild, NewValue: newChild})
				}
			}
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
		return entries
	}

	if !reflect.DeepEqual(oldValue, newValue) {
		path := basePath
		if path == "" {
			path = "(root)"
		}
		entries = append(entries, strategyDiffEntry{Path: path, ChangeType: strategyDiffChanged, OldValue: oldValue, NewValue: newValue})
	}
	return entries
}

func conflictPolicyFromRules(doc *pipelinespec.StrategyRulesDoc) *iproto.ConflictPolicy {
	if doc.Target == nil {
		return nil
	}
	return doc.Target.ConflictPolicy
}
