package http

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/manager/task"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

type iPipelinePrecheckRunner interface {
	RunPipelinePrecheck(context.Context, uuidx.UUID, uuidx.UUID) (*repo.PrecheckState, error)
}

type precheckHandler struct {
	db     *gorm.DB
	runner iPipelinePrecheckRunner
}

type precheckItemView struct {
	ID          string               `json:"id"`
	Item        string               `json:"item"`
	Level       model.PrecheckLevel  `json:"level"`
	Status      model.PrecheckStatus `json:"status"`
	Detail      model.JSON           `json:"detail,omitempty"`
	Confirmed   bool                 `json:"confirmed"`
	ConfirmedAt *time.Time           `json:"confirmedAt"`
}

type precheckView struct {
	BatchID       string             `json:"batchId"`
	ConfigVersion uint64             `json:"configVersion"`
	Complete      bool               `json:"complete"`
	CheckedAt     time.Time          `json:"checkedAt"`
	ExpiresAt     time.Time          `json:"expiresAt"`
	CanStart      bool               `json:"canStart"`
	Reason        string             `json:"reason,omitempty"`
	Items         []precheckItemView `json:"items"`
}

func registerPrecheckReadRoutes(api *gin.RouterGroup, db *gorm.DB, probers ...IEndpointProber) {
	h := &precheckHandler{db: db}
	if len(probers) > 0 {
		h.runner, _ = probers[0].(iPipelinePrecheckRunner)
	}
	api.POST("/pipelines/:id/precheck", RequirePipelineScope(db, true), h.run)
	api.GET("/pipelines/:id/precheck", RequirePipelineScope(db, false), h.get)
	api.POST("/pipelines/:id/precheck/confirm", RequirePipelineScope(db, true), h.confirm)
}

func precheckErrorCode(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, repo.ErrPrecheckConfigChanged):
		return "precheck_changed"
	case errors.Is(err, repo.ErrPrecheckExpired):
		return "precheck_expired"
	case errors.Is(err, repo.ErrPrecheckIncomplete):
		return "precheck_incomplete"
	case errors.Is(err, repo.ErrPrecheckBlocked):
		return "precheck_blocked"
	case errors.Is(err, repo.ErrPrecheckConfirmationRequired):
		return "precheck_confirmation_required"
	case errors.Is(err, repo.ErrPrecheckAlreadyConfirmed):
		return "precheck_already_confirmed"
	case errors.Is(err, repo.ErrPrecheckNotConfirmable):
		return "precheck_not_confirmable"
	case errors.Is(err, repo.ErrPrecheckInvalid):
		return "precheck_invalid"
	default:
		return "precheck_failed"
	}
}

func (h *precheckHandler) fail(c *gin.Context, err error) {
	switch {
	case errors.Is(err, task.ErrProbeUnavailable), errors.Is(err, task.ErrProbeQueueFull):
		Fail(c, http.StatusServiceUnavailable, "precheck_unavailable", "预检查服务尚未就绪或正在忙碌，请稍后重试")
		return
	case errors.Is(err, task.ErrProbeCrossCenter), errors.Is(err, task.ErrProbeNodeOffline):
		Fail(c, http.StatusConflict, "precheck_node_unavailable", "指定节点必须在线且与本侧端点同中心")
		return
	case errors.Is(err, task.ErrCatalogNotProbed):
		Fail(c, http.StatusConflict, "not_probed", "请先完成源与目标端点连接探测")
		return
	case (errors.Is(err, context.Canceled) || errors.Is(err, sql.ErrTxDone)) && errors.Is(c.Request.Context().Err(), context.Canceled):
		// The browser canceled this read (navigation, refresh or closing). No
		// client is waiting for an error response and this is not a server fault.
		c.Status(499)
		return
	case errors.Is(err, context.DeadlineExceeded):
		Fail(c, http.StatusGatewayTimeout, "precheck_timeout", "预检查超时，请重新执行并查询最新批次")
		return
	case errors.Is(err, repo.ErrTimestampConfirmationRequired):
		Fail(c, http.StatusConflict, "timestamp_confirmation_required", "请先确认时间点对应的实际事务边界")
		return
	case errors.Is(err, task.ErrPipelinePrecheckInput):
		Fail(c, http.StatusUnprocessableEntity, "precheck_configuration_invalid", "预检查配置不可执行，请检查两侧停止或暂停状态、同步范围、位点及连接配置")
		return
	}
	if errors.Is(err, repo.ErrPipelineNotFound) {
		Fail(c, http.StatusNotFound, "not_found", "管道不存在")
		return
	}
	if errors.Is(err, repo.ErrPrecheckNotFound) {
		Fail(c, http.StatusNotFound, "precheck_not_found", "尚未执行预检查")
		return
	}
	code := precheckErrorCode(err)
	if code != "precheck_failed" {
		Fail(c, http.StatusConflict, code, "检查结果不可用于此次操作，请刷新结果并处理阻断项或重新预检查")
		return
	}
	FailInternal(c, err, "precheck_failed")
}

func (h *precheckHandler) id(c *gin.Context) (uuidx.UUID, bool) {
	if !requireDB(c, h.db) {
		return uuidx.Nil, false
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return uuidx.Nil, false
	}
	return id, true
}

func (h *precheckHandler) render(c *gin.Context, id uuidx.UUID) {
	state, err := repo.GetPrecheckState(c.Request.Context(), h.db, id, time.Now())
	if err != nil {
		h.fail(c, err)
		return
	}
	h.renderState(c, state)
}

func (h *precheckHandler) renderState(c *gin.Context, state *repo.PrecheckState) {
	run := state.Stored.Run
	view := precheckView{BatchID: run.ID.String(), ConfigVersion: run.ConfigVersion, Complete: run.Complete, CheckedAt: run.CheckedAt, ExpiresAt: run.ExpiresAt, CanStart: state.GateError == nil, Reason: precheckErrorCode(state.GateError), Items: []precheckItemView{}}
	for _, item := range state.Stored.Items {
		view.Items = append(view.Items, precheckItemView{ID: item.ID.String(), Item: item.Item, Level: item.Level, Status: item.Status, Detail: item.Detail, Confirmed: !item.ConfirmedBy.IsZero(), ConfirmedAt: item.ConfirmedAt})
	}
	OK(c, view)
}

func (h *precheckHandler) run(c *gin.Context) {
	id, ok := h.id(c)
	if !ok {
		return
	}
	if h.runner == nil {
		h.fail(c, task.ErrProbeUnavailable)
		return
	}
	// Execution has no caller-defined catalogue, result or confirmation fields.
	decoder := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, 1024))
	decoder.DisallowUnknownFields()
	var request struct{}
	err := decoder.Decode(&request)
	if err != nil && !errors.Is(err, io.EOF) {
		Fail(c, http.StatusBadRequest, "invalid_precheck_request", "执行预检查不接受自定义检查结果或参数")
		return
	}
	if err == nil && decoder.Decode(&request) != io.EOF {
		Fail(c, http.StatusBadRequest, "invalid_precheck_request", "执行请求只接受一个空对象")
		return
	}
	user, err := uuidx.Parse(UserIDFromContext(c))
	if err != nil || user.IsZero() {
		Fail(c, http.StatusUnauthorized, "unauthorized", "登录会话无效")
		return
	}
	state, err := h.runner.RunPipelinePrecheck(c.Request.Context(), id, user)
	if err != nil {
		h.fail(c, err)
		return
	}
	if state == nil || state.Stored.Run.ID.IsZero() || state.Stored.Run.PipelineID != id {
		h.fail(c, task.ErrPrecheckProbeFailed)
		return
	}
	h.renderState(c, state)
}

func (h *precheckHandler) get(c *gin.Context) {
	id, ok := h.id(c)
	if ok {
		h.render(c, id)
	}
}

func (h *precheckHandler) confirm(c *gin.Context) {
	id, ok := h.id(c)
	if !ok {
		return
	}
	var request struct {
		BatchID string   `json:"batchId"`
		Items   []string `json:"items"`
		Confirm bool     `json:"confirm"`
	}
	if c.ShouldBindJSON(&request) != nil || !request.Confirm || len(request.Items) == 0 || len(request.Items) > 100 {
		Fail(c, http.StatusBadRequest, "confirmation_required", "请明确确认当前批次中的指定警告项")
		return
	}
	batch, err := uuidx.Parse(request.BatchID)
	if err != nil || batch.IsZero() {
		Fail(c, http.StatusBadRequest, "invalid_batch", "批次编号不合法")
		return
	}
	items := make([]uuidx.UUID, 0, len(request.Items))
	for _, s := range request.Items {
		item, err := uuidx.Parse(s)
		if err != nil || item.IsZero() {
			Fail(c, http.StatusBadRequest, "invalid_item", "检查项编号不合法")
			return
		}
		items = append(items, item)
	}
	user, err := uuidx.Parse(UserIDFromContext(c))
	if err != nil || user.IsZero() {
		Fail(c, http.StatusUnauthorized, "unauthorized", "登录会话无效")
		return
	}
	if err := repo.ConfirmPrecheckRun(c.Request.Context(), h.db, id, batch, items, user, time.Now()); err != nil {
		h.fail(c, err)
		return
	}
	h.render(c, id)
}
