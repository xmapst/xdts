package http

import (
	"errors"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/manager/task"
	"github.com/xmapst/xdts/internal/topologycheck"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

type reinitHandler struct {
	db     *gorm.DB
	prober IEndpointProber
	defs   iEndpointDefinitionsProber
}

type reinitView struct {
	PipelineID    string              `json:"pipelineId"`
	FromRunID     uint64              `json:"fromRunId"`
	ConfigVersion uint64              `json:"configVersion"`
	Digest        string              `json:"digest"`
	Document      repo.ReinitDocument `json:"document"`
	ConfirmedAt   *time.Time          `json:"confirmedAt"`
	NewRunID      uint64              `json:"newRunId"`
	SnapshotUUID  string              `json:"snapshotUuid"`
	SchemaUUID    string              `json:"schemaUuid"`
	PrepareUUID   string              `json:"prepareUuid"`
	CmdSeq        uint64              `json:"cmdSeq"`
	SourceApplied bool                `json:"sourceApplied"`
	TargetApplied bool                `json:"targetApplied"`
}

func viewReinit(stored *repo.StoredReinit) reinitView {
	r := stored.Record
	return reinitView{PipelineID: r.PipelineID.String(), FromRunID: r.FromRunID, ConfigVersion: r.ConfigVersion, Digest: r.Digest, Document: stored.Document, ConfirmedAt: r.ConfirmedAt, NewRunID: r.NewRunID, SnapshotUUID: optionalReinitUUID(r.SnapshotUUID), SchemaUUID: optionalReinitUUID(r.SchemaUUID), PrepareUUID: optionalReinitUUID(r.PrepareUUID), CmdSeq: r.CmdSeq, SourceApplied: r.SourceApplied, TargetApplied: r.TargetApplied}
}

func registerReinitRoutes(api *gin.RouterGroup, db *gorm.DB, prober IEndpointProber) {
	defs, _ := prober.(iEndpointDefinitionsProber)
	h := &reinitHandler{db: db, prober: prober, defs: defs}
	api.POST("/pipelines/:id/reinit/preview", RequirePipelineScope(db, true), h.preview)
	api.GET("/pipelines/:id/reinit", RequirePipelineScope(db, false), h.get)
	api.POST("/pipelines/:id/reinit/confirm", RequirePipelineScope(db, true), h.confirm)
}

func (h *reinitHandler) pipeline(c *gin.Context) *model.Pipeline {
	return (&schemaPlanHandler{db: h.db}).pipeline(c)
}

func (h *reinitHandler) fail(c *gin.Context, err error) {
	var invalid *reinitPreviewError
	switch {
	case errors.As(err, &invalid):
		Fail(c, http.StatusUnprocessableEntity, invalid.code, invalid.message)
	case errors.Is(err, repo.ErrReinitNotFound):
		Fail(c, http.StatusNotFound, "reinit_not_found", "本次运行尚无重新初始化预览")
	case errors.Is(err, repo.ErrReinitConflict):
		Fail(c, http.StatusConflict, "reinit_changed", "预览、配置或运行状态已变化，请重新预览")
	case errors.Is(err, repo.ErrReinitBlocked):
		Fail(c, http.StatusUnprocessableEntity, "reinit_blocked", "目标表或结构计划不满足重新初始化条件")
	case errors.Is(err, topologycheck.ErrBidirectionalInitialization):
		Fail(c, http.StatusConflict, "bidirectional_initialization_conflict", "双向线路只能由一个方向执行全量或结构迁移")
	case errors.Is(err, task.ErrCatalogFailed):
		Fail(c, http.StatusBadGateway, "catalog_failed", "节点未能完整读取源端库表目录")
	case errors.Is(err, task.ErrCatalogTooLarge):
		Fail(c, http.StatusUnprocessableEntity, "catalog_too_large", "源端库表数量超过目录查询上限")
	default:
		switch {
		case errors.Is(err, repo.ErrPipelineNotFound), errors.Is(err, task.ErrProbeUnavailable), errors.Is(err, task.ErrCatalogNotProbed), errors.Is(err, task.ErrProbeNodeOffline), errors.Is(err, task.ErrDefinitionsTooLarge), errors.Is(err, task.ErrDefinitionsFailed):
			(&schemaPlanHandler{}).fail(c, err)
		default:
			FailInternal(c, err, "reinit_failed")
		}
	}
}

func reinitStopped(p *model.Pipeline) bool {
	return p.DesiredState == model.DesiredStateStopped && !p.ReinitPending && p.RunID < math.MaxInt64 && p.ConfigVersion < math.MaxInt64 && p.LastCmdSeq < math.MaxInt64 &&
		(p.SourceActualState == model.ActualStateStopped || p.SourceActualState == model.ActualStateDraft) &&
		(p.TargetActualState == model.ActualStateStopped || p.TargetActualState == model.ActualStateDraft)
}

func (h *reinitHandler) get(c *gin.Context) {
	p := h.pipeline(c)
	if p == nil {
		return
	}
	fromRun := p.RunID
	if raw, specified := c.GetQuery("fromRunId"); specified {
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || value >= math.MaxInt64 {
			Fail(c, http.StatusBadRequest, "invalid_run", "旧运行编号不合法")
			return
		}
		fromRun = value
	} else if p.RunID > 0 {
		// Prefer an unconfirmed preview for this run, then the confirmation
		// that created the current run. An explicit query reads exact history.
		stored, err := repo.NewReinitializations(h.db).GetPreview(c.Request.Context(), p.ID, fromRun)
		if err == nil {
			OK(c, viewReinit(stored))
			return
		}
		if !errors.Is(err, repo.ErrReinitNotFound) {
			h.fail(c, err)
			return
		}
		fromRun--
	}
	stored, err := repo.NewReinitializations(h.db).GetPreview(c.Request.Context(), p.ID, fromRun)
	if err != nil {
		h.fail(c, err)
		return
	}
	OK(c, viewReinit(stored))
}

func (h *reinitHandler) confirm(c *gin.Context) {
	p := h.pipeline(c)
	if p == nil {
		return
	}
	var input struct {
		FromRunID *uint64 `json:"fromRunId"`
		Digest    string  `json:"digest"`
		Confirm   bool    `json:"confirm"`
	}
	if c.ShouldBindJSON(&input) != nil || !input.Confirm || input.FromRunID == nil || *input.FromRunID >= math.MaxInt64 {
		Fail(c, http.StatusBadRequest, "confirmation_required", "请明确确认预览摘要、旧运行编号和目标处理方式")
		return
	}
	user, err := currentReinitUser(c)
	if err != nil {
		return
	}
	stored, err := repo.NewReinitializations(h.db).GetPreview(c.Request.Context(), p.ID, *input.FromRunID)
	if err != nil {
		h.fail(c, err)
		return
	}
	if stored.Document.InputsDigest == "" {
		h.fail(c, repo.ErrReinitConflict)
		return
	}
	stored, err = repo.NewReinitializations(h.db).Confirm(c.Request.Context(), p.ID, *input.FromRunID, input.Digest, user, time.Now().UTC())
	if err != nil {
		h.fail(c, err)
		return
	}
	// The repository commits confirmation and audit atomically. Existing
	// reconciliation consumes pending without another configuration mutation.
	OK(c, viewReinit(stored))
}

func optionalReinitUUID(id uuidx.UUID) string {
	if id.IsZero() {
		return ""
	}
	return id.String()
}

func currentReinitUser(c *gin.Context) (uuidx.UUID, error) {
	user, err := uuidx.Parse(UserIDFromContext(c))
	if err != nil || user.IsZero() {
		Fail(c, http.StatusUnauthorized, "unauthorized", "登录会话无效")
		return uuidx.Nil, repo.ErrReinitConflict
	}
	return user, nil
}
