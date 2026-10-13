package http

import (
	"context"
	"errors"
	"math"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/dispatch"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/manager/task"
	"github.com/xmapst/xdts/pkg/uuidx"
	"google.golang.org/protobuf/encoding/protojson"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type iPipelinePositionProber interface {
	GetPipelinePosition(context.Context, uuidx.UUID, *iproto.PositionProbeArgs) (*iproto.PositionResult, error)
}
type positionHandler struct {
	db         *gorm.DB
	prober     iPipelinePositionProber
	audit      IAuditRecorder
	dispatcher dispatch.IPipelineDispatcher
}
type positionPreviewView struct {
	PipelineID    string     `json:"pipelineId"`
	RunID         uint64     `json:"runId"`
	ConfigVersion uint64     `json:"configVersion"`
	Digest        string     `json:"digest"`
	RequestedTSMS int64      `json:"requestedTsMs"`
	GTIDSet       string     `json:"gtidSet"`
	File          string     `json:"file"`
	Offset        uint64     `json:"offset"`
	CommitTSMS    int64      `json:"commitTsMs"`
	ConfirmedAt   *time.Time `json:"confirmedAt"`
}

func viewPosition(record *model.PositionPreview) (positionPreviewView, error) {
	var pos iproto.StartPosition
	if err := protojson.Unmarshal(record.Document, &pos); err != nil {
		return positionPreviewView{}, err
	}
	return positionPreviewView{PipelineID: record.PipelineID.String(), RunID: record.RunID, ConfigVersion: record.ConfigVersion, Digest: record.Digest, RequestedTSMS: pos.SourceTsMs, GTIDSet: pos.GtidSet, File: pos.SourceFile, Offset: pos.SourceOffset, CommitTSMS: pos.TxnCommitTsMs, ConfirmedAt: record.ConfirmedAt}, nil
}
func registerPositionRoutes(api *gin.RouterGroup, db *gorm.DB, prober IEndpointProber, audit IAuditRecorder, dispatcher dispatch.IPipelineDispatcher) {
	p, _ := prober.(iPipelinePositionProber)
	h := &positionHandler{db: db, prober: p, audit: audit, dispatcher: dispatcher}
	api.GET("/pipelines/:id/position-preview", RequirePipelineScope(db, false), h.get)
	api.POST("/pipelines/:id/position-preview", RequirePipelineScope(db, true), h.preview)
	api.POST("/pipelines/:id/position-preview/confirm", RequirePipelineScope(db, true), h.confirm)
	api.GET("/pipelines/:id/position", RequirePipelineScope(db, false), h.getPositions)
	api.PUT("/pipelines/:id/position", RequirePipelineScope(db, true), h.setPosition)
	api.POST("/pipelines/:id/position-change", RequirePipelineScope(db, true), h.previewChange)
	api.GET("/pipelines/:id/position-change", RequirePipelineScope(db, false), h.getChange)
}
func (h *positionHandler) pipeline(c *gin.Context) *model.Pipeline {
	if !requireDB(c, h.db) {
		return nil
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, 400, "invalid_id", "id 格式不合法")
		return nil
	}
	p, err := repo.GetPipeline(c.Request.Context(), h.db, id)
	if err != nil {
		h.fail(c, err)
		return nil
	}
	return p
}
func (h *positionHandler) fail(c *gin.Context, err error) {
	var boundary *task.PositionBoundaryError
	switch {
	case errors.As(err, &boundary) && boundary.Boundary != nil:
		b := boundary.Boundary
		FailWithDetails(c, 422, "position_boundary", "偏移未对齐事务起点，请选择前后最近边界", map[string]any{"file": b.File, "requested": b.Requested, "preceding": b.Preceding, "following": b.Following})
	case errors.Is(err, repo.ErrPositionJumpConfirmation):
		Fail(c, 409, "position_forward_confirmation_required", "新起点可能跳过尚未发布的事务，请明确确认后再保存")
	case errors.Is(err, repo.ErrPositionChangeConflict):
		Fail(c, 409, "position_change_changed", "位点预览、检查点或配置已变化，请在两侧停止后重新预览并确认")
	case errors.Is(err, repo.ErrPositionPreviewConflict):
		Fail(c, http.StatusConflict, "position_preview_changed", "位点预览或配置已变化，请在停止状态重新预览并确认")
	case errors.Is(err, repo.ErrPipelineNotFound):
		Fail(c, 404, "not_found", "pipeline 不存在")
	case errors.Is(err, task.ErrProbeUnavailable):
		Fail(c, 503, "probe_unavailable", "源节点位点探测服务不可用")
	case errors.Is(err, task.ErrPositionProbeFailed):
		Fail(c, 422, "position_probe_failed", "源节点未能验证时间点，请检查连接、复制权限和 binlog 保留范围")
	case errors.Is(err, task.ErrProbeNodeOffline):
		Fail(c, 409, "node_offline", "源节点未在线")
	case errors.Is(err, task.ErrCatalogNotProbed):
		Fail(c, 409, "not_probed", "请先完成源端点连接探测")
	default:
		FailInternal(c, err, "position_preview_failed")
	}
}
func (h *positionHandler) respond(c *gin.Context, record *model.PositionPreview) {
	view, err := viewPosition(record)
	if err != nil {
		h.fail(c, err)
		return
	}
	OK(c, view)
}
func (h *positionHandler) get(c *gin.Context) {
	p := h.pipeline(c)
	if p == nil {
		return
	}
	record, err := repo.GetPositionPreview(c.Request.Context(), h.db, p.ID, p.RunID)
	if err != nil {
		h.fail(c, err)
		return
	}
	h.respond(c, record)
}
func (h *positionHandler) preview(c *gin.Context) {
	p := h.pipeline(c)
	if p == nil {
		return
	}
	var input struct {
		TSMS int64 `json:"tsMs"`
	}
	if c.ShouldBindJSON(&input) != nil || input.TSMS <= 0 || input.TSMS > math.MaxInt64/1000 {
		Fail(c, 400, "invalid_timestamp", "请输入有效的起始时间点")
		return
	}
	if p.InitMode != model.InitModeIncrementalOnlyTimestamp || p.DesiredState != model.DesiredStateStopped || !p.SchemaUUID.IsZero() {
		h.fail(c, repo.ErrPositionPreviewConflict)
		return
	}
	if h.prober == nil {
		h.fail(c, task.ErrProbeUnavailable)
		return
	}
	var record *model.PositionPreview
	// Keep the server_id lease while the node finishes, even if the browser
	// disconnects. The signed request has its own deadline; the lock is bounded.
	probeCtx, cancel := context.WithTimeout(context.WithoutCancel(c.Request.Context()), 75*time.Second)
	defer cancel()
	err := h.db.WithContext(probeCtx).Transaction(func(tx *gorm.DB) error {
		var current model.Pipeline
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&current, "id = ?", p.ID).Error; err != nil {
			return err
		}
		if current.ConfigVersion != p.ConfigVersion || current.RunID != p.RunID || current.DesiredState != model.DesiredStateStopped {
			return repo.ErrPositionPreviewConflict
		}
		// Serialize concurrent temporary dump connections across Manager instances.
		// Holding this cold-path lock also keeps lifecycle writes from starting
		// the actual dump connection while the allocated server_id is in use.
		result, err := h.prober.GetPipelinePosition(probeCtx, p.ID, &iproto.PositionProbeArgs{Kind: iproto.InitPositionKind_INIT_POSITION_KIND_TIMESTAMP, TsMs: input.TSMS})
		if err != nil {
			return err
		}
		pos := &iproto.StartPosition{Kind: iproto.InitPositionKind_INIT_POSITION_KIND_TIMESTAMP, SourceTsMs: input.TSMS, GtidSet: result.GtidSet, SourceFile: result.BinlogFile, SourceOffset: result.BinlogOffset, TxnCommitTsMs: result.TxnCommitTsMs}
		record, err = repo.SavePositionPreview(probeCtx, tx, p.ID, p.RunID, p.ConfigVersion, pos)
		return err
	})
	if err != nil {
		h.fail(c, err)
		return
	}
	h.respond(c, record)
}
func (h *positionHandler) confirm(c *gin.Context) {
	p := h.pipeline(c)
	if p == nil {
		return
	}
	var input struct {
		Digest  string `json:"digest"`
		Confirm bool   `json:"confirm"`
	}
	if c.ShouldBindJSON(&input) != nil || !input.Confirm {
		Fail(c, 400, "confirmation_required", "请明确确认预览中的实际事务边界")
		return
	}
	user, err := uuidx.Parse(UserIDFromContext(c))
	if err != nil {
		Fail(c, 401, "unauthorized", "登录会话无效")
		return
	}
	record, err := repo.ConfirmPositionPreview(c.Request.Context(), h.db, p.ID, p.RunID, input.Digest, user)
	if err != nil {
		h.fail(c, err)
		return
	}
	// The confirmation and its audit record commit together in the repository.
	syncPipelineBestEffort(c.Request.Context(), h.dispatcher, p.ID)
	h.respond(c, record)
}
