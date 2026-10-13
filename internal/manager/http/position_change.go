package http

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/manager/task"
	"github.com/xmapst/xdts/pkg/uuidx"
	"google.golang.org/protobuf/encoding/protojson"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type positionChangeInput struct {
	Kind    string  `json:"kind"`
	GTIDSet *string `json:"gtidSet"`
	File    string  `json:"file"`
	Offset  uint64  `json:"offset"`
	TSMS    int64   `json:"tsMs"`
}

func (in positionChangeInput) args() (*iproto.PositionProbeArgs, error) {
	a := &iproto.PositionProbeArgs{IncludeCheckpoint: true}
	switch in.Kind {
	case "latest":
		a.Kind = iproto.InitPositionKind_INIT_POSITION_KIND_LATEST
	case "gtid_set":
		if in.GTIDSet == nil || len(*in.GTIDSet) > 1<<20 {
			return nil, repo.ErrPositionChangeConflict
		}
		if _, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, *in.GTIDSet); err != nil {
			return nil, repo.ErrPositionChangeConflict
		}
		a.Kind, a.GtidSet = iproto.InitPositionKind_INIT_POSITION_KIND_GTID_SET, *in.GTIDSet
	case "binlog_pos":
		if len(in.File) == 0 || len(in.File) > 255 || in.Offset < 4 || in.Offset > math.MaxUint32 {
			return nil, repo.ErrPositionChangeConflict
		}
		a.Kind, a.BinlogFile, a.BinlogOffset = iproto.InitPositionKind_INIT_POSITION_KIND_BINLOG_POS, in.File, in.Offset
	case "timestamp":
		if in.TSMS <= 0 || in.TSMS > math.MaxInt64/1000 {
			return nil, repo.ErrPositionChangeConflict
		}
		a.Kind, a.TsMs = iproto.InitPositionKind_INIT_POSITION_KIND_TIMESTAMP, in.TSMS
	default:
		return nil, repo.ErrPositionChangeConflict
	}
	if in.Kind != "gtid_set" && in.GTIDSet != nil || in.Kind != "binlog_pos" && (in.File != "" || in.Offset != 0) || in.Kind != "timestamp" && in.TSMS != 0 {
		return nil, repo.ErrPositionChangeConflict
	}
	return a, nil
}

func decodePositionRequest(c *gin.Context, v any) bool {
	d := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, (1<<20)+4096))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil || d.Decode(new(any)) != io.EOF {
		Fail(c, 400, "invalid_position_request", "位点参数无效或包含多余字段")
		return false
	}
	return true
}

type positionChangeView struct {
	PipelineID       string     `json:"pipelineId"`
	RunID            uint64     `json:"runId"`
	ConfigVersion    uint64     `json:"configVersion"`
	PositionRevision uint64     `json:"positionRevision"`
	Digest           string     `json:"digest"`
	Kind             string     `json:"kind"`
	GTIDSet          string     `json:"gtidSet"`
	File             string     `json:"file"`
	Offset           uint64     `json:"offset"`
	TSMS             int64      `json:"tsMs"`
	CommitTSMS       int64      `json:"commitTsMs"`
	PublishedGTIDSet *string    `json:"publishedGtidSet"`
	CheckpointEpoch  uint64     `json:"checkpointEpoch"`
	ForwardJump      bool       `json:"forwardJump"`
	Warnings         []string   `json:"warnings"`
	ExpiresAt        time.Time  `json:"expiresAt"`
	ConfirmedAt      *time.Time `json:"confirmedAt"`
}

type pipelinePositionsView struct {
	PipelineID       string                    `json:"pipelineId"`
	RunID            uint64                    `json:"runId"`
	ConfigVersion    uint64                    `json:"configVersion"`
	PositionRevision uint64                    `json:"positionRevision"`
	Editable         bool                      `json:"editable"`
	StartPosition    model.JSON                `json:"startPosition"`
	Observations     []progressObservationView `json:"observations"`
}

func positionChangeResponse(s *model.StoredPositionChange) positionChangeView {
	r, p, d := s.Record, s.Position, s.Document
	return positionChangeView{PipelineID: r.PipelineID.String(), RunID: r.RunID, ConfigVersion: r.ConfigVersion, PositionRevision: r.PositionRevision, Digest: r.Digest, Kind: d.Kind, GTIDSet: p.GtidSet, File: p.SourceFile, Offset: p.SourceOffset, TSMS: p.SourceTsMs, CommitTSMS: p.TxnCommitTsMs, PublishedGTIDSet: d.PublishedGTIDSet, CheckpointEpoch: d.CheckpointEpoch, ForwardJump: s.ForwardJump, Warnings: d.Warnings, ExpiresAt: r.ExpiresAt, ConfirmedAt: r.ConfirmedAt}
}

func (h *positionHandler) getChange(c *gin.Context) {
	p := h.pipeline(c)
	if p == nil {
		return
	}
	s, err := repo.GetPositionChange(c.Request.Context(), h.db, p.ID, p.RunID)
	if err != nil {
		h.fail(c, err)
		return
	}
	OK(c, positionChangeResponse(s))
}

func (h *positionHandler) previewChange(c *gin.Context) {
	p := h.pipeline(c)
	if p == nil {
		return
	}
	var input positionChangeInput
	if !decodePositionRequest(c, &input) {
		return
	}
	args, err := input.args()
	if err != nil {
		Fail(c, 400, "invalid_position_request", "请选择有效的起点方式及对应参数")
		return
	}
	if !repo.PositionChangeAllowed(p) {
		h.fail(c, repo.ErrPositionChangeConflict)
		return
	}
	if h.prober == nil {
		h.fail(c, task.ErrProbeUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(c.Request.Context()), 75*time.Second)
	defer cancel()
	var saved *model.StoredPositionChange
	err = h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current model.Pipeline
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&current, "id=?", p.ID).Error; err != nil {
			return err
		}
		if !repo.PositionChangeAllowed(&current) || current.ConfigVersion != p.ConfigVersion || current.RunID != p.RunID {
			return repo.ErrPositionChangeConflict
		}
		out, err := h.prober.GetPipelinePosition(ctx, p.ID, args)
		if err != nil {
			return err
		}
		if out == nil || out.Boundary != nil {
			return repo.ErrPositionChangeConflict
		}
		pos := &iproto.StartPosition{Kind: args.Kind, GtidSet: out.GtidSet, SourceFile: out.BinlogFile, SourceOffset: out.BinlogOffset, SourceTsMs: input.TSMS, TxnCommitTsMs: out.TxnCommitTsMs, Resolved: true, TimestampConfirmed: input.Kind == "timestamp"}
		blob, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(pos)
		if err != nil {
			return err
		}
		warnings := append([]string{}, out.Warnings...)
		saved, err = repo.SavePositionChange(ctx, tx, &current, model.PositionChangeDocument{Kind: input.Kind, Position: model.JSON(blob), PublishedGTIDSet: out.PublishedGtidSet, CheckpointEpoch: out.CheckpointEpoch, CheckpointPositionRevision: out.CheckpointPositionRevision, Warnings: warnings}, time.Now())
		return err
	})
	if err != nil {
		h.fail(c, err)
		return
	}
	OK(c, positionChangeResponse(saved))
}

func (h *positionHandler) setPosition(c *gin.Context) {
	p := h.pipeline(c)
	if p == nil {
		return
	}
	var input struct {
		Digest             string `json:"digest"`
		Confirm            bool   `json:"confirm"`
		ConfirmForwardJump bool   `json:"confirmForwardJump"`
	}
	if !decodePositionRequest(c, &input) {
		return
	}
	if !input.Confirm {
		Fail(c, 400, "confirmation_required", "请明确确认实际起点和可能的数据跳过")
		return
	}
	user, err := uuidx.Parse(UserIDFromContext(c))
	if err != nil || user.IsZero() {
		Fail(c, 401, "unauthorized", "登录会话无效")
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(c.Request.Context()), 75*time.Second)
	defer cancel()
	var saved *model.StoredPositionChange
	err = h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current model.Pipeline
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&current, "id=?", p.ID).Error; err != nil {
			return err
		}
		s, err := repo.GetPositionChange(ctx, tx, p.ID, p.RunID)
		if err != nil {
			return err
		}
		if s.Record.Digest != input.Digest {
			return repo.ErrPositionChangeConflict
		}
		if s.Record.ConfirmedAt == nil {
			if !repo.PositionChangeAllowed(&current) || s.Record.ConfigVersion != current.ConfigVersion || !time.Now().Before(s.Record.ExpiresAt) {
				return repo.ErrPositionChangeConflict
			}
			if s.ForwardJump && !input.ConfirmForwardJump {
				return repo.ErrPositionJumpConfirmation
			}
			if h.prober == nil {
				return task.ErrProbeUnavailable
			}
			out, err := h.prober.GetPipelinePosition(ctx, p.ID, &iproto.PositionProbeArgs{Kind: iproto.InitPositionKind_INIT_POSITION_KIND_GTID_SET, GtidSet: s.Position.GtidSet, IncludeCheckpoint: true})
			if err != nil {
				return err
			}
			if out == nil || out.GtidSet != s.Position.GtidSet || out.CheckpointEpoch != s.Document.CheckpointEpoch || out.CheckpointPositionRevision != s.Document.CheckpointPositionRevision || !equalOptionalGTID(out.PublishedGtidSet, s.Document.PublishedGTIDSet) {
				return repo.ErrPositionChangeConflict
			}
		}
		saved, err = repo.ConfirmPositionChange(ctx, tx, p.ID, p.RunID, input.Digest, input.ConfirmForwardJump, user, time.Now())
		return err
	})
	if err != nil {
		h.fail(c, err)
		return
	}
	syncPipelineBestEffort(c.Request.Context(), h.dispatcher, p.ID)
	OK(c, positionChangeResponse(saved))
}

func equalOptionalGTID(a, b *string) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

func (h *positionHandler) getPositions(c *gin.Context) {
	p := h.pipeline(c)
	if p == nil {
		return
	}
	var observations = []progressObservationView{}
	err := h.db.WithContext(c.Request.Context()).Transaction(func(tx *gorm.DB) error {
		var err error
		p, err = repo.GetPipeline(c.Request.Context(), tx, p.ID)
		if err != nil {
			return err
		}
		rows, err := repo.ListPipelineProgress(c.Request.Context(), tx, p.ID, p.RunID)
		if err != nil {
			return err
		}
		for _, row := range rows {
			var values model.ProgressValues
			if err := json.Unmarshal(row.Values, &values); err != nil {
				return err
			}
			observations = append(observations, progressObservationView{NodeID: row.NodeID.String(), Role: row.Side, RunID: row.RunID, ObservedAt: row.ObservedAt, Values: values})
		}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		h.fail(c, err)
		return
	}
	// The full values include independent source/target observation times.
	OK(c, pipelinePositionsView{PipelineID: p.ID.String(), RunID: p.RunID, PositionRevision: p.PositionRevision, ConfigVersion: p.ConfigVersion, Editable: repo.PositionChangeAllowed(p), StartPosition: p.StartPosition, Observations: observations})
}
