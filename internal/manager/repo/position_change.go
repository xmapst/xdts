package repo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"google.golang.org/protobuf/encoding/protojson"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrPositionChangeConflict = errors.New("position change unavailable or changed")
var ErrPositionJumpConfirmation = errors.New("forward position jump requires confirmation")

func PositionChangeAllowed(p *model.Pipeline) bool {
	return schemaPreviewAllowed(p)
}

func decodePositionChange(doc model.PositionChangeDocument) (*iproto.StartPosition, bool, error) {
	var pos iproto.StartPosition
	if len(doc.Position) > (1<<20)+4096 || protojson.Unmarshal(doc.Position, &pos) != nil || !pos.Resolved || len(pos.GtidSet) > 1<<20 || len(pos.SourceFile) > 255 || pos.SourceOffset > math.MaxUint32 || pos.SourceTsMs < 0 || pos.SourceTsMs > math.MaxInt64/1000 || pos.TxnCommitTsMs < 0 || doc.CheckpointEpoch > math.MaxInt64 || doc.CheckpointPositionRevision > math.MaxInt64 || len(doc.Warnings) > 16 {
		return nil, false, ErrPositionChangeConflict
	}
	want := map[string]iproto.InitPositionKind{"latest": iproto.InitPositionKind_INIT_POSITION_KIND_LATEST, "gtid_set": iproto.InitPositionKind_INIT_POSITION_KIND_GTID_SET, "binlog_pos": iproto.InitPositionKind_INIT_POSITION_KIND_BINLOG_POS, "timestamp": iproto.InitPositionKind_INIT_POSITION_KIND_TIMESTAMP}
	kind, ok := want[doc.Kind]
	if !ok || pos.Kind != kind {
		return nil, false, ErrPositionChangeConflict
	}
	if kind == iproto.InitPositionKind_INIT_POSITION_KIND_BINLOG_POS && (pos.SourceFile == "" || pos.SourceOffset < 4 || pos.SourceOffset > math.MaxUint32) {
		return nil, false, ErrPositionChangeConflict
	}
	if kind == iproto.InitPositionKind_INIT_POSITION_KIND_TIMESTAMP && (pos.SourceTsMs <= 0 || pos.SourceTsMs > math.MaxInt64/1000 || pos.TxnCommitTsMs < pos.SourceTsMs || !pos.TimestampConfirmed || pos.SourceFile == "" || pos.SourceOffset < 4 || pos.SourceOffset > math.MaxUint32) {
		return nil, false, ErrPositionChangeConflict
	}
	set, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, pos.GtidSet)
	if err != nil {
		return nil, false, ErrPositionChangeConflict
	}
	for _, warning := range doc.Warnings {
		if len(warning) == 0 || len(warning) > 4096 {
			return nil, false, ErrPositionChangeConflict
		}
	}
	if doc.PublishedGTIDSet == nil {
		return &pos, true, nil
	}
	if len(*doc.PublishedGTIDSet) > 1<<20 {
		return nil, false, ErrPositionChangeConflict
	}
	base, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, *doc.PublishedGTIDSet)
	if err != nil {
		return nil, false, ErrPositionChangeConflict
	}
	return &pos, !base.Contain(set), nil
}

func positionChangeDigest(p *model.PositionChange) (string, error) {
	var doc model.PositionChangeDocument
	if json.Unmarshal(p.Document, &doc) != nil {
		return "", ErrPositionChangeConflict
	}
	pos, _, err := decodePositionChange(doc)
	if err != nil {
		return "", err
	}
	canonical, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(pos)
	if err != nil {
		return "", err
	}
	doc.Position = model.JSON(canonical)
	blob, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%s:%d:%d:%d:%d:", p.PipelineID, p.RunID, p.ConfigVersion, p.PositionRevision, p.ExpiresAt.UnixMilli())
	h.Write(blob)
	return hex.EncodeToString(h.Sum(nil)), nil
}

func SavePositionChange(ctx context.Context, db *gorm.DB, p *model.Pipeline, doc model.PositionChangeDocument, now time.Time) (*model.StoredPositionChange, error) {
	if p == nil || !PositionChangeAllowed(p) || p.PositionRevision >= math.MaxInt64 || p.ConfigVersion == math.MaxUint64 || now.IsZero() {
		return nil, ErrPositionChangeConflict
	}
	if _, _, err := decodePositionChange(doc); err != nil {
		return nil, err
	}
	if doc.PublishedGTIDSet != nil && doc.CheckpointPositionRevision > p.PositionRevision || doc.PublishedGTIDSet == nil && (doc.CheckpointPositionRevision != 0 || doc.CheckpointEpoch != 0) {
		return nil, ErrPositionChangeConflict
	}
	blob, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	record := model.PositionChange{PipelineID: p.ID, RunID: p.RunID, ConfigVersion: p.ConfigVersion, PositionRevision: p.PositionRevision, Document: model.JSON(blob), ExpiresAt: now.Add(5 * time.Minute).UTC().Truncate(time.Millisecond)}
	record.Digest, err = positionChangeDigest(&record)
	if err != nil {
		return nil, err
	}
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current model.Pipeline
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&current, "id=?", p.ID).Error; err != nil {
			return err
		}
		if current.RunID != p.RunID || current.ConfigVersion != p.ConfigVersion || current.PositionRevision != p.PositionRevision || !PositionChangeAllowed(&current) {
			return ErrPositionChangeConflict
		}
		var prior model.PositionChange
		err := tx.Where("pipeline_id=? AND run_id=? AND position_revision=?", p.ID, p.RunID, p.PositionRevision).First(&prior).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return tx.Create(&record).Error
		}
		if err != nil {
			return err
		}
		return tx.Model(&prior).Updates(map[string]any{"config_version": record.ConfigVersion, "position_revision": record.PositionRevision, "digest": record.Digest, "document": record.Document, "expires_at": record.ExpiresAt, "confirmed_at": nil, "confirmed_by": nil}).Error
	})
	if err != nil {
		return nil, err
	}
	return GetPositionChange(ctx, db, p.ID, p.RunID)
}

func GetPositionChange(ctx context.Context, db *gorm.DB, id uuidx.UUID, run uint64) (*model.StoredPositionChange, error) {
	return getPositionChange(ctx, db.WithContext(ctx).Where("pipeline_id=? AND run_id=?", id, run).Order("position_revision DESC"))
}

// GetAppliedPositionChange retains the confirmed origin even while a newer
// preview is pending, so previewing alone cannot invalidate an existing start.
func GetAppliedPositionChange(ctx context.Context, db *gorm.DB, p *model.Pipeline) (*model.StoredPositionChange, error) {
	if p == nil || p.PositionRevision == 0 {
		return nil, ErrPositionChangeConflict
	}
	return getPositionChange(ctx, db.WithContext(ctx).Where("pipeline_id=? AND run_id=? AND position_revision=?", p.ID, p.RunID, p.PositionRevision-1))
}

func getPositionChange(ctx context.Context, query *gorm.DB) (*model.StoredPositionChange, error) {
	var stored model.StoredPositionChange
	if err := query.WithContext(ctx).First(&stored.Record).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrPositionChangeConflict
		}
		return nil, err
	}
	digest, err := positionChangeDigest(&stored.Record)
	if err != nil || digest != stored.Record.Digest {
		return nil, ErrPositionChangeConflict
	}
	if json.Unmarshal(stored.Record.Document, &stored.Document) != nil {
		return nil, ErrPositionChangeConflict
	}
	stored.Position, stored.ForwardJump, err = decodePositionChange(stored.Document)
	return &stored, err
}

// ConfirmPositionChange commits the new revision and its audit atomically.
// The caller revalidates the frozen GTID/baseline with the actual source node
// while retaining the same pipeline lock, before entering this transaction.
func ConfirmPositionChange(ctx context.Context, db *gorm.DB, id uuidx.UUID, run uint64, digest string, forward bool, user uuidx.UUID, now time.Time) (*model.StoredPositionChange, error) {
	if user.IsZero() || len(digest) != 64 {
		return nil, ErrPositionChangeConflict
	}
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var p model.Pipeline
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&p, "id=?", id).Error; err != nil {
			return err
		}
		stored, err := GetPositionChange(ctx, tx, id, run)
		if err != nil {
			return err
		}
		r := &stored.Record
		if r.Digest != digest || p.RunID != run {
			return ErrPositionChangeConflict
		}
		if r.ConfirmedAt != nil {
			if p.PositionRevision == r.PositionRevision+1 && p.ConfigVersion >= r.ConfigVersion+1 {
				return nil
			}
			return ErrPositionChangeConflict
		}
		if !now.Before(r.ExpiresAt) || !PositionChangeAllowed(&p) || p.ConfigVersion != r.ConfigVersion || p.PositionRevision != r.PositionRevision || p.PositionRevision >= math.MaxInt64 || p.ConfigVersion == math.MaxUint64 {
			return ErrPositionChangeConflict
		}
		if stored.ForwardJump && !forward {
			return ErrPositionJumpConfirmation
		}
		mode := map[string]model.InitMode{"latest": model.InitModeIncrementalOnlyLatest, "gtid_set": model.InitModeIncrementalOnlyGTIDSet, "binlog_pos": model.InitModeIncrementalOnlyBinlogPos, "timestamp": model.InitModeIncrementalOnlyTimestamp}[stored.Document.Kind]
		if err := tx.Model(&p).Updates(map[string]any{"init_mode": mode, "start_position": stored.Document.Position, "position_revision": p.PositionRevision + 1, "config_version": p.ConfigVersion + 1}).Error; err != nil {
			return err
		}
		if err := tx.Model(r).Updates(map[string]any{"confirmed_at": now.UTC().Truncate(time.Millisecond), "confirmed_by": user}).Error; err != nil {
			return err
		}
		detail, _ := json.Marshal(map[string]any{"digest": digest, "run_id": run, "position_revision": r.PositionRevision + 1, "forward_jump": stored.ForwardJump, "kind": stored.Document.Kind})
		return CreateAuditLog(ctx, tx, &model.AuditLog{UserID: user, Action: model.AuditActionPipelineSetPosition, TargetType: model.AuditTargetTypePipeline, TargetID: id, Detail: model.JSON(detail)}, now)
	})
	if err != nil {
		return nil, err
	}
	return GetPositionChange(ctx, db, id, run)
}
