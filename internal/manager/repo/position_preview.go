package repo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrPositionPreviewConflict = errors.New("position preview changed or unavailable")
var ErrTimestampConfirmationRequired = errors.New("timestamp position requires confirmed preview")

func positionDocument(id uuidx.UUID, run, version uint64, pos *iproto.StartPosition) ([]byte, string, error) {
	if pos == nil || pos.Kind != iproto.InitPositionKind_INIT_POSITION_KIND_TIMESTAMP || pos.SourceTsMs <= 0 || pos.SourceTsMs > math.MaxInt64/1000 || pos.SourceFile == "" || pos.SourceOffset < 4 || pos.SourceOffset > math.MaxUint32 || pos.TxnCommitTsMs < pos.SourceTsMs || pos.TimestampConfirmed {
		return nil, "", ErrPositionPreviewConflict
	}
	if _, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, pos.GtidSet); err != nil {
		return nil, "", ErrPositionPreviewConflict
	}
	blob, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(pos)
	if err != nil {
		return nil, "", err
	}
	// Hash deterministic protobuf; JSON object key order is not part of identity.
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(pos)
	if err != nil {
		return nil, "", err
	}
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%s:%d:%d:", id, run, version)
	h.Write(wire)
	return blob, hex.EncodeToString(h.Sum(nil)), nil
}

func positionPreviewAllowed(p *model.Pipeline) bool {
	return p.InitMode == model.InitModeIncrementalOnlyTimestamp && p.DesiredState == model.DesiredStateStopped && schemaPreviewAllowed(p) && p.SchemaUUID.IsZero()
}

func SavePositionPreview(ctx context.Context, db *gorm.DB, id uuidx.UUID, run, version uint64, pos *iproto.StartPosition) (*model.PositionPreview, error) {
	blob, digest, err := positionDocument(id, run, version, pos)
	if err != nil {
		return nil, err
	}
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var p model.Pipeline
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&p, "id = ?", id).Error; err != nil {
			return err
		}
		if p.RunID != run || p.ConfigVersion != version || !positionPreviewAllowed(&p) {
			return ErrPositionPreviewConflict
		}
		var prior model.PositionPreview
		err := tx.Where("pipeline_id = ? AND run_id = ?", id, run).First(&prior).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if prior.ConfirmedAt != nil {
			return ErrPositionPreviewConflict
		}
		record := model.PositionPreview{PipelineID: id, RunID: run, ConfigVersion: version, Digest: digest, Document: model.JSON(blob)}
		if prior.ID.IsZero() {
			return tx.Create(&record).Error
		}
		return tx.Model(&prior).Updates(map[string]any{"config_version": version, "digest": digest, "document": model.JSON(blob)}).Error
	})
	if err != nil {
		return nil, err
	}
	return GetPositionPreview(ctx, db, id, run)
}

func GetPositionPreview(ctx context.Context, db *gorm.DB, id uuidx.UUID, run uint64) (*model.PositionPreview, error) {
	var record model.PositionPreview
	if err := db.WithContext(ctx).Where("pipeline_id = ? AND run_id = ?", id, run).First(&record).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrPositionPreviewConflict
		}
		return nil, err
	}
	var pos iproto.StartPosition
	if err := protojson.Unmarshal(record.Document, &pos); err != nil {
		return nil, ErrPositionPreviewConflict
	}
	_, digest, err := positionDocument(id, run, record.ConfigVersion, &pos)
	if err != nil || digest != record.Digest {
		return nil, ErrPositionPreviewConflict
	}
	return &record, nil
}

func ConfirmPositionPreview(ctx context.Context, db *gorm.DB, id uuidx.UUID, run uint64, digest string, userID uuidx.UUID) (*model.PositionPreview, error) {
	if userID.IsZero() || len(digest) != 64 {
		return nil, ErrPositionPreviewConflict
	}
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var p model.Pipeline
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&p, "id = ?", id).Error; err != nil {
			return err
		}
		if p.RunID != run {
			return ErrPositionPreviewConflict
		}
		record, err := GetPositionPreview(ctx, tx, id, run)
		if err != nil {
			return err
		}
		if record.Digest != digest {
			return ErrPositionPreviewConflict
		}
		if record.ConfirmedAt != nil {
			if p.ConfigVersion != record.ConfigVersion+1 {
				return ErrPositionPreviewConflict
			}
			return nil
		}
		if p.ConfigVersion != record.ConfigVersion || !positionPreviewAllowed(&p) || p.ConfigVersion == math.MaxUint64 {
			return ErrPositionPreviewConflict
		}
		var pos iproto.StartPosition
		if err := protojson.Unmarshal(record.Document, &pos); err != nil {
			return err
		}
		pos.TimestampConfirmed = true
		blob, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(&pos)
		if err != nil {
			return err
		}
		if err := tx.Model(&p).Updates(map[string]any{"start_position": model.JSON(blob), "config_version": p.ConfigVersion + 1}).Error; err != nil {
			return err
		}
		now := time.Now()
		if err := tx.Model(record).Updates(map[string]any{"confirmed_at": now, "confirmed_by": userID}).Error; err != nil {
			return err
		}
		return CreateAuditLog(ctx, tx, &model.AuditLog{UserID: userID, Action: model.AuditActionPipelineSetPosition, TargetType: model.AuditTargetTypePipeline, TargetID: id, Detail: model.JSON(fmt.Sprintf(`{"digest":%q,"run_id":%d,"config_version":%d}`, digest, run, p.ConfigVersion))}, now)
	})
	if err != nil {
		return nil, err
	}
	return GetPositionPreview(ctx, db, id, run)
}

func CheckTimestampStart(ctx context.Context, db *gorm.DB, p *model.Pipeline) error {
	if p.InitMode != model.InitModeIncrementalOnlyTimestamp {
		return nil
	}
	if p.PositionRevision > 0 {
		stored, err := GetAppliedPositionChange(ctx, db, p)
		if err != nil || stored.Record.ConfirmedAt == nil || stored.Record.PositionRevision+1 != p.PositionRevision || !stored.Position.GetTimestampConfirmed() || !stored.Position.GetResolved() {
			return ErrTimestampConfirmationRequired
		}
		var current iproto.StartPosition
		if protojson.Unmarshal(p.StartPosition, &current) != nil || !proto.Equal(&current, stored.Position) {
			return ErrTimestampConfirmationRequired
		}
		return nil
	}
	var pos iproto.StartPosition
	if protojson.Unmarshal(p.StartPosition, &pos) != nil || !pos.TimestampConfirmed {
		return ErrTimestampConfirmationRequired
	}
	pos.TimestampConfirmed = false
	if _, _, err := positionDocument(p.ID, p.RunID, p.ConfigVersion, &pos); err != nil {
		return ErrTimestampConfirmationRequired
	}
	record, err := GetPositionPreview(ctx, db, p.ID, p.RunID)
	if err != nil || record.ConfirmedAt == nil || record.ConfirmedBy.IsZero() {
		return ErrTimestampConfirmationRequired
	}
	var confirmed iproto.StartPosition
	if protojson.Unmarshal(record.Document, &confirmed) != nil || !proto.Equal(&confirmed, &pos) {
		return ErrTimestampConfirmationRequired
	}
	return nil
}
