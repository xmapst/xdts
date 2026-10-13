package repo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const SourceEpochRangeSize uint64 = 65536

var ErrSourceRecoveryConflict = errors.New("source recovery ownership or identity changed")
var ErrSourceRecoveryUnavailable = errors.New("source recovery has no usable published checkpoint")

func decodeRecoveryCheckpoint(raw model.JSON) (*iproto.SourceCheckpoint, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var cp iproto.SourceCheckpoint
	if len(raw) > (1<<20)+4096 || protojson.Unmarshal(raw, &cp) != nil || iproto.ValidateSourceCheckpoint(&cp) != nil {
		return nil, ErrSourceRecoveryUnavailable
	}
	return &cp, nil
}

func sourceCheckpointMatches(cp *iproto.SourceCheckpoint, p *model.Pipeline) bool {
	return cp != nil && cp.PipelineId == p.ID.String() && cp.NodeId == p.SourceNodeID.String() && cp.RunId == p.RunID && cp.PositionRevision == p.PositionRevision
}

// RecordSourceCheckpoint serializes with position edits and epoch allocation.
// An authenticated old-run/revision report is ignored; a wrong owner is rejected.
func RecordSourceCheckpoint(ctx context.Context, db *gorm.DB, nodeID uuidx.UUID, cp *iproto.SourceCheckpoint) error {
	if iproto.ValidateSourceCheckpoint(cp) != nil || cp.NodeId != nodeID.String() {
		return ErrSourceRecoveryConflict
	}
	id, err := uuidx.Parse(cp.PipelineId)
	if err != nil {
		return ErrSourceRecoveryConflict
	}
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var p model.Pipeline
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&p, "id=?", id).Error; err != nil {
			return err
		}
		if p.SourceNodeID != nodeID || cp.RunId > p.RunID || cp.PositionRevision > p.PositionRevision && cp.RunId == p.RunID {
			return ErrSourceRecoveryConflict
		}
		if !sourceCheckpointMatches(cp, &p) {
			return nil
		}
		state, err := sourceRecoveryState(tx, id)
		if err != nil {
			return err
		}
		prior, err := decodeRecoveryCheckpoint(state.Checkpoint)
		if err != nil {
			return err
		}
		if sourceCheckpointMatches(prior, &p) && (cp.Epoch < prior.Epoch || cp.Epoch == prior.Epoch && (cp.BatchSeq < prior.BatchSeq || cp.ObservedTsMs <= prior.ObservedTsMs)) {
			return nil
		}
		raw, err := protojson.Marshal(cp)
		if err != nil {
			return err
		}
		state.Checkpoint = model.JSON(raw)
		if cp.Epoch > state.EpochCeiling {
			state.EpochCeiling = cp.Epoch
		}
		return tx.Save(state).Error
	})
}

func sourceRecoveryState(tx *gorm.DB, id uuidx.UUID) (*model.SourceRecoveryState, error) {
	var state model.SourceRecoveryState
	err := tx.First(&state, "pipeline_id=?", id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return &model.SourceRecoveryState{PipelineID: id}, nil
	}
	if err != nil {
		return nil, err
	}
	if state.EpochCeiling > math.MaxInt64 {
		return nil, ErrSourceRecoveryUnavailable
	}
	return &state, nil
}

// transferSourceRecovery runs under the migration's pipeline row lock. The
// published position belongs to the run; authentication belongs to its current
// node. Transfer both together while retaining every previously reserved epoch.
func transferSourceRecovery(tx *gorm.DB, p *model.Pipeline, newNodeID uuidx.UUID) error {
	state, err := sourceRecoveryState(tx, p.ID)
	if err != nil {
		return err
	}
	cp, err := decodeRecoveryCheckpoint(state.Checkpoint)
	if err != nil {
		return err
	}
	if cp == nil && p.LastCmdSeq == 0 {
		return nil // A never-started pipeline has no publication to hand over.
	}
	if !sourceCheckpointMatches(cp, p) {
		return ErrSourceRecoveryUnavailable
	}
	cp.NodeId = newNodeID.String()
	raw, err := protojson.Marshal(cp)
	if err != nil {
		return err
	}
	state.Checkpoint = model.JSON(raw)
	state.EpochCeiling = max(state.EpochCeiling, cp.Epoch)
	return tx.Save(state).Error
}

// ReserveSourceEpochs returns a durable idempotent reply under the pipeline lock.
// The ceiling includes epochs a disconnected source may still use locally.
func ReserveSourceEpochs(ctx context.Context, db *gorm.DB, nodeID uuidx.UUID, req *iproto.SourceRecoveryRequest, now time.Time) (*iproto.SourceRecoveryReply, error) {
	if req == nil || req.NodeId != nodeID.String() || req.PositionRevision > math.MaxInt64 || req.KnownEpochCeiling > math.MaxInt64 || now.UnixMilli() <= 0 {
		return nil, ErrSourceRecoveryConflict
	}
	id, err := uuidx.Parse(req.PipelineId)
	if err != nil || id.IsZero() {
		return nil, ErrSourceRecoveryConflict
	}
	requestID, err := uuidx.Parse(req.RequestId)
	if err != nil || requestID.IsZero() {
		return nil, ErrSourceRecoveryConflict
	}
	if req.LocalCheckpoint != nil && (iproto.ValidateSourceCheckpoint(req.LocalCheckpoint) != nil || req.LocalCheckpoint.PipelineId != req.PipelineId || req.LocalCheckpoint.NodeId != req.NodeId || req.LocalCheckpoint.RunId > req.RunId || req.LocalCheckpoint.RunId == req.RunId && req.LocalCheckpoint.PositionRevision > req.PositionRevision) {
		return nil, ErrSourceRecoveryConflict
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(req)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(encoded)
	digest := hex.EncodeToString(hash[:])
	var reply *iproto.SourceRecoveryReply
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var p model.Pipeline
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&p, "id=?", id).Error; err != nil {
			return err
		}
		if p.SourceNodeID != nodeID || p.RunID != req.RunId || p.PositionRevision != req.PositionRevision || p.ConfigVersion != req.ConfigVersion {
			return ErrSourceRecoveryConflict
		}
		var lease model.SourceEpochLease
		err := tx.First(&lease, "request_id=?", requestID).Error
		if err == nil {
			if lease.PipelineID != id || lease.RequestDigest != digest {
				return ErrSourceRecoveryConflict
			}
			reply = &iproto.SourceRecoveryReply{}
			if protojson.Unmarshal(lease.Reply, reply) != nil || iproto.ValidateSourceCheckpoint(reply.Checkpoint) != nil || !sourceCheckpointMatches(reply.Checkpoint, &p) || reply.RequestId != req.RequestId || reply.EpochFloor <= reply.Checkpoint.Epoch || reply.EpochCeiling < reply.EpochFloor || reply.EpochCeiling > math.MaxInt64 {
				return ErrSourceRecoveryUnavailable
			}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		state, err := sourceRecoveryState(tx, id)
		if err != nil {
			return err
		}
		cp, err := decodeRecoveryCheckpoint(state.Checkpoint)
		if err != nil {
			return err
		}
		ceiling := max(state.EpochCeiling, req.KnownEpochCeiling)
		if cp != nil {
			ceiling = max(ceiling, cp.Epoch)
		}
		if req.LocalCheckpoint != nil {
			ceiling = max(ceiling, req.LocalCheckpoint.Epoch)
		}
		if sourceCheckpointMatches(req.LocalCheckpoint, &p) && (!sourceCheckpointMatches(cp, &p) || req.LocalCheckpoint.Epoch > cp.Epoch || req.LocalCheckpoint.Epoch == cp.Epoch && req.LocalCheckpoint.BatchSeq >= cp.BatchSeq) {
			cp = proto.Clone(req.LocalCheckpoint).(*iproto.SourceCheckpoint)
		} else if !sourceCheckpointMatches(cp, &p) {
			if req.InitialGtidSet == nil {
				return ErrSourceRecoveryUnavailable
			}
			// epoch=1 is a validation-only initial baseline, never a reserved epoch.
			cp = &iproto.SourceCheckpoint{PipelineId: req.PipelineId, NodeId: req.NodeId, RunId: req.RunId, PositionRevision: req.PositionRevision, Epoch: 1, PublishedGtidSet: *req.InitialGtidSet, ObservedTsMs: now.UnixMilli()}
			if iproto.ValidateSourceCheckpoint(cp) != nil {
				return ErrSourceRecoveryConflict
			}
			ceiling = max(ceiling, cp.Epoch)
		}
		if ceiling > math.MaxInt64-SourceEpochRangeSize {
			return ErrSourceRecoveryUnavailable
		}
		reply = &iproto.SourceRecoveryReply{Checkpoint: cp, EpochFloor: ceiling + 1, EpochCeiling: ceiling + SourceEpochRangeSize, RequestId: req.RequestId}
		state.EpochCeiling = reply.EpochCeiling
		checkpointRaw, err := protojson.Marshal(cp)
		if err != nil {
			return err
		}
		state.Checkpoint = model.JSON(checkpointRaw)
		if err := tx.Save(state).Error; err != nil {
			return err
		}
		raw, err := protojson.Marshal(reply)
		if err != nil {
			return err
		}
		return tx.Create(&model.SourceEpochLease{RequestID: requestID, PipelineID: id, RequestDigest: digest, Reply: model.JSON(raw)}).Error
	})
	return reply, err
}
