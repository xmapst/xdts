package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xmapst/xdoc"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/pipeline/binlogin"
	"github.com/xmapst/xdts/internal/node/store"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// requestSourceRecovery runs in the loader's asynchronous startup workflow.
func (m *Module) requestSourceRecovery(ctx context.Context, req *iproto.SourceRecoveryRequest) (*iproto.SourceRecoveryReply, error) {
	if m.rpcClient == nil {
		return nil, errors.New("controller: source recovery RPC unavailable")
	}
	body, encoding, err := iproto.Encode(req, iproto.EncodeOptions{})
	if err != nil {
		return nil, err
	}
	env := iproto.NewEnvelope(body, iproto.NewEnvelopeParams{MessageID: uuidx.New().String(), Type: iproto.MessageTypeSourceRecovery, TimestampMS: m.now().UnixMilli(), ProtocolVersion: m.opts.ProtocolVersion, Center: m.opts.Center, NodeID: m.opts.NodeID, ContentEncoding: encoding})
	requestCtx, cancel := context.WithTimeout(ctx, defaultTargetedPullTimeout)
	defer cancel()
	msg, err := m.rpcClient.Call(requestCtx, transport.ExchangeRPC, transport.RoutingKeyRPC(), env, transport.WithEnvelopeDecorator(func(e *iproto.Envelope) error { e.WithSignature(iproto.Sign(m.signKey, e.SignParamsFor())); return nil }))
	if err != nil {
		return nil, fmt.Errorf("controller: source recovery request failed: %w", err)
	}
	var reply iproto.SourceRecoveryReply
	replyEnv, err := transport.ParseAndDecode(msg, &reply, &transport.VerifyOptions{Key: m.signKey, Now: m.now()})
	if err != nil || replyEnv.Properties.Type != iproto.MessageTypeSourceRecoveryReply || reply.RequestId != req.RequestId {
		return nil, errors.New("controller: invalid source recovery reply")
	}
	if reply.Error != nil {
		return nil, errors.New("controller: Manager could not establish source recovery checkpoint")
	}
	return &reply, nil
}

// prepareSourceRecovery persists a complete recovery baseline and epoch range
// before any dynamic module can begin reading or writing the checkpoint.
func (l *prodLoader) prepareSourceRecovery(ctx context.Context, item *iproto.PipelineConfigItem, opts *binlogin.Options) error {
	if l.nc.SourceRecovery == nil {
		return nil // Test loaders without a control-plane RPC retain their fixture contract.
	}
	if l.nc.Store == nil {
		return errors.New("controller: source recovery requires persistent store")
	}
	var doc map[string]any
	err := l.nc.Store.Get(ctx, "checkpoints", item.PipelineId+".source", &doc)
	var local *iproto.SourceCheckpoint
	var ceiling uint64
	if err == nil {
		run, runValid := store.CheckpointRun(doc)
		revision, revisionValid := store.CheckpointPositionRevision(doc)
		epoch, epochValid := store.CheckpointEpoch(doc)
		seq, seqValid := store.CheckpointBatchSequence(doc)
		var ceilingValid bool
		ceiling, ceilingValid = store.CheckpointEpochCeiling(doc)
		set, setValid := store.CheckpointPublishedGTID(doc)
		if runValid && run > item.GetCommon().GetRunId() || revisionValid && run == item.GetCommon().GetRunId() && revision > opts.PositionRevision {
			return errors.New("controller: future source checkpoint identity")
		}
		if runValid && revisionValid && epochValid && seqValid && ceilingValid && setValid && epoch > 0 {
			local = &iproto.SourceCheckpoint{PipelineId: item.PipelineId, NodeId: l.nc.NodeID, RunId: run, PositionRevision: revision, Epoch: epoch, BatchSeq: seq, PublishedGtidSet: set, ObservedTsMs: time.Now().UnixMilli()}
			if iproto.ValidateSourceCheckpoint(local) != nil {
				local = nil
			}
		}
	} else if !errors.Is(err, xdoc.ErrNotFound) {
		// A readable state file can still have a corrupt individual checkpoint.
		// Manager recovery must succeed before replacing that document.
		local, ceiling = nil, 0
	}
	if local != nil && local.RunId == opts.RunID && local.PositionRevision == opts.PositionRevision && ceiling > local.Epoch {
		opts.RequireEpochRange = true
		if !opts.Snapshotting {
			opts.StartGTIDSet, opts.StartGTIDSetProvided = local.PublishedGtidSet, false
		}
		return nil
	}
	// A confirmed new position/run may reuse the remainder of the global lease.
	// It changes the baseline, never the epoch owner or allocated ceiling.
	if local != nil && ceiling > local.Epoch && (opts.StartGTIDSetProvided || opts.StartGTIDSet != "") {
		set := opts.StartGTIDSet
		commitCtx, cancel := context.WithTimeout(ctx, DefaultPublishTimeout)
		defer cancel()
		restored := map[string]any{"run_id": opts.RunID, "position_revision": opts.PositionRevision, "read_gtid_set": set, "published_gtid_set": set, "published_gtid_set_known": true, "epoch": int64(local.Epoch), "epoch_ceiling": int64(ceiling), "batch_seq": int64(0), "updated_at_ms": time.Now().UnixMilli()}
		if err := l.nc.Store.PutCritical(commitCtx, store.Op{Collection: "checkpoints", ID: item.PipelineId + ".source", Doc: restored, Critical: true}); err != nil {
			return err
		}
		opts.RequireEpochRange = true
		return nil
	}
	req := &iproto.SourceRecoveryRequest{PipelineId: item.PipelineId, NodeId: l.nc.NodeID, RunId: opts.RunID, PositionRevision: opts.PositionRevision, ConfigVersion: item.ConfigVersion, KnownEpochCeiling: ceiling, LocalCheckpoint: local, RequestId: uuidx.New().String()}
	if opts.StartGTIDSetProvided || opts.StartGTIDSet != "" {
		initial := opts.StartGTIDSet
		req.InitialGtidSet = &initial
	}
	reply, err := l.nc.SourceRecovery(ctx, req)
	if err != nil {
		return err
	}
	if reply == nil || reply.Error != nil || reply.RequestId != req.RequestId || iproto.ValidateSourceCheckpoint(reply.Checkpoint) != nil || reply.Checkpoint.PipelineId != item.PipelineId || reply.Checkpoint.NodeId != l.nc.NodeID || reply.Checkpoint.RunId != opts.RunID || reply.Checkpoint.PositionRevision != opts.PositionRevision || reply.EpochFloor <= reply.Checkpoint.Epoch || reply.EpochCeiling < reply.EpochFloor || reply.EpochCeiling > 1<<63-1 || reply.EpochFloor <= ceiling {
		return errors.New("controller: inconsistent source recovery checkpoint or epoch range")
	}
	cp := reply.Checkpoint
	set := cp.PublishedGtidSet
	if opts.Snapshotting && opts.StartGTIDSetProvided {
		set = opts.StartGTIDSet // snapshot replay retains its durable L baseline.
	}
	restored := map[string]any{"run_id": opts.RunID, "position_revision": opts.PositionRevision, "read_gtid_set": set, "published_gtid_set": set, "published_gtid_set_known": true, "epoch": int64(reply.EpochFloor - 1), "epoch_ceiling": int64(reply.EpochCeiling), "batch_seq": int64(0), "updated_at_ms": time.Now().UnixMilli()}
	commitCtx, cancel := context.WithTimeout(ctx, DefaultPublishTimeout)
	defer cancel()
	if err := l.nc.Store.PutCritical(commitCtx, store.Op{Collection: "checkpoints", ID: item.PipelineId + ".source", Doc: restored, Critical: true}); err != nil {
		return fmt.Errorf("controller: persist recovered source checkpoint failed: %w", err)
	}
	opts.RequireEpochRange = true
	opts.StartGTIDSet, opts.StartGTIDSetProvided = set, opts.Snapshotting
	return nil
}
