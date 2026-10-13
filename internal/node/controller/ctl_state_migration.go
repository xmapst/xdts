package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strings"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdoc"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/pipeline/amqpin"
	"github.com/xmapst/xdts/internal/node/pipeline/snapwriter"
	"github.com/xmapst/xdts/internal/node/safeerror"
	"github.com/xmapst/xdts/internal/node/snapshot"
	"github.com/xmapst/xdts/internal/node/store"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
	"github.com/xmapst/xhive/chanrpc"
	"google.golang.org/protobuf/proto"
)

type iStateTransferStore interface {
	ExportState(context.Context, []store.StateSelection) ([]store.StateRecord, error)
	ImportState(context.Context, string, uint64, string, []store.StateRecord) error
}

type stateExportMessage struct {
	request               *iproto.PipelineStateExportRequest
	replyKey, correlation string
	ack                   iAckHandle
}

func (m *Module) onStateExportMessage(msg *transport.Message, env *iproto.Envelope, ack iAckHandle) error {
	node, _ := env.HeaderString(iproto.HeaderNodeID)
	center, _ := env.HeaderString(iproto.HeaderCenter)
	if node != m.opts.NodeID || center != m.opts.Center {
		return errors.New("controller: state export recipient differs")
	}
	if err := iproto.VerifyEnvelope(env, m.signKey, m.now(), 0); err != nil {
		return err
	}
	replyKey, ok := transport.ReplyRoutingKeyFor(env)
	if !ok || env.Properties.CorrelationID == "" {
		return errors.New("controller: state export reply identity missing")
	}
	var req iproto.PipelineStateExportRequest
	if err := iproto.DecodeLimited(env.Body, env.Properties.ContentEncoding, &req, 16<<10); err != nil {
		return err
	}
	return m.AsyncCall(ModuleName, stateExportMessage{request: &req, replyKey: replyKey, correlation: env.Properties.CorrelationID, ack: ack}, func(ret *chanrpc.RetInfo) {
		if ret != nil && ret.Err != nil {
			m.discard(ack)
		}
	})
}

func stateSelections(id string, run uint64, role iproto.PipelineRole) []store.StateSelection {
	base := fmt.Sprintf("%d:%s:%d", len(id), id, run)
	if role == iproto.PipelineRole_PIPELINE_ROLE_SOURCE {
		result := []store.StateSelection{
			{Collection: "snapshot_runs", ID: base + ".start"}, {Collection: "snapshot_runs", ID: base + ".end"},
			{Collection: "snapshot_runs", Prefix: base + ".creation."},
			{Collection: "snapshot_runs", Prefix: base + ".recovery."}, {Collection: "snapshot_chunks", Prefix: base + ".recovery."},
			{Collection: "snapshot_chunks", ID: base}, {Collection: "snapshot_chunks", Prefix: base + ".extension."}, {Collection: "snapshot_chunks", Prefix: base + ".published."},
			{Collection: "checkpoints", ID: id + ".source"}, {Collection: "schema_history", ID: fmt.Sprintf("%s.source.history.%d", id, run)}, {Collection: "schema_history", Prefix: fmt.Sprintf("%s.source.history-version.%d.", id, run)},
		}
		if run == 0 {
			result = append(result, store.StateSelection{Collection: "schema_history", ID: id + ".source"})
		}
		return result
	}
	return []store.StateSelection{
		{Collection: "snapshot_targets", ID: base}, {Collection: "checkpoints", ID: id + ".target"}, {Collection: "schema_migrations", Prefix: base + "."},
		{Collection: "snapshot_targets", Prefix: base + ".recovery."}, {Collection: "snapshot_targets", Prefix: base + ".recovery_deferred."},
		{Collection: "publication_window_cursors", Prefix: base + ":"}, {Collection: "skipped_transaction_sets", ID: base}, {Collection: "skipped_transactions", Prefix: base + ":"},
	}
}

func stateSnapshotItem(item *iproto.PipelineConfigItem) bool {
	return item.GetRole() == iproto.PipelineRole_PIPELINE_ROLE_SOURCE && needsSnapshot(item) || item.GetRole() == iproto.PipelineRole_PIPELINE_ROLE_TARGET && targetNeedsSnapshot(item)
}

func (m *Module) onStateExportReq(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
	msg, ok := ci.RequestAs[stateExportMessage]()
	if !ok || msg.request == nil || msg.ack == nil {
		return &chanrpc.RetInfo{Err: errors.New("controller: invalid state export message")}
	}
	req := proto.Clone(msg.request).(*iproto.PipelineStateExportRequest)
	key := appliedKey{pipelineID: req.PipelineId, role: req.Role}
	prior, exists := m.applied[key]
	transfer, available := m.opts.Store.(iStateTransferStore)
	var failure error
	if m.identityRetired || !m.mirrorReady || !available || !exists || prior.running || prior.unloadUncertain || m.pipelineBusy(req.PipelineId) || req.ConfigVersion == 0 || req.ConfigVersion >= math.MaxInt64 || prior.GetConfigVersion() != req.ConfigVersion || prior.GetCommon().GetRunId() != req.RunId || !stateSnapshotItem(prior.PipelineConfigItem) || !reinitUUID(req.RequestId, false) || !reinitUUID(req.DestinationNodeId, false) || req.DestinationNodeId == m.opts.NodeID {
		failure = errors.New("controller: export requires the stopped current full-run owner")
	}
	expectedUUID := prior.GetSource().GetSnapshotUuid()
	if req.Role == iproto.PipelineRole_PIPELINE_ROLE_TARGET {
		expectedUUID = prior.GetTarget().GetSnapshotUuid()
	}
	if expectedUUID != req.SnapshotUuid || !reinitUUID(req.SnapshotUuid, false) {
		failure = errors.New("controller: export snapshot identity differs")
	}
	claimed := failure == nil
	var exportItem *iproto.PipelineConfigItem
	if claimed {
		exportItem = proto.Clone(prior.PipelineConfigItem).(*iproto.PipelineConfigItem)
	}
	retryable := failure != nil && available && exists && !m.identityRetired && !prior.running && !prior.unloadUncertain && prior.GetCommon().GetRunId() == req.RunId && stateSnapshotItem(prior.PipelineConfigItem) && expectedUUID == req.SnapshotUuid && prior.GetConfigVersion() <= req.ConfigVersion && (prior.GetConfigVersion() < req.ConfigVersion || m.pipelineBusy(req.PipelineId))
	if claimed {
		m.pending[key] = true
	}
	var result *iproto.PipelineStateBundle
	m.AsyncGo("controller.stateexport."+req.PipelineId, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if failure == nil {
			// Export retires this stopped source's unused lease. Even if Manager's
			// CAS loses to START, its next start must acquire a fresh fenced range.
			if req.Role == iproto.PipelineRole_PIPELINE_ROLE_SOURCE {
				failure = releaseLocalSourceEpochs(ctx, m.opts.Store, req.PipelineId)
			}
			var records []store.StateRecord
			if failure == nil {
				records, failure = transfer.ExportState(ctx, stateSelections(req.PipelineId, req.RunId, req.Role))
			}
			if failure == nil {
				failure = validateTransferredRunState(ctx, records, exportItem)
			}
			if failure == nil {
				result = &iproto.PipelineStateBundle{PipelineId: req.PipelineId, RunId: req.RunId, Role: req.Role, SnapshotUuid: req.SnapshotUuid, SourceNodeId: m.opts.NodeID, DestinationNodeId: req.DestinationNodeId, ConfigVersion: req.ConfigVersion + 1}
				for _, record := range records {
					result.Records = append(result.Records, &iproto.PipelineStateRecord{Collection: record.Collection, Id: record.ID, Bson: record.Data})
				}
				slices.SortFunc(result.Records, func(a, b *iproto.PipelineStateRecord) int {
					return strings.Compare(a.Collection+"\x00"+a.Id, b.Collection+"\x00"+b.Id)
				})
				result.Digest, failure = iproto.StateBundleDigest(result)
				if failure == nil {
					failure = iproto.ValidateStateBundle(result)
				}
			}
		}
		reply := &iproto.PipelineStateExportReply{RequestId: req.RequestId, Bundle: result}
		if failure != nil {
			reply.Bundle = nil
			reply.Error = &iproto.Error{Code: "STATE_EXPORT_UNAVAILABLE", Message: "stopped snapshot state could not be exported"}
			if retryable {
				reply.Error.Code = "STATE_EXPORT_RETRY"
			}
			slog.Warn("stopped snapshot export failed", slog.String("pipeline_id", req.PipelineId), slog.Any("role", req.Role), slog.Uint64("config_version", req.ConfigVersion), slog.String("code", reply.Error.Code), slog.String("reason", safeerror.Public(failure)))
		}
		if err := m.publishStateExportReply(ctx, msg, reply); err != nil {
			m.discard(msg.ack)
		} else {
			m.ack(msg.ack)
		}
	}, func() {
		// Failed preconditions never claimed the pending slot of another task.
		if claimed {
			delete(m.pending, key)
			m.reevaluateTrailing(key)
		}
	})
	return nil
}

func (m *Module) publishStateExportReply(ctx context.Context, msg stateExportMessage, reply *iproto.PipelineStateExportReply) error {
	body, encoding, err := iproto.Encode(reply, iproto.EncodeOptions{Compress: true})
	if err != nil {
		return err
	}
	env := iproto.NewEnvelope(body, iproto.NewEnvelopeParams{MessageID: uuidx.New().String(), CorrelationID: msg.correlation, Type: iproto.MessageTypePipelineStateExportReply, TimestampMS: m.now().UnixMilli(), ProtocolVersion: m.opts.ProtocolVersion, Center: m.opts.Center, NodeID: m.opts.NodeID, ContentEncoding: encoding})
	env.WithSignature(iproto.Sign(m.signKey, env.SignParamsFor()))
	ctx, cancel := context.WithTimeout(ctx, DefaultPublishTimeout)
	defer cancel()
	outcome, err := m.opts.Publisher.PublishEnvelope(ctx, transport.ExchangeReply, msg.replyKey, env)
	if err != nil {
		return err
	}
	if outcome != transport.OutcomeConfirmed {
		return errors.New("controller: state export reply not confirmed")
	}
	return nil
}

func importStateBeforeAssembly(ctx context.Context, st iStoreAccess, node string, item *iproto.PipelineConfigItem) error {
	bundle := item.GetCommon().GetStateBundle()
	if bundle == nil {
		return nil
	}
	if iproto.ValidateStateBundle(bundle) != nil || bundle.PipelineId != item.PipelineId || bundle.RunId != item.GetCommon().GetRunId() || bundle.Role != item.Role || bundle.DestinationNodeId != node || bundle.ConfigVersion > item.ConfigVersion || !stateSnapshotItem(item) {
		return errors.New("controller: invalid assembly state handover")
	}
	uuid := item.GetSource().GetSnapshotUuid()
	if item.Role == iproto.PipelineRole_PIPELINE_ROLE_TARGET {
		uuid = item.GetTarget().GetSnapshotUuid()
	}
	if uuid != bundle.SnapshotUuid {
		return errors.New("controller: assembly handover snapshot differs")
	}
	transfer, ok := st.(iStateTransferStore)
	if !ok {
		return errors.New("controller: durable state import unavailable")
	}
	records := make([]store.StateRecord, 0, len(bundle.Records))
	for _, record := range bundle.Records {
		records = append(records, store.StateRecord{Collection: record.Collection, ID: record.Id, Data: slices.Clone(record.Bson)})
	}
	if err := validateTransferredRunState(ctx, records, item); err != nil {
		return err
	}
	digest, err := store.StateDigest(records)
	if err != nil {
		return err
	}
	marker := fmt.Sprintf("state_migration:%s:%d:%d", bundle.PipelineId, bundle.RunId, bundle.Role)
	return transfer.ImportState(ctx, marker, bundle.ConfigVersion, digest, records)
}

func validateTransferredRunState(ctx context.Context, records []store.StateRecord, item *iproto.PipelineConfigItem) error {
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if record.Collection != "checkpoints" {
			continue
		}
		doc, err := xdoc.DecodeDocument(record.Data)
		if err != nil {
			return err
		}
		var saved map[string]any
		if err := xdoc.UnmarshalValue(doc.Value(), &saved); err != nil {
			return err
		}
		if !store.MatchesCheckpointRun(saved, item.GetCommon().GetRunId()) {
			return errors.New("controller: transferred checkpoint run differs")
		}
		for _, read := range []func(map[string]any) (uint64, bool){store.CheckpointEpoch, store.CheckpointEpochCeiling, store.CheckpointBatchSequence, store.CheckpointPositionRevision} {
			if _, valid := read(saved); !valid {
				return errors.New("controller: invalid transferred checkpoint number")
			}
		}
		for _, field := range []string{"published_gtid_set", "applied_gtid_set", "consumed_gtid_set", "rejected_gtid_set"} {
			value, present := saved[field]
			if !present {
				continue
			}
			set, valid := value.(string)
			if _, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, set); !valid || err != nil {
				return errors.New("controller: invalid transferred checkpoint GTIDs")
			}
		}
	}
	if item.Role != iproto.PipelineRole_PIPELINE_ROLE_SOURCE {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := snapwriter.ValidateTransferState(records, item.PipelineId, item.GetCommon().GetRunId(), item.GetTarget().GetSnapshotUuid(), item.GetCommon().GetSnapshotPlanExtensionsV1(), item.GetCommon().GetSnapshotTableRecoveryV1()); err != nil {
			return err
		}
		return amqpin.ValidateTransferState(records, item.PipelineId, item.GetCommon().GetRunId())
	}
	src := item.GetSource()
	chunkRows := uint64(src.GetSnapshot().GetChunkRows())
	if chunkRows == 0 {
		chunkRows = 2000
	}
	readUUID := ""
	if src.GetSnapshot().GetReadFromReplica() {
		readUUID = src.GetSnapshotEndpoint().GetServerUuid()
	}
	return snapshot.ValidateTransferState(ctx, records, snapshot.RunOptions{PipelineID: item.PipelineId, RunID: item.GetCommon().GetRunId(), SnapshotUUID: src.GetSnapshotUuid(), ChunkRows: chunkRows, ReinitPlan: src.GetReinitPlan(), ReadEndpointUUID: readUUID, EnablePlanExtensions: item.GetCommon().GetSnapshotPlanExtensionsV1(), EnableTableCreation: item.GetCommon().GetSnapshotTableCreationV1(), EnableTableRecovery: item.GetCommon().GetSnapshotTableRecoveryV1()})
}
