package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
)

type IPipelineStateExporter interface {
	ExportPipelineState(context.Context, uuidx.UUID, model.Side, uuidx.UUID, uint64) (*iproto.PipelineStateBundle, error)
}

func (d *Dispatcher) ExportPipelineState(ctx context.Context, id uuidx.UUID, side model.Side, newNodeID uuidx.UUID, version uint64) (*iproto.PipelineStateBundle, error) {
	client := d.rpc.Load()
	if client == nil {
		return nil, repo.ErrPipelineMigrationSnapshotState
	}
	p, err := repo.GetPipeline(ctx, d.db, id)
	if err != nil {
		return nil, err
	}
	if p.ConfigVersion != version {
		return nil, repo.ErrPipelineConfigVersionConflict
	}
	if !side.Valid() || p.ReinitPending || p.DesiredState != model.DesiredStateStopped || p.SourceActualState != model.ActualStateStopped || p.TargetActualState != model.ActualStateStopped {
		return nil, repo.ErrPipelineMigrationRequiresStopped
	}
	if p.LastCmdSeq == 0 || p.InitMode != model.InitModeFullAndIncremental && p.InitMode != model.InitModeFullOnly {
		return nil, repo.ErrPipelineMigrationSnapshotState
	}
	oldID, endpointID, role := p.SourceNodeID, p.SourceEndpointID, iproto.PipelineRole_PIPELINE_ROLE_SOURCE
	if side == model.SideTarget {
		oldID, endpointID, role = p.TargetNodeID, p.TargetEndpointID, iproto.PipelineRole_PIPELINE_ROLE_TARGET
	}
	if oldID == newNodeID {
		return nil, repo.ErrPipelineMigrationSameNode
	}
	old, err := repo.GetNode(ctx, d.db, oldID)
	if err != nil {
		return nil, err
	}
	newNode, err := repo.GetNode(ctx, d.db, newNodeID)
	if err != nil {
		return nil, err
	}
	endpoint, err := repo.GetEndpoint(ctx, d.db, endpointID)
	if err != nil {
		return nil, err
	}
	if newNode.CenterID != endpoint.CenterID || old.CenterID != endpoint.CenterID {
		return nil, repo.ErrPipelineMigrationCenterMismatch
	}
	for _, node := range []*model.Node{old, newNode} {
		if node.Status != model.NodeStatusOnline {
			return nil, repo.ErrPipelineMigrationNodeUnavailable
		}
		var capabilities []string
		if json.Unmarshal(node.Capabilities, &capabilities) != nil || !slices.Contains(capabilities, iproto.CapabilityNodeMigration) || !slices.Contains(capabilities, iproto.CapabilitySnapshotMigration) {
			return nil, repo.ErrPipelineMigrationCapability
		}
	}
	center, err := repo.GetCenter(ctx, d.db, old.CenterID)
	if err != nil {
		return nil, err
	}
	req := &iproto.PipelineStateExportRequest{PipelineId: id.String(), RunId: p.RunID, Role: role, SnapshotUuid: p.SnapshotUUID.String(), ConfigVersion: version, DestinationNodeId: newNodeID.String(), RequestId: uuidx.New().String()}
	body, encoding, err := iproto.Encode(req, iproto.EncodeOptions{})
	if err != nil {
		return nil, err
	}
	env := iproto.NewEnvelope(body, iproto.NewEnvelopeParams{MessageID: uuidx.New().String(), Type: iproto.MessageTypePipelineStateExport, TimestampMS: d.now().UnixMilli(), ProtocolVersion: iproto.CurrentProtocolVersion(), Center: center.Code, NodeID: oldID.String(), ContentEncoding: encoding})
	key := iproto.DeriveSignKeyFromTokenHash(old.TokenHash)
	ctx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	d.pushConfigChanged(ctx, id, version, false, oldID)
	var reply iproto.PipelineStateExportReply
	for attempt := 0; ; attempt++ {
		message, err := client.Call(ctx, transport.ExchangeControl, transport.RoutingKeyNode(oldID.String()), env, transport.WithCallRetryPolicy(35*time.Second, 1), transport.WithEnvelopeDecorator(func(e *iproto.Envelope) error { e.WithSignature(iproto.Sign(key, e.SignParamsFor())); return nil }))
		if err != nil {
			return nil, repo.ErrPipelineMigrationSnapshotState
		}
		replyEnv, err := message.Envelope()
		if err != nil {
			return nil, repo.ErrPipelineMigrationSnapshotState
		}
		node, _ := replyEnv.HeaderString(iproto.HeaderNodeID)
		replyCenter, _ := replyEnv.HeaderString(iproto.HeaderCenter)
		if replyEnv.Properties.Type != iproto.MessageTypePipelineStateExportReply || node != oldID.String() || replyCenter != center.Code || iproto.VerifyEnvelope(replyEnv, key, d.now(), 0) != nil {
			return nil, repo.ErrPipelineMigrationSnapshotState
		}
		reply.Reset()
		if iproto.DecodeLimited(replyEnv.Body, replyEnv.Properties.ContentEncoding, &reply, iproto.MaxStateBundleBytes+1024) != nil || reply.RequestId != req.RequestId {
			return nil, repo.ErrPipelineMigrationSnapshotState
		}
		if reply.Error == nil {
			break
		}
		if reply.Error.Code != "STATE_EXPORT_RETRY" || attempt >= 19 {
			return nil, repo.ErrPipelineMigrationSnapshotState
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	if iproto.ValidateStateBundle(reply.Bundle) != nil {
		return nil, repo.ErrPipelineMigrationSnapshotState
	}

	bundle := reply.Bundle
	if bundle.PipelineId != req.PipelineId || bundle.RunId != req.RunId || bundle.Role != req.Role || bundle.SnapshotUuid != req.SnapshotUuid || bundle.SourceNodeId != oldID.String() || bundle.DestinationNodeId != req.DestinationNodeId || bundle.ConfigVersion != version+1 {
		return nil, repo.ErrPipelineMigrationSnapshotState
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.New("dispatch: state export expired before assignment")
	}
	return bundle, nil
}
