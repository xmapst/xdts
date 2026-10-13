package task

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
	"google.golang.org/protobuf/proto"
)

var ErrPositionProbeFailed = errors.New("source position probe failed")

type iPositionDispatcher interface {
	DispatchPosition(context.Context, model.Endpoint, model.Node, string, *iproto.PositionProbeArgs) (*iproto.ProbeResult, error)
}

func (d *RPCProbeDispatcher) DispatchPosition(ctx context.Context, ep model.Endpoint, node model.Node, id string, args *iproto.PositionProbeArgs) (*iproto.ProbeResult, error) {
	if args == nil {
		return nil, ErrPositionProbeFailed
	}
	return d.callProbe(ctx, &iproto.ProbeRequest{TaskId: id, Type: iproto.ProbeType_PROBE_TYPE_POSITION, Endpoint: probeEndpointConn(ep), Position: proto.Clone(args).(*iproto.PositionProbeArgs)}, node, id)
}

// The temporary dump connection reuses this stopped pipeline's allocated ID.
// Select the assigned source node, never an arbitrary node in another center.
func (l *InspectionLoop) GetPipelinePosition(ctx context.Context, id uuidx.UUID, args *iproto.PositionProbeArgs) (*iproto.PositionResult, error) {
	dispatcher, ok := l.dispatcherSnapshot().(iPositionDispatcher)
	if l.db == nil || !ok || args == nil {
		return nil, ErrProbeUnavailable
	}
	p, err := repo.GetPipeline(ctx, l.db, id)
	if err != nil {
		return nil, err
	}
	if p.DesiredState != model.DesiredStateStopped || (p.SourceActualState != model.ActualStateStopped && p.SourceActualState != model.ActualStateDraft) || p.BinlogServerID == nil {
		return nil, ErrPositionProbeFailed
	}
	ep, err := repo.GetEndpoint(ctx, l.db, p.SourceEndpointID)
	if err != nil {
		return nil, err
	}
	var prior struct {
		Reachable bool `json:"reachable"`
	}
	if json.Unmarshal(ep.LastProbeResult, &prior) != nil || !prior.Reachable {
		return nil, ErrCatalogNotProbed
	}
	node, err := repo.GetNode(ctx, l.db, p.SourceNodeID)
	if err != nil {
		return nil, err
	}
	if node.CenterID != ep.CenterID {
		return nil, ErrPositionProbeFailed
	}
	if node.Status != model.NodeStatusOnline {
		return nil, ErrProbeNodeOffline
	}
	input := proto.Clone(args).(*iproto.PositionProbeArgs)
	input.BinlogServerId = *p.BinlogServerID
	input.CheckpointRunId, input.PositionRevision, input.CheckpointPipelineId = p.RunID, p.PositionRevision, p.ID.String()
	queryCtx, cancel := context.WithTimeout(ctx, 70*time.Second)
	defer cancel()
	result, err := dispatcher.DispatchPosition(queryCtx, *ep, *node, uuidx.New().String(), input)
	if err == nil && result.GetPosition().GetBoundary() != nil {
		return nil, &PositionBoundaryError{Boundary: proto.Clone(result.GetPosition().GetBoundary()).(*iproto.PositionBoundary)}
	}
	if err != nil || !result.GetSuccess() || result.GetPosition() == nil {
		return nil, ErrPositionProbeFailed
	}
	return proto.Clone(result.GetPosition()).(*iproto.PositionResult), nil
}

// PositionBoundaryError preserves only typed local transaction boundaries.
type PositionBoundaryError struct{ Boundary *iproto.PositionBoundary }

func (*PositionBoundaryError) Error() string { return "source offset is not a transaction boundary" }
