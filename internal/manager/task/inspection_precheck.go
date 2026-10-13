package task

import (
	"context"
	"errors"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"google.golang.org/protobuf/proto"
)

var ErrPrecheckProbeFailed = errors.New("node precheck probe failed")

type iPrecheckDispatcher interface {
	DispatchPrecheck(context.Context, model.Endpoint, model.Node, string, string, iproto.PipelineRole, *iproto.PrecheckProbeArgs) (*iproto.ProbeResult, error)
}

func (d *RPCProbeDispatcher) DispatchPrecheck(ctx context.Context, ep model.Endpoint, node model.Node, taskID, pipelineID string, side iproto.PipelineRole, args *iproto.PrecheckProbeArgs) (*iproto.ProbeResult, error) {
	if args == nil || (side != iproto.PipelineRole_PIPELINE_ROLE_SOURCE && side != iproto.PipelineRole_PIPELINE_ROLE_TARGET) {
		return nil, ErrPrecheckProbeFailed
	}
	request := &iproto.ProbeRequest{TaskId: taskID, Type: iproto.ProbeType_PROBE_TYPE_PRECHECK,
		PipelineId: pipelineID, Side: side, Endpoint: probeEndpointConn(ep),
		Precheck: proto.Clone(args).(*iproto.PrecheckProbeArgs)}
	return d.callProbe(ctx, request, node, taskID)
}

// GetEndpointPrecheck uses the caller's assigned node, never an automatically
// selected inspector. It returns check failures as data; execution failure is an
// error. The orchestration layer must verify every required check before saving.
func (l *InspectionLoop) GetEndpointPrecheck(ctx context.Context, endpointID, nodeID, pipelineID uuidx.UUID, side iproto.PipelineRole, args *iproto.PrecheckProbeArgs) (*iproto.PrecheckResult, error) {
	dispatcher, ok := l.dispatcherSnapshot().(iPrecheckDispatcher)
	if l.db == nil || !ok || args == nil || pipelineID.IsZero() || (side != iproto.PipelineRole_PIPELINE_ROLE_SOURCE && side != iproto.PipelineRole_PIPELINE_ROLE_TARGET) {
		return nil, ErrProbeUnavailable
	}
	ep, node, err := l.loadManualProbeTargets(ctx, endpointID, nodeID)
	if err != nil {
		return nil, err
	}
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	result, err := dispatcher.DispatchPrecheck(queryCtx, *ep, *node, uuidx.New().String(), pipelineID.String(), side, args)
	if err != nil || !result.GetSuccess() || result.GetPrecheck() == nil {
		return nil, ErrPrecheckProbeFailed
	}
	return proto.Clone(result.GetPrecheck()).(*iproto.PrecheckResult), nil
}
