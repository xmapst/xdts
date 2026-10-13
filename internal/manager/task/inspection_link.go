package task

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"slices"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"google.golang.org/protobuf/proto"
)

type iLinkDispatcher interface {
	DispatchLink(context.Context, model.Node, string, string, iproto.PipelineRole, *iproto.LinkProbeArgs) (*iproto.ProbeResult, error)
}

func (d *RPCProbeDispatcher) DispatchLink(ctx context.Context, node model.Node, taskID, pipelineID string, side iproto.PipelineRole, args *iproto.LinkProbeArgs) (*iproto.ProbeResult, error) {
	if args == nil {
		return nil, ErrPrecheckProbeFailed
	}
	return d.callProbe(ctx, &iproto.ProbeRequest{TaskId: taskID, Type: iproto.ProbeType_PROBE_TYPE_LINK, PipelineId: pipelineID, Side: side, Link: proto.Clone(args).(*iproto.LinkProbeArgs)}, node, taskID)
}

// pipelineLink proves every data route used by this immutable precheck scope.
// Only the current request combines returned values. CompletePrecheckRun later
// checks the batch's configuration and sequence again before authorization.
func (l *InspectionLoop) pipelineLink(ctx context.Context, p model.Pipeline, scope *pipelinePrecheckScope) ([]*iproto.LinkResult, error) {
	dispatcher, ok := l.dispatcherSnapshot().(iLinkDispatcher)
	if !ok {
		return nil, ErrProbeUnavailable
	}
	_, source, err := l.loadManualProbeTargets(ctx, p.SourceEndpointID, p.SourceNodeID)
	if err != nil {
		return nil, err
	}
	_, target, err := l.loadManualProbeTargets(ctx, p.TargetEndpointID, p.TargetNodeID)
	if err != nil {
		return nil, err
	}
	preserveBacklog := p.LastCmdSeq > 0 && p.DesiredState == model.DesiredStateStopped && p.SourceActualState == model.ActualStateStopped && p.TargetActualState == model.ActualStateStopped
	for _, node := range []*model.Node{source, target} {
		var capabilities []string
		if json.Unmarshal(node.Capabilities, &capabilities) != nil || !slices.Contains(capabilities, iproto.CapabilityStoppedBacklogProbe) {
			preserveBacklog = false
		}
	}
	routes := []bool{false}
	if scope.source.GetSnapshot() {
		routes = append(routes, true)
	}
	evidence := []*iproto.LinkResult{}
	for _, snapshot := range routes {
		key := make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return nil, err
		}
		a := &iproto.LinkProbeArgs{ProbeId: uuidx.New().String(), RunId: p.RunID, ConfigVersion: p.ConfigVersion, SignKey: key, ExpiresAtMs: time.Now().Add(35 * time.Second).UnixMilli(), Snapshot: snapshot, PreserveBacklogV1: preserveBacklog, SourceNodeId: source.ID.String()}
		center, err := l.linkSourceCenter(ctx, source.CenterID)
		if err != nil {
			return nil, err
		}
		a.SourceCenter = center
		call := func(ctx context.Context, node model.Node, side iproto.PipelineRole, phase iproto.LinkProbePhase) (*iproto.LinkResult, error) {
			args := proto.Clone(a).(*iproto.LinkProbeArgs)
			args.Phase = phase
			result, err := dispatcher.DispatchLink(ctx, node, uuidx.New().String(), p.ID.String(), side, args)
			link := result.GetLink()
			if err != nil || !result.GetSuccess() || !link.GetOk() || link.GetProbeId() != a.ProbeId || link.GetRunId() != a.RunId || link.GetConfigVersion() != a.ConfigVersion || link.GetSnapshot() != snapshot || link.GetPreserveBacklogV1() != preserveBacklog || link.GetRttMs() < 0 {
				return nil, ErrPrecheckProbeFailed
			}
			return proto.Clone(link).(*iproto.LinkResult), nil
		}
		routeCtx, cancel := context.WithTimeout(ctx, 40*time.Second)
		if _, err := call(routeCtx, *target, iproto.PipelineRole_PIPELINE_ROLE_TARGET, iproto.LinkProbePhase_LINK_PROBE_PHASE_PREPARE); err != nil {
			cancel()
			return nil, err
		}
		type received struct {
			result *iproto.LinkResult
			err    error
		}
		done := make(chan received, 1)
		go func() {
			result, err := call(routeCtx, *target, iproto.PipelineRole_PIPELINE_ROLE_TARGET, iproto.LinkProbePhase_LINK_PROBE_PHASE_RECEIVE)
			done <- received{result: result, err: err}
		}()
		_, sentErr := call(routeCtx, *source, iproto.PipelineRole_PIPELINE_ROLE_SOURCE, iproto.LinkProbePhase_LINK_PROBE_PHASE_SEND)
		if sentErr != nil {
			cancel()
		}
		var receipt received
		select {
		case receipt = <-done:
		case <-routeCtx.Done():
			receipt.err = routeCtx.Err()
		}
		cancel()
		if sentErr != nil || receipt.err != nil {
			return nil, ErrPrecheckProbeFailed
		}
		evidence = append(evidence, receipt.result)
	}
	return evidence, nil
}

func (l *InspectionLoop) linkSourceCenter(ctx context.Context, id uuidx.UUID) (string, error) {
	var center model.Center
	if err := l.db.WithContext(ctx).Where("id=?", id).First(&center).Error; err != nil {
		return "", err
	}
	return center.Code, nil
}
