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

var ErrDefinitionsFailed = errors.New("endpoint definitions probe failed")
var ErrDefinitionsTooLarge = errors.New("endpoint definitions exceed limit")

// Capability extension preserves custom dispatchers that only support catalog.
type iDefinitionsDispatcher interface {
	DispatchDefinitions(context.Context, model.Endpoint, model.Node, string, *iproto.DefsProbeArgs) (*iproto.ProbeResult, error)
}

func (d *RPCProbeDispatcher) DispatchDefinitions(ctx context.Context, endpoint model.Endpoint, node model.Node, taskID string, args *iproto.DefsProbeArgs) (*iproto.ProbeResult, error) {
	if args == nil {
		return nil, ErrDefinitionsFailed
	}
	req := &iproto.ProbeRequest{TaskId: taskID, Type: iproto.ProbeType_PROBE_TYPE_DEFS, Endpoint: probeEndpointConn(endpoint), Defs: proto.Clone(args).(*iproto.DefsProbeArgs)}
	return d.callProbe(ctx, req, node, taskID)
}

// GetEndpointDefinitions performs a bounded, authenticated read through a node
// in the endpoint's own center. No SQL connection is opened by the Manager.
func (l *InspectionLoop) GetEndpointDefinitions(ctx context.Context, endpointID uuidx.UUID, args *iproto.DefsProbeArgs) (*iproto.DefsResult, error) {
	dispatcher, ok := l.dispatcherSnapshot().(iDefinitionsDispatcher)
	if l.db == nil || !ok || args == nil || len(args.GetSchemas()) == 0 {
		return nil, ErrProbeUnavailable
	}
	ep, err := repo.GetEndpoint(ctx, l.db, endpointID)
	if err != nil {
		return nil, err
	}
	var prior struct {
		Reachable bool `json:"reachable"`
	}
	if len(ep.LastProbeResult) == 0 || json.Unmarshal(ep.LastProbeResult, &prior) != nil || !prior.Reachable {
		return nil, ErrCatalogNotProbed
	}
	nodes, err := repo.ListNodes(ctx, l.db, ep.CenterID)
	if err != nil {
		return nil, err
	}
	node := pickInspectorCandidate(nodes)
	if node == nil {
		return nil, ErrProbeNodeOffline
	}
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	result, err := dispatcher.DispatchDefinitions(queryCtx, *ep, *node, uuidx.New().String(), args)
	if err != nil {
		return nil, ErrDefinitionsFailed
	}
	if !result.GetSuccess() || result.GetDefs() == nil {
		if result.GetError().GetCode() == "DEFS_TOO_LARGE" {
			return nil, ErrDefinitionsTooLarge
		}
		return nil, ErrDefinitionsFailed
	}
	return proto.Clone(result.GetDefs()).(*iproto.DefsResult), nil
}
