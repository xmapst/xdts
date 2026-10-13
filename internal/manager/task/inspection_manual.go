package task

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

var (
	ErrProbeUnavailable = errors.New("endpoint probe dispatcher unavailable")
	ErrProbeQueueFull   = errors.New("endpoint probe queue full")
	ErrProbeCrossCenter = errors.New("endpoint and node belong to different centers")
	ErrProbeNodeOffline = errors.New("endpoint probe node is offline")
)

type manualEndpointProbe struct {
	endpointID uuidx.UUID
	nodeID     uuidx.UUID
	taskID     string
	acceptedAt time.Time
}

// SubmitEndpointProbe 接收一次手动探测；结果异步写入端点行，以 taskID 关联本次请求。
func (l *InspectionLoop) SubmitEndpointProbe(ctx context.Context, endpointID, nodeID uuidx.UUID) (string, error) {
	if l.db == nil || l.dispatcherSnapshot() == nil {
		return "", ErrProbeUnavailable
	}
	if _, _, err := l.loadManualProbeTargets(ctx, endpointID, nodeID); err != nil {
		return "", err
	}
	taskID := uuidx.New().String()
	request := manualEndpointProbe{endpointID: endpointID, nodeID: nodeID, taskID: taskID, acceptedAt: time.Now()}
	l.manualMu.Lock()
	defer l.manualMu.Unlock()
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case l.manualQueue <- request:
		l.manualPending[endpointID]++
		return taskID, nil
	default:
		return "", ErrProbeQueueFull
	}
}

func (l *InspectionLoop) manualProbePending(endpointID uuidx.UUID) bool {
	l.manualMu.Lock()
	defer l.manualMu.Unlock()
	return l.manualPending[endpointID] > 0
}

func (l *InspectionLoop) loadManualProbeTargets(ctx context.Context, endpointID, nodeID uuidx.UUID) (*model.Endpoint, *model.Node, error) {
	ep, err := repo.GetEndpoint(ctx, l.db, endpointID)
	if err != nil {
		return nil, nil, err
	}
	node, err := repo.GetNode(ctx, l.db, nodeID)
	if err != nil {
		return nil, nil, err
	}
	if ep.CenterID != node.CenterID {
		return nil, nil, ErrProbeCrossCenter
	}
	if node.Status != model.NodeStatusOnline {
		return nil, nil, ErrProbeNodeOffline
	}
	return ep, node, nil
}

func (l *InspectionLoop) runManualEndpointProbes(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case request := <-l.manualQueue:
					l.recordManualProbeFailure(ctx, request, "MANAGER_SHUTDOWN", "Manager 关闭，探测未执行")
					l.finishManualProbe(request.endpointID)
				default:
					return
				}
			}
		case request := <-l.manualQueue:
			l.executeManualEndpointProbe(ctx, request)
		}
	}
}

func (l *InspectionLoop) executeManualEndpointProbe(ctx context.Context, request manualEndpointProbe) {
	defer l.finishManualProbe(request.endpointID)
	ep, node, err := l.loadManualProbeTargets(ctx, request.endpointID, request.nodeID)
	if err != nil {
		l.recordManualProbeFailure(ctx, request, "TARGET_UNAVAILABLE", "端点或探测节点已不可用")
		return
	}
	dispatcher := l.dispatcherSnapshot()
	if dispatcher == nil {
		l.recordManualProbeFailure(ctx, request, "DISPATCH_UNAVAILABLE", "探测派发服务暂不可用")
		return
	}
	result, err := dispatcher.DispatchConnTest(ctx, *ep, *node, request.taskID)
	if err != nil {
		slog.Warn("manual endpoint probe dispatch failed", slog.String("endpoint_id", ep.ID.String()),
			slog.String("task_id", request.taskID), slog.Any("error", err))
		l.recordManualProbeFailure(ctx, request, "DISPATCH_FAILED", "探测请求未能完成往返")
		return
	}
	if err := l.persistEndpointResult(ctx, *ep, *node, request.acceptedAt, request.taskID, result); err != nil {
		slog.Error("manual endpoint probe result persistence failed", slog.String("endpoint_id", ep.ID.String()),
			slog.String("task_id", request.taskID), slog.Any("error", err))
		if ctx.Err() != nil {
			l.recordManualProbeFailure(ctx, request, "MANAGER_SHUTDOWN", "Manager 关闭，探测结果未写入")
		}
	}
}

func (l *InspectionLoop) finishManualProbe(endpointID uuidx.UUID) {
	l.manualMu.Lock()
	l.manualPending[endpointID]--
	if l.manualPending[endpointID] == 0 {
		delete(l.manualPending, endpointID)
	}
	l.manualMu.Unlock()
}

func (l *InspectionLoop) recordManualProbeFailure(ctx context.Context, request manualEndpointProbe, code, message string) {
	if ctx.Err() != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
	}
	if err := l.persistEndpointFailure(ctx, request.endpointID, request.nodeID, request.acceptedAt, request.taskID, code, message); err != nil {
		slog.Error("manual endpoint probe failure persistence failed", slog.String("endpoint_id", request.endpointID.String()),
			slog.String("task_id", request.taskID), slog.Any("error", err))
	}
}
