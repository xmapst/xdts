package task

import (
	"context"
	"errors"
	"math"
	"math/bits"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

type brokerCapacityEvidence struct {
	Center        string `json:"center"`
	Node          string `json:"node,omitempty"`
	QueueBytes    uint64 `json:"queue_bytes"`
	SafeFreeBytes uint64 `json:"safe_free_bytes"`
	RequiredBytes uint64 `json:"required_bytes"`
	Code          string `json:"code,omitempty"`
}

func capacityRequired(e *iproto.SnapshotCapacityEstimate) (uint64, error) {
	if e == nil || !e.GetVerified() || e.GetEstimatedSeconds() == 0 || e.GetBinlogSampleMs() < 1000 || e.GetBinlogSampleMs() > 10000 {
		return 0, errors.New("source_capacity_unverified")
	}
	// 2x binlog growth for short-window uncertainty; snapshot payload already
	// has its own 2x encoding margin. 1 MiB reserves control/message overhead.
	if e.GetEstimatedSeconds() > math.MaxUint64/2000 {
		return 0, errors.New("estimate_overflow")
	}
	hi, lo := bits.Mul64(e.GetBinlogGrowthBytes(), e.GetEstimatedSeconds()*2000)
	if hi >= e.GetBinlogSampleMs() {
		return 0, errors.New("estimate_overflow")
	}
	backlog, rem := bits.Div64(hi, lo, e.GetBinlogSampleMs())
	if rem != 0 {
		if backlog == math.MaxUint64 {
			return 0, errors.New("estimate_overflow")
		}
		backlog++
	}
	if e.GetEstimatedSnapshotBytes() > math.MaxUint64-(1<<20) || backlog > math.MaxUint64-e.GetEstimatedSnapshotBytes()-(1<<20) {
		return 0, errors.New("estimate_overflow")
	}
	return e.GetEstimatedSnapshotBytes() + backlog + (1 << 20), nil
}

func checkBrokerCapacity(center string, required uint64, r *iproto.BrokerResult) (bool, []brokerCapacityEvidence) {
	if r == nil || !r.GetCapacityVerified() || len(r.GetNodes()) == 0 || len(r.GetNodes()) > 100 {
		return false, []brokerCapacityEvidence{{Center: center, RequiredBytes: required, Code: "broker_capacity_unverified"}}
	}
	pass := true
	seen := map[string]bool{}
	var evidence []brokerCapacityEvidence
	for _, n := range r.GetNodes() {
		e := brokerCapacityEvidence{Center: center, Node: n.GetNode(), RequiredBytes: required, QueueBytes: r.GetQueueBytes()}
		switch {
		case n == nil || n.GetNode() == "" || seen[n.GetNode()] || n.GetDiskFreeLimitBytes() == 0:
			e.Code = "broker_node_unverified"
		case n.GetDiskFreeAlarm() || n.GetDiskFreeBytes() <= n.GetDiskFreeLimitBytes():
			e.Code = "broker_disk_alarm"
		default:
			e.SafeFreeBytes = n.GetDiskFreeBytes() - n.GetDiskFreeLimitBytes()
			// Existing queue bytes are conservatively reserved in addition to the
			// live free reading to cover samples not yet reflected by disk usage.
			if r.GetQueueBytes() > e.SafeFreeBytes || required > e.SafeFreeBytes-r.GetQueueBytes() {
				e.Code = "broker_disk_insufficient"
			}
		}
		seen[n.GetNode()] = true
		pass = pass && e.Code == ""
		evidence = append(evidence, e)
	}
	return pass, evidence
}

func (l *InspectionLoop) precheckBrokerCapacity(ctx context.Context, scope *pipelinePrecheckScope, estimate *iproto.SnapshotCapacityEstimate) (bool, any) {
	required, err := capacityRequired(estimate)
	if err != nil {
		return false, map[string]any{"code": err.Error(), "sample": estimate}
	}
	d := l.dispatcherSnapshot()
	if d == nil {
		return false, map[string]string{"code": "broker_dispatch_unavailable"}
	}
	pass := true
	seen := map[uuidx.UUID]bool{}
	var evidence []brokerCapacityEvidence
	for _, id := range []uuidx.UUID{scope.sourceEndpoint.CenterID, scope.targetEndpoint.CenterID} {
		if seen[id] {
			continue
		}
		seen[id] = true
		center, err := repo.GetCenter(ctx, l.db, id)
		if err != nil {
			return false, map[string]string{"code": "center_unavailable"}
		}
		nodes, err := repo.ListNodes(ctx, l.db, center.ID)
		var node *model.Node
		if err == nil {
			for _, candidate := range nodes {
				if candidate.ID == center.InspectorNodeID && candidate.Status == model.NodeStatusOnline {
					copy := candidate
					node = &copy
					break
				}
			}
			if node == nil {
				node = pickInspectorCandidate(nodes)
			}
		}
		if err != nil || node == nil || node.CenterID != id || center.MQMgmtURL == "" {
			pass = false
			evidence = append(evidence, brokerCapacityEvidence{Center: center.Code, RequiredBytes: required, Code: "inspector_unavailable"})
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
		taskID := uuidx.New().String()
		r, err := d.DispatchBrokerProbe(probeCtx, *center, *node, taskID)
		cancel()
		if err != nil || !r.GetSuccess() || r.GetTaskId() != taskID || r.GetType() != iproto.ProbeType_PROBE_TYPE_BROKER {
			pass = false
			evidence = append(evidence, brokerCapacityEvidence{Center: center.Code, RequiredBytes: required, Code: "broker_probe_failed"})
			continue
		}
		ok, details := checkBrokerCapacity(center.Code, required, r.GetBroker())
		pass = pass && ok
		evidence = append(evidence, details...)
	}
	return pass, map[string]any{"sample": estimate, "broker_nodes": evidence, "binlog_multiplier": 2, "message_reserve_bytes": 1 << 20}
}
