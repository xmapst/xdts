package task

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

type alertSnapshot struct {
	nodes     []model.Node
	pipelines []model.Pipeline
	centers   []model.Center
	progress  []model.PipelineProgressLatest
	events    []model.Event
}
type alertObservation struct {
	known          bool
	firing         bool
	node, pipeline uuidx.UUID
	detail         model.JSON
}

func loadAlertSnapshot(ctx context.Context, db *gorm.DB, now time.Time) (alertSnapshot, error) {
	var s alertSnapshot
	queries := []struct {
		dest  any
		count func() int
		query *gorm.DB
	}{
		{&s.nodes, func() int { return len(s.nodes) }, db.Model(&model.Node{})},
		{&s.pipelines, func() int { return len(s.pipelines) }, db.Model(&model.Pipeline{})},
		{&s.centers, func() int { return len(s.centers) }, db.Model(&model.Center{})},
		{&s.progress, func() int { return len(s.progress) }, db.Model(&model.PipelineProgressLatest{})},
		{&s.events, func() int { return len(s.events) }, db.Model(&model.Event{}).Where("ts>=? AND ts<=? AND acked_at IS NULL AND type IN ?", now.Add(-24*time.Hour), now, []model.EventType{model.EventTypeUnknownOrigin, model.EventTypeLocalStoreRebuild})},
	}
	for _, q := range queries {
		if err := q.query.WithContext(ctx).Limit(10001).Find(q.dest).Error; err != nil {
			return s, err
		}
		if q.count() > 10000 {
			return s, errors.New("alert observation evaluation limit exceeded")
		}
	}
	return s, nil
}

func alertFresh(at *time.Time, now time.Time, window int64) bool {
	return at != nil && !at.After(now) && !at.Before(now.Add(-time.Duration(window)*time.Second))
}
func alertDetail(value any) model.JSON { raw, _ := json.Marshal(value); return model.JSON(raw) }

func (s alertSnapshot) evaluate(r model.AlertRule, now time.Time) (map[string]alertObservation, error) {
	t, err := model.ParseAlertThreshold(r.Threshold)
	if err != nil {
		return nil, err
	}
	out := map[string]alertObservation{}
	nodeCenters := make(map[uuidx.UUID]uuidx.UUID, len(s.nodes))
	for _, n := range s.nodes {
		nodeCenters[n.ID] = n.CenterID
	}
	centerPipelines := map[uuidx.UUID][]model.Pipeline{}
	for _, p := range s.pipelines {
		center := nodeCenters[p.TargetNodeID]
		centerPipelines[center] = append(centerPipelines[center], p)
	}
	switch r.Dimension {
	case model.AlertDimensionNodeOffline:
		for _, n := range s.nodes {
			out["node/"+n.ID.String()] = alertObservation{known: n.Status == model.NodeStatusOnline || n.Status == model.NodeStatusOffline || n.Status == model.NodeStatusDisabled || n.Status == model.NodeStatusRevoked, firing: n.Status == model.NodeStatusOffline, node: n.ID, detail: alertDetail(map[string]any{"status": n.Status})}
		}
	case model.AlertDimensionPipelineError:
		for _, p := range s.pipelines {
			out["pipeline/"+p.ID.String()] = alertObservation{known: p.SourceActualState != model.ActualStateUnknown && p.TargetActualState != model.ActualStateUnknown || p.SourceActualState == model.ActualStateError || p.TargetActualState == model.ActualStateError, firing: p.SourceActualState == model.ActualStateError || p.TargetActualState == model.ActualStateError, pipeline: p.ID, detail: alertDetail(map[string]any{"sourceState": p.SourceActualState, "targetState": p.TargetActualState})}
		}
	case model.AlertDimensionLatencyExceeded:
		latest := map[uuidx.UUID][]model.PipelineProgressLatest{}
		for _, p := range s.progress {
			latest[p.PipelineID] = append(latest[p.PipelineID], p)
		}
		for _, p := range s.pipelines {
			for _, side := range []model.Side{model.SideSource, model.SideTarget} {
				o := alertObservation{pipeline: p.ID}
				expected := p.SourceNodeID
				if side == model.SideTarget {
					expected = p.TargetNodeID
				}
				o.node = expected
				for _, v := range latest[p.ID] {
					if v.Side != side || v.RunID != p.RunID || v.NodeID != expected {
						continue
					}
					var values model.ProgressValues
					if json.Unmarshal(v.Values, &values) != nil || values.MetricsObservedTSMS == nil || *values.MetricsObservedTSMS <= 0 || *values.MetricsObservedTSMS > v.ObservedAt.UnixMilli() || values.LagMS < 0 || math.IsInf(values.LagMS, 0) || math.IsNaN(values.LagMS) {
						continue
					}
					at := time.UnixMilli(*values.MetricsObservedTSMS)
					if !alertFresh(&at, now, t.WindowSeconds) {
						continue
					}
					o.known = true
					o.firing = values.LagMS > float64(t.Value)
					o.detail = alertDetail(map[string]any{"lagMs": values.LagMS, "threshold": t.Value, "side": side, "observedAt": at})
				}
				out["pipeline/"+p.ID.String()+"/"+string(side)] = o
			}
		}
	case model.AlertDimensionQueueBacklog, model.AlertDimensionFederationLinkDown:
		for _, c := range s.centers {
			if r.Dimension == model.AlertDimensionFederationLinkDown {
				o := alertObservation{node: c.InspectorNodeID}
				if alertFresh(c.LastBrokerProbeAt, now, t.WindowSeconds) {
					var br iproto.BrokerResult
					var fields map[string]json.RawMessage
					if json.Unmarshal(c.LastBrokerProbeResult, &fields) == nil && fields["success"] == nil && json.Unmarshal(c.LastBrokerProbeResult, &br) == nil && br.CapacityVerified {
						o.known = true
						o.firing = !br.FederationOk
						o.detail = alertDetail(map[string]any{"centerId": c.ID.String(), "federationOk": br.FederationOk})
					}
				}
				out["center/"+c.ID.String()] = o
			} else {
				var br iproto.BrokerResult
				valid := alertFresh(c.LastBrokerProbeAt, now, t.WindowSeconds) && json.Unmarshal(c.LastBrokerProbeResult, &br) == nil && br.DataQueuesVerified
				queues := map[string]*iproto.BrokerBacklog{}
				for _, v := range br.DataQueues {
					if v != nil {
						if _, duplicate := queues[v.Queue]; duplicate {
							valid = false
						}
						queues[v.Queue] = v
					}
				}
				for _, p := range centerPipelines[c.ID] {
					for _, q := range []string{"xdts.pl." + p.ID.String(), "xdts.pl." + p.ID.String() + ".snap"} {
						o := alertObservation{pipeline: p.ID, node: p.TargetNodeID}
						if valid {
							v := queues[q]
							if v != nil && v.Messages >= 0 && v.MessagesReady >= 0 && v.MessagesUnacked >= 0 && v.Messages >= v.MessagesReady && v.Messages-v.MessagesReady == v.MessagesUnacked {
								o.known = true
								o.firing = v.Messages > t.Value
								o.detail = alertDetail(map[string]any{"queue": q, "messages": v.Messages, "messagesReady": v.MessagesReady, "messagesUnacked": v.MessagesUnacked, "threshold": t.Value})
							}
						}
						out["queue/"+q] = o
					}
				}
			}
		}
	case model.AlertDimensionUnregisteredOriginUUID, model.AlertDimensionLocalStoreAbnormal:
		kind := model.EventTypeUnknownOrigin
		if r.Dimension == model.AlertDimensionLocalStoreAbnormal {
			kind = model.EventTypeLocalStoreRebuild
		}
		for _, e := range s.events {
			if e.Type != kind || e.Ts.Before(now.Add(-time.Duration(t.WindowSeconds)*time.Second)) {
				continue
			}
			if r.Dimension == model.AlertDimensionLocalStoreAbnormal && e.Level == model.EventLevelInfo {
				continue
			}
			out["event/"+e.NodeID.String()+"/"+e.PipelineID.String()] = alertObservation{known: true, firing: true, node: e.NodeID, pipeline: e.PipelineID, detail: alertDetail(map[string]any{"eventId": e.ID.String(), "type": e.Type, "observedAt": e.Ts})}
		}
	default:
		return nil, errors.New("alert dimension invalid")
	}
	return out, nil
}
