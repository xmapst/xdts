package probe

import (
	"context"
	"errors"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
)

func (m *Module) probeLink(ctx context.Context, req *iproto.ProbeRequest) *iproto.ProbeResult {
	fail := func(code string) *iproto.ProbeResult {
		return &iproto.ProbeResult{TaskId: req.TaskId, Type: req.Type, Error: &iproto.Error{Code: code, Message: "未取得本次管道数据路由证明，请检查队列占用与联邦连接后重试"}}
	}
	a := req.GetLink()
	id, e := uuidx.Parse(req.GetPipelineId())
	probeID, pe := uuidx.Parse(a.GetProbeId())
	source, se := uuidx.Parse(a.GetSourceNodeId())
	remaining := time.UnixMilli(a.GetExpiresAtMs()).Sub(m.now())
	if a == nil || e != nil || id.IsZero() || pe != nil || probeID.IsZero() || se != nil || source.IsZero() || len(a.SignKey) != 32 || a.SourceCenter == "" || remaining <= 0 || remaining > 40*time.Second {
		return fail("LINK_INPUT_INVALID")
	}
	if req.Side != iproto.PipelineRole_PIPELINE_ROLE_SOURCE && req.Side != iproto.PipelineRole_PIPELINE_ROLE_TARGET {
		return fail("LINK_SIDE_INVALID")
	}
	sourceSide := req.Side == iproto.PipelineRole_PIPELINE_ROLE_SOURCE
	if sourceSide && (a.Phase != iproto.LinkProbePhase_LINK_PROBE_PHASE_SEND || a.SourceNodeId != m.opts.NodeID || a.SourceCenter != m.opts.Center) {
		return fail("LINK_SIDE_INVALID")
	}
	if !sourceSide && a.Phase != iproto.LinkProbePhase_LINK_PROBE_PHASE_PREPARE && a.Phase != iproto.LinkProbePhase_LINK_PROBE_PHASE_RECEIVE {
		return fail("LINK_SIDE_INVALID")
	}
	ctx, cancel := context.WithTimeout(ctx, min(remaining, transport.PipelineProbeTimeout))
	defer cancel()
	started := m.now()
	var err error
	switch a.Phase {
	case iproto.LinkProbePhase_LINK_PROBE_PHASE_PREPARE:
		if a.GetPreserveBacklogV1() {
			err = m.opts.Conn.PrepareResumingPipelineProbe(ctx, req.PipelineId, a.Snapshot, a.ProbeId)
		} else {
			err = m.opts.Conn.PreparePipelineProbe(ctx, req.PipelineId, a.Snapshot)
		}
	case iproto.LinkProbePhase_LINK_PROBE_PHASE_RECEIVE:
		validate := func(env *iproto.Envelope) error { return validateLinkEnvelope(env, req, m.now()) }
		if a.GetPreserveBacklogV1() {
			err = m.opts.Conn.ReceiveResumingPipelineProbe(ctx, a.ProbeId, validate)
		} else {
			err = m.opts.Conn.ReceivePipelineProbe(ctx, req.PipelineId, a.Snapshot, validate)
		}
	case iproto.LinkProbePhase_LINK_PROBE_PHASE_SEND:
		err = m.publishLink(ctx, req)
	default:
		return fail("LINK_INPUT_INVALID")
	}
	if err != nil {
		return fail("LINK_PROOF_FAILED")
	}
	return &iproto.ProbeResult{TaskId: req.TaskId, Type: req.Type, Success: true, Result: &iproto.ProbeResult_Link{Link: &iproto.LinkResult{Ok: true, ProbeId: a.ProbeId, RunId: a.RunId, ConfigVersion: a.ConfigVersion, Snapshot: a.Snapshot, PreserveBacklogV1: a.GetPreserveBacklogV1(), RttMs: m.now().Sub(started).Milliseconds()}}}
}

func validateLinkEnvelope(env *iproto.Envelope, req *iproto.ProbeRequest, now time.Time) error {
	a := req.GetLink()
	invalid := errors.New("probe: data link identity does not match")
	if env == nil || a == nil || len(a.SignKey) != 32 || env.Properties.Type != iproto.MessageTypeDataProbe || env.Properties.MessageID != a.ProbeId || now.UnixMilli() >= a.ExpiresAtMs {
		return invalid
	}
	var key iproto.SignKey
	copy(key[:], a.SignKey)
	if err := iproto.VerifyEnvelope(env, key, now, iproto.DefaultSignatureWindow); err != nil {
		return invalid
	}
	var data iproto.Probe
	if iproto.Decode(env.Body, env.Properties.ContentEncoding, &data) != nil || data.PipelineId != req.PipelineId || data.ProbeId != a.ProbeId || data.RunId != a.RunId || data.ConfigVersion != a.ConfigVersion || data.Snapshot != a.Snapshot || data.SourceNodeId != a.SourceNodeId || data.SourceCenter != a.SourceCenter {
		return invalid
	}
	pipeline, _ := env.HeaderString(iproto.HeaderPipelineID)
	node, _ := env.HeaderString(iproto.HeaderNodeID)
	center, _ := env.HeaderString(iproto.HeaderCenter)
	run, runOK := iproto.HeaderUint64FromString(env.Headers, iproto.HeaderRunID)
	version, verOK := iproto.HeaderUint64FromString(env.Headers, iproto.HeaderConfigVersion)
	if pipeline != req.PipelineId || node != a.SourceNodeId || center != a.SourceCenter || !runOK || run != a.RunId || !verOK || version != a.ConfigVersion {
		return invalid
	}
	return nil
}

func (m *Module) publishLink(ctx context.Context, req *iproto.ProbeRequest) error {
	a := req.Link
	body, encoding, err := iproto.Encode(&iproto.Probe{PipelineId: req.PipelineId, ProbeId: a.ProbeId, RunId: a.RunId, ConfigVersion: a.ConfigVersion, Snapshot: a.Snapshot, SourceNodeId: m.opts.NodeID, SourceCenter: m.opts.Center}, iproto.EncodeOptions{})
	if err != nil {
		return err
	}
	env := iproto.NewEnvelope(body, iproto.NewEnvelopeParams{MessageID: a.ProbeId, Type: iproto.MessageTypeDataProbe, TimestampMS: m.now().UnixMilli(), ExpirationMS: max(1, a.ExpiresAtMs-m.now().UnixMilli()), ProtocolVersion: m.opts.ProtocolVersion, Center: m.opts.Center, NodeID: m.opts.NodeID, ContentEncoding: encoding, HeaderExtra: map[string]any{iproto.HeaderPipelineID: req.PipelineId, iproto.HeaderRunID: iproto.HeaderUint64String(a.RunId), iproto.HeaderConfigVersion: iproto.HeaderUint64String(a.ConfigVersion)}})
	var key iproto.SignKey
	copy(key[:], a.SignKey)
	env.WithSignature(iproto.Sign(key, env.SignParamsFor()))
	route := transport.RoutingKeyPipelineData(req.PipelineId)
	if a.Snapshot {
		route = transport.RoutingKeyPipelineSnapshot(req.PipelineId)
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		outcome, err := m.opts.Publisher.PublishEnvelope(ctx, transport.ExchangeData, route, env)
		if err == nil && outcome == transport.OutcomeConfirmed {
			return nil
		}
		// Only a definite basic.return is retried. An uncertain confirm could have
		// delivered a copy; repeating it would leave a second message in the queue.
		if err != nil || outcome != transport.OutcomeReturned {
			return errors.New("probe: link publish not confirmed")
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
