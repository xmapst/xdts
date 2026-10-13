package controller

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/bootstrap"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
)

const timerIdentity = "controller.identity"

func (m *Module) confirmIdentity(ctx context.Context) error {
	request := &iproto.NodeHello{NodeId: m.opts.NodeID, ProtocolVersion: m.opts.ProtocolVersion, Capabilities: iproto.CurrentNodeCapabilities()}
	body, encoding, err := iproto.Encode(request, iproto.EncodeOptions{})
	if err != nil {
		return err
	}
	envelope := iproto.NewEnvelope(body, iproto.NewEnvelopeParams{MessageID: uuidx.New().String(),
		Type: iproto.MessageTypeNodeHello, TimestampMS: m.now().UnixMilli(), ProtocolVersion: m.opts.ProtocolVersion,
		Center: m.opts.Center, NodeID: m.opts.NodeID, ContentEncoding: encoding})
	message, err := m.rpcClient.Call(ctx, transport.ExchangeRPC, transport.RoutingKeyRPC(), envelope,
		transport.WithCallRetryPolicy(5*time.Second, 1), transport.WithEnvelopeDecorator(func(env *iproto.Envelope) error {
			env.WithSignature(iproto.Sign(m.signKey, env.SignParamsFor()))
			return nil
		}))
	if err != nil {
		return err
	}
	return m.verifyHelloReply(message)
}

func (m *Module) verifyHelloReply(message *transport.Message) error {
	if message.Properties.Type != iproto.MessageTypeNodeHelloReply {
		return errors.New("controller: unexpected identity reply")
	}
	var reply iproto.NodeHelloReply
	if _, err := transport.ParseAndDecode(message, &reply, &transport.VerifyOptions{
		Key: m.signKey, Now: m.now(), Window: iproto.DefaultSignatureWindow,
	}); err != nil {
		return errors.New("controller: identity reply verification failed")
	}
	if reply.CenterCode != m.opts.Center || reply.ProtocolVersion != iproto.CurrentProtocolVersion() {
		return errors.New("controller: identity reply center or protocol mismatch")
	}
	if reply.Error != nil {
		if reply.Error.Code == "NODE_IDENTITY_RETIRED" {
			return bootstrap.ErrIdentityRetired
		}
		return errors.New("controller: Manager rejected identity confirmation")
	}
	return nil
}

func (m *Module) onIdentityTimer(_ int64, _ map[string]string) {
	if !m.mirrorReady || m.identityChecking || m.identityRetired {
		return
	}
	m.identityChecking = true
	var err error
	m.AsyncGo("controller.identity", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		defer cancel()
		err = m.confirmIdentity(ctx)
	}, func() {
		m.identityChecking = false
		if errors.Is(err, bootstrap.ErrIdentityRetired) {
			m.beginIdentityRetirement()
		}
		// A failed network round trip does not revoke locally authorized work.
	})
}

func (m *Module) beginIdentityRetirement() {
	if m.identityRetired {
		return
	}
	m.identityRetired = true
	clear(m.trailingItem)
	slog.Warn("node identity retired, stopping all pipelines", slog.String("node_id", m.opts.NodeID))
	var persistErr error
	m.AsyncGo("controller.retirement.persist", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		persistErr = bootstrap.PersistRetirement(ctx, m.opts.Store, m.opts.NodeID)
	}, func() {
		if persistErr != nil {
			slog.Error("persist node retirement failed, shutting down", slog.Any("error", persistErr))
			m.requestRetirementStop()
			return
		}
		m.retirementCommitted = true
		m.stopRetiredPipelines()
	})
}

func (m *Module) stopRetiredPipelines() {
	if !m.identityRetired || !m.retirementCommitted || m.retirementStopRequested {
		return
	}
	for key, pipeline := range m.applied {
		if m.pending[key] || (!pipeline.running && !pipeline.unloadUncertain) {
			continue
		}
		m.pending[key] = true
		var err error
		m.AsyncGo("controller.retirement.stop."+key.pipelineID+"."+key.role.String(), func() {
			err = m.loader.Unload(key.pipelineID, key.role)
		}, func() {
			delete(m.pending, key)
			if err != nil {
				slog.Error("retired pipeline unload failed, shutting down", slog.String("pipeline_id", key.pipelineID), slog.Any("role", key.role), slog.Any("error", err))
				m.requestRetirementStop()
				return
			}
			pipeline.running, pipeline.paused, pipeline.unloadUncertain = false, false, false
			m.applied[key] = pipeline
			if err := persistAppliedState(m.opts.Store, m.applied, m.now().UnixMilli()); err != nil {
				slog.Warn("persist retired pipeline state failed", slog.Any("error", err))
			}
			m.stopRetiredPipelines()
		})
	}
	if len(m.pending) == 0 {
		m.requestRetirementStop()
	}
}

func (m *Module) requestRetirementStop() {
	if m.retirementStopRequested {
		return
	}
	m.retirementStopRequested = true
	if m.opts.RequestRetirementStop != nil {
		m.opts.RequestRetirementStop()
	}
}

// IdentityRetired is read only after xhive.Run has completed shutdown.
func (m *Module) IdentityRetired() bool { return m.identityRetired }
