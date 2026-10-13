package dispatch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// onNodeHelloReq accepts retired authentication material exclusively on hello.
// Verification precedes every reply; a caller cannot ask Manager to sign a
// retirement response by merely naming a node ID.
func (d *Dispatcher) onNodeHelloReq(ctx context.Context, env *iproto.Envelope) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var request iproto.NodeHello
	if err := iproto.Decode(env.Body, env.Properties.ContentEncoding, &request); err != nil {
		return errors.New("dispatch: invalid node hello")
	}
	rawID, _ := env.HeaderString(iproto.HeaderNodeID)
	id, err := uuidx.Parse(rawID)
	if err != nil || request.NodeId != rawID {
		return errors.New("dispatch: node hello identity mismatch")
	}
	var node model.Node
	if err := d.db.WithContext(ctx).Unscoped().First(&node, "id=?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return repo.ErrNodeNotFound
		}
		return err
	}
	key := iproto.DeriveSignKeyFromTokenHash(node.TokenHash)
	retired := node.Status == model.NodeStatusRevoked || node.DeletedAt.Valid
	if retired {
		credential, err := repo.GetNodeRetiredCredential(ctx, d.db, id)
		if err != nil {
			return err
		}
		key = iproto.DeriveSignKeyFromTokenHash(credential.TokenHash)
	}
	if err := iproto.VerifyEnvelope(env, key, d.now(), iproto.DefaultSignatureWindow); err != nil {
		return err
	}
	center, err := repo.GetCenter(ctx, d.db, node.CenterID)
	if err != nil {
		return err
	}
	claimedCenter, _ := env.HeaderString(iproto.HeaderCenter)
	if claimedCenter != center.Code {
		return errors.New("dispatch: node hello center mismatch")
	}
	if !retired && request.ProtocolVersion == iproto.CurrentProtocolVersion() {
		if err := repo.SetNodeCapabilities(ctx, d.db, id, request.GetCapabilities()); err != nil {
			return err
		}
	}
	reply := &iproto.NodeHelloReply{ProtocolVersion: iproto.CurrentProtocolVersion(), CenterCode: center.Code}
	switch {
	case retired:
		reply.Error = &iproto.Error{Code: "NODE_IDENTITY_RETIRED", Message: "节点身份已退役，请用新注册码与新状态目录重新注册"}
	case request.ProtocolVersion != iproto.CurrentProtocolVersion():
		reply.Error = &iproto.Error{Code: "PROTOCOL_INCOMPATIBLE", Message: "节点协议版本与 Manager 不匹配"}
	}
	body, encoding, err := iproto.Encode(reply, iproto.EncodeOptions{})
	if err != nil {
		return err
	}
	envelope := iproto.NewEnvelope(body, iproto.NewEnvelopeParams{MessageID: uuidx.New().String(),
		CorrelationID: env.Properties.CorrelationID, Type: iproto.MessageTypeNodeHelloReply,
		TimestampMS: d.now().UnixMilli(), ProtocolVersion: iproto.CurrentProtocolVersion(), ContentEncoding: encoding})
	envelope.WithSignature(iproto.Sign(key, envelope.SignParamsFor()))
	replyKey, ok := transport.ReplyRoutingKeyFor(env)
	if !ok {
		return errors.New("dispatch: node hello has no reply destination")
	}
	publisher := d.publisher()
	if publisher == nil {
		return errors.New("dispatch: hello publisher unavailable")
	}
	outcome, err := publisher.PublishEnvelope(ctx, transport.ExchangeReply, replyKey, envelope)
	if err != nil {
		return err
	}
	if outcome != transport.OutcomeConfirmed {
		return fmt.Errorf("dispatch: hello reply was not delivered: %s", outcome)
	}
	return nil
}
