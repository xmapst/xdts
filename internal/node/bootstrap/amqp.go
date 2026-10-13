package bootstrap

import (
	"context"
	"errors"
	"time"
	"uuid"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/transport"
)

type AMQPRegistrar struct{ conn *transport.Conn }

func NewAMQPRegistrar(conn *transport.Conn) *AMQPRegistrar { return &AMQPRegistrar{conn: conn} }

func (r *AMQPRegistrar) Register(ctx context.Context, request *iproto.RegisterRequest) (*iproto.RegisterReply, error) {
	declarator, err := transport.NewDeclarator(r.conn)
	if err != nil {
		return nil, err
	}
	err = declarator.DeclareExchange(transport.RegisterExchangeOptions())
	if err == nil {
		err = declarator.DeclareExchange(transport.ReplyExchangeOptions())
	}
	declarator.Close()
	if err != nil {
		return nil, err
	}
	client, err := transport.NewRPCClient(r.conn, uuid.NewV7().String())
	if err != nil {
		return nil, err
	}
	defer client.CloseWithTimeout(5 * time.Second)
	body, encoding, err := iproto.Encode(request, iproto.EncodeOptions{})
	if err != nil {
		return nil, err
	}
	msg, err := client.Call(ctx, transport.ExchangeRegister, transport.RoutingKeyRegister(), iproto.NewEnvelope(body, iproto.NewEnvelopeParams{Type: iproto.MessageTypeRegisterRequest, TimestampMS: time.Now().UnixMilli(), ProtocolVersion: iproto.CurrentProtocolVersion(), ContentEncoding: encoding}))
	if err != nil {
		return nil, err
	}
	if msg.Properties.Type != iproto.MessageTypeRegisterReply {
		return nil, errors.New("bootstrap: unexpected registration reply type")
	}
	var reply iproto.RegisterReply
	if err := iproto.Decode(msg.Body, msg.Properties.ContentEncoding, &reply); err != nil {
		return nil, errors.New("bootstrap: malformed registration reply")
	}
	return &reply, nil
}
