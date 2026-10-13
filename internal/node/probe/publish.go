package probe

import (
	"context"
	"fmt"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// correlationID 取自请求信封的 Properties.CorrelationID，必须原样回填（internal/transport/rpc.go
// 同一约定：Manager 的 RPCClient 按 correlation_id 而不是 task_id 把回复路由给等待中
// 的调用方），不能重新生成。
func (m *Module) publishResult(ctx context.Context, replyKey, correlationID string, result *iproto.ProbeResult) error {
	body, encoding, err := iproto.Encode(result, iproto.EncodeOptions{Compress: false})
	if err != nil {
		return fmt.Errorf("probe: 编码回执失败: %w", err)
	}

	env := iproto.NewEnvelope(body, iproto.NewEnvelopeParams{
		MessageID:       uuidx.New().String(),
		CorrelationID:   correlationID,
		Type:            iproto.MessageTypeProbeResult,
		TimestampMS:     m.now().UnixMilli(),
		ProtocolVersion: m.opts.ProtocolVersion,
		Center:          m.opts.Center,
		NodeID:          m.opts.NodeID,
		ContentEncoding: encoding,
	})
	// signKey 是 New() 构造期已经派生好的缓存值，不在每次回执时重新计算（与
	// reporter/logship 同一份理由：token 在模块生命周期内是常量）。
	sig := iproto.Sign(m.signKey, env.SignParamsFor())
	env.WithSignature(sig)

	pubCtx, cancel := context.WithTimeout(ctx, m.opts.PublishTimeout)
	defer cancel()
	outcome, err := m.opts.Publisher.PublishEnvelope(pubCtx, transport.ExchangeReply, replyKey, env)
	if err != nil {
		return fmt.Errorf("probe: 发布回执失败: %w", err)
	}
	if outcome != transport.OutcomeConfirmed {
		return fmt.Errorf("probe: 回执未被判定为已投递，outcome=%s", outcome)
	}
	return nil
}
