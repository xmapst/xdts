package probe

import (
	"errors"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/transport"
)

// ModuleName 是 probe 模块在 xhive 里注册的名字（静态模块，Priority 30）。
const ModuleName = "probe"

// Ack 的类型是 *transport.AckHandle（导出的具体类型）而不是本包内部的 iAckHandle
// （未导出接口）：本结构体要跨包传递给 controller 构造，controller 拿不到 iAckHandle
// 这个类型名；*transport.AckHandle 天然实现 iAckHandle（见 internal/node/probe/interfaces.go），跨过
// ChanRPC 边界后 onEnvelopeMsg 按 iAckHandle 使用不需要额外转换。
type EnvelopeMsg struct {
	Envelope *iproto.Envelope
	Ack      *transport.AckHandle
}

func (m EnvelopeMsg) validate() error {
	if m.Envelope == nil {
		return errors.New("probe: EnvelopeMsg.Envelope 不能为空")
	}
	if m.Ack == nil {
		return errors.New("probe: EnvelopeMsg.Ack 不能为空")
	}
	return nil
}
