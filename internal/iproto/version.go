package iproto

import "fmt"

// ProtocolVersion 是当前构建使用的协议版本。消息两端必须使用同一版本；协议变更
// 通过更新该值和所有生产/消费方一起发布。
const ProtocolVersion uint32 = 0x00010031

// CurrentProtocolVersion 返回当前构建写入消息信封和控制消息的协议版本。
func CurrentProtocolVersion() uint32 { return ProtocolVersion }

// ValidateProtocolVersion 校验所有消息面的信封版本，包括不验证对端签名的数据面。
func ValidateProtocolVersion(headers map[string]any) error {
	v, ok := headerUint64(headers, HeaderProtoVersion)
	if !ok {
		return ErrEnvelopeMissingProtocolVersion
	}
	if v != uint64(CurrentProtocolVersion()) {
		return fmt.Errorf("%w: peer=%d local=%d", ErrProtocolVersionMismatch, v, CurrentProtocolVersion())
	}
	return nil
}
