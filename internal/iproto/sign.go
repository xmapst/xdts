package iproto

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// DefaultSignatureWindow 是需求 MQ-10 要求的默认时间窗（±5 分钟），可通过 Verify 的
// window 参数覆盖（"默认 ±5 分钟，可配"）。
const DefaultSignatureWindow = 5 * time.Minute

// ErrSignatureInvalid/ErrTimestampOutOfWindow 是签名校验失败的两类原因，调用方用
// errors.Is 区分（如时间窗失败可能是时钟漂移，签名失败可能是 token 泄露/被吊销）。
// 两个哨兵错误的 Error() 文本均不包含 token、SignKey 或签名明文。
var (
	ErrSignatureInvalid     = errors.New("iproto: 签名校验失败")
	ErrTimestampOutOfWindow = errors.New("iproto: 时间戳超出允许窗口")
)

// The signing key is derived from SHA256(token), with a domain separator.
// Nodes derive it from their token; Manager derives it from the persisted token
// hash. Both use this package's derivation functions. Hashes and derived keys
// remain authentication material and must never be logged.

// signKeyDomain 是签名密钥派生的域分隔前缀，见上方大段说明。
const signKeyDomain = "xdts-node-sign-v1"

// SignKey 是参与 HMAC 计算的派生密钥，不是 node_token 本身。单独定义类型是
// 为了在类型层面区分"已经派生好的密钥"与"明文 token"，让 Sign/Verify 的签名
// 从根上排除"调用方误把明文 token 传进来"这种可能——明文 token 从来不在
// Manager 侧持久化（见 internal/manager/repo/node.go），两者语义完全不同，不能互换。
//
// 取到 SignKey 只有两条路：DeriveSignKeyFromToken（节点侧，从明文 token 出发）
// 与 DeriveSignKeyFromTokenHash（Manager 侧，从库里的 token_hash 出发）。
// 和 token 一样，SignKey 不得写日志——持有它就足以伪造签名，见上方的安全性质
// 澄清。
type SignKey [sha256.Size]byte

func DeriveSignKeyFromToken(plainToken string) SignKey {
	sum := sha256.Sum256([]byte(plainToken))
	return DeriveSignKeyFromTokenHash(hex.EncodeToString(sum[:]))
}

// DeriveSignKeyFromTokenHash 使用持久化的 token_hash 派生签名密钥。
// Manager 不需要保存或恢复明文 token。
func DeriveSignKeyFromTokenHash(tokenHashHex string) SignKey {
	h := sha256.New()
	h.Write([]byte(signKeyDomain))
	h.Write([]byte(tokenHashHex))
	var key SignKey
	copy(key[:], h.Sum(nil))
	return key
}

// SignParams 是参与签名计算的字段：MQ-10 要求 HMAC(签名密钥, message_id + timestamp +
// body_hash)。Body 必须是"最终 wire bytes"（即已确定 content_encoding 之后、实际发布到
// AMQP 的那份字节），而不是压缩前的 protobuf 原始字节——否则同一逻辑内容在选择压缩与否时
// 会产生不同签名对象，容易在联调时踩到"到底签的是哪一份"的分歧。
type SignParams struct {
	MessageID   string
	TimestampMS int64 // Unix 毫秒
	Body        []byte
}

// bodyHash 对消息体做 SHA-256，签名覆盖的是摘要而不是原始 body：一是控制 HMAC 输入长度，
// 二是让"body_hash"作为独立可核对的值出现在签名参数里，便于排查签名不一致时先核对
// body_hash 是否一致，再判断是不是签名计算本身出了问题。
func bodyHash(body []byte) [sha256.Size]byte {
	return sha256.Sum256(body)
}

func Sign(key SignKey, p SignParams) string {
	h := bodyHash(p.Body)
	mac := hmac.New(sha256.New, key[:])
	mac.Write([]byte(p.MessageID))
	_, _ = fmt.Fprintf(mac, "%d", p.TimestampMS)
	mac.Write(h[:])
	return hex.EncodeToString(mac.Sum(nil))
}

// key 与 Sign 一样是派生后的签名密钥，不是明文 node_token。
func Verify(key SignKey, p SignParams, sig string, now time.Time, window time.Duration) error {
	if window <= 0 {
		window = DefaultSignatureWindow
	}
	ts := time.UnixMilli(p.TimestampMS)
	if ts.Before(now.Add(-window)) || ts.After(now.Add(window)) {
		return fmt.Errorf("%w: message_id=%s", ErrTimestampOutOfWindow, p.MessageID)
	}
	expected := Sign(key, p)

	if !hmac.Equal([]byte(expected), []byte(sig)) {
		return fmt.Errorf("%w: message_id=%s", ErrSignatureInvalid, p.MessageID)
	}
	return nil
}

// 本函数不解析/校验 x-node-id，也不做 SignKey 的查库/派生——节点身份的
// 表示方式（如 Manager 侧的 uuidx.UUID）是调用方的领域概念，不是 iproto 的协议层
// 概念；key 由调用方解析好后传入（与 Verify 本身"key 由调用方提供"的参数约定
// 一致）。调用方仍需要自己先按 x-node-id（或其它身份来源）查到/派生出 key，本函数
// 只负责"信封结构+签名"这一段。
func VerifyEnvelope(env *Envelope, key SignKey, now time.Time, window time.Duration) error {
	if err := ValidateProtocolVersion(env.Headers); err != nil {
		return fmt.Errorf("%w: message_id=%s", err, env.Properties.MessageID)
	}

	sig, ok := env.HeaderString(HeaderSign)
	if !ok || sig == "" {
		return fmt.Errorf("%w: message_id=%s", ErrEnvelopeMissingSignature, env.Properties.MessageID)
	}

	return Verify(key, env.SignParamsFor(), sig, now, window)
}

// ErrEnvelopeMissingSignature、ErrEnvelopeMissingProtocolVersion 和
// ErrProtocolVersionMismatch 是信封结构校验的哨兵错误。
var (
	ErrEnvelopeMissingSignature       = errors.New("iproto: 信封缺少 x-sign header")
	ErrEnvelopeMissingProtocolVersion = errors.New("iproto: 信封缺少 x-proto-ver header")
	ErrProtocolVersionMismatch        = errors.New("iproto: 协议版本不匹配")
)
