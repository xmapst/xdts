package iproto

import (
	"errors"
	"fmt"
	"maps"
	"strconv"
	"uuid"
)

const (
	HeaderProtoVersion  = "x-proto-ver"
	HeaderCenter        = "x-center"
	HeaderNodeID        = "x-node-id"
	HeaderSign          = "x-sign"
	HeaderReplyKey      = "x-reply-key"
	HeaderPipelineID    = "x-pipeline-id"
	HeaderConfigVersion = "x-config-version"
	HeaderRunID         = "x-run-id"
	HeaderEpoch         = "x-epoch"
	HeaderBatchSeq      = "x-batch-seq"
	HeaderChunk         = "x-chunk"
	HeaderOrigin        = "x-origin"
	HeaderHops          = "x-hops"

	// TimestampMS 参与毫秒级签名，必须通过 x-ts-ms header 无损传输。
	// AMQP 原生 timestamp 只有秒级精度，不能用于恢复签名时间戳；缺少 header 时拒绝消息。
	HeaderTimestampMS = "x-ts-ms"
)

// AMQP properties 中协议关心的取值常量。
const (
	ContentTypeProtobuf = "application/x-protobuf"

	ContentEncodingZstd     = "zstd"
	ContentEncodingIdentity = "identity"
)

// Properties 对应 AMQP 消息属性中协议关心的子集，字段命名与含义遵循 AMQP 0-9-1
// basic.properties，由 internal/transport 映射到具体客户端库的 Publishing/Delivery 类型。
// 协议演进：只增字段，不复用/更改已有字段语义（演进规则同样适用于本包
// 手写的信封结构，不止 protobuf message）。
type Properties struct {
	MessageID     string
	CorrelationID string
	// 取值见 message_types.go（如 "register.request"）。
	Type        string
	TimestampMS int64 // Unix 毫秒；签名计算与 鉴权时间窗均以此为准
	// 控制类消息的 TTL（AMQP expiration，单位毫秒）；0 表示不设置。数据类消息不设 TTL
	// （xdts.data 队列"不设 TTL / 长度上限"）。
	ExpirationMS    int64
	ContentType     string // 恒为 ContentTypeProtobuf
	ContentEncoding string // ContentEncodingZstd 或 ContentEncodingIdentity
}

// Envelope 是一条 xdts 协议消息的信封：properties + headers + 消息体字节。
// Body 是已完成 Protobuf 序列化（可能已按 ContentEncoding 压缩）的最终 wire bytes；
// 本包不在 Envelope 内重复解释 Body 的业务含义，调用方按 Properties.Type 反序列化
// （Decode 需要调用方提供目标消息类型的指针）。
type Envelope struct {
	Properties Properties
	Headers    map[string]any
	Body       []byte
}

// NewEnvelopeParams 提供信封字段和额外 header。HeaderExtra 使用 Header* 常量。
// Header 值必须通过 ValidateHeaders 校验；uint64 使用 HeaderUint64String 编码，
// 避免 AMQP field table 拒绝该类型或在整数转换时截断。
type NewEnvelopeParams struct {
	MessageID       string
	CorrelationID   string
	Type            string
	TimestampMS     int64
	ExpirationMS    int64
	ProtocolVersion uint32
	// Center/NodeID 为空时不写入对应 header（如 Manager 发出的消息没有 x-node-id）。
	Center          string
	NodeID          string
	ContentEncoding string // 为空时默认 ContentEncodingIdentity
	HeaderExtra     map[string]any
}

// NewEnvelope 按 组装信封：公共 header（x-proto-ver、以及非空时的 x-center/x-node-id）
// 加上调用方通过 HeaderExtra 传入的、该消息类型特有的字段。
//
// 自动生成只保证这条 MessageID 非空、且不与同一时刻构造的其它信封相撞，不携带任何业务
// 语义（不是幂等键，不代表消息内容或业务实体的身份），调用之间也不保证稳定——每次调用
// NewEnvelope 都会生成新值。调用方如果需要 MessageID 参与业务判重或承载其它业务标识
// （例如同一逻辑请求的多次重试要复用同一个可识别的值），必须显式传入 p.MessageID，不能
// 依赖这里的自动生成；一个已知的对照例子是 internal/transport.RPCClient.Call，它按
// attempt 派生 MessageID 并要求签名与之同步重算，不经过这里的空值兜底路径。
func NewEnvelope(body []byte, p NewEnvelopeParams) *Envelope {
	messageID := p.MessageID
	if messageID == "" {
		messageID = uuid.NewV7().String()
	}
	encoding := p.ContentEncoding
	if encoding == "" {
		encoding = ContentEncodingIdentity
	}
	headers := make(map[string]any, len(p.HeaderExtra)+4)
	headers[HeaderProtoVersion] = p.ProtocolVersion
	// 恒写入，不像 Center/NodeID 那样按空值判断是否携带：TimestampMS 是
	// int64 零值即合法时间戳（1970-01-01），没有"空值代表未设置"这种
	// 语义可用来判断要不要写 header，且这个 header 本来就要求接收方恒定
	// 存在（见 HeaderTimestampMS 的说明），写入端理应恒定提供。
	headers[HeaderTimestampMS] = p.TimestampMS
	if p.Center != "" {
		headers[HeaderCenter] = p.Center
	}
	if p.NodeID != "" {
		headers[HeaderNodeID] = p.NodeID
	}
	maps.Copy(headers, p.HeaderExtra)
	return &Envelope{
		Properties: Properties{
			MessageID:       messageID,
			CorrelationID:   p.CorrelationID,
			Type:            p.Type,
			TimestampMS:     p.TimestampMS,
			ExpirationMS:    p.ExpirationMS,
			ContentType:     ContentTypeProtobuf,
			ContentEncoding: encoding,
		},
		Headers: headers,
		Body:    body,
	}
}

// WithSignature 设置 x-sign header（MQ-10）。签名对象是 Body 与 Properties.TimestampMS
// 确定之后的 SignParams（见 internal/iproto/sign.go），本方法只负责把结果塞进 headers。返回 e 本身，
// 便于链式调用（NewEnvelope(...).WithSignature(sig)）。
func (e *Envelope) WithSignature(sig string) *Envelope {
	if e.Headers == nil {
		e.Headers = make(map[string]any, 1)
	}
	e.Headers[HeaderSign] = sig
	return e
}

// SignParamsFor 从信封当前状态构造 SignParams，避免调用方在 Sign 调用点重复拼接
// MessageID/TimestampMS/Body 三个字段、可能因为改了其中一处忘了改另一处而产生签名不一致。
func (e *Envelope) SignParamsFor() SignParams {
	return SignParams{
		MessageID:   e.Properties.MessageID,
		TimestampMS: e.Properties.TimestampMS,
		Body:        e.Body,
	}
}

// ParseEnvelope 校验消息类型、明确的 protobuf content_type、精确协议版本和毫秒时间戳。
// 签名由 VerifyEnvelope 验证，业务消息由 Decode 解码。
func ParseEnvelope(props Properties, headers map[string]any, body []byte) (*Envelope, error) {
	if props.Type == "" {
		return nil, fmt.Errorf("%w: %w", ErrEnvelopeMissingType, ErrMalformedMessage)
	}
	if props.ContentType != ContentTypeProtobuf {
		return nil, fmt.Errorf("%w: %s: %w", ErrEnvelopeUnsupportedContentType, props.ContentType, ErrMalformedMessage)
	}
	if err := ValidateProtocolVersion(headers); err != nil {
		return nil, fmt.Errorf("%w: %w", err, ErrMalformedMessage)
	}
	ts, ok := TimestampMSFromHeaders(headers)
	if !ok {
		return nil, fmt.Errorf("%w: %s header, type=%s（AMQP 原生 timestamp property 只有秒级精度，不作为回退来源）: %w",
			ErrEnvelopeMissingTimestamp, HeaderTimestampMS, props.Type, ErrMalformedMessage)
	}
	props.TimestampMS = ts
	return &Envelope{Properties: props, Headers: headers, Body: body}, nil
}

var (
	ErrEnvelopeMissingType            = errors.New("iproto: 信封缺少 type 属性")
	ErrEnvelopeUnsupportedContentType = errors.New("iproto: 不支持的 content_type")
	ErrEnvelopeMissingTimestamp       = errors.New("iproto: 信封缺少或时间戳 header 值不合法，无法确定可信时间戳")
)

// ErrMalformedMessage 标记无法重试恢复的信封或编码错误。
// ParseEnvelope 的缺失或错误版本也归入该类，保留具体错误供调用方识别。
// 签名和鉴权时间窗失败使用独立哨兵。
var ErrMalformedMessage = errors.New("iproto: 消息结构性错误（永久性，不应重试）")

// HeaderString 读取 AMQP 文本 header，接受 string 和 []byte；其它类型拒绝转换。
func (e *Envelope) HeaderString(key string) (string, bool) {
	v, ok := e.Headers[key]
	if !ok {
		return "", false
	}
	switch t := v.(type) {
	case string:
		return t, true
	case []byte:
		return string(t), true
	default:
		return "", false
	}
}

// HeaderUint64 读取一个数值型 header（如 x-run-id、x-epoch、x-batch-seq、x-proto-ver、
// x-hops）。不同 AMQP 客户端库/broker 对整数 header 的实际 Go 类型不一致（int32/int64/
// uint64 等常见），因此逐类型适配而不是断言单一类型；负数视为不合法（这些字段语义上都是
// 非负计数或版本号），返回 ok=false 而不是静默截断，呼应 "边界输入必须校验"。
func (e *Envelope) HeaderUint64(key string) (uint64, bool) {
	return headerUint64(e.Headers, key)
}

func headerUint64(headers map[string]any, key string) (uint64, bool) {
	v, ok := headers[key]
	if !ok {
		return 0, false
	}
	switch t := v.(type) {
	case uint64:
		return t, true
	case uint32:
		return uint64(t), true
	case uint16:
		return uint64(t), true
	case uint8:
		return uint64(t), true
	case int64:
		if t < 0 {
			return 0, false
		}
		return uint64(t), true
	case int32:
		if t < 0 {
			return 0, false
		}
		return uint64(t), true
	case int:
		if t < 0 {
			return 0, false
		}
		return uint64(t), true
	default:
		return 0, false
	}
}

func TimestampMSFromHeaders(headers map[string]any) (int64, bool) {
	ts, ok := headerUint64(headers, HeaderTimestampMS)
	if !ok {
		return 0, false
	}
	return int64(ts), true
}

// ValidateHeaderValue 判断 v 是否是可以安全放进 Envelope.Headers 的取值：既要能被
// amqp091-go 的 field table 编码接受（否则整条发布直接失败），也要能在跨 broker
// 往返之后保持类型和值都不变——"能发出去"与"发出去且保真"是两条独立的要求（HeaderTimestampMS
// 的说明记录过同一类问题：time.Time 就是"能发但不保真"的例子）。
//
// 调用方需要携带 uint64 语义的取值（如 protobuf 的 RunId/Epoch/BatchSeq/ChunkNo 这类
// 字段）时，改用 HeaderUint64String 编码成十进制字符串再放进 header，不要绕过本函数
// 直接塞 uint64。
func ValidateHeaderValue(v any) error {
	switch v.(type) {
	case bool, int8, int16, int32, int64, uint16, uint32, uint8, float32, float64, string, []byte:
		return nil
	default:
		// 不支持的 header 类型在发布前拒绝，避免网络编码时失真或失败。
		return fmt.Errorf("iproto: header 取值类型 %T 不在 AMQP field table 安全白名单内"+
			"（不支持编码，或编码后无法保真往返；完整名单与理由见 ValidateHeaderValue 文档）: %w",
			v, ErrUnsupportedHeaderValueType)
	}
}

var ErrUnsupportedHeaderValueType = errors.New("iproto: header 取值类型不在安全白名单内")

// ValidateHeaders 对 headers 里的每一个取值调用 ValidateHeaderValue，错误信息附带具体的
// header 键名，方便定位是哪一个字段用错了类型。nil/空 map 视为合法。
//
// internal/transport.Publisher.PublishEnvelope 在真正把信封交给 AMQP 客户端库之前会调用
// 本函数：类型不安全的 header 会在本地、发布调用的最开始就报错（不产生任何网络 I/O、
// 不注册任何待确认状态），不会等到 amqp091-go 深处的 writeField 才失败——那种失败发生
// 得太晚，错误信息也不指向具体是哪个字段、哪种类型出的问题。
func ValidateHeaders(headers map[string]any) error {
	for k, v := range headers {
		if err := ValidateHeaderValue(v); err != nil {
			return fmt.Errorf("iproto: header %q 校验失败: %w", k, err)
		}
	}
	return nil
}

// HeaderUint64String 把一个 uint64 值编码成十进制字符串，用于放进 Envelope.Headers——
// uint64 本身不在 ValidateHeaderValue 的安全白名单内（amqp091-go 直接拒绝编码该类型，
// 见该函数文档），十进制字符串精度无损、类型也在安全白名单内（string），是替代表达。
// protobuf 里 RunId/Epoch/BatchSeq/ChunkNo 等 uint64 字段需要放进 header 时，统一用本
// 函数编码。
func HeaderUint64String(v uint64) string {
	return strconv.FormatUint(v, 10)
}

// HeaderUint64FromString 是 HeaderUint64String 的反向解析：读取一个用十进制字符串编码
// 的 uint64 header。与 headerUint64（处理原生数值类型的数值型 header，如 x-proto-ver）
// 是两条独立的读取路径，不合并成一个函数——字符串编码的 header 在 headers map 里的 Go
// 类型就是 string，不会落进 headerUint64 的数值类型分支，调用方按 header 的实际编码方式
// 选择调用哪一个。
func HeaderUint64FromString(headers map[string]any, key string) (uint64, bool) {
	v, ok := headers[key]
	if !ok {
		return 0, false
	}
	s, ok := v.(string)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}
