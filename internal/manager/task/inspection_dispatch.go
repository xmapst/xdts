package task

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/transport"
)

// ErrProbeResultUnverified 标记"RPC 往返本身成功、但回执验签没通过"这一类
// 失败——调用方（internal/manager/task/inspection.go 的 inspectCenter）必须能把它从"请求没走完
// 全程"（往返失败，落 DISPATCH_FAILED）里单独识别出来并区别对待，进一步按
// 具体是 iproto.ErrSignatureInvalid 还是 iproto.ErrTimestampOutOfWindow
// 分流出 SIGNATURE_INVALID / TIMESTAMP_OUT_OF_WINDOW 两个独立的落库 code
// （见 inspectCenter）。
//
// 导出是因为 internal/manager/task/inspection.go 需要在另一个文件里 errors.Is 判断。
var ErrProbeResultUnverified = errors.New("task: 探测回执验签未通过")

// logProbeReplyVerifyFailure records authentication failures without raw reply data.
func logProbeReplyVerifyFailure(ctx context.Context, nodeID, taskID, messageID, msgType string, err error) {
	fields := []slog.Attr{
		slog.String("node_id", nodeID),
		slog.String("task_id", taskID),
		slog.String("message_id", messageID),
		slog.String("type", msgType),
	}
	switch {
	case errors.Is(err, iproto.ErrSignatureInvalid):
		slog.LogAttrs(ctx, slog.LevelError, "probe result signature verification failed", fields...)
	case errors.Is(err, iproto.ErrTimestampOutOfWindow):
		slog.LogAttrs(ctx, slog.LevelWarn, "probe result timestamp outside allowed window", fields...)
	default:
		slog.LogAttrs(ctx, slog.LevelError, "probe result verify precondition failed", fields...)
	}
}

// Broker probes use bounded per-attempt deadlines and a finite retry count.
const (
	DispatchBrokerProbePerAttemptTimeout = 25 * time.Second
	DispatchBrokerProbeMaxAttempts       = 2
)

// RPCProbeDispatcher 用 *transport.RPCClient 实现 IProbeDispatcher（见
// internal/manager/task/inspection.go），是巡检循环唯一的生产实现。
//
// 签名必须在 transport.WithEnvelopeDecorator 的回调里完成，不能在调用
// Call 之前——Call 每次尝试都会往 Properties.MessageID 追加 ".attemptN"
// 后缀，而 MessageID 参与 HMAC 计算（iproto.SignParams），提前签名会导致
// 签名覆盖的 MessageID 与实际发布的物理消息不一致，验签方必然拒绝，
// 见 internal/transport/rpc.go 文件头文档第 5 点——这正是本包接入巡检
// 节点时发现、报告给 transport 并已由其修复的缺陷，不能在这里重犯。
type RPCProbeDispatcher struct {
	rpc *transport.RPCClient
}

var _ IProbeDispatcher = (*RPCProbeDispatcher)(nil)

// NewRPCProbeDispatcher 包装一个已经建好的 *transport.RPCClient。rpc 必须
// 非 nil——调用方（cmd/manager/main.go）只应该在 AMQP 接线成功之后才
// 构造本类型并通过 InspectionLoop.SetDispatcher 接入。
func NewRPCProbeDispatcher(rpc *transport.RPCClient) *RPCProbeDispatcher {
	return &RPCProbeDispatcher{rpc: rpc}
}

// DispatchBrokerProbe 实现 IProbeDispatcher：组装 ProbeRequest（编码 →
// 签名，写法与 internal/node/probe/publish.go 的 publishResult 一致，
// 只是签名时机改成了每次 RPC 重试尝试都重新签一遍，见上面类型文档）→
// 通过 RPCClient.Call 发到 node 的控制队列 → 校验并解码回执。
//
// Manager 发出的信封不写 x-center/x-node-id 两个 header（NewEnvelopeParams
// 的既有文档："Manager 发出的消息没有 x-node-id"），Center/NodeID 两个
// 参数留空。
func (d *RPCProbeDispatcher) DispatchBrokerProbe(ctx context.Context, center model.Center, node model.Node, taskID string) (*iproto.ProbeResult, error) {
	req := &iproto.ProbeRequest{
		TaskId:     taskID,
		Type:       iproto.ProbeType_PROBE_TYPE_BROKER,
		CenterCode: center.Code,
		BrokerMgmt: &iproto.ManagementAPIConn{
			Url:      center.MQMgmtURL,
			Username: center.MQMgmtUser,
			Password: center.MQMgmtPassword,
		},
	}
	return d.callProbe(ctx, req, node, taskID)
}

// callProbe 是两种探测共用的"编码 → 签名发出 → 验签解码回执"往返。
func (d *RPCProbeDispatcher) callProbe(ctx context.Context, req *iproto.ProbeRequest, node model.Node, taskID string) (*iproto.ProbeResult, error) {
	// 控制面消息体积很小，不压缩——与 publishResult 的既有选择一致。
	body, contentEncoding, err := iproto.Encode(req, iproto.EncodeOptions{Compress: false})
	if err != nil {
		return nil, fmt.Errorf("task: 序列化 ProbeRequest 失败: %w", err)
	}

	// Use the task ID for correlation so retries remain one logical probe.
	env := iproto.NewEnvelope(body, iproto.NewEnvelopeParams{
		MessageID:       taskID,
		CorrelationID:   taskID,
		Type:            iproto.MessageTypeProbeRequest,
		TimestampMS:     time.Now().UnixMilli(),
		ProtocolVersion: iproto.CurrentProtocolVersion(),
		ContentEncoding: contentEncoding,
	})
	if req.GetType() == iproto.ProbeType_PROBE_TYPE_POSITION {
		env.Properties.ExpirationMS = 70000
	}
	if req.GetType() == iproto.ProbeType_PROBE_TYPE_LINK {
		env.Properties.ExpirationMS = 40000
	}
	if req.GetType() == iproto.ProbeType_PROBE_TYPE_DATA_CHECK {
		env.Properties.ExpirationMS = 130000
	}

	signKey := iproto.DeriveSignKeyFromTokenHash(node.TokenHash)
	callOptions := []transport.CallOption{transport.WithEnvelopeDecorator(func(env *iproto.Envelope) error {
		env.WithSignature(iproto.Sign(signKey, env.SignParamsFor()))
		return nil
	})}
	if req.GetType() == iproto.ProbeType_PROBE_TYPE_POSITION {
		// 60s scan + 5s reply + transport margin; no automatic duplicate dump.
		callOptions = append(callOptions, transport.WithCallRetryPolicy(70*time.Second, 1))
	}
	if req.GetType() == iproto.ProbeType_PROBE_TYPE_LINK {
		callOptions = append(callOptions, transport.WithCallRetryPolicy(40*time.Second, 1))
	}
	if req.GetType() == iproto.ProbeType_PROBE_TYPE_DATA_CHECK {
		callOptions = append(callOptions, transport.WithCallRetryPolicy(130*time.Second, 1))
	}
	msg, err := d.rpc.Call(ctx, transport.ExchangeControl, transport.RoutingKeyNode(node.ID.String()), env, callOptions...)
	if err != nil {
		// 文案带上探测类型：本函数抽出来之后由 broker 与 conn_test 共用，
		// 写死 "broker" 会让端点探测的失败在日志里冒充成 broker 失败。
		return nil, fmt.Errorf("task: 派发 %s 探测请求失败: %w", req.GetType(), err)
	}

	var result iproto.ProbeResult
	if _, err := transport.ParseAndDecode(msg, &result, &transport.VerifyOptions{
		Key:    signKey,
		Now:    time.Now(),
		Window: iproto.DefaultSignatureWindow,
	}); err != nil {
		logProbeReplyVerifyFailure(ctx, node.ID.String(), taskID, msg.Properties.MessageID, msg.Properties.Type, err)
		return nil, fmt.Errorf("%w: %w", ErrProbeResultUnverified, err)
	}
	if msg.Properties.Type != iproto.MessageTypeProbeResult || result.GetTaskId() != taskID || result.GetType() != req.GetType() {
		return nil, fmt.Errorf("%w: probe reply identity mismatch", ErrProbeResultUnverified)
	}
	return &result, nil
}

// DispatchConnTest 见 IProbeDispatcher 的方法文档（含默认关闸的理由）。
//
// EndpointConn 只填 probeConnTest 真正会读的四个字段（host/port/username/
// password），**不复用 pipelinespec.buildEndpointConn**：那个函数还会解析
// TLS 与 params，而节点侧 `internal/node/probe/conntest.go` 拼 DSN 时明确不接这两者
// （见该函数注释"DSN 故意不接 TLS/params……是全仓共享的已知边界"）。
// 填了对端不读的字段，会让读代码的人以为这条链路支持 TLS。
func (d *RPCProbeDispatcher) DispatchConnTest(ctx context.Context, endpoint model.Endpoint, node model.Node, taskID string) (*iproto.ProbeResult, error) {
	req := &iproto.ProbeRequest{
		TaskId:   taskID,
		Type:     iproto.ProbeType_PROBE_TYPE_CONN_TEST,
		Endpoint: probeEndpointConn(endpoint),
	}
	return d.callProbe(ctx, req, node, taskID)
}

func (d *RPCProbeDispatcher) DispatchCatalog(ctx context.Context, endpoint model.Endpoint, node model.Node, taskID string) (*iproto.ProbeResult, error) {
	req := &iproto.ProbeRequest{TaskId: taskID, Type: iproto.ProbeType_PROBE_TYPE_CATALOG,
		Endpoint: probeEndpointConn(endpoint)}
	return d.callProbe(ctx, req, node, taskID)
}

func probeEndpointConn(endpoint model.Endpoint) *iproto.EndpointConn {
	return &iproto.EndpointConn{
		EndpointId: endpoint.ID.String(), Host: endpoint.Host, Port: uint32(endpoint.Port),
		Username: endpoint.Username, Password: endpoint.Password,
	}
}
