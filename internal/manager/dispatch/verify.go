package dispatch

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// TokenKeyFunc 按节点 ID 解析用于 HMAC 验签的 iproto.SignKey——与
// internal/manager/ingest.TokenKeyFunc 同形状但各自独立定义，见 internal/manager/dispatch/dispatch.go
// Dispatcher.tokenKey 字段的注释。
type TokenKeyFunc func(ctx context.Context, nodeID uuidx.UUID) (iproto.SignKey, error)

// defaultTokenKeyFunc 返回不带缓存、直接查库派生的 TokenKeyFunc——与
// ingest 包同名函数的实现、理由都完全一致（Manager 从不持有明文
// node_token，签名 key 统一由 token_hash 派生，见 internal/iproto/sign.go）。
func defaultTokenKeyFunc(db *gorm.DB) TokenKeyFunc {
	return func(ctx context.Context, nodeID uuidx.UUID) (iproto.SignKey, error) {
		n, err := repo.GetNode(ctx, db, nodeID)
		if err != nil {
			return iproto.SignKey{}, fmt.Errorf("dispatch: 查询节点失败: %w", err)
		}
		return iproto.DeriveSignKeyFromTokenHash(n.TokenHash), nil
	}
}

// verifyNode 解析 x-node-id/x-sign header、校验协议版本、调用
// iproto.Verify 验签——与 internal/manager/ingest.Service.verify 是同一套
// 逻辑（决定权在 internal/iproto/sign.go，两个包各自的控制面/上报面消费者
// 都要遵守同一个验签口径，不应该出现一个更松或更严的第二套实现）。
//
// 返回的 nodeID 在非 nil error 时也可能是有效值（只要 x-node-id header
// 本身解析成功）——调用方把它带进日志字段，验签失败时也能定位是哪个
// 节点的消息出了问题。
func (d *Dispatcher) verifyNode(ctx context.Context, env *iproto.Envelope, now time.Time) (uuidx.UUID, error) {
	nodeIDStr, ok := env.HeaderString(iproto.HeaderNodeID)
	if !ok || nodeIDStr == "" {
		return uuidx.Nil, fmt.Errorf("dispatch: 信封缺少 %s header", iproto.HeaderNodeID)
	}
	nodeID, err := uuidx.Parse(nodeIDStr)
	if err != nil {
		return uuidx.Nil, fmt.Errorf("dispatch: %s header 不是合法 UUID: %w", iproto.HeaderNodeID, err)
	}

	v, ok := env.HeaderUint64(iproto.HeaderProtoVersion)
	if !ok {
		return nodeID, fmt.Errorf("dispatch: 信封缺少 %s header", iproto.HeaderProtoVersion)
	}
	if v != uint64(iproto.CurrentProtocolVersion()) {
		return nodeID, fmt.Errorf("dispatch: 协议版本不匹配（peer=%d, local=%d）", v, iproto.CurrentProtocolVersion())
	}

	sig, ok := env.HeaderString(iproto.HeaderSign)
	if !ok || sig == "" {
		return nodeID, fmt.Errorf("dispatch: 信封缺少 %s header", iproto.HeaderSign)
	}

	key, err := d.tokenKey(ctx, nodeID)
	if err != nil {
		return nodeID, fmt.Errorf("dispatch: 解析验签 key 失败: %w", err)
	}

	if err := iproto.Verify(key, env.SignParamsFor(), sig, now, iproto.DefaultSignatureWindow); err != nil {
		return nodeID, err
	}
	return nodeID, nil
}
