package ingest

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// TokenKeyFunc 按节点 ID 解析用于 HMAC 验签的 iproto.SignKey。
type TokenKeyFunc func(ctx context.Context, nodeID uuidx.UUID) (iproto.SignKey, error)

// defaultTokenKeyFunc 返回不带缓存、直接查库派生的 TokenKeyFunc（见上方
// TokenKeyFunc 的文档）。
func defaultTokenKeyFunc(db *gorm.DB) TokenKeyFunc {
	return func(ctx context.Context, nodeID uuidx.UUID) (iproto.SignKey, error) {
		n, err := repo.GetNode(ctx, db, nodeID)
		if err != nil {
			return iproto.SignKey{}, fmt.Errorf("ingest: 查询节点失败: %w", err)
		}
		return iproto.DeriveSignKeyFromTokenHash(n.TokenHash), nil
	}
}

// verify 解析 x-node-id/x-sign header、校验协议版本、调用
// iproto.Verify 验签。返回的 nodeID 在任何非 nil error 的情况下也可能是
// 有效值（只要 x-node-id header 本身能解析成功）——调用方（internal/manager/ingest/handler.go）
// 把它带进日志字段，即使验签失败也能定位是哪个节点的消息出了问题。
func (s *Service) verify(ctx context.Context, env *iproto.Envelope, now time.Time) (uuidx.UUID, error) {
	nodeIDStr, ok := env.HeaderString(iproto.HeaderNodeID)
	if !ok || nodeIDStr == "" {
		return uuidx.Nil, fmt.Errorf("ingest: 信封缺少 %s header", iproto.HeaderNodeID)
	}
	nodeID, err := uuidx.Parse(nodeIDStr)
	if err != nil {
		return uuidx.Nil, fmt.Errorf("ingest: %s header 不是合法 UUID: %w", iproto.HeaderNodeID, err)
	}

	v, ok := env.HeaderUint64(iproto.HeaderProtoVersion)
	if !ok {
		return nodeID, fmt.Errorf("ingest: 信封缺少 %s header", iproto.HeaderProtoVersion)
	}
	if v != uint64(iproto.CurrentProtocolVersion()) {
		return nodeID, fmt.Errorf("ingest: 协议版本不匹配（peer=%d, local=%d）", v, iproto.CurrentProtocolVersion())
	}

	sig, ok := env.HeaderString(iproto.HeaderSign)
	if !ok || sig == "" {
		return nodeID, fmt.Errorf("ingest: 信封缺少 %s header", iproto.HeaderSign)
	}

	key, err := s.opts.TokenKey(ctx, nodeID)
	if err != nil {
		return nodeID, fmt.Errorf("ingest: 解析验签 key 失败: %w", err)
	}

	if err := iproto.Verify(key, env.SignParamsFor(), sig, now, s.opts.SignatureWindow); err != nil {
		return nodeID, err
	}
	return nodeID, nil
}
