package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// ErrSessionNotFound 表示令牌不存在、已被撤销，或已过期。三种情况用同一个
// 错误，避免给攻击者提供「令牌是否存在过」的旁路信息。
var ErrSessionNotFound = errors.New("auth: 会话不存在或已过期")

// 会话令牌是 32 字节随机数；Cookie 保存明文，服务端仅保存摘要。
type Session struct {
	ID         string // 会话 ID，日志/审计引用它而不是令牌或其哈希
	TokenHash  string // hex(sha256(明文令牌))
	UserID     string
	Username   string     // 冗余存一份，方便审计/日志不必每次都反查用户
	Role       Role       // 签发时的角色快照，仅供审计参考；鉴权判定用当前角色（见 SessionAuth）
	AuthSource AuthSource // 签发这个会话时用的登录方式（本地/LDAP/OIDC），会话生命周期内不变
	CreatedAt  time.Time
	ExpiresAt  time.Time
	ClientIP   string
	UserAgent  string
}

// Expired 判定会话在 now 时刻是否已过期。
func (s Session) Expired(now time.Time) bool {
	return !now.Before(s.ExpiresAt)
}

type ISessionStore interface {
	// Issue 签发一个新会话，返回明文令牌（只在这一次返回；服务端自己只存
	// 令牌的哈希）。ttl 由调用方传入（通常是 cfg.Auth.SessionTTL）。
	// authSource 记录这次登录用的是哪种方式，本地/LDAP/OIDC 三条登录路径
	// 都要传各自对应的值（需求 MGR-USR-04）。
	Issue(ctx context.Context, userID, username string, role Role, authSource AuthSource, clientIP, userAgent string, ttl time.Duration) (token string, sess Session, err error)
	// Verify 按明文令牌查会话；令牌不存在、已撤销或已过期都返回
	// ErrSessionNotFound。
	Verify(ctx context.Context, token string) (Session, error)
	// Renew 延长会话有效期（滑动续期），返回更新后的会话。
	Renew(ctx context.Context, token string, ttl time.Duration) (Session, error)
	// Revoke 撤销单个会话（用户主动登出）。撤销一个不存在的令牌不是错误。
	Revoke(ctx context.Context, token string) error

	RevokeAllForUser(ctx context.Context, userID string) error
}

// hashToken 计算令牌的 SHA-256 十六进制摘要。
func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// tokenByteLength 是令牌的随机字节长度（32 字节随机数）。
const tokenByteLength = 32

// newToken 生成一个新的随机令牌，十六进制编码。
func newToken() (string, error) {
	b := make([]byte, tokenByteLength)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("auth: 生成会话令牌失败: %w", err)
	}
	return hex.EncodeToString(b), nil
}
