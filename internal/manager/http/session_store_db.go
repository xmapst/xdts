package http

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/auth"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// Session verification reads the current user, so role or account changes apply
// without keeping an independent authorization snapshot in each session.
type DBSessionStore struct {
	db *gorm.DB
}

// NewDBSessionStore 构造落库版本的 ISessionStore。db 必须是已经完成迁移、
// user_sessions 表已存在的连接。
func NewDBSessionStore(db *gorm.DB) *DBSessionStore {
	return &DBSessionStore{db: db}
}

var _ auth.ISessionStore = (*DBSessionStore)(nil)

// toAuthSession 把 repo 层的 *model.UserSession 与另外拿到的
// username/role/authSource 拼成 auth.Session。三个字段的来源因调用方而
// 异（Issue 用调用方直接传入的值；Verify/Renew 用现查 repo.GetUser 拿到
// 的当前值），本函数本身不关心来源，只负责拼装。
func toAuthSession(sess *model.UserSession, username string, role auth.Role, authSource auth.AuthSource) auth.Session {
	return auth.Session{
		ID:         sess.ID.String(),
		TokenHash:  sess.TokenHash,
		UserID:     sess.UserID.String(),
		Username:   username,
		Role:       role,
		AuthSource: authSource,
		CreatedAt:  sess.CreatedAt,
		ExpiresAt:  sess.ExpiresAt,
		ClientIP:   sess.ClientIP,
		UserAgent:  sess.UserAgent,
	}
}

// Issue 实现 auth.ISessionStore。不需要 join：username/role/authSource
// 由调用方直接传入，原样回填进返回的 Session（见上方类型注释）。
func (s *DBSessionStore) Issue(ctx context.Context, userID, username string, role auth.Role, authSource auth.AuthSource, clientIP, userAgent string, ttl time.Duration) (string, auth.Session, error) {
	uid, err := uuidx.Parse(userID)
	if err != nil {
		return "", auth.Session{}, fmt.Errorf("http: 用户 ID 不是合法 UUID: %w", err)
	}
	token, sess, err := repo.IssueUserSession(ctx, s.db, uid, clientIP, userAgent, ttl, time.Now())
	if err != nil {
		return "", auth.Session{}, err
	}
	return token, toAuthSession(sess, username, role, authSource), nil
}

// Verify 实现 auth.ISessionStore。
func (s *DBSessionStore) Verify(ctx context.Context, token string) (auth.Session, error) {
	sess, err := repo.GetUserSessionByToken(ctx, s.db, token, time.Now())
	if err != nil {
		return s.translateSessionLookupError(err)
	}
	return s.joinUser(ctx, sess)
}

// Renew 实现 auth.ISessionStore。
func (s *DBSessionStore) Renew(ctx context.Context, token string, ttl time.Duration) (auth.Session, error) {
	sess, err := repo.RenewUserSession(ctx, s.db, token, ttl, time.Now())
	if err != nil {
		return s.translateSessionLookupError(err)
	}
	return s.joinUser(ctx, sess)
}

// translateSessionLookupError 把 repo.ErrUserSessionNotFound 翻译成
// auth.ErrSessionNotFound——两边的文档注释都明确"令牌不存在/已撤销/已
// 过期用同一个错误，不泄露具体是哪一种"，这里保持这个约定；除此之外的
// 错误（真实 DB 故障）原样透传，不能被这层翻译掩盖成"你的会话过期了"。
func (s *DBSessionStore) translateSessionLookupError(err error) (auth.Session, error) {
	if errors.Is(err, repo.ErrUserSessionNotFound) {
		return auth.Session{}, auth.ErrSessionNotFound
	}
	return auth.Session{}, err
}

// joinUser 是 Verify/Renew 共用的"查到会话之后再查一次用户"步骤（决策 4）。
func (s *DBSessionStore) joinUser(ctx context.Context, sess *model.UserSession) (auth.Session, error) {
	u, err := repo.GetUser(ctx, s.db, sess.UserID)
	if err != nil {
		if errors.Is(err, repo.ErrUserNotFound) {
			return auth.Session{}, auth.ErrSessionNotFound
		}
		return auth.Session{}, err
	}
	return toAuthSession(sess, u.Username, auth.Role(u.Role), auth.AuthSource(u.AuthSource)), nil
}

// Revoke 实现 auth.ISessionStore。撤销一个不存在的令牌不是错误——
// repo.RevokeUserSession 的文档注释已经是这个约定，直接转发。
func (s *DBSessionStore) Revoke(ctx context.Context, token string) error {
	return repo.RevokeUserSession(ctx, s.db, token)
}

// RevokeAllForUser 实现 auth.ISessionStore。
func (s *DBSessionStore) RevokeAllForUser(ctx context.Context, userID string) error {
	uid, err := uuidx.Parse(userID)
	if err != nil {
		return fmt.Errorf("http: 用户 ID 不是合法 UUID: %w", err)
	}
	return repo.RevokeAllUserSessionsForUser(ctx, s.db, uid)
}
