package repo

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// sessionTokenByteLength 是会话令牌的随机字节长度（32 字节
// 随机数），与 auth.Session 已经在用的口径一致。
const sessionTokenByteLength = 32

// ErrUserSessionNotFound 表示令牌不存在、已撤销，或已过期。三种情况用
// 同一个错误，不向调用方泄露「令牌是否存在过」的旁路信息——与
// auth.ErrSessionNotFound 的既有约定保持一致。
var ErrUserSessionNotFound = errors.New("repo: 会话不存在或已过期")

func hashSessionToken(plainToken string) string {
	sum := sha256.Sum256([]byte(plainToken))
	return hex.EncodeToString(sum[:])
}

// IssueUserSession 签发一个新会话（需求 MGR-USR-04）。明文令牌只在本次
// 调用的返回值里出现一次，库中只存 SHA-256 哈希。
func IssueUserSession(ctx context.Context, db *gorm.DB, userID uuidx.UUID, clientIP, userAgent string, ttl time.Duration, now time.Time) (plainToken string, sess *model.UserSession, err error) {
	b := make([]byte, sessionTokenByteLength)
	if _, err := rand.Read(b); err != nil {
		return "", nil, fmt.Errorf("repo: 生成会话令牌失败: %w", err)
	}
	plainToken = hex.EncodeToString(b)

	sess = &model.UserSession{
		UserID:    userID,
		TokenHash: hashSessionToken(plainToken),
		ExpiresAt: now.Add(ttl),
		ClientIP:  clientIP,
		UserAgent: userAgent,
	}
	if err := db.WithContext(ctx).Create(sess).Error; err != nil {
		return "", nil, fmt.Errorf("repo: 创建会话失败: %w", err)
	}
	return plainToken, sess, nil
}

// GetUserSessionByToken 按明文令牌查询会话：先哈希再查，不存在、已撤销
// （软删除）或已过期都返回 ErrUserSessionNotFound。gorm 的软删除默认
// scope 已经排除 deleted_at 非空的行，这里只需要额外判断 expires_at。
func GetUserSessionByToken(ctx context.Context, db *gorm.DB, plainToken string, now time.Time) (*model.UserSession, error) {
	var sess model.UserSession
	err := db.WithContext(ctx).First(&sess, "token_hash = ?", hashSessionToken(plainToken)).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrUserSessionNotFound
		}
		return nil, fmt.Errorf("repo: 查询会话失败: %w", err)
	}
	if !sess.ExpiresAt.After(now) {
		return nil, ErrUserSessionNotFound
	}
	return &sess, nil
}

// RenewUserSession 延长会话有效期（滑动续期）。令牌不存在、已撤销或已
// 过期都返回 ErrUserSessionNotFound，不做「续期一个已经过期的会话」这种
// 隐式复活。
func RenewUserSession(ctx context.Context, db *gorm.DB, plainToken string, ttl time.Duration, now time.Time) (*model.UserSession, error) {
	hash := hashSessionToken(plainToken)
	res := db.WithContext(ctx).Model(&model.UserSession{}).
		Where("token_hash = ? AND expires_at > ?", hash, now).
		Update("expires_at", now.Add(ttl))
	if res.Error != nil {
		return nil, fmt.Errorf("repo: 续期会话失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return nil, ErrUserSessionNotFound
	}
	var sess model.UserSession
	if err := db.WithContext(ctx).First(&sess, "token_hash = ?", hash).Error; err != nil {
		return nil, fmt.Errorf("repo: 查询续期后的会话失败: %w", err)
	}
	return &sess, nil
}

func RevokeUserSession(ctx context.Context, db *gorm.DB, plainToken string) error {
	if err := db.WithContext(ctx).Where("token_hash = ?", hashSessionToken(plainToken)).
		Delete(&model.UserSession{}).Error; err != nil {
		return fmt.Errorf("repo: 撤销会话失败: %w", err)
	}
	return nil
}

func RevokeAllUserSessionsForUser(ctx context.Context, db *gorm.DB, userID uuidx.UUID) error {
	_, err := RevokeAllUserSessionsForUserCount(ctx, db, userID)
	return err
}

// RevokeAllUserSessionsForUserCount 返回实际撤销的会话数。
func RevokeAllUserSessionsForUserCount(ctx context.Context, db *gorm.DB, userID uuidx.UUID) (int64, error) {
	res := db.WithContext(ctx).Where("user_id = ?", userID).Delete(&model.UserSession{})
	if res.Error != nil {
		return 0, fmt.Errorf("repo: 批量撤销用户会话失败: %w", res.Error)
	}
	return res.RowsAffected, nil
}

func DeleteExpiredUserSessions(ctx context.Context, db *gorm.DB, now time.Time) (int64, error) {
	res := db.WithContext(ctx).Unscoped().
		Where("expires_at <= ? OR deleted_at IS NOT NULL", now).
		Delete(&model.UserSession{})
	if res.Error != nil {
		return 0, fmt.Errorf("repo: 清理过期会话失败: %w", res.Error)
	}
	return res.RowsAffected, nil
}
