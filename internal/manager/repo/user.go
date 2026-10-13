// 用户仓储维护账号、认证来源和 active/disabled 状态。
// 连续登录失败计数和临时锁定由 login_lockouts 仓储管理。
package repo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
)

var (
	// ErrUserNotFound 表示用户不存在。
	ErrUserNotFound = errors.New("repo: 用户不存在")
	// ErrUsernameConflict 表示用户名与已有记录冲突（uk_users_username）。
	ErrUsernameConflict = errors.New("repo: 用户名已存在")
)

// CreateUser 创建用户（本地账号，或 LDAP/OIDC 首次登录自动建档，需求
// MGR-USR-01/02/03）。
func CreateUser(ctx context.Context, db *gorm.DB, u *model.User) error {
	if err := u.Validate(); err != nil {
		return err
	}
	if err := db.WithContext(ctx).Create(u).Error; err != nil {
		if isDuplicateKeyError(err) {
			return fmt.Errorf("%w: username=%q", ErrUsernameConflict, u.Username)
		}
		return fmt.Errorf("repo: 创建用户失败: %w", err)
	}
	return nil
}

// GetUser 按主键查询用户。
func GetUser(ctx context.Context, db *gorm.DB, id uuidx.UUID) (*model.User, error) {
	var u model.User
	if err := db.WithContext(ctx).First(&u, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrUserNotFound
		}
		return nil, fmt.Errorf("repo: 查询用户失败: %w", err)
	}
	return &u, nil
}

// GetUserByUsername 按用户名查询用户（本地登录路径：先按用户名找到账号，
// 再校验密码哈希）。
func GetUserByUsername(ctx context.Context, db *gorm.DB, username string) (*model.User, error) {
	var u model.User
	if err := db.WithContext(ctx).First(&u, "username = ?", username).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrUserNotFound
		}
		return nil, fmt.Errorf("repo: 按用户名查询用户失败: %w", err)
	}
	return &u, nil
}

// GetUserByExternalID 按认证来源 + 外部身份查询用户（LDAP DN / OIDC
// subject，需求 MGR-USR-02/03：首次登录自动创建用户记录前，先按外部身份
// 而不是用户名查找是否已存在账号——外部用户名可能与本地账号重名，但外部
// 身份不会）。
func GetUserByExternalID(ctx context.Context, db *gorm.DB, authSource model.AuthSource, externalID string) (*model.User, error) {
	var u model.User
	err := db.WithContext(ctx).First(&u, "auth_source = ? AND external_identity_key = ?", authSource, externalID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrUserNotFound
		}
		return nil, fmt.Errorf("repo: 按外部身份查询用户失败: %w", err)
	}
	return &u, nil
}

// ListUsers 列出全部用户（用户管理页面）。
func ListUsers(ctx context.Context, db *gorm.DB) ([]model.User, error) {
	var us []model.User
	if err := db.WithContext(ctx).Order("created_at ASC").Find(&us).Error; err != nil {
		return nil, fmt.Errorf("repo: 查询用户列表失败: %w", err)
	}
	return us, nil
}

// UpdateUser 更新用户的基础信息（用户名、角色），不包括 password_hash
// （见 SetUserPassword）、status（见 SetUserStatus）、auth_source /
// external_id（定义账号身份，不通过通用更新改写）、last_login_at（登录
// 路径专属写入）。这样通用更新不可能意外带着调用方读到的旧密码哈希或
// 旧状态把并发的密码修改 / 锁定动作覆盖回去。
func UpdateUser(ctx context.Context, db *gorm.DB, u *model.User) error {
	if !u.Role.Valid() {
		return fmt.Errorf("model: 未知的用户角色 %q", u.Role)
	}
	res := db.WithContext(ctx).Model(&model.User{}).Where("id = ?", u.ID).
		Updates(map[string]any{
			"username": u.Username,
			"role":     u.Role,
		})
	if res.Error != nil {
		if isDuplicateKeyError(res.Error) {
			return fmt.Errorf("%w: username=%q", ErrUsernameConflict, u.Username)
		}
		return fmt.Errorf("repo: 更新用户失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrUserNotFound
	}
	return nil
}

// SetUserPassword 修改本地账号密码哈希（需求 MGR-USR-01）。调用方负责用
// argon2id / bcrypt 算好哈希（密码复杂度策略校验、明文密码本身都不应该
// 到达仓储层）；本函数只负责落库。
func SetUserPassword(ctx context.Context, db *gorm.DB, id uuidx.UUID, passwordHash string) error {
	res := db.WithContext(ctx).Model(&model.User{}).Where("id = ?", id).
		Updates(map[string]any{"password_hash": passwordHash, "must_change_password": false})
	if res.Error != nil {
		return fmt.Errorf("repo: 修改密码失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrUserNotFound
	}
	return nil
}

// SetUserStatus 修改账号状态（启用 / 禁用 / 锁定，需求 MGR-USR-01/05）。
func SetUserStatus(ctx context.Context, db *gorm.DB, id uuidx.UUID, status model.UserStatus) error {
	if !status.Valid() {
		return fmt.Errorf("model: 未知的用户状态 %q", status)
	}
	res := db.WithContext(ctx).Model(&model.User{}).Where("id = ?", id).
		Update("status", status)
	if res.Error != nil {
		return fmt.Errorf("repo: 修改用户状态失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrUserNotFound
	}
	return nil
}

// RecordUserLoginSuccess 记录一次成功登录（刷新 LastLoginAt）。
func RecordUserLoginSuccess(ctx context.Context, db *gorm.DB, id uuidx.UUID, now time.Time) error {
	res := db.WithContext(ctx).Model(&model.User{}).Where("id = ?", id).
		Update("last_login_at", now)
	if res.Error != nil {
		return fmt.Errorf("repo: 记录登录时间失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrUserNotFound
	}
	return nil
}

func DeleteUser(ctx context.Context, db *gorm.DB, id uuidx.UUID) error {
	res := db.WithContext(ctx).Delete(&model.User{}, "id = ?", id)
	if res.Error != nil {
		return fmt.Errorf("repo: 删除用户失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrUserNotFound
	}
	return nil
}
