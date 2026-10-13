package repo

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/xmapst/xdts/internal/manager/model"
)

var ErrExternalIdentityDenied = errors.New("repo: 外部身份不可登录")

// UpsertExternalUser 按不可变外部身份建档。唯一索引串行化并发首次登录，
// 当前锁读不覆盖管理员的禁用/删除；角色只在成功目录认证后更新。
func UpsertExternalUser(ctx context.Context, db *gorm.DB, source model.AuthSource, externalID, username string, role model.UserRole, now time.Time) (*model.User, error) {
	if (source != model.AuthSourceLDAP && source != model.AuthSourceOIDC) || !role.Valid() || externalID == "" || username == "" || !utf8.ValidString(externalID) || !utf8.ValidString(username) || utf8.RuneCountInString(externalID) > 255 || utf8.RuneCountInString(username) > 128 || strings.ContainsRune(externalID, 0) || strings.ContainsRune(username, 0) {
		return nil, ErrExternalIdentityDenied
	}
	var result model.User
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		candidate := model.User{Username: username, AuthSource: source, ExternalID: externalID, Role: role, Status: model.UserStatusActive, LastLoginAt: &now}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&candidate).Error; err != nil {
			return errors.New("repo: 外部用户建档失败")
		}
		if err := tx.Unscoped().Clauses(clause.Locking{Strength: "UPDATE"}).First(&result, "auth_source = ? AND external_identity_key = ?", source, externalID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrExternalIdentityDenied
			}
			return errors.New("repo: 外部用户查询失败")
		}
		if result.DeletedAt.Valid || result.Status != model.UserStatusActive {
			return ErrExternalIdentityDenied
		}
		// 保留管理员改过的用户名；目录 DN 是身份，显示名不是第二身份。
		if err := tx.Model(&model.User{}).Where("id = ?", result.ID).Updates(map[string]any{"role": role, "last_login_at": now}).Error; err != nil {
			return errors.New("repo: 外部用户更新失败")
		}
		result.Role, result.LastLoginAt = role, &now
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}
