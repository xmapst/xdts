package repo

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"time"

	"github.com/xmapst/xdts/internal/manager/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

var ErrOIDCStateInvalid = errors.New("repo: OIDC 登录状态不可用")

func HashOIDCValue(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func CreateOIDCLoginState(ctx context.Context, db *gorm.DB, record model.OIDCLoginState, now time.Time) error {
	if len(record.StateHash) != 64 || len(record.BrowserHash) != 64 || len(record.ConfigHash) != 64 || len(record.Nonce) != 43 || len(record.Verifier) != 43 || !record.ExpiresAt.After(now) || record.ExpiresAt.After(now.Add(15*time.Minute)) {
		return ErrOIDCStateInvalid
	}
	// Gorm 的 SQL 错误/慢查询可能打印绑定值；该表含 nonce/verifier，
	// 禁止在此路径输出 SQL，错误由调用方使用固定文案记录。
	return db.Session(&gorm.Session{Logger: logger.Default.LogMode(logger.Silent)}).WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("expires_at <= ?", now).Delete(&model.OIDCLoginState{}).Error; err != nil {
			return errors.New("repo: OIDC 过期状态清理失败")
		}
		if err := tx.Create(&record).Error; err != nil {
			return errors.New("repo: OIDC 状态保存失败")
		}
		return nil
	})
}

// ConsumeOIDCLoginState 校验后删除且提交，在令牌交换之前保证只消费一次。
func ConsumeOIDCLoginState(ctx context.Context, db *gorm.DB, state, browser, configHash string, now time.Time) (model.OIDCLoginState, error) {
	if len(state) != 43 || len(browser) != 43 {
		return model.OIDCLoginState{}, ErrOIDCStateInvalid
	}
	var record model.OIDCLoginState
	err := db.Session(&gorm.Session{Logger: logger.Default.LogMode(logger.Silent)}).WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&record, "state_hash = ?", HashOIDCValue(state)).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrOIDCStateInvalid
			}
			return errors.New("repo: OIDC 状态查询失败")
		}
		if !record.ExpiresAt.After(now) || subtle.ConstantTimeCompare([]byte(record.BrowserHash), []byte(HashOIDCValue(browser))) != 1 || record.ConfigHash != configHash {
			return ErrOIDCStateInvalid
		}
		if err := tx.Delete(&record).Error; err != nil {
			return errors.New("repo: OIDC 状态消费失败")
		}
		return nil
	})
	if err != nil {
		return model.OIDCLoginState{}, err
	}
	return record, nil
}
