// 只做仓储层：生成 / 作废 / 校验 / 核销（幂等签发）四个动作各自独立成
// 函数，不假设调用方的调用形态。
//
// 注册码仓储提供生成、列表、作废和核销能力。
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

// registerCodeByteLength 是注册码的随机字节长度，与 auth.Session 的会话
// 令牌取相同长度（32 字节随机数），保持同一强度约定。
const registerCodeByteLength = 32

var (
	// ErrRegisterCodeNotFound 表示注册码不存在。
	ErrRegisterCodeNotFound = errors.New("repo: 注册码不存在")
	// ErrRegisterCodeRevoked 表示注册码已作废。
	ErrRegisterCodeRevoked = errors.New("repo: 注册码已作废")
	// ErrRegisterCodeExpired 表示注册码已过期。
	ErrRegisterCodeExpired = errors.New("repo: 注册码已过期")
	// ErrRegisterCodeExhausted 表示注册码可用次数已耗尽。需求 O-04：
	// max_uses 固定为 1（一码一节点，同一注册码同时用作该节点本地状态库
	// 的加密口令，复用会让多个节点共用同一口令），因此耗尽即等价于「已被
	// 用过一次」。
	ErrRegisterCodeExhausted = errors.New("repo: 注册码已被使用，不能重复注册节点")
)

// NewNodeRegisterCodeParams 是生成注册码的入参（需求 MGR-NODE-01）。
type NewNodeRegisterCodeParams struct {
	CenterID     uuidx.UUID
	ExpiresAt    time.Time
	PresetName   string
	PresetLabels model.JSON
	CreatedBy    uuidx.UUID
}

// GenerateNodeRegisterCode 生成一个新注册码并落库。
//
// max_uses 在仓储层固定写死为 1，不接受调用方传入：需求 O-04 明确「可用
// 次数固定为 1，不开放配置」，这不是一个业务层可覆盖的默认值。
func GenerateNodeRegisterCode(ctx context.Context, db *gorm.DB, p NewNodeRegisterCodeParams) (plainCode string, rec *model.NodeRegisterCode, err error) {
	b := make([]byte, registerCodeByteLength)
	if _, err := rand.Read(b); err != nil {
		return "", nil, fmt.Errorf("repo: 生成注册码随机数失败: %w", err)
	}
	plainCode = hex.EncodeToString(b)

	rec = &model.NodeRegisterCode{
		CodeHash:     hashRegisterCode(plainCode),
		ExpiresAt:    p.ExpiresAt,
		MaxUses:      1,
		UsedCount:    0,
		CenterID:     p.CenterID,
		PresetName:   p.PresetName,
		PresetLabels: p.PresetLabels,
		CreatedBy:    p.CreatedBy,
	}
	if err := db.WithContext(ctx).Create(rec).Error; err != nil {
		return "", nil, fmt.Errorf("repo: 创建注册码失败: %w", err)
	}
	return plainCode, rec, nil
}

// hashRegisterCode 计算注册码的 SHA-256 十六进制摘要，与 auth.Session 的
// 令牌哈希方式一致（都是「高熵随机值，用快哈希即可，不需要 argon2/bcrypt
// 这类为低熵口令设计的慢哈希」）。
func hashRegisterCode(plainCode string) string {
	sum := sha256.Sum256([]byte(plainCode))
	return hex.EncodeToString(sum[:])
}

// RevokeNodeRegisterCode 作废一个注册码（需求 MGR-NODE-02）。
//
// 作废只阻止后续注册，不影响已用该码注册的节点继续打开本地状态库（口令
// 与注册状态解耦）——本函数只置 revoked_at，不触碰 nodes 表，天然满足
// 这一点。
//
// 幂等：已作废的码再次调用不报错、不覆盖首次作废时间；不存在的 id 返回
// ErrRegisterCodeNotFound。
func RevokeNodeRegisterCode(ctx context.Context, db *gorm.DB, id uuidx.UUID, now time.Time) error {
	res := db.WithContext(ctx).Model(&model.NodeRegisterCode{}).
		Where("id = ? AND revoked_at IS NULL", id).
		Update("revoked_at", now)
	if res.Error != nil {
		return fmt.Errorf("repo: 作废注册码失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		// 未命中：区分「不存在」与「已作废」，便于上层返回准确的错误。
		var rec model.NodeRegisterCode
		err := db.WithContext(ctx).Select("id", "revoked_at").First(&rec, "id = ?", id).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrRegisterCodeNotFound
		}
		if err != nil {
			return fmt.Errorf("repo: 查询注册码失败: %w", err)
		}
		// 走到这里说明记录存在且已作废（revoked_at 非 NULL），按幂等语义
		// 视为成功，不返回错误。
	}
	return nil
}

// ListNodeRegisterCodes 列出注册码（按创建时间倒序）。centerID 为
// uuidx.Nil 时不按中心过滤（需求 MGR-NODE-02：注册码列表）。
func ListNodeRegisterCodes(ctx context.Context, db *gorm.DB, centerID uuidx.UUID) ([]model.NodeRegisterCode, error) {
	q := db.WithContext(ctx).Order("created_at DESC")
	if !centerID.IsZero() {
		q = q.Where("center_id = ?", centerID)
	}
	var recs []model.NodeRegisterCode
	if err := q.Find(&recs).Error; err != nil {
		return nil, fmt.Errorf("repo: 查询注册码列表失败: %w", err)
	}
	return recs, nil
}

// ConsumeNodeRegisterCode 校验并核销一枚注册码。
//
// 用一条带条件的 UPDATE 原子递增 used_count，把「校验可用」与「占用」
// 合并成一步：并发的两次注册请求用同一枚码时，只有一条 UPDATE 能命中行
// （used_count < max_uses 的条件在提交时刻重新求值），另一条天然拿到
// RowsAffected=0，不会出现两次核销同一枚 max_uses=1 的码。
//
// 「幂等」体现在失败路径上：条件不满足时 UPDATE 不产生任何副作用，调用方
// 可以安全重试——重试只会得到同样明确的拒绝原因（不存在 / 已作废 / 已
// 过期 / 已用尽），不会把状态重复推进或损坏。真正把「同一次节点注册的
// 网络重试识别为同一次签发」需要调用方自己的幂等键设计，不属于本函数职责范围。
func ConsumeNodeRegisterCode(ctx context.Context, db *gorm.DB, plainCode string, now time.Time) (*model.NodeRegisterCode, error) {
	hash := hashRegisterCode(plainCode)
	res := db.WithContext(ctx).Model(&model.NodeRegisterCode{}).
		Where("code_hash = ? AND revoked_at IS NULL AND expires_at > ? AND used_count < max_uses", hash, now).
		UpdateColumn("used_count", gorm.Expr("used_count + 1"))
	if res.Error != nil {
		return nil, fmt.Errorf("repo: 核销注册码失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		rec, err := findNodeRegisterCodeByHash(ctx, db, hash)
		if err != nil {
			return nil, err
		}
		return nil, checkNodeRegisterCodeUsable(rec, now)
	}
	return findNodeRegisterCodeByHash(ctx, db, hash)
}

func findNodeRegisterCodeByHash(ctx context.Context, db *gorm.DB, hash string) (*model.NodeRegisterCode, error) {
	var rec model.NodeRegisterCode
	if err := db.WithContext(ctx).First(&rec, "code_hash = ?", hash).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrRegisterCodeNotFound
		}
		return nil, fmt.Errorf("repo: 查询注册码失败: %w", err)
	}
	return &rec, nil
}

// checkNodeRegisterCodeUsable 返回注册码不可用的具体原因；可用返回 nil。
func checkNodeRegisterCodeUsable(rec *model.NodeRegisterCode, now time.Time) error {
	if rec.RevokedAt != nil {
		return ErrRegisterCodeRevoked
	}
	if !rec.ExpiresAt.After(now) {
		return ErrRegisterCodeExpired
	}
	if rec.UsedCount >= rec.MaxUses {
		return ErrRegisterCodeExhausted
	}
	return nil
}
