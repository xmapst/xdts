// 只做仓储层：CRUD + 版本列表 + 删除校验。MGR-STR-02（表单/JSON 编辑前的
// 正则可编译、表达式可编译、规则互斥校验）不在这里——那是规则内容本身的
// 语义校验，需要 internal/rule 的编译器，不是仓储层能独立完成的（本层只
// 保证 Rules 是合法 JSON，不解析其业务含义）。
package repo

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
)

var (
	// ErrStrategyNotFound 表示策略不存在。
	ErrStrategyNotFound = errors.New("repo: 策略不存在")
	// ErrStrategyNameConflict 表示策略名称与已有记录冲突。
	ErrStrategyNameConflict = errors.New("repo: 策略名称已存在")
	// ErrStrategyHasDependents 表示策略仍被未删除的 pipeline 引用，不能
	// 删除（需求 MGR-STR-04：策略整体删除需先解除全部引用）。
	ErrStrategyHasDependents = errors.New("repo: 策略被 pipeline 引用，不能删除")
	// ErrStrategyVersionNotFound 表示指定的策略版本不存在。
	ErrStrategyVersionNotFound = errors.New("repo: 策略版本不存在")
	// ErrStrategyVersionHasDependents 表示指定策略版本仍被 pipeline（含
	// 已软删除的）引用，不能删除（需求 MGR-STR-04：「被 pipeline 引用的」
	// 策略版本不可删除——这个限定词蕴含未被引用的版本可以删）。
	ErrStrategyVersionHasDependents = errors.New("repo: 策略版本被 pipeline 引用，不能删除")

	ErrStrategyVersionIsCurrent = errors.New("repo: 该版本是策略当前版本，不能删除")
)

// CreateStrategy 创建策略（空壳，不含任何版本；第一个版本通过
// CreateStrategyVersion 单独创建）。
func CreateStrategy(ctx context.Context, db *gorm.DB, s *model.Strategy) error {
	if err := db.WithContext(ctx).Create(s).Error; err != nil {
		if isDuplicateKeyError(err) {
			return fmt.Errorf("%w: name=%q", ErrStrategyNameConflict, s.Name)
		}
		return fmt.Errorf("repo: 创建策略失败: %w", err)
	}
	return nil
}

// GetStrategy 按主键查询策略。
func GetStrategy(ctx context.Context, db *gorm.DB, id uuidx.UUID) (*model.Strategy, error) {
	var s model.Strategy
	if err := ReadResourceScope(ctx, db.WithContext(ctx), ReadStrategy).First(&s, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrStrategyNotFound
		}
		return nil, fmt.Errorf("repo: 查询策略失败: %w", err)
	}
	return &s, nil
}

// ListStrategies 列出全部策略。
func ListStrategies(ctx context.Context, db *gorm.DB) ([]model.Strategy, error) {
	var ss []model.Strategy
	if err := ReadResourceScope(ctx, db.WithContext(ctx), ReadStrategy).Order("created_at ASC").Find(&ss).Error; err != nil {
		return nil, fmt.Errorf("repo: 查询策略列表失败: %w", err)
	}
	return ss, nil
}

// UpdateStrategyMeta 更新策略的名称与描述（不含 current_version——那由
// CreateStrategyVersion 在创建新版本时一并推进，不通过通用更新单独
// 改写，避免把 current_version 指向一个不存在的版本号）。
func UpdateStrategyMeta(ctx context.Context, db *gorm.DB, id uuidx.UUID, name, description string) error {
	res := db.WithContext(ctx).Model(&model.Strategy{}).Where("id = ?", id).
		Updates(map[string]any{"name": name, "description": description})
	if res.Error != nil {
		if isDuplicateKeyError(res.Error) {
			return fmt.Errorf("%w: name=%q", ErrStrategyNameConflict, name)
		}
		return fmt.Errorf("repo: 更新策略失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrStrategyNotFound
	}
	return nil
}

// DeleteStrategy rejects deletion while an active pipeline references the strategy.
func DeleteStrategy(ctx context.Context, db *gorm.DB, id uuidx.UUID) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if _, err := GetStrategy(ctx, tx, id); err != nil {
			return err
		}

		var pipelineCount int64
		if err := tx.WithContext(ctx).Model(&model.Pipeline{}).Where("strategy_id = ?", id).Count(&pipelineCount).Error; err != nil {
			return fmt.Errorf("repo: 统计策略下 pipeline 失败: %w", err)
		}
		if pipelineCount > 0 {
			return fmt.Errorf("%w: 仍有 %d 条 pipeline 引用该策略", ErrStrategyHasDependents, pipelineCount)
		}

		if err := tx.WithContext(ctx).Delete(&model.Strategy{}, "id = ?", id).Error; err != nil {
			return fmt.Errorf("repo: 删除策略失败: %w", err)
		}
		return nil
	})
}

// CreateStrategyVersion 创建策略的下一个版本（需求 MGR-STR-01：版本
// 不可变，修改即生成新版本）。版本号自动取「当前最大版本号 + 1」（首个
// 版本为 1），并在同一事务里把 Strategy.CurrentVersion 推进到这个新版本
// ——新建 pipeline 默认引用 CurrentVersion，而「修改即生成
// 新版本」意味着一次修改操作的落地结果就是新版本立刻成为默认版本；已经
// 显式固定版本号的旧 pipeline 不受影响（它们记录的是创建时的版本号，不
// 是「当前版本」这个可变指针）。
func CreateStrategyVersion(ctx context.Context, db *gorm.DB, strategyID uuidx.UUID, rules model.JSON, createdBy uuidx.UUID) (*model.StrategyVersion, error) {
	var result model.StrategyVersion
	err := db.Transaction(func(tx *gorm.DB) error {
		var locked model.Strategy
		if err := tx.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).First(&locked, "id=?", strategyID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrStrategyNotFound
			}
			return err
		}
		if _, err := GetStrategy(ctx, tx, strategyID); err != nil {
			return err
		}

		var maxVersion uint32
		if err := tx.WithContext(ctx).Model(&model.StrategyVersion{}).
			Where("strategy_id = ?", strategyID).
			Select("COALESCE(MAX(version), 0)").Scan(&maxVersion).Error; err != nil {
			return fmt.Errorf("repo: 查询策略当前最大版本号失败: %w", err)
		}

		sv := model.StrategyVersion{
			StrategyID: strategyID, Version: maxVersion + 1, Rules: rules, CreatedBy: createdBy,
		}
		if err := tx.WithContext(ctx).Create(&sv).Error; err != nil {
			return fmt.Errorf("repo: 创建策略版本失败: %w", err)
		}

		if err := tx.WithContext(ctx).Model(&model.Strategy{}).Where("id = ?", strategyID).
			Update("current_version", sv.Version).Error; err != nil {
			return fmt.Errorf("repo: 推进策略当前版本失败: %w", err)
		}

		result = sv
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// GetStrategyVersion 按 (strategyID, version) 查询某个具体版本（需求
// MGR-STR-01：版本对比需要分别取两个版本的内容）。
func GetStrategyVersion(ctx context.Context, db *gorm.DB, strategyID uuidx.UUID, version uint32) (*model.StrategyVersion, error) {
	var sv model.StrategyVersion
	err := ReadResourceScope(ctx, db.WithContext(ctx), ReadStrategyVersion).First(&sv, "strategy_id = ? AND version = ?", strategyID, version).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrStrategyVersionNotFound
		}
		return nil, fmt.Errorf("repo: 查询策略版本失败: %w", err)
	}
	return &sv, nil
}

// ListStrategyVersions 列出某策略的全部版本（按版本号升序，需求
// MGR-STR-01：版本列表）。
func ListStrategyVersions(ctx context.Context, db *gorm.DB, strategyID uuidx.UUID) ([]model.StrategyVersion, error) {
	var svs []model.StrategyVersion
	if err := ReadResourceScope(ctx, db.WithContext(ctx), ReadStrategyVersion).Where("strategy_id = ?", strategyID).Order("version ASC").Find(&svs).Error; err != nil {
		return nil, fmt.Errorf("repo: 查询策略版本列表失败: %w", err)
	}
	return svs, nil
}

// 两条互相独立的拒绝条件，缺一不可（不是"满足一条就够"，是各自单独判
// 断）：
func DeleteStrategyVersion(ctx context.Context, db *gorm.DB, strategyID uuidx.UUID, version uint32) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if _, err := GetStrategyVersion(ctx, tx, strategyID, version); err != nil {
			return err
		}

		s, err := GetStrategy(ctx, tx, strategyID)
		if err != nil {
			return err
		}
		if s.CurrentVersion == version {
			return fmt.Errorf("%w: 请先发布新版本使 current_version 指向别处，再删除该版本", ErrStrategyVersionIsCurrent)
		}

		var pipelineCount int64
		err = tx.WithContext(ctx).Unscoped().Model(&model.Pipeline{}).
			Where("strategy_id = ? AND strategy_version = ?", strategyID, version).
			Count(&pipelineCount).Error
		if err != nil {
			return fmt.Errorf("repo: 统计策略版本下 pipeline 失败: %w", err)
		}
		if pipelineCount > 0 {
			return fmt.Errorf("%w: 仍有 %d 条 pipeline 引用该版本", ErrStrategyVersionHasDependents, pipelineCount)
		}

		if err := tx.WithContext(ctx).Delete(&model.StrategyVersion{}, "strategy_id = ? AND version = ?", strategyID, version).Error; err != nil {
			return fmt.Errorf("repo: 删除策略版本失败: %w", err)
		}
		return nil
	})
}
