// 只做仓储层：CRUD + 唯一性校验 + 删除校验，不假设调用方的调用形态。
// HTTP 路由在 `internal/manager/http`。
package repo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
)

var (
	// ErrCenterNotFound 表示中心不存在。
	ErrCenterNotFound = errors.New("repo: 中心不存在")
	// ErrCenterCodeConflict 表示中心编码（code）与已有记录冲突（需求
	// MGR-CTR-01：编码唯一，用于路由键）。
	ErrCenterCodeConflict = errors.New("repo: 中心编码已存在")
	// ErrCenterVersionConflict 表示更新时乐观锁版本已过期（并发修改）。
	ErrCenterVersionConflict = errors.New("repo: 中心已被并发修改，请刷新后重试")
	// ErrCenterHasDependents 表示中心下仍有归属的端点 / 节点 /
	// pipeline，不能删除（需求 MGR-CTR-04）。
	ErrCenterHasDependents = errors.New("repo: 中心下仍有归属的端点、节点或 pipeline，不能删除")

	// ErrOriginUUIDNotFound 表示来源身份记录不存在。
	ErrOriginUUIDNotFound = errors.New("repo: 来源身份 UUID 不存在")
	// ErrOriginUUIDConflict 表示该 UUID 已被登记（需求 MGR-DS-03：任何两个
	// 实例的 server_uuid 不得相同，也不得与任何合成 UUID 相同）。
	// center_origin_uuids.uuid 上的全局唯一索引覆盖全部 Kind，因此本错误
	// 同时代表「与另一个 server_uuid 冲突」和「与某个合成 UUID 冲突」两种
	// 情况，不需要也不应该分开判断。
	ErrOriginUUIDConflict = errors.New("repo: 该 UUID 已被登记为来源身份，冲突拒绝保存")
)

// CreateCenter 创建中心（需求 MGR-CTR-01）。code 唯一，冲突时返回
// ErrCenterCodeConflict。
func CreateCenter(ctx context.Context, db *gorm.DB, c *model.Center) error {
	if err := normalizeResourceName(&c.Name); err != nil {
		return err
	}
	if err := db.WithContext(ctx).Create(c).Error; err != nil {
		if duplicateIndex(err, "uk_centers_name") {
			return ErrCenterNameConflict
		}
		if isDuplicateKeyError(err) {
			return fmt.Errorf("%w: code=%q", ErrCenterCodeConflict, c.Code)
		}
		return fmt.Errorf("repo: 创建中心失败: %w", err)
	}
	return nil
}

// GetCenter 按主键查询中心。
func GetCenter(ctx context.Context, db *gorm.DB, id uuidx.UUID) (*model.Center, error) {
	var c model.Center
	if err := ReadResourceScope(ctx, db.WithContext(ctx), ReadCenter).First(&c, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrCenterNotFound
		}
		return nil, fmt.Errorf("repo: 查询中心失败: %w", err)
	}
	return &c, nil
}

// GetCenterByCode 按编码查询中心（路由键场景，如联邦绑定、跨中心 RPC
// 寻址）。
func GetCenterByCode(ctx context.Context, db *gorm.DB, code string) (*model.Center, error) {
	var c model.Center
	if err := db.WithContext(ctx).First(&c, "code = ?", code).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrCenterNotFound
		}
		return nil, fmt.Errorf("repo: 按编码查询中心失败: %w", err)
	}
	return &c, nil
}

// ListCenters 按 (created_at, code) 升序返回中心。code 唯一，用于稳定排序时间戳相同的行。
func ListCenters(ctx context.Context, db *gorm.DB) ([]model.Center, error) {
	var cs []model.Center
	if err := ReadResourceScope(ctx, db.WithContext(ctx), ReadCenter).Order("created_at ASC, code ASC").Find(&cs).Error; err != nil {
		return nil, fmt.Errorf("repo: 查询中心列表失败: %w", err)
	}
	return cs, nil
}

// UpdateCenter 更新中心（需求 MGR-CTR-01），用 c.LockVersion 做乐观锁：
// 只有 lock_version 与调用方持有的一致才会更新成功，并把 lock_version
// 原子加一。c 必须是先前 Get 得到、未经修改字段类型的完整记录（本函数按
// 白名单更新可编辑字段，不包括 id / created_at / deleted_at）。
func UpdateCenter(ctx context.Context, db *gorm.DB, c *model.Center) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		current, err := GetCenter(ctx, tx, c.ID)
		if err != nil {
			return err
		}
		if current.Code != c.Code {
			// A code rename changes origin-policy interpretation in every graph.
			// Lock roots in one ordered query, including graphs whose first edge
			// is concurrently being created. Lifecycle and edge writes lock roots
			// before touching pipeline rows, so the lock order stays consistent.
			var roots []model.Topology
			if err := tx.Select("id").Clauses(clause.Locking{Strength: "UPDATE"}).Order("id ASC").Find(&roots).Error; err != nil {
				return err
			}
		}
		return updateCenter(ctx, tx, c)
	})
}

func updateCenter(ctx context.Context, db *gorm.DB, c *model.Center) error {
	if err := normalizeResourceName(&c.Name); err != nil {
		return err
	}
	res := db.WithContext(ctx).Model(&model.Center{}).
		Where("id = ? AND lock_version = ?", c.ID, c.LockVersion).
		Updates(map[string]any{
			"code":              c.Code,
			"name":              c.Name,
			"region":            c.Region,
			"is_manager_center": c.IsManagerCenter,
			"mq_mgmt_url":       c.MQMgmtURL,
			"mq_mgmt_user":      c.MQMgmtUser,
			"mq_mgmt_password":  c.MQMgmtPassword,
			"inspector_node_id": c.InspectorNodeID,
			"description":       c.Description,
			"lock_version":      gorm.Expr("lock_version + 1"),
		})
	if res.Error != nil {
		if duplicateIndex(res.Error, "uk_centers_name") {
			return ErrCenterNameConflict
		}
		if isDuplicateKeyError(res.Error) {
			return fmt.Errorf("%w: code=%q", ErrCenterCodeConflict, c.Code)
		}
		return fmt.Errorf("repo: 更新中心失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		if _, err := GetCenter(ctx, db, c.ID); err != nil {
			return err // ErrCenterNotFound 或查询失败
		}
		return ErrCenterVersionConflict
	}
	return nil
}

// SetCenterInspectorNode updates automatic probe assignment without changing
// the user's configuration timestamp.
func SetCenterInspectorNode(ctx context.Context, db *gorm.DB, id uuidx.UUID, inspectorNodeID uuidx.UUID, lockVersion int64) error {
	res := db.WithContext(ctx).Model(&model.Center{}).
		Where("id = ? AND lock_version = ?", id, lockVersion).
		Updates(map[string]any{
			"inspector_node_id": inspectorNodeID,
			"lock_version":      gorm.Expr("lock_version + 1"),
		})
	if res.Error != nil {
		return fmt.Errorf("repo: 设置中心巡检节点失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		if _, err := GetCenter(ctx, db, id); err != nil {
			return err // ErrCenterNotFound 或查询失败
		}
		return ErrCenterVersionConflict
	}
	return nil
}

// RecordCenterBrokerProbeResult updates observations without changing the user's
// center configuration timestamp.
func RecordCenterBrokerProbeResult(ctx context.Context, db *gorm.DB, id uuidx.UUID, at time.Time, result model.JSON) error {
	res := db.WithContext(ctx).Model(&model.Center{}).Where("id = ?", id).
		Updates(map[string]any{
			"last_broker_probe_at":     at,
			"last_broker_probe_result": result,
		})
	if res.Error != nil {
		return fmt.Errorf("repo: 写回中心 Broker 巡检结果失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrCenterNotFound
	}
	return nil
}

// DeleteCenter 删除中心前做需求 MGR-CTR-04 的校验：无归属的端点、
// 节点、pipeline 时才允许删除，否则拒绝并返回 ErrCenterHasDependents。
// 软删除（沿用 model.Center.DeletedAt），不做级联删除。
func DeleteCenter(ctx context.Context, db *gorm.DB, id uuidx.UUID) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if _, err := GetCenter(ctx, tx, id); err != nil {
			return err
		}

		var deCount int64
		if err := tx.WithContext(ctx).Model(&model.Endpoint{}).Where("center_id = ?", id).Count(&deCount).Error; err != nil {
			return fmt.Errorf("repo: 统计中心下端点失败: %w", err)
		}
		if deCount > 0 {
			return fmt.Errorf("%w: 仍有 %d 个端点归属该中心", ErrCenterHasDependents, deCount)
		}

		var nodeCount int64
		if err := tx.WithContext(ctx).Model(&model.Node{}).Where("center_id = ?", id).Count(&nodeCount).Error; err != nil {
			return fmt.Errorf("repo: 统计中心下节点失败: %w", err)
		}
		if nodeCount > 0 {
			return fmt.Errorf("%w: 仍有 %d 个节点归属该中心", ErrCenterHasDependents, nodeCount)
		}

		var pipelineCount int64
		err := tx.WithContext(ctx).Unscoped().Model(&model.Pipeline{}).
			Where(`
				NOT (deleted_at IS NOT NULL AND cleanup_state = 'done')
				AND (
					source_endpoint_id IN (SELECT id FROM endpoints WHERE center_id = ?) OR
					target_endpoint_id IN (SELECT id FROM endpoints WHERE center_id = ?) OR
					source_node_id IN (SELECT id FROM nodes WHERE center_id = ?) OR
					target_node_id IN (SELECT id FROM nodes WHERE center_id = ?)
				)`, id, id, id, id).
			Count(&pipelineCount).Error
		if err != nil {
			return fmt.Errorf("repo: 统计中心下 pipeline 失败: %w", err)
		}
		if pipelineCount > 0 {
			return fmt.Errorf("%w: 仍有 %d 条 pipeline 归属该中心（含待清理未完成的）", ErrCenterHasDependents, pipelineCount)
		}

		if err := tx.WithContext(ctx).Delete(&model.Center{}, "id = ?", id).Error; err != nil {
			return fmt.Errorf("repo: 删除中心失败: %w", err)
		}
		return nil
	})
}

// CreateCenterOriginUUID 登记一条来源身份（需求 MGR-CTR-02）。uuid 全局
// 唯一（需求 MGR-DS-03：不论是 server_uuid 探测汇入、手工补充，还是全量 /
// 结构迁移分配的合成 UUID，都落在同一张表、同一个唯一索引下），冲突时
// 返回 ErrOriginUUIDConflict，不落库。
func CreateCenterOriginUUID(ctx context.Context, db *gorm.DB, rec *model.CenterOriginUUID) error {
	if err := rec.Validate(); err != nil {
		return err
	}
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(rec).Error; err != nil {
			if isDuplicateKeyError(err) {
				return fmt.Errorf("%w: uuid=%s", ErrOriginUUIDConflict, rec.UUID)
			}
			return fmt.Errorf("repo: 登记来源身份失败: %w", err)
		}
		return markOriginRefresh(ctx, tx, rec.CenterID)
	})
}

// GetCenterOriginUUID 按主键查询来源身份记录。
func GetCenterOriginUUID(ctx context.Context, db *gorm.DB, id uuidx.UUID) (*model.CenterOriginUUID, error) {
	var rec model.CenterOriginUUID
	if err := db.WithContext(ctx).First(&rec, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrOriginUUIDNotFound
		}
		return nil, fmt.Errorf("repo: 查询来源身份失败: %w", err)
	}
	return &rec, nil
}

// GetCenterOriginUUIDByUUID 按 UUID 取值查询来源身份记录，供回环识别
// （ADR-01）按来源 UUID 反查所属中心使用。
func GetCenterOriginUUIDByUUID(ctx context.Context, db *gorm.DB, uuid uuidx.UUID) (*model.CenterOriginUUID, error) {
	var rec model.CenterOriginUUID
	if err := db.WithContext(ctx).First(&rec, "uuid = ?", uuid).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrOriginUUIDNotFound
		}
		return nil, fmt.Errorf("repo: 按 UUID 查询来源身份失败: %w", err)
	}
	return &rec, nil
}

// ListCenterOriginUUIDs 列出某中心的来源身份集合（需求 MGR-CTR-02：随配置
// 下发给所有相关节点），按 (created_at, uuid) 升序。
//
// **这里的定序是正确性要求，不是整洁**：本函数的结果经
// `internal/manager/pipelinespec` 的 `buildOriginMap`（它直接调用本函数）进入
// 配置组装，而那个包对组装结果算
// computeConfigDigest（json.Marshal，切片按下标序）并用 proto.Equal 比较。
// 顺序变一次 ⇒ digest 变、proto.Equal 不等 ⇒ 配置内容一字未改却触发一次版本
// bump 与一次节点热更新（热更新是"先卸载再重装"）。与 GetTopologyGraph 的
// centers 是同一族缺陷，详见那边的注释。
//
// created_at 单列不是全序（DATETIME(3)，同一批插入必然全并列，实测见
// ListCenters 的注释），故补 `uuid` 作次键——它有 uk_center_origin_uuids_uuid
// 唯一约束，且同样不是主键，所以这条 ORDER BY 被删掉时断言能红。
func ListCenterOriginUUIDs(ctx context.Context, db *gorm.DB, centerID uuidx.UUID) ([]model.CenterOriginUUID, error) {
	var recs []model.CenterOriginUUID
	if err := ReadResourceScope(ctx, db.WithContext(ctx), ReadCenterOrigin).Where("center_id = ?", centerID).Order("created_at ASC, uuid ASC").Find(&recs).Error; err != nil {
		return nil, fmt.Errorf("repo: 查询来源身份列表失败: %w", err)
	}
	return recs, nil
}

// DeleteCenterOriginUUID 删除一条来源身份记录（硬删除：该表没有
// deleted_at 字段，来源身份被移除即应立刻停止参与回环识别，不需要软删除
// 语义）。
func DeleteCenterOriginUUID(ctx context.Context, db *gorm.DB, id uuidx.UUID) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		record, err := GetCenterOriginUUID(ctx, tx, id)
		if err != nil {
			return err
		}
		res := tx.Delete(&model.CenterOriginUUID{}, "id = ?", id)
		if res.Error != nil {
			return fmt.Errorf("repo: 删除来源身份失败: %w", res.Error)
		}
		if res.RowsAffected == 0 {
			return ErrOriginUUIDNotFound
		}
		return markOriginRefresh(ctx, tx, record.CenterID)
	})
}
