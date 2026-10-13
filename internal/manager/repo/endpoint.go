// Endpoint repository operations validate references and persist connection probe results.
package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/mysqlx"
	"github.com/xmapst/xdts/pkg/uuidx"
)

var (
	// ErrEndpointNotFound 表示端点不存在。
	ErrEndpointNotFound = errors.New("repo: 端点不存在")
	// ErrEndpointVersionConflict 表示更新时乐观锁版本已过期（并发修改）。
	ErrEndpointVersionConflict = errors.New("repo: 端点已被并发修改，请刷新后重试")

	ErrEndpointHasDependents = errors.New("repo: 端点被 pipeline 引用，不能删除")
	// ErrEndpointProbeResultIncomplete 表示探测结果里 version 或 server_uuid
	// 为空，不予写入权威状态（理由见 UpdateEndpointProbeResult 的
	// 「为什么空 version / 空 server_uuid 在这里被挡下」一节）。
	ErrEndpointProbeResultIncomplete = errors.New("repo: 探测结果的 version/server_uuid 为空，拒绝写入")
)

// CreateEndpoint 创建端点（需求 MGR-DS-01）。
func CreateEndpoint(ctx context.Context, db *gorm.DB, e *model.Endpoint) error {
	if err := normalizeResourceName(&e.Name); err != nil {
		return err
	}
	if err := e.Validate(); err != nil {
		return err
	}
	if err := db.WithContext(ctx).Create(e).Error; err != nil {
		if duplicateIndex(err, "uk_endpoints_name") {
			return ErrEndpointNameConflict
		}
		return fmt.Errorf("repo: 创建端点失败: %w", err)
	}
	return nil
}

// GetEndpoint 按主键查询端点。密码字段（Password）随整行一起返回，
// 未做接口层脱敏——「接口只写不读，查询返回脱敏值」是
// internal/manager/http 的职责：本函数只负责读出完整记录，调用方展示给前端前
// 必须调用 model.Endpoint.Redacted()，或者只投影需要的字段。
//
// 已验证：.Redacted() 的生产调用点及其所在出口。未验证：将来新增端点出口时是否
// 还会记得调——这一条今天只有注释级保护，没有任何测试断言「每条出口都脱敏」。
func GetEndpoint(ctx context.Context, db *gorm.DB, id uuidx.UUID) (*model.Endpoint, error) {
	var e model.Endpoint
	if err := ReadResourceScope(ctx, db.WithContext(ctx), ReadEndpoint).First(&e, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrEndpointNotFound
		}
		return nil, fmt.Errorf("repo: 查询端点失败: %w", err)
	}
	return &e, nil
}

func ListEndpoints(ctx context.Context, db *gorm.DB, centerID uuidx.UUID) ([]model.Endpoint, error) {
	q := ReadResourceScope(ctx, db.WithContext(ctx), ReadEndpoint).Order("created_at ASC")
	if !centerID.IsZero() {
		q = q.Where("center_id = ?", centerID)
	}
	var es []model.Endpoint
	if err := q.Find(&es).Error; err != nil {
		return nil, fmt.Errorf("repo: 查询端点列表失败: %w", err)
	}
	return es, nil
}

// UpdateEndpoint 更新端点的连接信息，用
// e.LockVersion 做乐观锁。不包括 center_id（换中心需要专门流程，理由与
// UpdateNode 一致）、version / server_uuid / probe_node_id / last_probe_at /
// last_probe_result（这些字段由探测回写专属函数
// UpdateEndpointProbeResult 维护，不接受管理员直接改写）。
func UpdateEndpoint(ctx context.Context, db *gorm.DB, e *model.Endpoint) error {
	if err := normalizeResourceName(&e.Name); err != nil {
		return err
	}
	if err := e.Validate(); err != nil {
		return err
	}
	res := db.WithContext(ctx).Model(&model.Endpoint{}).
		Where("id = ? AND lock_version = ?", e.ID, e.LockVersion).
		Updates(map[string]any{
			"name":     e.Name,
			"host":     e.Host,
			"port":     e.Port,
			"username": e.Username,
			"password": e.Password,
			"tls":      e.TLS,
			"params":   e.Params,
			// UpdateEndpoint 和探测回写（UpdateEndpointProbeResult）
			// 会并发命中同一行（设计对 LockVersion 字段的注释），因此这里
			// 和探测回写一样，都要经过 lock_version 校验并递增，任何一方
			// 落后就会拿到 ErrEndpointVersionConflict，需要重新读取。
			"lock_version": gorm.Expr("lock_version + 1"),
		})
	if res.Error != nil {
		if duplicateIndex(res.Error, "uk_endpoints_name") {
			return ErrEndpointNameConflict
		}
		return fmt.Errorf("repo: 更新端点失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		if _, err := GetEndpoint(ctx, db, e.ID); err != nil {
			return err
		}
		return ErrEndpointVersionConflict
	}
	return nil
}

// EndpointProbeResult carries verified endpoint metadata and connection capabilities.
type EndpointProbeResult struct {
	ProbeNodeID         uuidx.UUID
	ProbedAt            time.Time
	ExpectedLockVersion int64
	Version             string
	ServerUUID          string
	GroupUUID           string
	LowerCaseTableNames mysqlx.LowerCaseTableNames
	Detail              model.JSON
}

// UpdateEndpointProbeResult atomically writes a successful probe's metadata.
// Probe results use the expected row revision to reject concurrent endpoint edits.
func UpdateEndpointProbeResult(ctx context.Context, db *gorm.DB, id uuidx.UUID, r EndpointProbeResult) error {
	if r.Version == "" || r.ServerUUID == "" {
		return fmt.Errorf("%w (endpoint_id=%s, version_empty=%t, server_uuid_empty=%t)",
			ErrEndpointProbeResultIncomplete, id.String(), r.Version == "", r.ServerUUID == "")
	}

	lowerCaseTableNames := ""
	if r.LowerCaseTableNames != mysqlx.LowerCaseTableNamesUnspecified {
		lowerCaseTableNames = r.LowerCaseTableNames.String()
	}

	serverID, err := parseEndpointOrigin(r.ServerUUID)
	if err != nil {
		return err
	}
	var groupID uuidx.UUID
	if r.GroupUUID != "" {
		groupID, err = parseEndpointOrigin(r.GroupUUID)
		if err != nil || groupID == serverID {
			return ErrEndpointOriginInvalid
		}
	}
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var endpoint model.Endpoint
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&endpoint, "id=?", id).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrEndpointNotFound
			}
			return err
		}
		if endpoint.LastProbeAt != nil && endpoint.LastProbeAt.After(r.ProbedAt) {
			return nil
		}
		if endpoint.LockVersion != r.ExpectedLockVersion {
			return ErrEndpointVersionConflict
		}
		node, err := GetNode(ctx, tx, r.ProbeNodeID)
		if err != nil || node.CenterID != endpoint.CenterID || node.Status != model.NodeStatusOnline {
			return ErrEndpointProbeNodeInvalid
		}
		if _, err := GetCenter(ctx, tx, endpoint.CenterID); err != nil {
			return err
		}
		if err := registerEndpointOrigin(ctx, tx, &endpoint, serverID, model.OriginKindServer); err != nil {
			return err
		}
		if !groupID.IsZero() {
			if err := registerEndpointOrigin(ctx, tx, &endpoint, groupID, model.OriginKindGroup); err != nil {
				return err
			}
		}
		return tx.Model(&model.Endpoint{}).Where("id=?", id).Updates(map[string]any{
			"probe_node_id": r.ProbeNodeID, "last_probe_at": r.ProbedAt,
			"last_probe_result": r.Detail, "version": r.Version,
			"server_uuid": serverID.String(), "lower_case_table_names": lowerCaseTableNames,
			"lock_version": gorm.Expr("lock_version + 1"),
		}).Error
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}

// UpdateEndpointProbeFailure 记录一次失败尝试，同时保留上一次有效的实例参数。
func UpdateEndpointProbeFailure(ctx context.Context, db *gorm.DB, id, nodeID uuidx.UUID, probedAt time.Time, detail model.JSON) error {
	res := db.WithContext(ctx).Model(&model.Endpoint{}).
		Where("id = ? AND (last_probe_at IS NULL OR last_probe_at <= ?)", id, probedAt).
		Updates(map[string]any{
			"probe_node_id":     nodeID,
			"last_probe_at":     probedAt,
			"last_probe_result": detail,
			"lock_version":      gorm.Expr("lock_version + 1"),
		})
	if res.Error != nil {
		return fmt.Errorf("repo: 写回探测失败结果失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		if _, err := GetEndpoint(ctx, db, id); err != nil {
			return err
		}
	}
	return nil
}

// DeleteEndpoint rejects deletion while an active pipeline references the endpoint.
func DeleteEndpoint(ctx context.Context, db *gorm.DB, id uuidx.UUID) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if _, err := GetEndpoint(ctx, tx, id); err != nil {
			return err
		}

		var pipelineCount int64
		err := tx.WithContext(ctx).Unscoped().Model(&model.Pipeline{}).
			Where(`
				NOT (deleted_at IS NOT NULL AND cleanup_state = 'done')
				AND (source_endpoint_id = ? OR target_endpoint_id = ? OR `+pipelineReplicaEndpointReference+`)`, id, id, id.String()).
			Count(&pipelineCount).Error
		if err != nil {
			return fmt.Errorf("repo: 统计端点下 pipeline 失败: %w", err)
		}
		if pipelineCount > 0 {
			return fmt.Errorf("%w: 仍有 %d 条 pipeline 引用该端点（含待清理未完成的）", ErrEndpointHasDependents, pipelineCount)
		}

		if err := tx.WithContext(ctx).Delete(&model.Endpoint{}, "id = ?", id).Error; err != nil {
			return fmt.Errorf("repo: 删除端点失败: %w", err)
		}
		return nil
	})
}
