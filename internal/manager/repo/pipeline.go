// 只做仓储层：CRUD + 状态/配置版本专属写入点 + 两步删除。
// 本文件不实现完整的 pipeline 状态机编排；run_id/合成 UUID 重新分配由 internal/manager/repo/reinit.go 的
// 确认事务实现（包括具体目标表处理计划与审计；internal/manager/http/reinit.go 接入预览/确认，
// dispatch 下发节点 REINIT 命令），需要
// 联动 center_origin_uuids 登记新的合成 UUID，跨越 pipeline 与 center_origin_uuids
// 两张表）。
package repo

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
)

var (
	ErrPipelineNotFound = errors.New("repo: pipeline 不存在")
	// ErrPipelineNameConflict 表示 pipeline 名称与已有记录冲突
	// （uk_pipelines_name）。
	ErrPipelineNameConflict = errors.New("repo: pipeline 名称已存在")
	// ErrPipelineConfigVersionConflict 表示更新配置时 config_version 已
	// 过期（并发修改，或调用方持有的是旧版本）。
	ErrPipelineConfigVersionConflict = errors.New("repo: pipeline 配置已被并发修改，请刷新后重试")
	// ErrPipelineBinlogServerIDConflict 表示分配的 server_id 已被另一条
	// pipeline 占用（uk_pipelines_binlog_server_id，需求 MGR-PL-06）。
	ErrPipelineBinlogServerIDConflict = errors.New("repo: binlog_server_id 已被占用")
	// ErrPipelineCmdSeqConflict 表示递增 cmd_seq 时调用方持有的
	// expectedCmdSeq 已过期（并发调用，或调用方两次读取之间已经有别的
	// 调用者分配过新值）——与 ErrPipelineConfigVersionConflict 是两个
	// 独立的错误，不复用同一个：二者对应的是两个不相关的列，混用会让
	// 调用方无法分辨到底哪一列冲突了。
	ErrPipelineCmdSeqConflict           = errors.New("repo: pipeline cmd_seq 已被并发分配，请重新获取当前值后重试")
	ErrPipelineMigrationRequiresStopped = errors.New("repo: pipeline 必须停止后才能迁移节点")
	ErrPipelineDeleteRequiresStopped    = errors.New("repo: 请先停止 pipeline 并等待两端停止后再删除")
	ErrPipelineMigrationNodeUnavailable = errors.New("repo: 目标节点不可用于迁移")
	ErrPipelineMigrationCenterMismatch  = errors.New("repo: 目标节点与端点不在同一中心")
	ErrPipelineMigrationSameNode        = errors.New("repo: 目标节点与当前节点相同")
	ErrPipelineMigrationCapability      = errors.New("repo: 迁移两端节点必须显式支持 node_migration_v1")
	ErrPipelineMigrationSnapshotState   = errors.New("repo: 全量运行的持久计划与完成边界尚未支持节点交接")
)

// PipelineNodeMigration 是一次同中心节点迁移提交后的事实。
type PipelineNodeMigration struct {
	PipelineID    uuidx.UUID
	Side          model.Side
	OldNodeID     uuidx.UUID
	NewNodeID     uuidx.UUID
	ConfigVersion uint64
}

// MigratePipelineNode 在持有 pipeline 行锁的事务中替换一侧节点。迁移只
// 修改节点归属和配置版本，保留 run/checkpoint/endpoint；运行中的 pipeline
// 必须先停止，节点切换由上层在提交后发送配置变更通知完成。
func MigratePipelineNode(ctx context.Context, db *gorm.DB, id uuidx.UUID, side model.Side, newNodeID uuidx.UUID, expectedConfigVersion uint64) (*PipelineNodeMigration, error) {
	return MigratePipelineNodeWithState(ctx, db, id, side, newNodeID, expectedConfigVersion, nil)
}

// MigratePipelineNodeWithState accepts an authenticated old-role export for a
// previously started full run. Its durable handover and assignment are one CAS.
func MigratePipelineNodeWithState(ctx context.Context, db *gorm.DB, id uuidx.UUID, side model.Side, newNodeID uuidx.UUID, expectedConfigVersion uint64, bundle *iproto.PipelineStateBundle) (*PipelineNodeMigration, error) {
	if !side.Valid() {
		return nil, fmt.Errorf("model: 未知的 side %q", side)
	}
	if newNodeID.IsZero() {
		return nil, ErrNodeNotFound
	}
	var result PipelineNodeMigration
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var p model.Pipeline
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&p, "id = ?", id).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrPipelineNotFound
			}
			return err
		}
		if p.ConfigVersion != expectedConfigVersion {
			return ErrPipelineConfigVersionConflict
		}
		if p.ReinitPending || p.DesiredState != model.DesiredStateStopped {
			return ErrPipelineMigrationRequiresStopped
		}
		needsState := p.LastCmdSeq > 0 && (p.InitMode == model.InitModeFullAndIncremental || p.InitMode == model.InitModeFullOnly)
		if needsState && bundle == nil || !needsState && bundle != nil {
			return ErrPipelineMigrationSnapshotState
		}
		oldNodeID := p.SourceNodeID
		endpointID := p.SourceEndpointID
		actual := p.SourceActualState
		if side == model.SideTarget {
			oldNodeID = p.TargetNodeID
			endpointID = p.TargetEndpointID
			actual = p.TargetActualState
		}
		if oldNodeID == newNodeID {
			return ErrPipelineMigrationSameNode
		}
		if actual != model.ActualStateDraft && actual != model.ActualStateStopped {
			return ErrPipelineMigrationRequiresStopped
		}
		for _, state := range []model.ActualState{p.SourceActualState, p.TargetActualState} {
			if state != model.ActualStateDraft && state != model.ActualStateStopped {
				return ErrPipelineMigrationRequiresStopped
			}
		}

		var endpoint model.Endpoint
		if err := tx.First(&endpoint, "id = ?", endpointID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrEndpointNotFound
			}
			return err
		}
		var migrationNodes []model.Node
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id IN ?", []uuidx.UUID{oldNodeID, newNodeID}).Order("id").Find(&migrationNodes).Error; err != nil {
			return err
		}
		if len(migrationNodes) != 2 {
			return ErrNodeNotFound
		}
		if needsState {
			if err := validateMigrationState(&p, side, oldNodeID, newNodeID, bundle); err != nil {
				return err
			}
		}
		var creationRequired, dependenciesRequired, recoveryRequired bool
		for _, candidate := range migrationNodes {
			if candidate.ID == oldNodeID && p.LastCmdSeq > 0 {
				var capabilities []string
				if json.Unmarshal(candidate.Capabilities, &capabilities) == nil && slices.Contains(capabilities, iproto.CapabilityDDLSourceDependencies) {
					dependenciesRequired = true
				}
			}
		}
		if needsState {
			for _, candidate := range migrationNodes {
				if candidate.ID == oldNodeID {
					var capabilities []string
					if json.Unmarshal(candidate.Capabilities, &capabilities) == nil && slices.Contains(capabilities, iproto.CapabilitySnapshotTableRecovery) {
						recoveryRequired = true
					}
					if json.Unmarshal(candidate.Capabilities, &capabilities) == nil && slices.Contains(capabilities, iproto.CapabilitySnapshotTableCreation) {
						creationRequired = true
					}
				}
			}
			for _, record := range bundle.GetRecords() {
				if record.GetCollection() == "snapshot_runs" || record.GetCollection() == "snapshot_chunks" || record.GetCollection() == "snapshot_targets" {
					if strings.Contains(record.GetId(), ".recovery.") || strings.Contains(record.GetId(), ".recovery_deferred.") {
						recoveryRequired = true
					}
				}
				if record.GetCollection() == "snapshot_runs" && strings.Contains(record.GetId(), ".creation.") {
					creationRequired = true
					break
				}
			}
		}
		var node model.Node
		for _, candidate := range migrationNodes {
			var capabilities []string
			if json.Unmarshal(candidate.Capabilities, &capabilities) != nil || !slices.Contains(capabilities, iproto.CapabilityNodeMigration) {
				return ErrPipelineMigrationCapability
			}
			if creationRequired && !slices.Contains(capabilities, iproto.CapabilitySnapshotTableCreation) {
				return ErrPipelineMigrationCapability
			}
			if recoveryRequired && !slices.Contains(capabilities, iproto.CapabilitySnapshotTableRecovery) {
				return ErrPipelineMigrationCapability
			}
			if dependenciesRequired && !slices.Contains(capabilities, iproto.CapabilityDDLSourceDependencies) {
				return ErrPipelineMigrationCapability
			}
			if p.InitMode == model.InitModeFullOnly && side == model.SideTarget && !slices.Contains(capabilities, iproto.CapabilityFullOnlyStop) {
				return ErrPipelineMigrationCapability
			}
			if needsState && (!slices.Contains(capabilities, iproto.CapabilitySnapshotMigration) || candidate.Status != model.NodeStatusOnline) {
				return ErrPipelineMigrationCapability
			}
			if candidate.ID == newNodeID {
				node = candidate
			}
		}
		if node.CenterID != endpoint.CenterID {
			return ErrPipelineMigrationCenterMismatch
		}
		if node.DeletedAt.Valid || node.Status == model.NodeStatusDisabled || node.Status == model.NodeStatusRevoked || node.Status == model.NodeStatusOffline || node.Status == model.NodeStatusRegistering {
			return ErrPipelineMigrationNodeUnavailable
		}
		if node.MaxPipelines != 0 {
			var count int64
			if err := tx.Unscoped().Model(&model.Pipeline{}).
				Where("id <> ? AND (source_node_id = ? OR target_node_id = ?) AND (deleted_at IS NULL OR cleanup_state <> ?)", id, newNodeID, newNodeID, model.CleanupStateDone).
				Count(&count).Error; err != nil {
				return err
			}
			if count >= int64(node.MaxPipelines) {
				return &NodePipelineQuotaError{NodeID: newNodeID, Limit: node.MaxPipelines, Assigned: int(count)}
			}
		}

		if needsState {
			data, err := proto.MarshalOptions{Deterministic: true}.Marshal(bundle)
			if err != nil {
				return err
			}
			stored := model.PipelineStateBundle{PipelineID: id, Side: side, Data: data}
			if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "pipeline_id"}, {Name: "side"}}, DoUpdates: clause.AssignmentColumns([]string{"bundle"})}).Create(&stored).Error; err != nil {
				return err
			}
		}
		column := "source_node_id"
		if side == model.SideTarget {
			column = "target_node_id"
		} else if err := transferSourceRecovery(tx, &p, newNodeID); err != nil {
			return err
		}
		if err := tx.Model(&model.Pipeline{}).Where("id = ? AND config_version = ?", id, expectedConfigVersion).Updates(map[string]any{column: newNodeID, "config_version": gorm.Expr("config_version + 1")}).Error; err != nil {
			return err
		}
		result = PipelineNodeMigration{PipelineID: id, Side: side, OldNodeID: oldNodeID, NewNodeID: newNodeID, ConfigVersion: expectedConfigVersion + 1}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("repo: 迁移 pipeline 节点失败: %w", err)
	}
	return &result, nil
}

// CreatePipeline 创建流水线，默认无待回收资源。
// 调用方必须显式提供源端和目标端实际状态；校验完成后在拓扑锁保护下插入。
func CreatePipeline(ctx context.Context, db *gorm.DB, p *model.Pipeline) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := LockTopology(ctx, tx, p.TopologyID); err != nil {
			return err
		}
		if err := CheckBidirectionalInitialization(ctx, tx, p, false); err != nil {
			return err
		}
		return createPipeline(ctx, tx, p)
	})
}

func createPipeline(ctx context.Context, db *gorm.DB, p *model.Pipeline) error {
	if err := normalizeResourceName(&p.Name); err != nil {
		return err
	}
	if p.CleanupState == "" {
		p.CleanupState = model.CleanupStateDone
	}
	if err := p.Validate(); err != nil {
		return err
	}
	if err := checkPipelineNodeQuotas(ctx, db, p); err != nil {
		return err
	}
	if err := db.WithContext(ctx).Create(p).Error; err != nil {
		if isDuplicateKeyError(err) {
			return fmt.Errorf("%w: name=%q", ErrPipelineNameConflict, p.Name)
		}
		return fmt.Errorf("repo: 创建 pipeline 失败: %w", err)
	}
	return nil
}

func GetPipeline(ctx context.Context, db *gorm.DB, id uuidx.UUID) (*model.Pipeline, error) {
	var p model.Pipeline
	if err := db.WithContext(ctx).First(&p, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrPipelineNotFound
		}
		return nil, fmt.Errorf("repo: 查询 pipeline 失败: %w", err)
	}
	return &p, nil
}

// ListPipelines 列出 pipeline（需求 MGR-PL-02：列表）。topologyID 为
// uuidx.Nil 时不按拓扑过滤。不含已软删除的记录。
func ListPipelines(ctx context.Context, db *gorm.DB, topologyID uuidx.UUID) ([]model.Pipeline, error) {
	q := ReadResourceScope(ctx, db.WithContext(ctx), ReadPipeline).Order("created_at ASC")
	if !topologyID.IsZero() {
		q = q.Where("topology_id = ?", topologyID)
	}
	var ps []model.Pipeline
	if err := q.Find(&ps).Error; err != nil {
		return nil, fmt.Errorf("repo: 查询 pipeline 列表失败: %w", err)
	}
	return ps, nil
}

// ListPipelinesByNode 列出某节点当前承载的 pipeline：条件是
// source_node_id = ? OR target_node_id = ?，只走 gorm 默认 scope（不含
// 已软删除的记录）。
func ListPipelinesByNode(ctx context.Context, db *gorm.DB, nodeID uuidx.UUID) ([]model.Pipeline, error) {
	var ps []model.Pipeline
	err := ReadResourceScope(ctx, db.WithContext(ctx), ReadPipeline).
		Where("source_node_id = ? OR target_node_id = ?", nodeID, nodeID).
		Order("created_at ASC").
		Find(&ps).Error
	if err != nil {
		return nil, fmt.Errorf("repo: 查询节点承载的 pipeline 失败: %w", err)
	}
	return ps, nil
}

// 一条 pipeline 的 source/target 恰好引用同一个端点时，只返回一条记录、
// 不是两条：OR 条件命中同一行不会重复选出该行，这是 SQL WHERE 求值的
// 基本行为，不是本函数额外去重的结果——但如果将来被改写成「按源端查一次
// 再按目标端查一次、结果相加」或者改成两次 JOIN，这个性质就可能被破坏，
// 测试专门钉了这一点，改动前先确认没有破坏它。
func ListPipelinesByEndpoint(ctx context.Context, db *gorm.DB, endpointID uuidx.UUID) ([]model.Pipeline, error) {
	var ps []model.Pipeline
	err := db.WithContext(ctx).
		Where("source_endpoint_id = ? OR target_endpoint_id = ? OR "+pipelineReplicaEndpointReference, endpointID, endpointID, endpointID.String()).
		Order("created_at ASC").
		Find(&ps).Error
	if err != nil {
		return nil, fmt.Errorf("repo: 查询端点关联的 pipeline 失败: %w", err)
	}
	return ps, nil
}

// ListPipelinesNeedingReconciliation selects pipelines with a desired/observed
// mismatch on either role. Unknown observations do not establish convergence.
func ListPipelinesNeedingReconciliation(ctx context.Context, db *gorm.DB) ([]model.Pipeline, error) {
	var ps []model.Pipeline
	// 将两侧不一致条件作为整体 OR 分组，同时保留软删除过滤。
	err := db.WithContext(ctx).
		Where("((desired_state <> source_actual_state AND source_actual_state <> ?)"+
			" OR (desired_state <> target_actual_state AND target_actual_state <> ?)"+
			" OR (reinit_pending = TRUE AND (source_actual_state <> ? OR target_actual_state <> ?)))",
			model.ActualStateUnknown, model.ActualStateUnknown,
			model.ActualStateUnknown, model.ActualStateUnknown).
		Order("created_at ASC").
		Find(&ps).Error
	if err != nil {
		return nil, fmt.Errorf("repo: 查询待对账 pipeline 列表失败: %w", err)
	}
	return ps, nil
}

// PipelineConfigUpdate 是 pipeline 可编辑配置字段的白名单（需求
// MGR-PL-02）。不含 topology_id / source_*/target_* endpoint、node
// （定义 pipeline 身份的端点，创建后不可变——改端点等同于创建一条新
// pipeline，不通过通用更新完成）。
type PipelineConfigUpdate struct {
	Name            string
	StrategyID      uuidx.UUID
	StrategyVersion uint32
	InitMode        model.InitMode
	StartPosition   model.JSON
}

// UpdatePipelineConfig 更新 pipeline 配置（需求 MGR-PL-02），用
// expectedConfigVersion 做乐观锁——与其它表的 LockVersion 不同，这里复用
// ConfigVersion 本身：它既是乐观锁令牌，也是节点据以拒绝版本倒退的配置
// 版本号（「任何配置字段变化即递增」），一次更新只需要维护这一个
// 计数器，不必再叠加一个独立的 lock_version 列。
func UpdatePipelineConfig(ctx context.Context, db *gorm.DB, id uuidx.UUID, expectedConfigVersion uint64, p PipelineConfigUpdate) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		current, err := GetPipeline(ctx, tx, id)
		if err != nil {
			return err
		}
		if err := LockTopology(ctx, tx, current.TopologyID); err != nil {
			return err
		}
		current, err = GetPipeline(ctx, tx, id)
		if err != nil {
			return err
		}
		if current.PositionRevision > 0 && current.InitMode != p.InitMode {
			return ErrPipelineConfigVersionConflict
		}
		current.InitMode = p.InitMode
		if err := CheckBidirectionalInitialization(ctx, tx, current, false); err != nil {
			return err
		}
		return updatePipelineConfig(ctx, tx, id, expectedConfigVersion, p)
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}

func updatePipelineConfig(ctx context.Context, db *gorm.DB, id uuidx.UUID, expectedConfigVersion uint64, p PipelineConfigUpdate) error {
	if err := normalizeResourceName(&p.Name); err != nil {
		return err
	}
	if !p.InitMode.Valid() {
		return fmt.Errorf("model: 未知的初始化方式 %q", p.InitMode)
	}
	res := db.WithContext(ctx).Model(&model.Pipeline{}).
		Where("id = ? AND config_version = ?", id, expectedConfigVersion).
		Where("reinit_pending = ?", false).
		// Initialization determines which source transactions are meaningful for
		// a run. Changing it after START could reinterpret intentionally excluded
		// full-only transactions as missing incremental data.
		Where("last_cmd_seq = 0 OR init_mode <> ? OR init_mode = ?", model.InitModeFullOnly, p.InitMode).
		// A confirmed migration plan is tied to the initialization and immutable
		// strategy version. A new selection requires a new run and preview.
		Where("schema_uuid IS NULL OR (strategy_id = ? AND strategy_version = ? AND init_mode = ?)", p.StrategyID, p.StrategyVersion, p.InitMode).
		Updates(map[string]any{
			"name":             p.Name,
			"strategy_id":      p.StrategyID,
			"strategy_version": p.StrategyVersion,
			"init_mode":        p.InitMode,
			"start_position":   p.StartPosition,
			"config_version":   gorm.Expr("config_version + 1"),
		})
	if res.Error != nil {
		if isDuplicateKeyError(res.Error) {
			return fmt.Errorf("%w: name=%q", ErrPipelineNameConflict, p.Name)
		}
		return fmt.Errorf("repo: 更新 pipeline 配置失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		if _, err := GetPipeline(ctx, db, id); err != nil {
			return err
		}
		return ErrPipelineConfigVersionConflict
	}
	return nil
}

// UpdatePipelineRules replaces only the immutable rule selection. The caller
// validates migration compatibility and topology inside the same transaction.
func UpdatePipelineRules(ctx context.Context, db *gorm.DB, id uuidx.UUID, expected uint64, strategyID uuidx.UUID, version uint32) error {
	res := db.WithContext(ctx).Model(&model.Pipeline{}).Where("id=? AND config_version=? AND reinit_pending=?", id, expected, false).Updates(map[string]any{
		"strategy_id": strategyID, "strategy_version": version, "config_version": gorm.Expr("config_version + 1"),
	})
	if res.Error != nil {
		return fmt.Errorf("repo: 更新 pipeline 规则失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		if _, err := GetPipeline(ctx, db, id); err != nil {
			return err
		}
		return ErrPipelineConfigVersionConflict
	}
	return nil
}

// BumpPipelineConfigVersion advances the persisted version using compare-and-swap.
func BumpPipelineConfigVersion(ctx context.Context, db *gorm.DB, id uuidx.UUID, expectedConfigVersion uint64) (uint64, error) {
	res := db.WithContext(ctx).Model(&model.Pipeline{}).
		Where("id = ? AND config_version = ?", id, expectedConfigVersion).
		Where("reinit_pending = ?", false).
		Update("config_version", gorm.Expr("config_version + 1"))
	if res.Error != nil {
		return 0, fmt.Errorf("repo: 递增 pipeline 配置版本失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		if _, err := GetPipeline(ctx, db, id); err != nil {
			return 0, err
		}
		return 0, ErrPipelineConfigVersionConflict
	}
	return expectedConfigVersion + 1, nil
}

// BumpPipelineCmdSeq allocates a command identity using compare-and-swap.
// Pending REINIT identities must be resolved before allocating incompatible intent.
func BumpPipelineCmdSeq(ctx context.Context, db *gorm.DB, id uuidx.UUID, expectedCmdSeq uint64) (uint64, error) {
	var next uint64
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current model.Pipeline
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&current, "id=?", id).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrPipelineNotFound
			}
			return err
		}
		if current.LastCmdSeq != expectedCmdSeq || expectedCmdSeq >= math.MaxInt64 {
			return ErrPipelineCmdSeqConflict
		}
		if current.ReinitPending {
			if current.RunID == 0 {
				return ErrReinitConflict
			}
			stored, err := NewReinitializations(tx).GetPreview(ctx, id, current.RunID-1)
			if err != nil || stored.Record.ConfirmedAt == nil || stored.Record.CmdSeq != expectedCmdSeq || stored.Record.NewRunID != current.RunID {
				return ErrReinitConflict
			}
			if err := tx.Model(&stored.Record).Updates(map[string]any{"cmd_seq": expectedCmdSeq + 1, "source_applied": false, "target_applied": false}).Error; err != nil {
				return err
			}
		}
		var err error
		next, err = bumpPipelineCmdSeq(ctx, tx, id, expectedCmdSeq)
		return err
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	return next, err
}

func bumpPipelineCmdSeq(ctx context.Context, db *gorm.DB, id uuidx.UUID, expectedCmdSeq uint64) (uint64, error) {
	res := db.WithContext(ctx).Model(&model.Pipeline{}).
		Where("id = ? AND last_cmd_seq = ?", id, expectedCmdSeq).
		Update("last_cmd_seq", gorm.Expr("last_cmd_seq + 1"))
	if res.Error != nil {
		return 0, fmt.Errorf("repo: 递增 pipeline cmd_seq 失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		if _, err := GetPipeline(ctx, db, id); err != nil {
			return 0, err
		}
		return 0, ErrPipelineCmdSeqConflict
	}
	return expectedCmdSeq + 1, nil
}

// SetPipelineDesiredState writes the enum-validated intent. HTTP validates the
// lifecycle, precheck and topology gates under the same transaction's pipeline
// lock before allocating cmd_seq and calling this storage primitive.
func SetPipelineDesiredState(ctx context.Context, db *gorm.DB, id uuidx.UUID, state model.DesiredState) error {
	if !state.Valid() {
		return fmt.Errorf("model: 未知的期望状态 %q", state)
	}
	res := db.WithContext(ctx).Model(&model.Pipeline{}).Where("id = ?", id).Update("desired_state", state)
	if res.Error != nil {
		return fmt.Errorf("repo: 设置 pipeline 期望状态失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrPipelineNotFound
	}
	return nil
}

// 与 PipelineStatus 消息本身的粒度一致：一条消息只携带一个 role，本函数
// 一次调用也只写一侧；调用方需要标记"这个节点承载的全部角色"时（例如
// 该节点同时是某条 pipeline 的源端与目标端——internal/manager/dispatch/reconcile.go 的
// dispatchCommand 去重逻辑说明这种情形存在），按角色各调一次，不在本
// 函数里做隐式的"猜一个节点在这条 pipeline 上扮演了哪些角色"再展开——
// 那属于调用方已经拿到的 model.Pipeline.SourceNodeID/TargetNodeID 可以
// 直接比对出的信息，本函数不重复这层判断。
//
// The ingest caller validates ownership, run, ordering and lifecycle under a
// pipeline row lock and commits this write with snapshot/status observations.
// Other internal callers must supply their own authority and transaction.
func SetPipelineRoleActualState(ctx context.Context, db *gorm.DB, id uuidx.UUID, side model.Side, state model.ActualState, lastError string) error {
	if !side.Valid() {
		return fmt.Errorf("model: 未知的 side %q", side)
	}
	if !state.Valid() {
		return fmt.Errorf("model: 未知的实际状态 %q", state)
	}
	if state == model.ActualStateUnknown {
		return fmt.Errorf("repo: 不能通过 SetPipelineRoleActualState 写入 Unknown，标记离线请调用 MarkPipelineRoleUnknown（会保留该侧原有的 last_error，不会覆盖）")
	}
	var updates map[string]any
	switch side {
	case model.SideSource:
		updates = map[string]any{"source_actual_state": state, "source_last_error": lastError}
	case model.SideTarget:
		updates = map[string]any{"target_actual_state": state, "target_last_error": lastError}
	}
	res := db.WithContext(ctx).Model(&model.Pipeline{}).Where("id = ?", id).Updates(updates)
	if res.Error != nil {
		return fmt.Errorf("repo: 设置 pipeline %s 侧实际状态失败: %w", side, res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrPipelineNotFound
	}
	return nil
}

// MarkPipelineRoleUnknown 仅用于 Manager 判定的离线侧，不能设置其它实际状态。
func MarkPipelineRoleUnknown(ctx context.Context, db *gorm.DB, id uuidx.UUID, side model.Side) error {
	if !side.Valid() {
		return fmt.Errorf("model: 未知的 side %q", side)
	}
	var column string
	switch side {
	case model.SideSource:
		column = "source_actual_state"
	case model.SideTarget:
		column = "target_actual_state"
	}
	res := db.WithContext(ctx).Model(&model.Pipeline{}).Where("id = ?", id).Update(column, model.ActualStateUnknown)
	if res.Error != nil {
		return fmt.Errorf("repo: 标记 pipeline %s 侧实际状态未知失败: %w", side, res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrPipelineNotFound
	}
	return nil
}

// 没有配套的"清零"函数：语义上不需要——调用方（Reconciler）用
// `RecoveryExhaustedCmdSeq == LastCmdSeq` 判断是否跳过自动下发，
// LastCmdSeq 一旦因为真正的新意图前进，这条记录自动作废，见字段文档
// 注释里"为什么选 cmd_seq 不选 bool"那一段。
func SetPipelineRecoveryExhausted(ctx context.Context, db *gorm.DB, id uuidx.UUID, cmdSeq uint64) error {
	res := db.WithContext(ctx).Model(&model.Pipeline{}).Where("id = ?", id).
		Update("recovery_exhausted_cmd_seq", cmdSeq)
	if res.Error != nil {
		return fmt.Errorf("repo: 标记 pipeline 恢复停止失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrPipelineNotFound
	}
	return nil
}

// AllocatePipelineBinlogServerID allocates a unique ID within the configured range.
// Transactional locking serializes concurrent allocation; exhaustion returns an error.
func AllocatePipelineBinlogServerID(ctx context.Context, db *gorm.DB, id uuidx.UUID, serverID uint32) error {
	res := db.WithContext(ctx).Model(&model.Pipeline{}).Where("id = ?", id).
		Update("binlog_server_id", serverID)
	if res.Error != nil {
		if isDuplicateKeyError(res.Error) {
			return fmt.Errorf("%w: server_id=%d", ErrPipelineBinlogServerIDConflict, serverID)
		}
		return fmt.Errorf("repo: 分配 binlog_server_id 失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrPipelineNotFound
	}
	return nil
}

func SoftDeletePipeline(ctx context.Context, db *gorm.DB, id uuidx.UUID, now time.Time) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		current, err := GetPipeline(ctx, tx, id)
		if err != nil {
			return err
		}
		if err := LockTopology(ctx, tx, current.TopologyID); err != nil {
			return err
		}
		current, err = LockPipelineIncludingDeleted(ctx, tx, id)
		if err != nil {
			return err
		}
		if current.DeletedAt.Valid {
			return ErrPipelineNotFound
		}
		inactive := func(s model.ActualState) bool { return s == model.ActualStateDraft || s == model.ActualStateStopped }
		if current.DesiredState != model.DesiredStateStopped || current.ReinitPending || !inactive(current.SourceActualState) || !inactive(current.TargetActualState) {
			return ErrPipelineDeleteRequiresStopped
		}
		return softDeletePipeline(ctx, tx, id, now)
	})
}

func softDeletePipeline(ctx context.Context, db *gorm.DB, id uuidx.UUID, now time.Time) error {
	res := db.WithContext(ctx).Model(&model.Pipeline{}).
		Where("id = ? AND deleted_at IS NULL", id).
		Updates(map[string]any{
			"deleted_at":     now,
			"cleanup_state":  model.CleanupStatePending,
			"desired_state":  model.DesiredStateStopped,
			"config_version": gorm.Expr("config_version + 1"),
			"last_cmd_seq":   gorm.Expr("last_cmd_seq + 1"),
		})
	if res.Error != nil {
		return fmt.Errorf("repo: 软删除 pipeline 失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrPipelineNotFound
	}
	return nil
}

// ListPendingCleanupPipelines 返回「待清理」列表：
// deleted_at IS NOT NULL AND cleanup_state <> 'done'。用 Unscoped 绕开
// gorm 对已软删除记录的默认过滤——这些记录本身就是本函数要找的对象。
func ListPendingCleanupPipelines(ctx context.Context, db *gorm.DB) ([]model.Pipeline, error) {
	var ps []model.Pipeline
	err := db.WithContext(ctx).Unscoped().
		Where("deleted_at IS NOT NULL AND cleanup_state <> ?", model.CleanupStateDone).
		Order("deleted_at ASC").
		Find(&ps).Error
	if err != nil {
		return nil, fmt.Errorf("repo: 查询待清理 pipeline 列表失败: %w", err)
	}
	return ps, nil
}

// 用 Unscoped：pipeline 此时已经是软删除状态（deleted_at 非空），
// gorm 默认 scope 会把它过滤掉，必须显式绕开，否则 RowsAffected 恒为 0，
// 看起来像「记录不存在」，实际是把一次真实的清理进度更新悄悄丢弃。
func RecordPipelineCleanupAttempt(ctx context.Context, db *gorm.DB, id uuidx.UUID, now time.Time, detail model.JSON, newState model.CleanupState, lastError string) error {
	if !newState.Valid() {
		return fmt.Errorf("model: 未知的资源回收状态 %q", newState)
	}
	updates := map[string]any{
		"cleanup_detail":          detail,
		"cleanup_attempts":        gorm.Expr("cleanup_attempts + 1"),
		"cleanup_last_error":      lastError,
		"cleanup_last_attempt_at": now,
		"cleanup_state":           newState,
	}
	if newState == model.CleanupStateDone {
		p, err := GetPipelineIncludingDeleted(ctx, db, id)
		if err != nil {
			return err
		}
		if p.SourceCleanupConfirmedAt == nil || p.TargetCleanupConfirmedAt == nil {
			return fmt.Errorf("repo: pipeline unload confirmations are incomplete")
		}
		updates["binlog_server_id"] = nil
	}

	res := db.WithContext(ctx).Unscoped().Model(&model.Pipeline{}).
		Where("id = ? AND deleted_at IS NOT NULL AND cleanup_state <> ?", id, model.CleanupStateDone).
		Updates(updates)
	if res.Error != nil {
		return fmt.Errorf("repo: 记录 pipeline 资源回收进度失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		p, err := GetPipelineIncludingDeleted(ctx, db, id)
		if err == nil && p.DeletedAt.Valid && p.CleanupState == model.CleanupStateDone {
			return nil
		}
		return ErrPipelineNotFound
	}
	return nil
}
