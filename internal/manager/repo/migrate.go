// Package repo owns Manager persistence and versioned schema migration.
// Migrations run in increasing version order and reject schemas newer than the
// binary. Failed or incomplete migration state cannot be treated as successful.
// Partition creation and retention operate on validated table and time boundaries.
package repo

import (
	"context"
	"fmt"
	"slices"
	"time"

	"gorm.io/gorm"
)

// migration 是一个版本化、幂等的 DDL 步骤。
type migration struct {
	Version     int64
	Description string
	Statements  []string // 每个元素是一条独立的 SQL 语句，不含结尾分号
}

// schemaVersionsDDL 创建迁移执行记录表，用版本号识别已执行迁移。
const schemaVersionsDDL = `
CREATE TABLE IF NOT EXISTS schema_versions (
  version BIGINT NOT NULL,
  description VARCHAR(255) NOT NULL,
  applied_at DATETIME(3) NOT NULL,
  PRIMARY KEY (version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

// Migrator 是管理库迁移的入口，包装一个 *gorm.DB 连接。
type Migrator struct {
	db *gorm.DB
}

// NewMigrator 用已建立的 gorm 连接构造 Migrator。
func NewMigrator(db *gorm.DB) *Migrator { return &Migrator{db: db} }

// staticMigrations 是不需要按当前时间生成分区的普通建表迁移，按 Version
// 升序排列（Version 是人工分配的固定序号，新增迁移只应追加更大的序号，不
// 得修改已发布版本的语句）。
func staticMigrations() []migration {
	return []migration{
		usersMigration(),
		userSessionsMigration(),
		centersMigration(),
		centerOriginUUIDsMigration(),
		datasourcesMigration(),
		nodesMigration(),
		nodeRegisterCodesMigration(),
		strategiesMigration(),
		strategyVersionsMigration(),
		topologiesMigration(),
		pipelinesMigration(),
		pipelineSnapshotsMigration(),
		pipelineSnapshotTablesMigration(),
		schemaMigrationsMigration(),
		pipelineReleasesMigration(),
		pipelineCheckpointsMigration(),
		pipelinePrechecksMigration(),
		eventsMigration(),
		conflictsMigration(),
		ddlRecordsMigration(),
		alertRulesMigration(),
		alertsMigration(),
		auditLogsMigration(),
		settingsMigration(),
	}
}

// allMigrations 组装本二进制内置的全部迁移，按 Version 升序。now 用于生成
// 分区表（logs、pipeline_metrics）初始分区的时间窗口。
//
// pipelinesCleanupFieldsMigration（v27）、loginLockoutsMigration（v28）、
// nodesCleanupFieldsMigration（v29）、nodeLogLevelFieldsMigration
// （v30）、eventsConflictsTsIndexMigration（v31）、
// dataEndpointRenameMigration（v32）、
// centerBrokerProbeFieldsMigration（v33）、
// endpointRenameMigration（v34）、
// endpointLowerCaseTableNamesMigration（v35）、
// pipelineCmdSeqFieldMigration（v36）、
// pipelineRecoveryExhaustedFieldMigration（v37）、
// pipelineRoleActualStateFieldMigration（v38）追加在最后，晚于
// logs/pipeline_metrics（v25/v26）：staticMigrations() 内部顺序对应
// v1~v24，不在其中插入新版本，避免打乱既有排列，新迁移一律接在
// append 链尾部。
func allMigrations(now time.Time) []migration {
	return append(staticMigrations(), logsMigration(now), pipelineMetricsMigration(now),
		pipelinesCleanupFieldsMigration(), loginLockoutsMigration(), nodesCleanupFieldsMigration(),
		nodeLogLevelFieldsMigration(), eventsConflictsTsIndexMigration(), dataEndpointRenameMigration(),
		centerBrokerProbeFieldsMigration(), endpointRenameMigration(), endpointLowerCaseTableNamesMigration(),
		pipelineCmdSeqFieldMigration(), pipelineRecoveryExhaustedFieldMigration(),
		pipelineRoleActualStateFieldMigration(),
		pipelineDropLegacyActualStateIndexMigration(),
		pipelineDropLegacyActualStateColumnMigration(),
		pipelineRecoveryExhaustedNullableMigration(),
		pipelineRecoveryExhaustedZeroToNullMigration(),
		eventAckedAtFieldMigration(),
		alertsDedupeFieldsMigration(),
		settingUpdatedByFieldMigration(),
		alertRuleIDNullableMigration(),
		nodeRegisterCodeIDFieldMigration(),
		userMustChangePasswordMigration(), schemaPlansMigration(), topologyConfirmationsMigration(), pipelinePairRequestsMigration(), snapshotCatchupProgressMigration(), positionPreviewsMigration(), reinitializationsMigration(), pipelineReinitPendingMigration(), nodeRegistrationAttemptsMigration(), centerOriginRefreshMigration(), nodeRetirementMigration(), externalUserIdentityMigration(), oidcLoginStatesMigration(), precheckRunsMigration(), precheckBatchMigration(), snapshotTableMetricsMigration(), pipelineProgressLatestMigration(), pipelineMetricDetailsMigration(), pipelinePositionRevisionMigration(), positionChangesMigration(), sourceRecoveryStatesMigration(), sourceEpochLeasesMigration(), pipelineCleanupAcknowledgementsMigration(), nodePipelineQuotaMigration(), migrationStatementProgressMigration(), pipelineStatusLatestMigration(), nodeCapabilitiesMigration(), userGrantsMigration(), userScopePolicyMigration(), userScopePolicyBackfillMigration(), snapshotExtensionsMigration(), pipelineStateBundlesMigration(), automaticTopologyMigration(), pipelineRuleOwnershipMigration(), resourceNamesMigration(), centerNamesMigration(), endpointNamesMigration(), dataCheckRunsMigration())
}

// validateVersionsStrictlyIncreasing 校验迁移列表版本号严格递增。这是对
// **源码**的校验（新增迁移时重号、或插进了 append 链中间），与数据库状态
// 无关，因此是纯函数——见 allMigrations 关于"不要藏在 MySQL 可达性之后"
// 的说明。
func validateVersionsStrictlyIncreasing(all []migration) error {
	for i := 1; i < len(all); i++ {
		if all[i].Version <= all[i-1].Version {
			return fmt.Errorf("repo: 迁移版本号必须严格递增，version=%d 与前一个版本 %d 冲突（迁移 %q）",
				all[i].Version, all[i-1].Version, all[i].Description)
		}
	}
	return nil
}

// Migrate 执行全部迁移：先建 schema_versions，再按版本号升序依次执行未应用
// 的迁移。now 用于生成分区表（logs、pipeline_metrics）初始分区的时间窗口，
// 由调用方（控制层）传入，符合「可测试逻辑通过参数接收 now」的约定。
func (m *Migrator) Migrate(ctx context.Context, now time.Time) error {
	return m.withMigrationLock(ctx, func(locked *Migrator) error { return locked.migrateLocked(ctx, now) })
}

func (m *Migrator) migrateLocked(ctx context.Context, now time.Time) error {
	if err := m.db.WithContext(ctx).Exec(schemaVersionsDDL).Error; err != nil {
		return fmt.Errorf("repo: 创建 schema_versions 失败: %w", err)
	}

	all := allMigrations(now)
	if err := validateVersionsStrictlyIncreasing(all); err != nil {
		return err
	}

	applied, err := m.appliedVersions(ctx)
	if err != nil {
		return err
	}

	// 库里存在本二进制不认识的更高版本号 ⇒ 这个库被一个更新的 Manager
	// 迁移过，当前二进制比库旧。拒绝启动，理由见
	// checkNoFutureAppliedVersions 的文档注释。必须在任何一条语句执行
	// 之前检查：旧二进制对新库做的写入是按旧列集合进行的，一旦写下去
	// 就无法靠"发现得早"挽回。
	if err := checkNoFutureAppliedVersions(applied, maxKnownVersion(all)); err != nil {
		return err
	}
	if err := m.db.WithContext(ctx).Exec(schemaMigrationStatementsDDL).Error; err != nil {
		return fmt.Errorf("repo: create migration statement progress failed: %w", err)
	}

	for _, mig := range all {
		if applied[mig.Version] {
			continue
		}
		if err := m.applyWithProgress(ctx, mig, now); err != nil {
			return fmt.Errorf("repo: 迁移 v%d(%s) 执行失败: %w", mig.Version, mig.Description, err)
		}
	}
	return nil
}

// maxKnownVersion 返回本二进制内置的迁移列表里最大的版本号。all 恒非空
// （staticMigrations() 至少返回 usersMigration），因此不处理空切片：真的
// 传空切片进来会返回 0，而 0 不是任何一条真实迁移的版本号（版本号从 1
// 起），checkNoFutureAppliedVersions 届时会把库里任何一行都判成"未来
// 版本"——这个失败方向是安全的（拒绝启动而不是静默继续）。
func maxKnownVersion(all []migration) int64 {
	var maxVer int64
	for _, mig := range all {
		if mig.Version > maxVer {
			maxVer = mig.Version
		}
	}
	return maxVer
}

// Reject applied migration versions newer than this binary before executing DDL.
func checkNoFutureAppliedVersions(applied map[int64]bool, known int64) error {
	var future []int64
	for v := range applied {
		if v > known {
			future = append(future, v)
		}
	}
	if len(future) == 0 {
		return nil
	}
	slices.Sort(future)
	return fmt.Errorf("repo: 管理库的 schema 版本比当前 Manager 二进制更新，拒绝启动"+
		"（schema_versions 中存在本二进制不认识的版本 %v，本二进制已知最大版本 v%d）"+
		"——这个库被一个更新的 Manager 迁移过；用旧二进制继续运行会按旧列集合读写，"+
		"新列上的状态会被静默绕过。请改用不低于该 schema 版本的 Manager 构建",
		future, known)
}

func (m *Migrator) appliedVersions(ctx context.Context) (map[int64]bool, error) {
	var versions []int64
	if err := m.db.WithContext(ctx).Raw("SELECT version FROM schema_versions").Scan(&versions).Error; err != nil {
		return nil, fmt.Errorf("repo: 读取 schema_versions 失败: %w", err)
	}
	applied := make(map[int64]bool, len(versions))
	for _, v := range versions {
		applied[v] = true
	}
	return applied, nil
}

func (m *Migrator) apply(ctx context.Context, mig migration, now time.Time) error {
	return m.withMigrationLock(ctx, func(locked *Migrator) error {
		for _, ddl := range []string{schemaVersionsDDL, schemaMigrationStatementsDDL} {
			if err := locked.db.WithContext(ctx).Exec(ddl).Error; err != nil {
				return err
			}
		}
		return locked.applyWithProgress(ctx, mig, now)
	})
}
