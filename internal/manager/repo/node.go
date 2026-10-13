// 只做仓储层：CRUD + 心跳/离线判定 + 两步注销，不假设调用方的调用形态。
package repo

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// nodeTokenByteLength 是节点 token 的随机字节长度，与注册码、会话令牌
// 取相同长度（32 字节随机数），保持同一强度约定。
const nodeTokenByteLength = 32

var (
	// ErrNodeNotFound 表示节点不存在。
	ErrNodeNotFound = errors.New("repo: 节点不存在")
	// ErrNodeVersionConflict 表示更新时乐观锁版本已过期（并发修改）。
	ErrNodeVersionConflict = errors.New("repo: 节点已被并发修改，请刷新后重试")

	ErrNodeHasDependents = errors.New("repo: 节点仍被 pipeline 使用，不能禁用、吊销 token 或注销")
	// ErrNodeDisabledOrRevoked 表示节点已被禁用或吊销，拒绝处理心跳。
	ErrNodeDisabledOrRevoked = errors.New("repo: 节点已禁用或已吊销，拒绝心跳")
)

func hashNodeToken(plainToken string) string {
	sum := sha256.Sum256([]byte(plainToken))
	return hex.EncodeToString(sum[:])
}

// NewNodeParams 是创建节点记录的入参，不含 TokenHash——由 CreateNode 负责
// 生成，明文只在返回值里出现一次。
type NewNodeParams struct {
	Name     string
	CenterID uuidx.UUID
	Labels   model.JSON
	Hostname string
	IPs      string
	OSArch   string
	Version  string

	RegisterCodeID uuidx.UUID
}

// CreateNode 创建一个节点记录并生成节点 token（需求 MGR-NODE-03「签发
// node_id 与 node_token」）。明文 token 只在本次调用的返回值里出现一次，
// 库中只存 SHA-256 哈希。新建节点状态固定为 Registering，
// 第一次心跳后由 RecordNodeHeartbeat 转为 Online。
func CreateNode(ctx context.Context, db *gorm.DB, p NewNodeParams, now time.Time) (plainToken string, n *model.Node, err error) {
	b := make([]byte, nodeTokenByteLength)
	if _, err := rand.Read(b); err != nil {
		return "", nil, fmt.Errorf("repo: 生成节点 token 随机数失败: %w", err)
	}
	plainToken = hex.EncodeToString(b)

	n = &model.Node{
		Name:           p.Name,
		CenterID:       p.CenterID,
		Labels:         p.Labels,
		Hostname:       p.Hostname,
		IPs:            p.IPs,
		OSArch:         p.OSArch,
		Version:        p.Version,
		TokenHash:      hashNodeToken(plainToken),
		Status:         model.NodeStatusRegistering,
		RegisteredAt:   now,
		RegisterCodeID: p.RegisterCodeID,
		// 插入时显式提供默认值，避免 GORM 在数据库默认值生效前改变字段语义。
		CleanupState: model.CleanupStateDone,
		LogLevel:     model.LogLevelInfo,
		LogShipLevel: model.LogLevelInfo,
	}
	if strings.TrimSpace(n.Name) == "" {
		n.Name = "node-" + uuidx.New().String()
	}
	if err := normalizeResourceName(&n.Name); err != nil {
		return "", nil, err
	}
	if err := n.Validate(); err != nil {
		return "", nil, err
	}
	if err := db.WithContext(ctx).Create(n).Error; err != nil {
		if duplicateIndex(err, "uk_nodes_name") {
			return "", nil, ErrNodeNameConflict
		}
		return "", nil, fmt.Errorf("repo: 创建节点失败: %w", err)
	}
	return plainToken, n, nil
}

// RegisterNodeParams 是节点注册请求的入参（需求 MGR-NODE-03）。
type RegisterNodeParams struct {
	PlainRegisterCode string
	// Name 为空时取注册码的 PresetName。
	Name                           string
	Hostname, IPs, OSArch, Version string
}

// GetNode 按主键查询节点。
func GetNode(ctx context.Context, db *gorm.DB, id uuidx.UUID) (*model.Node, error) {
	var n model.Node
	if err := ReadResourceScope(ctx, db.WithContext(ctx), ReadNode).First(&n, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNodeNotFound
		}
		return nil, fmt.Errorf("repo: 查询节点失败: %w", err)
	}
	return &n, nil
}

// GetNodeByToken 按明文 token 查询节点（鉴权路径用：先哈希再查，不在库里
// 反查明文）。
func GetNodeByToken(ctx context.Context, db *gorm.DB, plainToken string) (*model.Node, error) {
	var n model.Node
	if err := db.WithContext(ctx).First(&n, "token_hash = ?", hashNodeToken(plainToken)).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNodeNotFound
		}
		return nil, fmt.Errorf("repo: 按 token 查询节点失败: %w", err)
	}
	return &n, nil
}

// ListNodes 列出节点（需求 MGR-NODE-04：节点列表）。centerID 为 uuidx.Nil
// 时不按中心过滤。
func ListNodes(ctx context.Context, db *gorm.DB, centerID uuidx.UUID) ([]model.Node, error) {
	q := ReadResourceScope(ctx, db.WithContext(ctx), ReadNode).Order("created_at ASC")
	if !centerID.IsZero() {
		q = q.Where("center_id = ?", centerID)
	}
	var ns []model.Node
	if err := q.Find(&ns).Error; err != nil {
		return nil, fmt.Errorf("repo: 查询节点列表失败: %w", err)
	}
	return ns, nil
}

// UpdateNode 更新节点的管理员可编辑字段（名称、标签、主机信息），用
// n.LockVersion 做乐观锁。不包括 center_id（换中心是更重的操作，涉及
// pipeline/拓扑一致性，需要专门的流程，不通过通用更新完成）、status /
// token_hash / last_heartbeat_at / runtime（这些字段由心跳与生命周期
// 状态机专属函数维护，不接受管理员直接改写，避免绕过状态机）。
func UpdateNode(ctx context.Context, db *gorm.DB, n *model.Node) error {
	if n.MaxPipelines > MaxNodePipelineQuota {
		return fmt.Errorf("repo: max_pipelines must be between 0 and %d", MaxNodePipelineQuota)
	}
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current model.Node
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&current, "id = ?", n.ID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrNodeNotFound
			}
			return err
		}
		if current.LockVersion != n.LockVersion {
			return ErrNodeVersionConflict
		}
		if n.MaxPipelines != 0 {
			count, err := countNodeAllocations(ctx, tx, n.ID)
			if err != nil {
				return err
			}
			if count > int(n.MaxPipelines) {
				return &NodePipelineQuotaError{NodeID: n.ID, Limit: n.MaxPipelines, Assigned: count}
			}
		}
		return updateNode(ctx, tx, n)
	})
}

func updateNode(ctx context.Context, db *gorm.DB, n *model.Node) error {
	if err := normalizeResourceName(&n.Name); err != nil {
		return err
	}
	res := db.WithContext(ctx).Model(&model.Node{}).
		Where("id = ? AND lock_version = ?", n.ID, n.LockVersion).
		Updates(map[string]any{
			"name":          n.Name,
			"labels":        n.Labels,
			"max_pipelines": n.MaxPipelines,
			"hostname":      n.Hostname,
			"ips":           n.IPs,
			"os_arch":       n.OSArch,
			"version":       n.Version,
			"lock_version":  gorm.Expr("lock_version + 1"),
		})
	if res.Error != nil {
		if duplicateIndex(res.Error, "uk_nodes_name") {
			return ErrNodeNameConflict
		}
		return fmt.Errorf("repo: 更新节点失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		if _, err := GetNode(ctx, db, n.ID); err != nil {
			return err
		}
		return ErrNodeVersionConflict
	}
	return nil
}

// PipelinesBlockingNodeDeletion 返回仍然「挂在」该节点上、会阻断节点注销
// 的 pipeline 完整记录列表——返回记录
// 供界面展示阻断者清单，不只返回布尔或数量。
//
// 与 internal/manager/repo/pipeline.go 的 ListPipelinesByNode/ListPipelinesByEndpoint、
// internal/manager/repo/strategy.go 的 DeleteStrategyVersion 是四种不同的「按节点/端点/策略查
// pipeline 引用」语义——四者的差异对比见 ListPipelinesByNode 的 doc
// 注释，不要在这几个函数之间复用或合并判定条件。
func PipelinesBlockingNodeDeletion(ctx context.Context, db *gorm.DB, id uuidx.UUID) ([]model.Pipeline, error) {
	var ps []model.Pipeline
	err := db.WithContext(ctx).Unscoped().
		Where(`
			NOT (deleted_at IS NOT NULL AND cleanup_state = 'done')
			AND (source_node_id = ? OR target_node_id = ?)`, id, id).
		Order("created_at ASC").
		Find(&ps).Error
	if err != nil {
		return nil, fmt.Errorf("repo: 查询阻断节点注销的 pipeline 失败: %w", err)
	}
	return ps, nil
}

// SoftDeleteNode starts two-phase removal; physical cleanup completes asynchronously.
func SoftDeleteNode(ctx context.Context, db *gorm.DB, id uuidx.UUID, now time.Time) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := lockUnusedNode(ctx, tx, id); err != nil {
			return err
		}

		res := tx.WithContext(ctx).Model(&model.Node{}).
			Where("id = ? AND deleted_at IS NULL", id).
			Updates(map[string]any{
				"deleted_at":    now,
				"cleanup_state": model.CleanupStatePending,
			})
		if res.Error != nil {
			return fmt.Errorf("repo: 软删除节点失败: %w", res.Error)
		}
		if res.RowsAffected == 0 {
			return ErrNodeNotFound
		}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}

// ListPendingCleanupNodes 返回「待清理」节点列表：
// deleted_at IS NOT NULL AND cleanup_state <> 'done'。用 Unscoped 绕开
// gorm 对已软删除记录的默认过滤——这些记录本身就是本函数要找的对象。
func ListPendingCleanupNodes(ctx context.Context, db *gorm.DB) ([]model.Node, error) {
	var ns []model.Node
	err := db.WithContext(ctx).Unscoped().
		Where("deleted_at IS NOT NULL AND cleanup_state <> ?", model.CleanupStateDone).
		Order("deleted_at ASC").
		Find(&ns).Error
	if err != nil {
		return nil, fmt.Errorf("repo: 查询待清理节点列表失败: %w", err)
	}
	return ns, nil
}

// 用 Unscoped：节点此时已经是软删除状态（deleted_at 非空），gorm 默认
// scope 会把它过滤掉，必须显式绕开，否则 RowsAffected 恒为 0，看起来像
// 「记录不存在」，实际是把一次真实的清理进度更新悄悄丢弃。
func RecordNodeCleanupAttempt(ctx context.Context, db *gorm.DB, id uuidx.UUID, now time.Time, detail model.JSON, newState model.CleanupState, lastError string) error {
	if !newState.Valid() {
		return fmt.Errorf("model: 未知的资源回收状态 %q", newState)
	}
	res := db.WithContext(ctx).Unscoped().Model(&model.Node{}).
		Where("id = ? AND deleted_at IS NOT NULL", id).
		Updates(map[string]any{
			"cleanup_detail":          detail,
			"cleanup_attempts":        gorm.Expr("cleanup_attempts + 1"),
			"cleanup_last_error":      lastError,
			"cleanup_last_attempt_at": now,
			"cleanup_state":           newState,
		})
	if res.Error != nil {
		return fmt.Errorf("repo: 记录节点资源回收进度失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrNodeNotFound
	}
	return nil
}

// RecordNodeHeartbeat 记录一次心跳（需求 MGR-NODE-04/05）：刷新
// LastHeartbeatAt 与 Runtime，并把状态置为 Online——心跳能送达即说明节点
// 当前可达，不论之前是 Registering（首次心跳）还是 Offline（心跳恢复）。
// 已 Disabled / Revoked 的节点拒绝心跳，返回 ErrNodeDisabledOrRevoked：
// 心跳不应该把管理员禁用或已吊销 token 的节点悄悄拉回 Online。
func RecordNodeHeartbeat(ctx context.Context, db *gorm.DB, id uuidx.UUID, now time.Time, runtime model.JSON) error {
	res := db.WithContext(ctx).Model(&model.Node{}).
		Where("id = ? AND status NOT IN (?,?)", id, model.NodeStatusDisabled, model.NodeStatusRevoked).
		Updates(map[string]any{
			"last_heartbeat_at": now,
			"runtime":           runtime,
			"status":            model.NodeStatusOnline,
			"lock_version":      gorm.Expr("lock_version + 1"),
		})
	if res.Error != nil {
		return fmt.Errorf("repo: 记录心跳失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		n, err := GetNode(ctx, db, id)
		if err != nil {
			return err
		}
		return fmt.Errorf("%w: 当前状态 %s", ErrNodeDisabledOrRevoked, n.Status)
	}
	return nil
}

//   - 第一跳：`internal/manager/ingest/ctl_offline.go` 的 checkOffline 调用本函数
//     （`s.opts.MarkOfflineNodes`，默认实现在 `internal/manager/ingest/ingest.go` 里注入为
//     `repo.MarkOfflineNodes`，所以生产路径确实走到这里）。
//   - 第二跳：同一个 checkOffline 随后按侧调用 MarkPipelineRoleUnknown
//     （source / target 各一次），并写事件。两步都真的做了，不是只有
//     一个名字。
//
// 写成可注入的函数字段而不是直接调用，是 ingest 的测试需要——所以"谁是编排
// 者"这个问题在 internal/manager/ingest/ingest.go 的默认值那一行才有答案，光看调用点看不出来。
func MarkOfflineNodes(ctx context.Context, db *gorm.DB, now time.Time, timeout time.Duration) ([]model.Node, error) {
	cutoff := now.Add(-timeout)

	var stale []model.Node
	if err := db.WithContext(ctx).
		Where("status = ? AND last_heartbeat_at IS NOT NULL AND last_heartbeat_at < ?", model.NodeStatusOnline, cutoff).
		Find(&stale).Error; err != nil {
		return nil, fmt.Errorf("repo: 查询心跳超时节点失败: %w", err)
	}
	if len(stale) == 0 {
		return nil, nil
	}

	ids := make([]uuidx.UUID, len(stale))
	for i, n := range stale {
		ids[i] = n.ID
	}
	if err := db.WithContext(ctx).Model(&model.Node{}).Where("id IN ?", ids).
		Updates(map[string]any{"status": model.NodeStatusOffline, "lock_version": gorm.Expr("lock_version + 1")}).Error; err != nil {
		return nil, fmt.Errorf("repo: 批量标记节点离线失败: %w", err)
	}
	for i := range stale {
		stale[i].Status = model.NodeStatusOffline
		stale[i].LockVersion++
	}
	return stale, nil
}

// DisableNode 禁用节点（需求 MGR-NODE-06）。幂等：已禁用的节点再次调用
// 不报错。
func DisableNode(ctx context.Context, db *gorm.DB, id uuidx.UUID) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockUnusedNode(ctx, tx, id); err != nil {
			return err
		}
		return tx.Model(&model.Node{}).
			Where("id = ? AND status <> ?", id, model.NodeStatusDisabled).
			Updates(map[string]any{"status": model.NodeStatusDisabled, "lock_version": gorm.Expr("lock_version + 1")}).Error
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}

// ReadCommitted sees allocations committed before the node lock was acquired.
// No allocation row locks are taken, so node migration can retain its pipeline lock.
func lockUnusedNode(ctx context.Context, tx *gorm.DB, id uuidx.UUID) error {
	if _, err := GetNode(ctx, tx.Clauses(clause.Locking{Strength: "UPDATE"}), id); err != nil {
		return err
	}
	var count int64
	err := tx.WithContext(ctx).Unscoped().Model(&model.Pipeline{}).
		Where("(source_node_id = ? OR target_node_id = ?) AND (deleted_at IS NULL OR cleanup_state <> ?)", id, id, model.CleanupStateDone).Count(&count).Error
	if err != nil {
		return err
	}
	if count > 0 {
		return ErrNodeHasDependents
	}
	return nil
}

// EnableNode 解除禁用，节点回到 Offline（等下一次心跳自然转 Online），
// 只允许从 Disabled 状态解除；对非 Disabled 状态的节点调用视为幂等成功、
// 不报错，也不改变其当前状态（比如不会把一个正常 Online 的节点误改成
// Offline）。
func EnableNode(ctx context.Context, db *gorm.DB, id uuidx.UUID) error {
	res := db.WithContext(ctx).Model(&model.Node{}).
		Where("id = ? AND status = ?", id, model.NodeStatusDisabled).
		Updates(map[string]any{"status": model.NodeStatusOffline, "lock_version": gorm.Expr("lock_version + 1")})
	if res.Error != nil {
		return fmt.Errorf("repo: 启用节点失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		if _, err := GetNode(ctx, db, id); err != nil {
			return err
		}
	}
	return nil
}

// RevokeNodeToken atomically retires the node and replaces its current token.
// The old hash is retained exclusively for authenticated node.hello rejection;
// ordinary control and report messages cannot authenticate with it.
func RevokeNodeToken(ctx context.Context, db *gorm.DB, id uuidx.UUID) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockUnusedNode(ctx, tx, id); err != nil {
			return err
		}
		var node model.Node
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&node, "id=?", id).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrNodeNotFound
			}
			return err
		}
		if node.Status == model.NodeStatusRevoked {
			return nil
		}
		b := make([]byte, nodeTokenByteLength)
		if _, err := rand.Read(b); err != nil {
			return err
		}
		retired := NodeRetiredCredential{NodeID: id, TokenHash: node.TokenHash, RetiredAt: time.Now().UTC()}
		if err := tx.Create(&retired).Error; err != nil {
			return err
		}
		return tx.Model(&model.Node{}).Where("id=?", id).Updates(map[string]any{
			"status": model.NodeStatusRevoked, "token_hash": hashNodeToken(hex.EncodeToString(b)),
			"lock_version": gorm.Expr("lock_version + 1"),
		}).Error
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}

// SetNodeLogLevel 远程调整节点日志级别（需求 NODE-RPT-04）：logLevel 控制
// 节点自己往本地文件写什么，logShipLevel 控制往 Manager 上报什么，两者
// 分开设置、分开校验。上报级别通常应该比本地级别更严（只把关键的送回来，
// 减少上报流量），但仓储层不强制这个大小关系——调用方（controller 下发
// cmd.node/set_log_level）决定具体取值组合是否合理，本函数只负责如实
// 落库两个独立的合法枚举值。
func SetNodeLogLevel(ctx context.Context, db *gorm.DB, id uuidx.UUID, logLevel, logShipLevel model.LogLevel) error {
	if !logLevel.Valid() {
		return fmt.Errorf("model: 未知的节点日志级别 %q", logLevel)
	}
	if !logShipLevel.Valid() {
		return fmt.Errorf("model: 未知的节点上报日志级别 %q", logShipLevel)
	}
	res := db.WithContext(ctx).Model(&model.Node{}).Where("id = ?", id).
		Updates(map[string]any{
			"log_level":      logLevel,
			"log_ship_level": logShipLevel,
			"lock_version":   gorm.Expr("lock_version + 1"),
		})
	if res.Error != nil {
		return fmt.Errorf("repo: 设置节点日志级别失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrNodeNotFound
	}
	return nil
}
