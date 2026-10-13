package repo

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
)

const AutomaticTopologyID = "00000000-0000-4000-8000-000000000001"

func topologyPipelineFilter(id uuidx.UUID) uuidx.UUID {
	if id.String() == AutomaticTopologyID {
		return uuidx.Nil
	}
	return id
}

var (
	// ErrTopologyNotFound 表示拓扑不存在。
	ErrTopologyNotFound = errors.New("repo: 拓扑不存在")
	// ErrTopologyNameConflict 表示拓扑名称与已有记录冲突。
	ErrTopologyNameConflict = errors.New("repo: 拓扑名称已存在")
	// ErrTopologyHasDependents 表示拓扑仍被未删除的 pipeline 引用，不能
	// 删除。
	ErrTopologyHasDependents = errors.New("repo: 拓扑被 pipeline 引用，不能删除")
)

// CreateTopology 创建拓扑。
func CreateTopology(ctx context.Context, db *gorm.DB, t *model.Topology) error {
	if !t.Type.Valid() {
		return fmt.Errorf("model: 未知的拓扑类型 %q", t.Type)
	}
	if err := db.WithContext(ctx).Create(t).Error; err != nil {
		if isDuplicateKeyError(err) {
			return fmt.Errorf("%w: name=%q", ErrTopologyNameConflict, t.Name)
		}
		return fmt.Errorf("repo: 创建拓扑失败: %w", err)
	}
	return nil
}

// GetTopology 按主键查询拓扑。
func GetTopology(ctx context.Context, db *gorm.DB, id uuidx.UUID) (*model.Topology, error) {
	var t model.Topology
	if err := topologyReadQuery(ctx, db, id).First(&t, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrTopologyNotFound
		}
		return nil, fmt.Errorf("repo: 查询拓扑失败: %w", err)
	}
	return &t, nil
}

// ListTopologies 列出全部拓扑。
func ListTopologies(ctx context.Context, db *gorm.DB) ([]model.Topology, error) {
	var ts []model.Topology
	if err := ReadResourceScope(ctx, db.WithContext(ctx), ReadTopology).Where("id<>?", uuidx.MustParse(AutomaticTopologyID)).Order("created_at ASC").Find(&ts).Error; err != nil {
		return nil, fmt.Errorf("repo: 查询拓扑列表失败: %w", err)
	}
	return ts, nil
}

// UpdateTopology 更新拓扑的名称、类型、描述。
func UpdateTopology(ctx context.Context, db *gorm.DB, id uuidx.UUID, name string, typ model.TopologyType, description string) error {
	if id.String() == AutomaticTopologyID {
		return ErrTopologyHasDependents
	}
	if !typ.Valid() {
		return fmt.Errorf("model: 未知的拓扑类型 %q", typ)
	}
	res := db.WithContext(ctx).Model(&model.Topology{}).Where("id = ?", id).
		Updates(map[string]any{"name": name, "type": typ, "description": description})
	if res.Error != nil {
		if isDuplicateKeyError(res.Error) {
			return fmt.Errorf("%w: name=%q", ErrTopologyNameConflict, name)
		}
		return fmt.Errorf("repo: 更新拓扑失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrTopologyNotFound
	}
	return nil
}

// DeleteTopology 删除拓扑前校验是否仍被未删除的 pipeline 引用。软删除
// （沿用 model.Topology.DeletedAt）。判定口径与 DeleteStrategy 一致——
// 只看 gorm 默认 scope（排除已软删除的 pipeline），拓扑同样只是分组信息，
// 不是 pipeline 回收期间需要回连的基础设施资源。
func DeleteTopology(ctx context.Context, db *gorm.DB, id uuidx.UUID) error {
	if id.String() == AutomaticTopologyID {
		return ErrTopologyHasDependents
	}
	return db.Transaction(func(tx *gorm.DB) error {
		if _, err := GetTopology(ctx, tx, id); err != nil {
			return err
		}

		var pipelineCount int64
		if err := tx.WithContext(ctx).Model(&model.Pipeline{}).Where("topology_id = ?", id).Count(&pipelineCount).Error; err != nil {
			return fmt.Errorf("repo: 统计拓扑下 pipeline 失败: %w", err)
		}
		if pipelineCount > 0 {
			return fmt.Errorf("%w: 仍有 %d 条 pipeline 归属该拓扑", ErrTopologyHasDependents, pipelineCount)
		}

		if err := tx.WithContext(ctx).Delete(&model.Topology{}, "id = ?", id).Error; err != nil {
			return fmt.Errorf("repo: 删除拓扑失败: %w", err)
		}
		return nil
	})
}

// TopologyGraphCenter 是 GetTopologyGraph 返回的中心节点（对应
// proto/config.proto 的 CenterInfo）。
type TopologyGraphCenter struct {
	ID              uuidx.UUID
	Code            string
	Name            string
	IsManagerCenter bool
}

// TopologyGraphEdge 是 GetTopologyGraph 返回的一条边：一条 pipeline
// 连接的源、目标中心（对应 proto/config.proto 的 TopologyEdge）。
type TopologyGraphEdge struct {
	PipelineID       uuidx.UUID
	SourceCenterCode string
	TargetCenterCode string
}

// GetTopologyGraph derives center connections from live pipelines. Automatic
// topology includes all groups; other IDs retain their configured filter.
func GetTopologyGraph(ctx context.Context, db *gorm.DB, topologyID uuidx.UUID) (centers []TopologyGraphCenter, edges []TopologyGraphEdge, err error) {
	pipelines, err := ListPipelines(ctx, db, topologyPipelineFilter(topologyID))
	if err != nil {
		return nil, nil, err
	}
	if len(pipelines) == 0 {
		return nil, nil, nil
	}

	endpointIDSet := make(map[uuidx.UUID]struct{}, len(pipelines)*2)
	for _, p := range pipelines {
		endpointIDSet[p.SourceEndpointID] = struct{}{}
		endpointIDSet[p.TargetEndpointID] = struct{}{}
	}
	endpointIDs := make([]uuidx.UUID, 0, len(endpointIDSet))
	for id := range endpointIDSet {
		endpointIDs = append(endpointIDs, id)
	}
	// 定序：Go 的 map 遍历序每次运行都不同，不排的话同一份数据每次生成的
	// SQL 里 IN 列表顺序都不一样。这一处不影响返回内容（下面只拿它填 map），
	// 排它是为了**不把随机序带进 SQL 文本**——日志、慢查询比对、执行计划
	// 缓存都会因此变得不可比。
	slices.SortFunc(endpointIDs, func(a, b uuidx.UUID) int { return a.Compare(b) })
	var endpoints []model.Endpoint
	if err := db.WithContext(ctx).Where("id IN ?", endpointIDs).Find(&endpoints).Error; err != nil {
		return nil, nil, fmt.Errorf("repo: 查询拓扑涉及的端点失败: %w", err)
	}
	endpointCenterID := make(map[uuidx.UUID]uuidx.UUID, len(endpoints))
	centerIDSet := make(map[uuidx.UUID]struct{}, len(endpoints))
	for _, e := range endpoints {
		endpointCenterID[e.ID] = e.CenterID
		centerIDSet[e.CenterID] = struct{}{}
	}

	centerIDs := make([]uuidx.UUID, 0, len(centerIDSet))
	for id := range centerIDSet {
		centerIDs = append(centerIDs, id)
	}
	slices.SortFunc(centerIDs, func(a, b uuidx.UUID) int { return a.Compare(b) })

	// 中心按唯一 code 排序，保证拓扑配置和摘要的输出稳定。
	var centerRows []model.Center
	if err := db.WithContext(ctx).Where("id IN ?", centerIDs).Order("code ASC").Find(&centerRows).Error; err != nil {
		return nil, nil, fmt.Errorf("repo: 查询拓扑涉及的中心失败: %w", err)
	}
	centerByID := make(map[uuidx.UUID]model.Center, len(centerRows))
	centers = make([]TopologyGraphCenter, 0, len(centerRows))
	for _, c := range centerRows {
		centerByID[c.ID] = c
		centers = append(centers, TopologyGraphCenter{ID: c.ID, Code: c.Code, Name: c.Name, IsManagerCenter: c.IsManagerCenter})
	}

	edges = make([]TopologyGraphEdge, 0, len(pipelines))
	for _, p := range pipelines {
		srcCenterID, ok := endpointCenterID[p.SourceEndpointID]
		if !ok {
			return nil, nil, fmt.Errorf("repo: pipeline %s 的源端点 %s 未查到所属中心", p.ID, p.SourceEndpointID)
		}
		tgtCenterID, ok := endpointCenterID[p.TargetEndpointID]
		if !ok {
			return nil, nil, fmt.Errorf("repo: pipeline %s 的目标端点 %s 未查到所属中心", p.ID, p.TargetEndpointID)
		}
		srcCenter, ok := centerByID[srcCenterID]
		if !ok {
			return nil, nil, fmt.Errorf("repo: pipeline %s 的源端点所属中心 %s 未查到", p.ID, srcCenterID)
		}
		tgtCenter, ok := centerByID[tgtCenterID]
		if !ok {
			return nil, nil, fmt.Errorf("repo: pipeline %s 的目标端点所属中心 %s 未查到", p.ID, tgtCenterID)
		}
		edges = append(edges, TopologyGraphEdge{
			PipelineID:       p.ID,
			SourceCenterCode: srcCenter.Code,
			TargetCenterCode: tgtCenter.Code,
		})
	}
	return centers, edges, nil
}

func topologyReadQuery(ctx context.Context, db *gorm.DB, id uuidx.UUID) *gorm.DB {
	if id.String() == AutomaticTopologyID {
		return db.WithContext(ctx)
	}
	return ReadResourceScope(ctx, db.WithContext(ctx), ReadTopology)
}
