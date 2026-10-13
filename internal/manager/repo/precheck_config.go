package repo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type PrecheckConfig struct {
	Pipeline model.Pipeline
	Digest   string
}

// GetPrecheckConfig reads a consistent snapshot without opening business DBs.
// Its hash input contains credentials, but only the digest leaves this function.
func GetPrecheckConfig(ctx context.Context, db *gorm.DB, id uuidx.UUID) (*PrecheckConfig, error) {
	var config *PrecheckConfig
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		config, err = readPrecheckConfig(ctx, tx, id, false)
		return err
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	return config, err
}

func precheckPipelineMaterial(p model.Pipeline) any {
	return struct {
		ID, Topology, SourceEndpoint, SourceNode, TargetEndpoint, TargetNode, Strategy, Snapshot, Schema uuidx.UUID
		Version, Run, PositionRevision                                                                   uint64
		StrategyVersion                                                                                  uint32
		Mode                                                                                             model.InitMode
		Start                                                                                            model.JSON
		ServerID                                                                                         *uint32
	}{p.ID, p.TopologyID, p.SourceEndpointID, p.SourceNodeID, p.TargetEndpointID, p.TargetNodeID, p.StrategyID, p.SnapshotUUID, p.SchemaUUID, p.ConfigVersion, p.RunID, p.PositionRevision, p.StrategyVersion, p.InitMode, p.StartPosition, p.BinlogServerID}
}

// Lock order is topology -> owning pipeline -> ordered endpoints/nodes/centers.
// Callers must use READ COMMITTED; locking reads serialize configuration writes.
func readPrecheckConfig(ctx context.Context, tx *gorm.DB, id uuidx.UUID, lock bool) (*PrecheckConfig, error) {
	p, err := GetPipeline(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	if lock {
		if err := LockTopology(ctx, tx, p.TopologyID); err != nil {
			return nil, err
		}
		if err := tx.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).First(p, "id=?", id).Error; err != nil {
			return nil, err
		}
	}
	var topology model.Topology
	if err := tx.WithContext(ctx).First(&topology, "id=?", p.TopologyID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrTopologyNotFound
		}
		return nil, err
	}
	var graph []model.Pipeline
	graphQuery := tx.WithContext(ctx)
	if err := graphQuery.Order("id ASC").Limit(4001).Find(&graph).Error; err != nil {
		return nil, err
	}
	if len(graph) > 4000 {
		return nil, ErrPrecheckConfigChanged
	}
	materials := make([]any, 0, len(graph))
	for _, edge := range graph {
		materials = append(materials, precheckPipelineMaterial(edge))
	}
	query := func() *gorm.DB {
		q := tx.WithContext(ctx)
		if lock {
			q = q.Clauses(clause.Locking{Strength: "SHARE"})
		}
		return q
	}
	endpointIDs := []uuidx.UUID{p.SourceEndpointID, p.TargetEndpointID}
	var strategy model.StrategyVersion
	if err := query().Where("strategy_id=? AND version=?", p.StrategyID, p.StrategyVersion).First(&strategy).Error; err != nil {
		return nil, err
	}
	if id, enabled, err := SnapshotReplicaEndpointID(strategy.Rules); err != nil {
		return nil, err
	} else if enabled {
		endpointIDs = append(endpointIDs, id)
	}
	slices.SortFunc(endpointIDs, func(a, b uuidx.UUID) int { return bytes.Compare(a[:], b[:]) })
	endpointIDs = slices.Compact(endpointIDs)
	var endpoints []model.Endpoint
	if err := query().Where("id IN ?", endpointIDs).Order("id ASC").Find(&endpoints).Error; err != nil {
		return nil, err
	}
	expected := len(endpointIDs)
	if len(endpoints) != expected {
		return nil, ErrEndpointNotFound
	}
	epMaterial := []any{}
	centerIDs := []uuidx.UUID{}
	for _, ep := range endpoints {
		epMaterial = append(epMaterial, struct {
			ID, Center                                      uuidx.UUID
			Host, User, Password, Version, ServerUUID, Case string
			Port                                            uint16
			TLS, Params                                     model.JSON
		}{ep.ID, ep.CenterID, ep.Host, ep.Username, ep.Password, ep.Version, ep.ServerUUID, ep.LowerCaseTableNames, ep.Port, ep.TLS, ep.Params})
		centerIDs = append(centerIDs, ep.CenterID)
	}
	var nodes []model.Node
	if err := query().Where("id IN ?", []uuidx.UUID{p.SourceNodeID, p.TargetNodeID}).Order("id ASC").Find(&nodes).Error; err != nil {
		return nil, err
	}
	expected = 2
	if p.SourceNodeID == p.TargetNodeID {
		expected = 1
	}
	if len(nodes) != expected {
		return nil, ErrNodeNotFound
	}
	nodeMaterial := []any{}
	for _, node := range nodes {
		nodeMaterial = append(nodeMaterial, struct {
			ID, Center        uuidx.UUID
			Status            model.NodeStatus
			Identity, Version string
			Labels            model.JSON
			Capabilities      model.JSON `json:",omitempty"`
		}{node.ID, node.CenterID, node.Status, node.TokenHash, node.Version, node.Labels, node.Capabilities})
	}
	var centers []model.Center
	slices.SortFunc(centerIDs, func(a, b uuidx.UUID) int { return bytes.Compare(a[:], b[:]) })
	centerIDs = slices.Compact(centerIDs)
	if err := query().Where("id IN ?", centerIDs).Order("id ASC").Find(&centers).Error; err != nil {
		return nil, err
	}
	expected = len(centerIDs)
	if len(centers) != expected {
		return nil, ErrCenterNotFound
	}
	centerMaterial := []any{}
	for _, center := range centers {
		centerMaterial = append(centerMaterial, struct {
			ID, Inspector             uuidx.UUID
			Code, URL, User, Password string
			Manager                   bool
		}{center.ID, center.InspectorNodeID, center.Code, center.MQMgmtURL, center.MQMgmtUser, center.MQMgmtPassword, center.IsManagerCenter})
	}
	var origins []model.CenterOriginUUID
	if err := query().Where("center_id IN ?", centerIDs).Order("uuid ASC").Limit(20001).Find(&origins).Error; err != nil {
		return nil, err
	}
	if len(origins) > 20000 {
		return nil, ErrPrecheckConfigChanged
	}
	originMaterial := []any{}
	for _, origin := range origins {
		originMaterial = append(originMaterial, struct {
			Center, UUID uuidx.UUID
			Kind         model.OriginKind
			Ref          string
		}{origin.CenterID, origin.UUID, origin.Kind, origin.RefID})
	}
	graphIDs := make([]uuidx.UUID, 0, len(graph))
	for _, edge := range graph {
		graphIDs = append(graphIDs, edge.ID)
	}
	var versions []model.StrategyVersion
	if err := tx.WithContext(ctx).Table("strategy_versions AS sv").Select("sv.*").Joins("JOIN pipelines AS p ON p.strategy_id=sv.strategy_id AND p.strategy_version=sv.version").Where("p.id IN ? AND p.deleted_at IS NULL", graphIDs).Group("sv.id").Order("sv.id ASC").Find(&versions).Error; err != nil {
		return nil, err
	}
	versionMaterial := []any{}
	for _, version := range versions {
		versionMaterial = append(versionMaterial, struct {
			ID      uuidx.UUID
			Version uint32
			Rules   model.JSON
		}{version.StrategyID, version.Version, version.Rules})
	}
	var plan model.SchemaPlan
	err = query().Where("pipeline_id=? AND run_id=?", p.ID, p.RunID).Take(&plan).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	planMaterial := struct {
		Digest    string
		Version   uint64
		Confirmed bool
	}{plan.Digest, plan.ConfigVersion, plan.ConfirmedAt != nil}
	analysis, err := GetTopologyAnalysis(ctx, tx, uuidx.MustParse(AutomaticTopologyID))
	if err != nil {
		return nil, err
	}
	material := struct {
		Pipeline                                            any
		TopologyType                                        model.TopologyType
		Graph, Endpoints, Nodes, Centers, Origins, Versions []any
		Plan                                                any
		TopologyPolicy                                      string
	}{precheckPipelineMaterial(*p), topology.Type, materials, epMaterial, nodeMaterial, centerMaterial, originMaterial, versionMaterial, planMaterial, analysis.PolicyDigest}
	encoded, err := json.Marshal(material)
	if err != nil {
		return nil, errors.New("precheck configuration cannot be encoded")
	}
	hash := sha256.Sum256(encoded)
	return &PrecheckConfig{Pipeline: *p, Digest: hex.EncodeToString(hash[:])}, nil
}
