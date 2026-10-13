package repo

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/topologycheck"
	"github.com/xmapst/xdts/pkg/uuidx"
	"google.golang.org/protobuf/encoding/protojson"
	"gorm.io/gorm"
)

type TopologyAnalysisCenter struct {
	Code            string `json:"code"`
	Name            string `json:"name"`
	IsManagerCenter bool   `json:"isManagerCenter"`
}

type TopologyAnalysis struct {
	TopologyID   string                   `json:"topologyId"`
	Digest       string                   `json:"digest"`
	PolicyDigest string                   `json:"policyDigest"`
	Centers      []TopologyAnalysisCenter `json:"centers"`
	Edges        []topologycheck.Edge     `json:"edges"`
	topologycheck.Result
}

// GetTopologyAnalysis reads a consistent configuration snapshot. Source policy
// is decoded locally to avoid making repo depend on pipelinespec (which uses repo).
func GetTopologyAnalysis(ctx context.Context, db *gorm.DB, id uuidx.UUID) (*TopologyAnalysis, error) {
	var result *TopologyAnalysis
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if _, err := GetTopology(ctx, tx, id); err != nil {
			return err
		}
		centers, edges, err := GetTopologyGraph(ctx, tx, id)
		if err != nil {
			return err
		}
		result = &TopologyAnalysis{TopologyID: id.String(), Centers: []TopologyAnalysisCenter{}, Edges: []topologycheck.Edge{}}
		codes := make([]string, 0, len(centers))
		if len(centers) > 1000 || len(edges) > 4000 {
			return fmt.Errorf("topology graph exceeds size limit")
		}
		pipelines, err := ListPipelines(ctx, tx, topologyPipelineFilter(id))
		if err != nil {
			return err
		}
		byID := make(map[uuidx.UUID]model.Pipeline, len(pipelines))
		for _, p := range pipelines {
			byID[p.ID] = p
		}
		// Bound round trips independently of edge count. Shared immutable
		// strategy versions are read once for the complete graph.
		pipelineIDs := make([]uuidx.UUID, 0, len(pipelines))
		for _, p := range pipelines {
			pipelineIDs = append(pipelineIDs, p.ID)
		}
		var versions []model.StrategyVersion
		if len(pipelines) > 0 {
			if err := tx.WithContext(ctx).Table("strategy_versions AS sv").Select("sv.*").Joins("JOIN pipelines AS p ON p.strategy_id=sv.strategy_id AND p.strategy_version=sv.version").Where("p.id IN ? AND p.deleted_at IS NULL", pipelineIDs).Group("sv.id").Find(&versions).Error; err != nil {
				return err
			}
		}
		byVersion := map[string]model.StrategyVersion{}
		for _, v := range versions {
			byVersion[fmt.Sprintf("%s:%d", v.StrategyID, v.Version)] = v
		}
		for _, c := range centers {
			result.Centers = append(result.Centers, TopologyAnalysisCenter{Code: c.Code, Name: c.Name, IsManagerCenter: c.IsManagerCenter})
			codes = append(codes, c.Code)
		}
		for _, edge := range edges {
			p, ok := byID[edge.PipelineID]
			if !ok {
				return ErrPipelineNotFound
			}
			version, ok := byVersion[fmt.Sprintf("%s:%d", p.StrategyID, p.StrategyVersion)]
			if !ok {
				return ErrStrategyVersionNotFound
			}
			var document struct {
				Source struct {
					Origin json.RawMessage `json:"origin_policy"`
				} `json:"source"`
			}
			if err := json.Unmarshal(version.Rules, &document); err != nil {
				return fmt.Errorf("decode topology strategy: %w", err)
			}
			policy := topologycheck.Policy{Mode: "all", Centers: []string{}}
			if len(document.Source.Origin) > 0 && string(document.Source.Origin) != "null" {
				var origin iproto.OriginPolicy
				if err := protojson.Unmarshal(document.Source.Origin, &origin); err != nil {
					return fmt.Errorf("decode topology origin policy: %w", err)
				}
				switch origin.GetMode() {
				case iproto.OriginPolicyMode_ORIGIN_POLICY_MODE_LOCAL_ONLY:
					policy.Mode = "local_only"
				case iproto.OriginPolicyMode_ORIGIN_POLICY_MODE_INCLUDE_ORIGINS:
					policy.Mode = "include_origins"
				case iproto.OriginPolicyMode_ORIGIN_POLICY_MODE_EXCLUDE_ORIGINS:
					policy.Mode = "exclude_origins"
				case iproto.OriginPolicyMode_ORIGIN_POLICY_MODE_ALL:
					policy.Mode = "all"
				default:
					return fmt.Errorf("invalid topology origin policy")
				}
				policy.Centers = append(policy.Centers, origin.GetCenters()...)
				sort.Strings(policy.Centers)
				policy.MaxHops = origin.GetMaxHops()
			}
			result.Edges = append(result.Edges, topologycheck.Edge{ID: p.ID.String(), Source: edge.SourceCenterCode, Target: edge.TargetCenterCode, Policy: policy, ConfigVersion: p.ConfigVersion})
		}
		sort.Slice(result.Edges, func(i, j int) bool { return result.Edges[i].ID < result.Edges[j].ID })
		result.Result, err = topologycheck.Analyze(codes, result.Edges)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(result)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(raw)
		result.Digest = hex.EncodeToString(digest[:])
		policyEdges := make([]topologycheck.Edge, len(result.Edges))
		copy(policyEdges, result.Edges)
		for i := range policyEdges {
			policyEdges[i].ConfigVersion = 0
		}
		policyRaw, err := json.Marshal(struct {
			ID      string
			Centers []string
			Edges   []topologycheck.Edge
		}{id.String(), codes, policyEdges})
		if err != nil {
			return err
		}
		policyDigest := sha256.Sum256(policyRaw)
		result.PolicyDigest = hex.EncodeToString(policyDigest[:])
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	return result, err
}
