package http

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/pipelinespec"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/topologycheck"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type pipelinePairSideRequest struct {
	Rules           json.RawMessage `json:"rules,omitempty"`
	EndpointID      string          `json:"endpointId" binding:"required"`
	NodeID          string          `json:"nodeId" binding:"required"`
	StrategyID      string          `json:"strategyId"`
	StrategyVersion uint32          `json:"strategyVersion"`
}

type pipelinePairRequest struct {
	RequestID  string                  `json:"requestId" binding:"required"`
	TopologyID string                  `json:"topologyId"`
	Name       string                  `json:"name" binding:"required"`
	A          pipelinePairSideRequest `json:"a" binding:"required"`
	B          pipelinePairSideRequest `json:"b" binding:"required"`
}

type pipelinePairSide struct {
	endpoint, node, strategy uuidx.UUID
	version                  uint32
	rules                    json.RawMessage
}

func parsePipelinePairSide(input pipelinePairSideRequest) (pipelinePairSide, error) {
	var side pipelinePairSide
	var err error
	if side.endpoint, err = uuidx.Parse(input.EndpointID); err != nil {
		return side, err
	}
	if side.node, err = uuidx.Parse(input.NodeID); err != nil {
		return side, err
	}
	if hasPipelineRules(input.Rules) {
		if input.StrategyID != "" || input.StrategyVersion != 0 {
			return side, errors.New("rules and strategy reference are mutually exclusive")
		}
		side.rules, err = canonicalPipelineRules(input.Rules)
		if err != nil {
			return side, err
		}
	} else if side.strategy, err = uuidx.Parse(input.StrategyID); err != nil {
		return side, err
	}
	side.version = input.StrategyVersion
	if side.endpoint.IsZero() || side.node.IsZero() || (len(side.rules) == 0 && (side.strategy.IsZero() || side.version == 0)) {
		return side, errors.New("invalid pair side")
	}
	return side, nil
}

func (h *pipelineHandler) createBidirectional(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	var input pipelinePairRequest
	if c.ShouldBindJSON(&input) != nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "请求参数不合法")
		return
	}
	requestID, err := uuidx.Parse(input.RequestID)
	if err != nil || requestID.IsZero() {
		Fail(c, http.StatusBadRequest, "invalid_request_id", "requestId 格式不合法")
		return
	}
	if input.TopologyID == "" {
		input.TopologyID = repo.AutomaticTopologyID
	}
	topologyID, err := uuidx.Parse(input.TopologyID)
	if err != nil || topologyID.IsZero() {
		Fail(c, http.StatusBadRequest, "invalid_topology_id", "topologyId 格式不合法")
		return
	}
	userID, err := uuidx.Parse(UserIDFromContext(c))
	if err != nil || userID.IsZero() {
		Fail(c, http.StatusForbidden, "forbidden", "没有权限执行此操作")
		return
	}
	a, err := parsePipelinePairSide(input.A)
	if err != nil {
		if _, ok := errors.AsType[*pipelineValidationError](err); ok {
			writePipelineValidationError(c, err)
			return
		}
		Fail(c, http.StatusBadRequest, "invalid_side", "A 侧资源引用不合法")
		return
	}
	b, err := parsePipelinePairSide(input.B)
	if err != nil {
		if _, ok := errors.AsType[*pipelineValidationError](err); ok {
			writePipelineValidationError(c, err)
			return
		}
		Fail(c, http.StatusBadRequest, "invalid_side", "B 侧资源引用不合法")
		return
	}
	input.Name = strings.TrimSpace(input.Name)
	if input.Name == "" || len([]rune(input.Name)) > 110 {
		Fail(c, http.StatusBadRequest, "invalid_name", "双向线路名称应为 1～110 个字符")
		return
	}
	// Canonical UUID strings prevent equivalent spelling from changing retries.
	input.RequestID = requestID.String()
	input.TopologyID = topologyID.String()
	input.A.EndpointID = a.endpoint.String()
	input.A.NodeID = a.node.String()
	if len(a.rules) > 0 {
		input.A.Rules = a.rules
	} else {
		input.A.StrategyID = a.strategy.String()
	}
	input.B.EndpointID = b.endpoint.String()
	input.B.NodeID = b.node.String()
	if len(b.rules) > 0 {
		input.B.Rules = b.rules
	} else {
		input.B.StrategyID = b.strategy.String()
	}
	raw, err := json.Marshal(input)
	if err != nil {
		FailInternal(c, err, "encode_bidirectional_request_failed")
		return
	}
	digest := sha256.Sum256(raw)
	pair, err := h.createPipelinePair(c.Request.Context(), requestID, userID, topologyID, hex.EncodeToString(digest[:]), input.Name, a, b, UsernameFromContext(c))
	if err != nil {
		if errors.Is(err, repo.ErrNodeDisabledOrRevoked) {
			Fail(c, http.StatusConflict, "node_unavailable", "节点已禁用、吊销或注销，请重新选择节点")
			return
		}

		if writeNodeQuotaError(c, err) {
			return
		}
		if writeResourceNameError(c, err) || writePipelineOverlapError(c, err) {
			return
		}
		switch {
		case errors.Is(err, repo.ErrPipelinePairRequestConflict):
			Fail(c, http.StatusConflict, "request_conflict", "该 requestId 已用于其他双向创建请求")
		case errors.Is(err, repo.ErrPipelinePairResultUnavailable):
			Fail(c, http.StatusConflict, "result_unavailable", "此前创建的线路已删除，请使用新的 requestId")
		case errors.Is(err, repo.ErrPipelineNameConflict), errors.Is(err, repo.ErrStrategyNameConflict):
			Fail(c, http.StatusConflict, "name_conflict", "双向线路或生成策略名称已存在")
		case errors.Is(err, repo.ErrTopologyNotFound):
			Fail(c, http.StatusNotFound, "not_found", "历史线路分组不存在")
		case errors.Is(err, topologycheck.ErrBidirectionalInitialization):
			Fail(c, http.StatusConflict, "bidirectional_initialization_conflict", "现有反向线路不满足单方向全量和反向当前最新的初始化规则")
		default:
			if _, ok := errors.AsType[*pipelineValidationError](err); ok {
				writePipelineValidationError(c, err)
			} else {
				FailInternal(c, err, "create_bidirectional_failed")
			}
		}
		return
	}
	OK(c, struct {
		Forward pipelineView `json:"forward"`
		Reverse pipelineView `json:"reverse"`
	}{toPipelineView(c.Request.Context(), h.db, pair[0], nodeHeartbeats{}), toPipelineView(c.Request.Context(), h.db, pair[1], nodeHeartbeats{})})
}

// createPipelinePair owns the complete transaction, including request identity
// and audit. A lost HTTP response can be retried without creating another pair.
func (h *pipelineHandler) createPipelinePair(ctx context.Context, requestID, userID, topologyID uuidx.UUID, digest, name string, a, b pipelinePairSide, username string) ([2]model.Pipeline, error) {
	var pair [2]model.Pipeline
	err := h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := repo.LockTopology(ctx, tx, topologyID); err != nil {
			return err
		}
		prior, err := repo.GetPipelinePairRequest(ctx, tx, requestID)
		if err == nil {
			if prior.Digest != digest || prior.RequestedBy != userID {
				return repo.ErrPipelinePairRequestConflict
			}
			for i, id := range []uuidx.UUID{prior.ForwardID, prior.ReverseID} {
				p, err := repo.GetPipeline(ctx, tx, id)
				if err != nil {
					if errors.Is(err, repo.ErrPipelineNotFound) {
						return repo.ErrPipelinePairResultUnavailable
					}
					return err
				}
				pair[i] = *p
			}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		topology, err := repo.GetTopology(ctx, tx, topologyID)
		if err != nil {
			return err
		}
		if topology.Type != model.TopologyTypeBidirectional && topology.ID.String() != repo.AutomaticTopologyID {
			return &pipelineValidationError{"invalid_topology_type", "历史线路分组不支持双向创建"}
		}
		if err := validatePipelineEndpoints(ctx, tx, a.endpoint, a.node, b.endpoint, b.node); err != nil {
			return err
		}
		source, err := repo.GetEndpoint(ctx, tx, a.endpoint)
		if err != nil {
			return err
		}
		target, err := repo.GetEndpoint(ctx, tx, b.endpoint)
		if err != nil {
			return err
		}
		if source.CenterID == target.CenterID {
			return &pipelineValidationError{"same_center", "双向快捷创建需要两个不同中心"}
		}
		// A pair acquires both endpoint locks in UUID order before saving either
		// direction, so opposite-direction pair requests cannot deadlock.
		endpointIDs := []uuidx.UUID{a.endpoint, b.endpoint}
		if bytes.Compare(endpointIDs[0][:], endpointIDs[1][:]) > 0 {
			endpointIDs[0], endpointIDs[1] = endpointIDs[1], endpointIDs[0]
		}
		for _, endpointID := range endpointIDs {
			var endpoint model.Endpoint
			if err := tx.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).First(&endpoint, "id=?", endpointID).Error; err != nil {
				return err
			}
		}
		transactionHandler := *h
		transactionHandler.db = tx
		for i, side := range []pipelinePairSide{a, b} {
			strategy, err := h.cloneLocalStrategy(ctx, tx, side, userID, requestID, i)
			if err != nil {
				return err
			}
			src, dst := a, b
			mode := model.InitModeFullAndIncremental
			suffix := " A→B"
			if i == 1 {
				src, dst = b, a
				mode = model.InitModeIncrementalOnlyLatest
				suffix = " B→A"
			}
			p := model.Pipeline{Name: name + suffix, TopologyID: topologyID, SourceEndpointID: src.endpoint, SourceNodeID: src.node, TargetEndpointID: dst.endpoint, TargetNodeID: dst.node, StrategyID: strategy, StrategyVersion: 1, InitMode: mode, DesiredState: model.DesiredStateStopped, SourceActualState: model.ActualStateDraft, TargetActualState: model.ActualStateDraft,
				ID: uuidx.New()}
			if err := repo.AssignPipelineRulesOwner(ctx, tx, strategy, p.ID); err != nil {
				return err
			}
			if err := transactionHandler.createPipelineWithBinlogServerID(ctx, &p); err != nil {
				return err
			}
			pair[i] = p
			detail, err := json.Marshal(map[string]any{"username": username, "success": true, "changes": map[string]any{"request_id": requestID.String(), "direction": suffix, "origin_policy": "local_only"}})
			if err != nil {
				return err
			}
			if err := repo.CreateAuditLog(ctx, tx, &model.AuditLog{UserID: userID, Action: model.AuditActionPipelineCreate, TargetType: model.AuditTargetTypePipeline, TargetID: p.ID, Detail: model.JSON(detail)}, time.Now()); err != nil {
				return err
			}
		}
		return repo.CreatePipelinePairRequest(ctx, tx, &model.PipelinePairRequest{ID: requestID, RequestedBy: userID, Digest: digest, ForwardID: pair[0].ID, ReverseID: pair[1].ID})
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	return pair, err
}

func (h *pipelineHandler) cloneLocalStrategy(ctx context.Context, tx *gorm.DB, side pipelinePairSide, userID, requestID uuidx.UUID, index int) (uuidx.UUID, error) {
	rawRules := side.rules
	if len(rawRules) == 0 {
		if err := validateStrategyVersionExists(ctx, tx, side.strategy, side.version); err != nil {
			return uuidx.Nil, err
		}
		version, err := repo.GetStrategyVersion(ctx, tx, side.strategy, side.version)
		if err != nil {
			return uuidx.Nil, err
		}
		rawRules = json.RawMessage(version.Rules)
	}
	doc, err := pipelinespec.ParseStrategyRules(rawRules)
	if err != nil {
		return uuidx.Nil, &pipelineValidationError{"invalid_strategy_rules", "所选策略规则无法解析"}
	}
	if doc.Source == nil {
		doc.Source = &pipelinespec.StrategySourceRules{}
	}
	doc.Source.OriginPolicy = &iproto.OriginPolicy{Mode: iproto.OriginPolicyMode_ORIGIN_POLICY_MODE_LOCAL_ONLY}
	raw, err := pipelinespec.MarshalStrategyRules(doc)
	if err != nil {
		return uuidx.Nil, err
	}
	if err := validateStrategyRules(raw); err != nil {
		return uuidx.Nil, &pipelineValidationError{"invalid_strategy_rules", "所选策略规则不合法"}
	}
	if err := pipelinespec.ValidateObjectFilterRules(doc); err != nil {
		return uuidx.Nil, &pipelineValidationError{"invalid_strategy_rules", "所选策略过滤规则不合法"}
	}
	if pipelinespec.ValidateColumnFilterRules(doc) != nil || pipelinespec.ValidateRowFilterRules(doc) != nil || pipelinespec.ValidateTransformRules(doc) != nil || pipelinespec.ValidateMappingRules(doc) != nil || pipelinespec.ValidateDDLPolicyRules(doc) != nil || pipelinespec.ValidateWritePolicyRules(doc) != nil {
		return uuidx.Nil, &pipelineValidationError{"invalid_strategy_rules", "所选策略过滤规则不合法"}
	}
	strategy := model.Strategy{Name: "bidirectional-" + requestID.String() + []string{"-A", "-B"}[index], Description: "local_only copy of " + side.strategy.String()}
	if err := repo.CreateStrategy(ctx, tx, &strategy); err != nil {
		return uuidx.Nil, err
	}
	if _, err := repo.CreateStrategyVersion(ctx, tx, strategy.ID, model.JSON(raw), userID); err != nil {
		return uuidx.Nil, err
	}
	return strategy.ID, nil
}
