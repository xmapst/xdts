package dispatch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"google.golang.org/protobuf/proto"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/pipelinespec"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// onConfigPullReq returns configurations assigned to the authenticated node.
// Source and target roles are included independently, including same-node copies.
// An assembly failure skips that pipeline without failing the entire response.
func (d *Dispatcher) onConfigPullReq(ctx context.Context, env *iproto.Envelope, nodeID uuidx.UUID) error {
	var req iproto.ConfigPullRequest
	if err := iproto.Decode(env.Body, env.Properties.ContentEncoding, &req); err != nil {
		return fmt.Errorf("dispatch: 解码 config.pull 请求失败: %w", err)
	}
	if req.GetNodeId() != "" && req.GetNodeId() != nodeID.String() {
		return fmt.Errorf("dispatch: config.pull 请求体 node_id=%q 与验签确认的节点 %s 不一致，拒绝",
			req.GetNodeId(), nodeID)
	}

	pipelineIDs, err := d.resolvePullPipelineIDs(ctx, nodeID, req.GetPipelineIds())
	if err != nil {
		return d.replyConfigPullError(ctx, env, nodeID, "PIPELINE_LOOKUP_FAILED", err.Error())
	}

	configs := make([]*iproto.PipelineConfigItem, 0, len(pipelineIDs)*2)
	for _, pipelineID := range pipelineIDs {
		p, err := repo.GetPipelineIncludingDeleted(ctx, d.db, pipelineID)
		if err != nil {
			return err
		}
		if p.DeletedAt.Valid {
			configs = append(configs, deletedPipelineConfigs(*p, nodeID)...)
			continue
		}
		released, err := d.releasedPipelineConfigs(ctx, p, nodeID)
		if err != nil {
			return err
		}
		configs = append(configs, released...)
		if p.SourceNodeID != nodeID && p.TargetNodeID != nodeID {
			continue
		}
		if err := repo.EnsurePipelineSnapshotRun(ctx, d.db, pipelineID); err != nil {
			slog.Error("snapshot allocation failed", slog.String("pipeline_id", pipelineID.String()))
			continue
		}
		p, err = repo.GetPipeline(ctx, d.db, pipelineID)
		if err != nil {
			slog.Error("dispatch: config.pull skip pipeline, get failed",
				slog.String("pipeline_id", pipelineID.String()), slog.String("node_id", nodeID.String()),
				slog.Any("error", err))
			continue
		}
		result, err := pipelinespec.Assemble(ctx, d.db, pipelineID)
		if err != nil {
			slog.Error("dispatch: config.pull skip pipeline, assemble failed",
				slog.String("pipeline_id", pipelineID.String()), slog.String("node_id", nodeID.String()),
				slog.Any("error", err))
			continue
		}
		pendingOrigins, err := repo.PipelineHasPendingOriginRefresh(ctx, d.db, p.TopologyID)
		if err != nil {
			return err
		}
		if pendingOrigins {
			// A REINIT configuration can be delivered by command-triggered pull
			// before SyncPipeline creates releases. Record the actual projection
			// before replying, so its real application ACK can complete the refresh.
			if p.SourceNodeID == nodeID {
				if err := d.recordOriginPull(ctx, p, model.SideSource, nodeID, result.SourceDigest); err != nil {
					return err
				}
			}
			if p.TargetNodeID == nodeID {
				if err := d.recordOriginPull(ctx, p, model.SideTarget, nodeID, result.TargetDigest); err != nil {
					return err
				}
			}
		}
		if p.SourceNodeID == nodeID {
			configs = append(configs, result.Source)
		}
		if p.TargetNodeID == nodeID {
			configs = append(configs, result.Target)
		}
	}

	return d.replyConfigPull(ctx, env, nodeID, &iproto.ConfigPullReply{Configs: configs})
}

// resolvePullPipelineIDs limits pulls to pipelines owned by this node.
// Full pulls exclude unpublished configurations. Explicit pulls remain available
// for lifecycle commands; Node separately checks authorization before assembly.
func (d *Dispatcher) resolvePullPipelineIDs(ctx context.Context, nodeID uuidx.UUID, requested []string) ([]uuidx.UUID, error) {
	if len(requested) == 0 {
		ps, err := repo.ListPipelinesForNodeCleanupPull(ctx, d.db, nodeID)
		if err != nil {
			return nil, fmt.Errorf("查询节点承载的 pipeline 失败: %w", err)
		}
		ids := make([]uuidx.UUID, 0, len(ps))
		for _, p := range ps {
			// A first explicit START can execute revision 0 without a Sync
			// release. Preserve that command's configuration on restart; the
			// node still requires its own durable prior running authorization.
			// Never-commanded revision 0 remains excluded from full pulls.
			if p.ConfigVersion == 0 && p.LastCmdSeq == 0 && !p.DeletedAt.Valid {
				slog.Debug("dispatch: config.pull full pull skips unpublished pipeline",
					slog.String("pipeline_id", p.ID.String()),
					slog.String("node_id", nodeID.String()))
				continue
			}
			ids = append(ids, p.ID)
		}
		return ids, nil
	}

	ids := make([]uuidx.UUID, 0, len(requested))
	for _, raw := range requested {
		id, err := uuidx.Parse(raw)
		if err != nil {
			slog.Error("dispatch: config.pull skip pipeline_id, not a valid uuid",
				slog.String("pipeline_id", raw), slog.String("node_id", nodeID.String()), slog.Any("error", err))
			continue
		}
		p, err := repo.GetPipelineIncludingDeleted(ctx, d.db, id)
		if err != nil {
			if errors.Is(err, repo.ErrPipelineNotFound) {
				slog.Warn("dispatch: config.pull skip pipeline_id, not found",
					slog.String("pipeline_id", raw), slog.String("node_id", nodeID.String()))
				continue
			}
			return nil, fmt.Errorf("查询 pipeline %s 失败: %w", raw, err)
		}
		if p.SourceNodeID != nodeID && p.TargetNodeID != nodeID {
			released, err := d.releasedPipelineConfigs(ctx, p, nodeID)
			if err != nil {
				return nil, err
			}
			if len(released) > 0 {
				ids = append(ids, id)
				continue
			}
			slog.Error("dispatch: config.pull rejected, node does not own requested pipeline",
				slog.String("pipeline_id", raw), slog.String("node_id", nodeID.String()))
			continue
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// replyConfigPull/replyConfigPullError 把回复发到 x-reply-key 指向的
// ExchangeReply 路由键——响应方构造回复的既定做法（internal/transport/rpc.go
// 文档第 4 点、internal/node/probe.publishResult 的既有先例），不经过
// RPCClient，直接复用 Dispatcher 自己持有的 Publisher。
func (d *Dispatcher) replyConfigPull(ctx context.Context, env *iproto.Envelope, nodeID uuidx.UUID, reply *iproto.ConfigPullReply) error {
	return d.publishReply(ctx, env, nodeID, iproto.MessageTypeConfigPullReply, reply)
}

func (d *Dispatcher) replyConfigPullError(ctx context.Context, env *iproto.Envelope, nodeID uuidx.UUID, code, message string) error {
	return d.replyConfigPull(ctx, env, nodeID, &iproto.ConfigPullReply{
		Error: &iproto.Error{Code: code, Message: message},
	})
}

// publishReply 是 config.pull.reply 这类"响应方直接回复"场景的公共实现：
// 编码 → 用请求方（nodeID）的 SignKey 签名 → 发到 x-reply-key。
// CorrelationID 原样透传请求信封的值——internal/transport/rpc.go 文档："Manager 的 RPCClient
// 按 correlation_id 而不是 task_id 把回复路由给等待中的调用方"，必须
// 原样回填，不能重新生成。
func (d *Dispatcher) publishReply(ctx context.Context, reqEnv *iproto.Envelope, nodeID uuidx.UUID, msgType string, body proto.Message) error {
	pub := d.publisher()
	if pub == nil {
		return fmt.Errorf("dispatch: amqp publisher not wired yet, cannot reply to %s", msgType)
	}
	replyKey, ok := transport.ReplyRoutingKeyFor(reqEnv)
	if !ok {
		return fmt.Errorf("dispatch: 请求信封缺少 %s header，无法回复", iproto.HeaderReplyKey)
	}

	encoded, encoding, err := iproto.Encode(body, iproto.EncodeOptions{Compress: false})
	if err != nil {
		return fmt.Errorf("dispatch: 编码 %s 回复失败: %w", msgType, err)
	}
	replyEnv := iproto.NewEnvelope(encoded, iproto.NewEnvelopeParams{

		MessageID:       uuidx.New().String(),
		CorrelationID:   reqEnv.Properties.CorrelationID,
		Type:            msgType,
		TimestampMS:     d.now().UnixMilli(),
		ProtocolVersion: iproto.CurrentProtocolVersion(),
		ContentEncoding: encoding,
	})
	key, err := d.tokenKey(ctx, nodeID)
	if err != nil {
		return fmt.Errorf("dispatch: 解析节点 %s 签名 key 失败: %w", nodeID, err)
	}
	replyEnv.WithSignature(iproto.Sign(key, replyEnv.SignParamsFor()))

	outcome, err := pub.PublishEnvelope(ctx, transport.ExchangeReply, replyKey, replyEnv)
	if err != nil {
		return fmt.Errorf("dispatch: 发布 %s 回复失败: %w", msgType, err)
	}
	if outcome != transport.OutcomeConfirmed {
		return fmt.Errorf("dispatch: %s 回复未被判定为已投递，outcome=%s", msgType, outcome)
	}
	return nil
}
