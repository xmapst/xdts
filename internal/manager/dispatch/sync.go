package dispatch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/pipelinespec"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

// SyncPipeline 实现 IPipelineDispatcher.SyncPipeline，见该方法的接口文档。
//
// 流程：组装当前配置 → 分别比较 source/target 两侧的摘要与
// repo.GetLatestAppliedPipelineRelease 的既有摘要 → 任一侧不同则递增
// config_version（repo.BumpPipelineConfigVersion，CAS 乐观锁，命中并发
// 冲突时的处理见调用点注释）、用新版本号重新组装、记录两行新的下发记录
// （各侧各一行）、推送 config.changed 给两端节点。两侧都相同则是纯粹的
// no-op，不写任何东西、不发任何消息。
func (d *Dispatcher) SyncPipeline(ctx context.Context, pipelineID uuidx.UUID) error {
	if err := repo.EnsurePipelineSnapshotRun(ctx, d.db, pipelineID); err != nil {
		return fmt.Errorf("dispatch: prepare snapshot run failed: %w", err)
	}
	p, err := repo.GetPipeline(ctx, d.db, pipelineID)
	if err != nil {
		return fmt.Errorf("dispatch: 读取 pipeline 失败: %w", err)
	}

	result, err := pipelinespec.Assemble(ctx, d.db, pipelineID)
	if err != nil {
		return fmt.Errorf("dispatch: 组装配置失败: %w", err)
	}

	sourceChanged, err := d.digestChanged(ctx, pipelineID, model.SideSource, result.SourceDigest)
	if err != nil {
		return err
	}
	targetChanged, err := d.digestChanged(ctx, pipelineID, model.SideTarget, result.TargetDigest)
	if err != nil {
		return err
	}
	if !sourceChanged && !targetChanged {
		return d.syncDeliveryVersion(ctx, p, result)
	}

	newVersion, err := repo.BumpPipelineConfigVersion(ctx, d.db, pipelineID, p.ConfigVersion)
	if err != nil {
		if errors.Is(err, repo.ErrPipelineConfigVersionConflict) || errors.Is(err, repo.ErrPipelineNotFound) {
			// 两者都是"这次调用期间，别人已经先一步改动了这条 pipeline"
			// 的良性竞争，不是故障：ErrPipelineConfigVersionConflict
			// 表示另一个并发触发源（比如 internal/manager/http/centers.go 的级联）已经抢先
			// 递增了同一个版本号——repo.BumpPipelineConfigVersion 文档
			// 明确说这类竞争是设计上预期存在的，赢的一方会完整地组装 +
			// 落库 + 推送，输的一方没有必要重试：重试只会把"组装 + 落库
			// + 推送"整套流程再跑一遍，而 CAS 存在的意义正是提前识别出
			// 这一点、让输的一方直接退出。ErrPipelineNotFound 表示这条
			// pipeline 在本方法开头的 GetPipeline 与这里的 CAS 之间被
			// 并发软删除，卸载通知是删除 handler 自己调用 NotifyDeleted
			// 的职责（见 internal/manager/http/pipelines.go delete handler），不是本方法要
			// 补的。两种情况都不需要向上传播成错误让调用方以为出了
			// 故障，留一条 Info 方便观测这类竞争确实在发生、发生频率
			// 是否符合预期。
			slog.Info("dispatch: sync pipeline yielded to a concurrent change",
				slog.String("pipeline_id", pipelineID.String()), slog.Any("reason", err))
			return nil
		}
		return fmt.Errorf("dispatch: 递增 config_version 失败: %w", err)
	}

	// Advance configuration metadata before recomputing role digests and publishing.
	result, err = pipelinespec.Assemble(ctx, d.db, pipelineID)
	if err != nil {
		return fmt.Errorf("dispatch: 递增版本号后重新组装配置失败: %w", err)
	}

	if err := repo.CreatePipelineRelease(ctx, d.db, &model.PipelineRelease{
		PipelineID:    pipelineID,
		ConfigVersion: newVersion,
		Side:          model.SideSource,
		NodeID:        p.SourceNodeID,
		ConfigDigest:  result.SourceDigest,
		Status:        model.ReleaseStatusPending,
	}); err != nil {
		return fmt.Errorf("dispatch: 记录源端下发失败: %w", err)
	}
	if err := repo.CreatePipelineRelease(ctx, d.db, &model.PipelineRelease{
		PipelineID:    pipelineID,
		ConfigVersion: newVersion,
		Side:          model.SideTarget,
		NodeID:        p.TargetNodeID,
		ConfigDigest:  result.TargetDigest,
		Status:        model.ReleaseStatusPending,
	}); err != nil {
		return fmt.Errorf("dispatch: 记录目标端下发失败: %w", err)
	}

	d.pushConfigChanged(ctx, pipelineID, newVersion, false, p.SourceNodeID, p.TargetNodeID)
	return nil
}

// NotifyDeleted prompts an authoritative tombstone pull. Cleanup progress and
// unload confirmations are durable independently of pipeline_releases; the
// reclamation loop repeats this notification after delivery or receipt loss.
func (d *Dispatcher) NotifyDeleted(ctx context.Context, pipelineID uuidx.UUID, configVersion uint64, sourceNodeID, targetNodeID uuidx.UUID) error {
	d.pushConfigChanged(ctx, pipelineID, configVersion, true, sourceNodeID, targetNodeID)
	return nil
}

// NotifyNodeMigration publishes the unchanged pipeline configuration to the
// replacement node and to the previous node. Its historical release authorizes
// an explicit ownership_released projection which preserves shared queues;
// the new node receives a normal projection. A release row for the new
// owner is required so its apply receipt can be matched by node identity.
func (d *Dispatcher) NotifyNodeMigration(ctx context.Context, pipelineID uuidx.UUID, configVersion uint64, side model.Side, oldNodeID, newNodeID uuidx.UUID) error {
	p, err := repo.GetPipeline(ctx, d.db, pipelineID)
	if err != nil {
		return fmt.Errorf("dispatch: 读取迁移 pipeline 失败: %w", err)
	}
	if p.ConfigVersion != configVersion {
		return fmt.Errorf("dispatch: 迁移配置版本不匹配: pipeline=%d requested=%d", p.ConfigVersion, configVersion)
	}
	result, err := pipelinespec.Assemble(ctx, d.db, pipelineID)
	if err != nil {
		return fmt.Errorf("dispatch: 组装迁移配置失败: %w", err)
	}
	// Both projections carry the new common version. Recording only the moved
	// role lets origin refresh mistake the other projection for an unissued
	// change and advance the version while the next handover is in progress.
	if err := d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, release := range []model.PipelineRelease{
			{PipelineID: pipelineID, ConfigVersion: configVersion, Side: model.SideSource, NodeID: p.SourceNodeID, ConfigDigest: result.SourceDigest, Status: model.ReleaseStatusPending},
			{PipelineID: pipelineID, ConfigVersion: configVersion, Side: model.SideTarget, NodeID: p.TargetNodeID, ConfigDigest: result.TargetDigest, Status: model.ReleaseStatusPending},
		} {
			if err := repo.CreatePipelineRelease(ctx, tx, &release); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("dispatch: 记录迁移双侧下发失败: %w", err)
	}
	// The unchanged role must observe the same committed common version before
	// a second role migration can request its stopped-state export.
	d.pushConfigChanged(ctx, pipelineID, configVersion, false, oldNodeID, newNodeID, p.SourceNodeID, p.TargetNodeID)
	return nil
}

// digestChanged 判断某一侧当前应下发的摘要是否与"已确认应用"的摘要不同。
// 从未成功应用过（repo.ErrPipelineReleaseNotFound）视为"变了"——没有已知
// 生效配置本身就应该触发一次下发。
func (d *Dispatcher) digestChanged(ctx context.Context, pipelineID uuidx.UUID, side model.Side, currentDigest string) (bool, error) {
	last, err := repo.GetLatestAppliedPipelineRelease(ctx, d.db, pipelineID, side)
	if err != nil {
		if errors.Is(err, repo.ErrPipelineReleaseNotFound) {
			return true, nil
		}
		return false, fmt.Errorf("dispatch: 查询最后一次成功下发记录失败: %w", err)
	}
	return last.ConfigDigest != currentDigest, nil
}

// pushConfigChanged 向去重后的节点集合各推一次 config.changed。发布失败
// （编码、查节点、AMQP 发布任一环节）只记日志、不向上返回 error——这条
// 推送是"尽快让节点知道"的优化路径，不是正确性的唯一来源：
// pipeline_releases 已经落库的 pending 记录本身就是可重试状态的证据。
// 调用方（http handler）的编辑早已经提交成功，不应该因为这一步网络推送
// 失败就让用户以为自己的操作没有生效。
func (d *Dispatcher) pushConfigChanged(ctx context.Context, pipelineID uuidx.UUID, configVersion uint64, deleted bool, nodeIDs ...uuidx.UUID) {
	pub := d.publisher()
	if pub == nil {
		slog.Warn("dispatch: config.changed push skipped, amqp publisher not wired yet",
			slog.String("pipeline_id", pipelineID.String()))
		return
	}

	seen := make(map[uuidx.UUID]struct{}, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		if nodeID.IsZero() {
			continue
		}
		if _, dup := seen[nodeID]; dup {
			// source_node_id == target_node_id（同一节点身兼两侧）时只
			// 推一次——节点收到后按 pipeline_id 发起 config.pull，
			// Manager 的 config.pull 处理器本来就会把该节点在这条
			// pipeline 上的全部角色一并返回，不需要重复的触发信号。
			continue
		}
		seen[nodeID] = struct{}{}
		d.pushConfigChangedToNode(ctx, pub, nodeID, pipelineID, configVersion, deleted)
	}
}

func (d *Dispatcher) pushConfigChangedToNode(ctx context.Context, pub iPublisher, nodeID, pipelineID uuidx.UUID, configVersion uint64, deleted bool) {
	logFields := []any{
		slog.String("node_id", nodeID.String()),
		slog.String("pipeline_id", pipelineID.String()),
	}

	node, err := repo.GetNode(ctx, d.db, nodeID)
	if err != nil {
		slog.Error("dispatch: config.changed push skipped, node lookup failed",
			append(logFields, slog.Any("error", err))...)
		return
	}

	msg := &iproto.ConfigChanged{
		PipelineId:    pipelineID.String(),
		ConfigVersion: configVersion,
		Deleted:       deleted,
	}
	// 控制面消息体积很小，不压缩——与 probe.publishResult/
	// controller.publishApplyResult 的既有选择一致。
	body, encoding, err := iproto.Encode(msg, iproto.EncodeOptions{Compress: false})
	if err != nil {
		slog.Error("dispatch: encode config.changed failed",
			append(logFields, slog.Any("error", err))...)
		return
	}
	env := iproto.NewEnvelope(body, iproto.NewEnvelopeParams{

		MessageID:       uuidx.New().String(),
		Type:            iproto.MessageTypeConfigChanged,
		TimestampMS:     d.now().UnixMilli(),
		ProtocolVersion: iproto.CurrentProtocolVersion(),
		ContentEncoding: encoding,
		// Center/NodeID 留空："Manager 发出的消息没有 x-node-id"
		// （iproto.NewEnvelopeParams 既有文档，internal/manager/task/inspection_dispatch.go
		// 同样的处理方式——这一点先例本身没错，错的是 MessageID 那条，
		// 见上）。
	})
	// 签名用 iproto.DeriveSignKeyFromTokenHash(node.TokenHash)——与
	// task.RPCProbeDispatcher 下行 ProbeRequest 的既有做法一致，Manager
	// 只持有 token_hash，节点侧持有明文 token，两侧各自独立派生出同一个
	// SignKey，不需要分发任何材料。
	signKey := iproto.DeriveSignKeyFromTokenHash(node.TokenHash)
	env.WithSignature(iproto.Sign(signKey, env.SignParamsFor()))

	outcome, err := pub.PublishEnvelope(ctx, transport.ExchangeControl, transport.RoutingKeyNode(nodeID.String()), env)
	if err != nil {
		slog.Error("dispatch: publish config.changed failed",
			append(logFields, slog.Any("error", err))...)
		return
	}
	if outcome != transport.OutcomeConfirmed {
		slog.Error("dispatch: config.changed not confirmed delivered",
			append(logFields, slog.Any("outcome", outcome))...)
	}
}

// A rule save or explicit reapply can advance delivery metadata while retaining
// the same content digest. Both roles must still acknowledge that version.
func (d *Dispatcher) syncDeliveryVersion(ctx context.Context, p *model.Pipeline, result *pipelinespec.AssembleResult) error {
	pending := false
	err := d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, role := range []struct {
			side   model.Side
			node   uuidx.UUID
			digest string
		}{
			{model.SideSource, p.SourceNodeID, result.SourceDigest}, {model.SideTarget, p.TargetNodeID, result.TargetDigest},
		} {
			var release model.PipelineRelease
			err := tx.Where("pipeline_id=? AND side=? AND node_id=? AND config_version=?", p.ID, role.side, role.node, p.ConfigVersion).Order("created_at DESC, id DESC").First(&release).Error
			if err == nil && release.Status == model.ReleaseStatusApplied {
				continue
			}
			pending = true
			if err == nil {
				continue
			}
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			if err := repo.CreatePipelineRelease(ctx, tx, &model.PipelineRelease{PipelineID: p.ID, Side: role.side, NodeID: role.node, ConfigVersion: p.ConfigVersion, ConfigDigest: role.digest, Status: model.ReleaseStatusPending}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if pending {
		d.pushConfigChanged(ctx, p.ID, p.ConfigVersion, false, p.SourceNodeID, p.TargetNodeID)
	}
	return nil
}
