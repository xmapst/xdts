package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/redact"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// onConfigApplyResultReq records application results using the authenticated header identity.
// It sends no reply and does not update runtime state, which belongs to ingest.
// Duplicate or pull-generated results may have no pending release; database errors
// and versions beyond Manager's authority remain errors.
func (d *Dispatcher) onConfigApplyResultReq(ctx context.Context, env *iproto.Envelope, nodeID uuidx.UUID) error {
	var result iproto.ConfigApplyResult
	if err := iproto.Decode(env.Body, env.Properties.ContentEncoding, &result); err != nil {
		return fmt.Errorf("dispatch: 解码 config.apply.result 失败: %w", err)
	}

	pipelineID, err := uuidx.Parse(result.GetPipelineId())
	if err != nil {
		return fmt.Errorf("dispatch: config.apply.result 的 pipeline_id=%q 不是合法 UUID: %w", result.GetPipelineId(), err)
	}
	side, err := roleToSide(result.GetRole())
	if err != nil {
		return fmt.Errorf("dispatch: config.apply.result（pipeline_id=%s, node_id=%s）: %w", pipelineID, nodeID, err)
	}
	// Authentication proves the sender's identity; pipeline ownership and the
	// authoritative version must also be checked before any persistent receipt.
	p, err := repo.GetPipelineIncludingDeleted(ctx, d.db, pipelineID)
	if err != nil {
		return fmt.Errorf("dispatch: config.apply.result 查询 pipeline 失败: %w", err)
	}
	owner := p.SourceNodeID
	if side == model.SideTarget {
		owner = p.TargetNodeID
	}
	if owner != nodeID {
		if result.GetConfigVersion() == p.ConfigVersion && result.GetChangeKind() == iproto.PipelineChangeKind_PIPELINE_CHANGE_KIND_UNLOAD {
			released, err := d.releasedPipelineConfigs(ctx, p, nodeID)
			if err != nil {
				return err
			}
			for _, item := range released {
				if item.GetRole() == result.GetRole() {
					// This receipt cannot update the new owner's release or state.
					return nil
				}
			}
		}
		return fmt.Errorf("dispatch: config.apply.result 节点不属于 pipeline 指定侧（pipeline_id=%s, side=%s, node_id=%s）", pipelineID, side, nodeID)
	}
	if p.DeletedAt.Valid {
		return d.recordPipelineUnload(ctx, p, nodeID, side, &result)
	}
	if result.GetConfigVersion() > p.ConfigVersion {
		return fmt.Errorf("dispatch: config.apply.result 携带的 config_version=%d 超过 pipeline 当前记录的 config_version=%d（pipeline_id=%s, side=%s, node_id=%s）",
			result.GetConfigVersion(), p.ConfigVersion, pipelineID, side, nodeID)
	}

	// 每一条回执都记一行 events，**含 change_kind == NONE**，而且写在分支
	// 之前——这两点都是判据要求的一部分，不是顺手：
	//
	//   - 写在分支之前：判据③观测的是**配置拉取**，而拉取路径上 Manager
	//     从来没建过 pending release（见本函数上方"无匹配不是错误"那段），
	//     回执走 handleUnmatchedApplyResult。若在两个分支各写一次，漏掉
	//     拉取那支就会让判据在它唯一要看的路径上查出空集。一个写入点没有
	//     "漏掉某条路径"这种失败模式。
	//   - 含 NONE：判据③断言"零次 rebuild"。**如果这条记录路径压根没跑，
	//     查出来同样是零**，判据照样绿、而且绿得更有说服力（查的是结构化
	//     记录不是日志）。记下全部 kind 之后，判据可以同时断言
	//     `rebuild == 0` 与 `总计数 > 0`——后者才是区分"没发生 rebuild"
	//     与"什么都没记"的那一位。
	//
	// 写失败只记日志、不中断：把回执写回 pipeline_releases 是本函数的
	// 主职责，不能因为一条观测记录写不进去就丢掉它。
	d.recordConfigApplied(ctx, pipelineID, side, nodeID, &result)

	release, err := d.findPendingRelease(ctx, pipelineID, side, nodeID, result.GetConfigVersion())
	if err != nil {
		if errors.Is(err, errNoPendingRelease) {
			return d.handleUnmatchedApplyResult(ctx, pipelineID, side, nodeID, &result)
		}
		return err
	}

	status := releaseStatusForApplyResult(&result)
	message := ""
	if !result.GetSuccess() {
		if e := result.GetError(); e != nil {
			// Sanitize remote errors before persistence so all readers receive safe diagnostics.
			message = fmt.Sprintf("%s: %s", e.GetCode(), redact.PotentialCredentials(e.GetMessage()))
		}
	}
	// The inbound sender was authenticated by processRPC; ownership and the
	// authoritative version were validated before recording this receipt.

	if err := repo.RecordPipelineReleaseResult(ctx, d.db, release.ID, status, message, d.now()); err != nil {
		return fmt.Errorf("dispatch: 写回 config.apply.result 失败: %w", err)
	}
	return nil
}

func releaseStatusForApplyResult(result *iproto.ConfigApplyResult) model.ReleaseStatus {
	if result.GetSuccess() {
		return model.ReleaseStatusApplied
	}
	if result.GetError().GetCode() == "CONFIG_VERSION_REGRESSED" {
		return model.ReleaseStatusRejected
	}
	return model.ReleaseStatusFailed
}

// errNoPendingRelease 是 findPendingRelease 找不到匹配行时返回的哨兵
// 错误，供 onConfigApplyResultReq 用 errors.Is 区分"确实没有这一行"
// （良性，见该函数文档"无匹配 pending 记录不是错误"一节）与
// ListPipelineReleases 查询本身失败（真错误，DB 出了问题）——不能只看
// "findPendingRelease 返回了 error"就一律当同一种情况处理。
var errNoPendingRelease = errors.New("dispatch: 未找到匹配的待确认下发记录")

// 找不到匹配行返回 errNoPendingRelease（包裹了具体上下文，仍可用
// errors.Is 识别）——这本身不是错误状态，是否需要按错误处理由调用方
// （onConfigApplyResultReq）结合更多上下文判断，本函数只负责如实报告
// "有没有找到"，不替调用方下结论。
func (d *Dispatcher) findPendingRelease(ctx context.Context, pipelineID uuidx.UUID, side model.Side, nodeID uuidx.UUID, configVersion uint64) (*model.PipelineRelease, error) {
	all, err := repo.ListPipelineReleases(ctx, d.db, pipelineID)
	if err != nil {
		return nil, fmt.Errorf("dispatch: 查询 pipeline 下发历史失败: %w", err)
	}
	for _, r := range all { // ListPipelineReleases 按 created_at DESC 排序，第一条命中即最新。
		if r.Side == side && r.NodeID == nodeID && r.ConfigVersion == configVersion && r.Status == model.ReleaseStatusPending {
			release := r
			return &release, nil
		}
	}
	return nil, fmt.Errorf("%w（pipeline_id=%s, side=%s, node_id=%s, config_version=%d）",
		errNoPendingRelease, pipelineID, side, nodeID, configVersion)
}

// handleUnmatchedApplyResult 处理 findPendingRelease 找不到匹配 pending
// 行的回执（见 onConfigApplyResultReq 文档"无匹配 pending 记录不是错误"
// 一节的完整背景）。区分三种情况：
func (d *Dispatcher) handleUnmatchedApplyResult(ctx context.Context, pipelineID uuidx.UUID, side model.Side, nodeID uuidx.UUID, result *iproto.ConfigApplyResult) error {
	p, err := repo.GetPipeline(ctx, d.db, pipelineID)
	if err != nil {
		return fmt.Errorf("dispatch: config.apply.result 无匹配下发记录，查询 pipeline 失败: %w", err)
	}
	if result.GetConfigVersion() > p.ConfigVersion {
		return fmt.Errorf(
			"dispatch: config.apply.result 携带的 config_version=%d 超过 pipeline 当前记录的 config_version=%d"+
				"（pipeline_id=%s, side=%s, node_id=%s），可能存在 Manager 未感知的下发来源",
			result.GetConfigVersion(), p.ConfigVersion, pipelineID, side, nodeID)
	}

	if !result.GetSuccess() {
		attrs := []any{
			slog.String("pipeline_id", pipelineID.String()),
			slog.String("side", string(side)),
			slog.String("node_id", nodeID.String()),
			slog.Uint64("config_version", result.GetConfigVersion()),
		}
		if e := result.GetError(); e != nil {

			attrs = append(attrs, slog.String("error_code", e.GetCode()), slog.String("error_message", redact.PotentialCredentials(e.GetMessage())))
		}
		slog.Warn("dispatch: config.apply.result reports apply failure with no matching pending release, treated as pull-triggered or duplicate apply", attrs...)
		return nil
	}

	slog.Debug("dispatch: config.apply.result has no matching pending release, treated as pull-triggered or duplicate apply",
		slog.String("pipeline_id", pipelineID.String()),
		slog.String("side", string(side)),
		slog.String("node_id", nodeID.String()),
		slog.Uint64("config_version", result.GetConfigVersion()),
		slog.Bool("success", result.GetSuccess()))
	return nil
}

// roleToSide 把协议的 PipelineRole 转换成落库用的 model.Side——两者是
// "同一件事在不同层的表示"（iproto 供网络协议自描述，model 是管理库落盘
// 类型， 不共享），转换函数因此需要在某处存在，放在这里
// 是因为目前只有 config.apply.result 这一个方向（协议→落库）需要它。
func roleToSide(role iproto.PipelineRole) (model.Side, error) {
	switch role {
	case iproto.PipelineRole_PIPELINE_ROLE_SOURCE:
		return model.SideSource, nil
	case iproto.PipelineRole_PIPELINE_ROLE_TARGET:
		return model.SideTarget, nil
	default:
		return "", fmt.Errorf("未知或未指定的 role %v", role)
	}
}

// recordConfigApplied 把一次配置应用回执记成一行 events。
//
// 本包另外两种 events 行（cmd.result 失败、放弃自动恢复）都是
// model.EventTypeError，靠 payload 里的 `kind` 互相区分。这一种有自己的
// EventType（config_applied），判别已经由类型承担，再加一个 payload 键
// 等于同一事实两个来源。
func (d *Dispatcher) recordConfigApplied(ctx context.Context, pipelineID uuidx.UUID, side model.Side, nodeID uuidx.UUID, result *iproto.ConfigApplyResult) {
	// change_kind 与本包处理 resulting_state / cmd_type 同一个形状：枚举
	// 字符串名原样带走，UNSPECIFIED 照常带，不折成默认值。
	fields := map[string]any{
		"change_kind":    result.GetChangeKind().String(),
		"config_version": result.GetConfigVersion(),
		"side":           string(side),
		"success":        result.GetSuccess(),
	}
	level := model.EventLevelInfo
	if !result.GetSuccess() {
		level = model.EventLevelWarning
		fields["error_code"] = result.GetError().GetCode()
	}

	payload, err := json.Marshal(fields)
	if err != nil {
		slog.Error("dispatch: encode config applied event payload failed",
			slog.String("pipeline_id", pipelineID.String()),
			slog.Any("error", err))
		return
	}

	event := model.Event{
		Ts:         d.now(),
		NodeID:     nodeID,
		PipelineID: pipelineID,
		Type:       model.EventTypeConfigApplied,
		Level:      level,
		Payload:    model.JSON(payload),
	}
	if err := repo.InsertEvents(ctx, d.db, []model.Event{event}); err != nil {
		slog.Error("dispatch: insert config applied event failed",
			slog.String("pipeline_id", pipelineID.String()),
			slog.String("node_id", nodeID.String()),
			slog.String("side", string(side)),
			slog.Uint64("config_version", result.GetConfigVersion()),
			slog.Any("error", err))
	}
}
