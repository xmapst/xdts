package controller

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/safeerror"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// buildApplyResult 构造一次配置应用的回执（NODE-CFG-03）。applyErr 为 nil 表示这条
// pipeline 的这次装配/卸载/无操作确认已经成功，success=true 且 Error 留空；非 nil 时
// success=false，Error.Message 经 safeerror.Public 映射；底层错误文本
// 可能包含连接凭据，不允许原样出节点。
//
// 是纯函数：不做网络调用，方便单独验证"给定装配结果，回执的字段应该是什么"，不需要
// 每次都真的发一条消息出去才能测。
func buildApplyResult(item *iproto.PipelineConfigItem, kind changeKind, applyErr error) *iproto.ConfigApplyResult {
	result := &iproto.ConfigApplyResult{
		PipelineId:    item.GetPipelineId(),
		ConfigVersion: item.GetConfigVersion(),
		Role:          item.GetRole(),
		Success:       applyErr == nil,
		ChangeKind:    kind.toProto(),
	}
	if applyErr != nil {
		result.Error = &iproto.Error{
			Code:    "CONFIG_APPLY_FAILED",
			Message: safeerror.Public(applyErr),
		}
	}
	return result
}

func buildRejectedApplyResult(item *iproto.PipelineConfigItem, appliedVersion uint64) *iproto.ConfigApplyResult {
	return &iproto.ConfigApplyResult{
		PipelineId:    item.GetPipelineId(),
		ConfigVersion: item.GetConfigVersion(),
		Role:          item.GetRole(),
		Success:       false,
		ChangeKind:    changeReject.toProto(),
		Error: &iproto.Error{
			Code:    "CONFIG_VERSION_REGRESSED",
			Message: fmt.Sprintf("配置版本 %d 早于已应用版本 %d，已拒绝", item.GetConfigVersion(), appliedVersion),
		},
	}
}

// publishApplyResult 组装 ConfigApplyResult 信封（编码 → 签名）并发布到
// transport.ExchangeRPC/RoutingKeyRPC()——config.apply.result 是节点主动发起的
// 单向通知（NODE-CFG-03 的回执本身不期待 Manager 再回一条确认，proto/control.proto
// 没有定义 ConfigApplyResultReply 这个类型），与 config.pull 这类"发出去等一个匹配
// 回复"的请求不同，不经过 transport.RPCClient，直接用 Publisher 发布即可——与
// reporter.publishMessage/probe.publishResult 是同一个"编码→签名→发布→按 Outcome
// 判定"骨架，只是这里的目标交换机/路由键固定是 ExchangeRPC/RoutingKeyRPC()
// （"xdts.rpc...发布方=节点...路由键=rpc"，Manager 侧按 properties.type 分派，
// 不靠路由键区分具体是哪种节点主动消息）。
func (m *Module) publishApplyResult(ctx context.Context, result *iproto.ConfigApplyResult) error {
	body, encoding, err := iproto.Encode(result, iproto.EncodeOptions{Compress: false})
	if err != nil {
		return fmt.Errorf("controller: 编码 config.apply.result 失败: %w", err)
	}

	env := iproto.NewEnvelope(body, iproto.NewEnvelopeParams{
		MessageID:       uuidx.New().String(),
		Type:            iproto.MessageTypeConfigApplyResult,
		TimestampMS:     m.now().UnixMilli(),
		ProtocolVersion: m.opts.ProtocolVersion,
		Center:          m.opts.Center,
		NodeID:          m.opts.NodeID,
		ContentEncoding: encoding,
		HeaderExtra: map[string]any{
			iproto.HeaderPipelineID:    result.GetPipelineId(),
			iproto.HeaderConfigVersion: iproto.HeaderUint64String(result.GetConfigVersion()),
		},
	})
	sig := iproto.Sign(m.signKey, env.SignParamsFor())
	env.WithSignature(sig)

	pubCtx, cancel := context.WithTimeout(ctx, m.opts.PublishTimeout)
	defer cancel()
	outcome, err := m.opts.Publisher.PublishEnvelope(pubCtx, transport.ExchangeRPC, transport.RoutingKeyRPC(), env)
	if err != nil {
		return fmt.Errorf("controller: 发布 config.apply.result 失败: %w", err)
	}
	if outcome != transport.OutcomeConfirmed {
		return fmt.Errorf("controller: config.apply.result 未被判定为已投递，outcome=%s", outcome)
	}
	return nil
}

// publishCmdResult 编码并签名指令回执，发布到 Manager 的 RPC 队列。
// CmdResult 确认具体指令的执行结果；流水线收敛仍以 PipelineStatus 为准。
func (m *Module) publishCmdResult(ctx context.Context, result *iproto.CmdResult) error {
	body, encoding, err := iproto.Encode(result, iproto.EncodeOptions{Compress: false})
	if err != nil {
		return fmt.Errorf("controller: 编码 cmd.result 失败: %w", err)
	}

	env := iproto.NewEnvelope(body, iproto.NewEnvelopeParams{
		MessageID:       uuidx.New().String(),
		Type:            iproto.MessageTypeCmdResult,
		TimestampMS:     m.now().UnixMilli(),
		ProtocolVersion: m.opts.ProtocolVersion,
		Center:          m.opts.Center,
		NodeID:          m.opts.NodeID,
		ContentEncoding: encoding,
		HeaderExtra: map[string]any{
			iproto.HeaderPipelineID: result.GetPipeline().GetPipelineId(),
		},
	})
	sig := iproto.Sign(m.signKey, env.SignParamsFor())
	env.WithSignature(sig)

	pubCtx, cancel := context.WithTimeout(ctx, m.opts.PublishTimeout)
	defer cancel()
	outcome, err := m.opts.Publisher.PublishEnvelope(pubCtx, transport.ExchangeRPC, transport.RoutingKeyRPC(), env)
	if err != nil {
		return fmt.Errorf("controller: 发布 cmd.result 失败: %w", err)
	}
	if outcome != transport.OutcomeConfirmed {
		return fmt.Errorf("controller: cmd.result 未被判定为已投递，outcome=%s", outcome)
	}
	return nil
}

// buildCmdResult reports the command identity and sanitized failure details.
// Only transient failures are marked retryable.
func buildCmdResult(pipelineID string, key cmdPipelineKey, resultingState iproto.PipelineState,
	execErr error, outcomes []cmdSideOutcome) *iproto.CmdResult {
	result := &iproto.CmdResult{
		CmdSeq:  key.CmdSeq,
		Success: execErr == nil,
		Target: &iproto.CmdResult_Pipeline{Pipeline: &iproto.PipelineCmdResult{
			PipelineId:     pipelineID,
			ConfigVersion:  key.ConfigVersion,
			Retryable:      outcomesRetryable(outcomes),
			ResultingState: resultingState,
			CmdType:        key.Type,
			Sides:          buildSideResults(outcomes),
		}},
	}
	if execErr != nil {
		result.Error = &iproto.Error{
			Code:    "CMD_EXEC_FAILED",
			Message: safeerror.Public(execErr),
		}
		if len(outcomes) == 0 {

			slog.Error("controller: cmd result reports failure with no side detail",
				slog.String("pipeline_id", pipelineID),
				slog.Uint64("cmd_seq", key.CmdSeq),
				slog.Any("cmd_type", key.Type))
		}
	}
	return result
}

// buildSideResults 列出实际参与执行的各侧结果；派发前拒绝时列表为空。
// 角色来自执行上下文，顶层成功状态与所有侧的结果一致。
func buildSideResults(outcomes []cmdSideOutcome) []*iproto.PipelineSideResult {
	if len(outcomes) == 0 {
		return nil
	}
	sides := make([]*iproto.PipelineSideResult, 0, len(outcomes))
	for _, o := range outcomes {
		side := &iproto.PipelineSideResult{Role: o.role, ResultingState: o.state}
		if o.err != nil {
			// 与顶层 Error 同一个 Code：这一侧的失败与整条指令的失败是同一类，
			// 区别只在归属粒度；与顶层使用同一个出站边界。
			side.Error = &iproto.Error{Code: "CMD_EXEC_FAILED", Message: safeerror.Public(o.err)}
		}
		sides = append(sides, side)
	}
	return sides
}
