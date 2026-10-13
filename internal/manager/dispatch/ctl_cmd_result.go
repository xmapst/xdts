// `PipelineCmdResult.resulting_state` 不做 proto 枚举 → model.ActualState
// 的转换层，取值原样带（就是枚举的字符串名，含
// `PIPELINE_STATE_UNSPECIFIED`），只给人看。这一条同时消掉了两个错误
// 选项：
//
// "畸形消息"与"UNSPECIFIED"因此是分得开的两件事：前者解码失败或
// pipeline_id 不是合法 UUID，返回 error（processRPC 记 Error 日志，消息
// 仍然被 ack，不会顶住消费者）；后者是一个正常取值，照常记录。
package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/redact"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// eventKindCmdResultFailure/eventKindRecoveryExhausted 区分本包写进
// events 的两种记录——两者的 type/level 都是 error/error，只靠行本身
// 分不出是"某一条指令执行失败"还是"已经放弃自动恢复"，而这两件事运维
// 要做的处置完全不同。
const (
	eventKindCmdResultFailure  = "cmd_result_failure"
	eventKindRecoveryExhausted = "recovery_exhausted"
)

// onCmdResultReq 处理一条 cmd.result。
//
// 返回 error 只用于"这条消息本身有问题"（解码失败、pipeline_id 非法）；
// 节点如实报告的执行失败**不是**本函数的错误，它走 events + Warn 日志。
// 两者混在一起会让 processRPC 的 Error 日志里同时出现"协议坏了"和
// "目标库没连上"，而这两件事该找的人不是同一个。
func (d *Dispatcher) onCmdResultReq(ctx context.Context, env *iproto.Envelope, nodeID uuidx.UUID) error {
	var res iproto.CmdResult
	if err := iproto.Decode(env.Body, env.Properties.ContentEncoding, &res); err != nil {
		return fmt.Errorf("dispatch: 解码 cmd.result 失败: %w", err)
	}

	pipelineResult := res.GetPipeline()
	if pipelineResult == nil {

		slog.Debug("dispatch: cmd.result carries no pipeline payload, nothing to record",
			slog.String("node_id", nodeID.String()),
			slog.String("message_id", env.Properties.MessageID),
			slog.Uint64("cmd_seq", res.GetCmdSeq()))
		return nil
	}

	pipelineID, err := uuidx.Parse(pipelineResult.GetPipelineId())
	if err != nil {
		return fmt.Errorf("dispatch: cmd.result 携带的 pipeline_id %q 不是合法 UUID: %w", pipelineResult.GetPipelineId(), err)
	}

	// 纯诊断：枚举的字符串名原样带走，不翻译成 model.ActualState，
	// PIPELINE_STATE_UNSPECIFIED 也照常带（见本文件顶部文档）。
	resultingState := pipelineResult.GetResultingState().String()

	// UNSPECIFIED 不得归到某个默认类型：proto3 标量缺席与零值编码相同，
	// 这里收到的就是 UNSPECIFIED。把它
	// 折成 START 之类的默认值，等于给一条"不知道是什么指令"的记录安上一个
	// 看起来可信的类型——排查的人会据此排除掉真正的那条指令。
	cmdType := pipelineResult.GetCmdType().String()

	logFields := []any{
		slog.String("node_id", nodeID.String()),
		slog.String("pipeline_id", pipelineID.String()),
		slog.Uint64("cmd_seq", res.GetCmdSeq()),
		slog.String("cmd_type", cmdType),
		slog.String("resulting_state", resultingState),
	}
	if err := d.recordReinitResult(ctx, pipelineID, nodeID, res.GetCmdSeq(), pipelineResult); err != nil {
		return err
	}

	if res.GetSuccess() {
		// REINIT 的成功侧已由 recordReinitResult 验证并持久化；两侧均
		// 确认后才清除 pending。其它成功命令仍只记日志。真实 MySQL
		// 回执测试同时验证过期序号被忽略及 Running 状态不能替代确认。
		slog.Debug("dispatch: cmd.result reported success", logFields...)
		return nil
	}

	sides, sidesAbsence := summarizeSideResults(pipelineResult.GetSides(), res.GetError().GetCode())

	slog.Warn("dispatch: node reported pipeline command failure",
		append(logFields,
			slog.String("error_code", res.GetError().GetCode()),
			slog.String("error_message", redact.PotentialCredentials(res.GetError().GetMessage())))...)

	kind := eventKindCmdResultFailure
	trigger := ""
	if pipelineResult.Retryable != nil && !pipelineResult.GetRetryable() {
		kind = eventKindRecoveryExhausted
		trigger = recoveryTriggerExplicitNonRetryable
	}
	payload, err := json.Marshal(map[string]any{
		"kind":           kind,
		"trigger":        trigger,
		"config_version": pipelineResult.GetConfigVersion(),
		"retryable":      pipelineResult.Retryable,
		"cmd_seq":        res.GetCmdSeq(),

		"cmd_type":        cmdType,
		"resulting_state": resultingState,

		"sides":         sides,
		"sides_absence": sidesAbsence,
		"error_code":    res.GetError().GetCode(),

		"error_message": redact.PotentialCredentials(res.GetError().GetMessage()),
	})
	if err != nil {
		return fmt.Errorf("dispatch: 编码 cmd.result 失败事件 payload 失败: %w", err)
	}

	event := model.Event{
		Ts:         d.now(),
		NodeID:     nodeID,
		PipelineID: pipelineID,
		Type:       model.EventTypeError,
		Level:      model.EventLevelError,
		Payload:    model.JSON(payload),
	}
	if trigger != "" {
		return repo.RecordNonRetryableCommand(ctx, d.db, pipelineID, nodeID, res.GetCmdSeq(), pipelineResult.GetConfigVersion(), event, d.now())
	}
	if err := repo.InsertEvents(ctx, d.db, []model.Event{event}); err != nil {
		return fmt.Errorf("dispatch: 写入 cmd.result 失败事件失败: %w", err)
	}
	return nil
}

// 把结论存下来而不是让读的人回头再推一遍：判定依据是 Code 的集合，
// 而 Code 集合会随节点演进变化，事后拿新集合去解释记录会得出错误答案。
const (
	sidesAbsenceNotEmpty     = ""
	sidesAbsenceNoSideRan    = "no_side_executed"
	sidesAbsenceUndetermined = "undetermined"
)

// noSideRanErrorCodes 是"零侧参与执行"那一类的 Code 集合。
// CMD_TYPE_ 前缀用 HasPrefix 匹配：该前缀下今天有三个取值
// （NOT_IMPLEMENTED / REINIT_NOT_IMPLEMENTED / UNRECOGNIZED），它们共同的
// 性质是"指令类型这一关就没过去"，新增同类取值时不必回来改这里。
var noSideRanErrorCodes = map[string]struct{}{
	"CONFIG_PULL_FAILED":    {},
	"REINIT_CONFIG_INVALID": {},
	"PIPELINE_UNKNOWN":      {},
}

// summarizeSideResults 把协议的按侧结果摊平成可落库的结构，并在 sides 为空
// 时给出可决的缺席原因。
//
// 枚举一律取字符串名原样带走、不翻译成 model 层类型——与同一文件里
// resulting_state / cmd_type 完全同一个形状。
func summarizeSideResults(sides []*iproto.PipelineSideResult, topLevelCode string) ([]map[string]any, string) {
	if len(sides) == 0 {
		switch {
		case topLevelCode == "CMD_EXEC_FAILED":
			return nil, sidesAbsenceUndetermined
		case strings.HasPrefix(topLevelCode, "CMD_TYPE_"):
			return nil, sidesAbsenceNoSideRan
		default:
			if _, ok := noSideRanErrorCodes[topLevelCode]; ok {
				return nil, sidesAbsenceNoSideRan
			}
			// Code 不在任何已知集合里：不猜。两种结论都会被后续读者当成
			// 事实，而这里没有依据选其中一个。
			return nil, sidesAbsenceUndetermined
		}
	}

	out := make([]map[string]any, 0, len(sides))
	for _, sd := range sides {
		entry := map[string]any{
			"role":            sd.GetRole().String(),
			"resulting_state": sd.GetResultingState().String(),
		}
		if e := sd.GetError(); e != nil {
			entry["error_code"] = e.GetCode()

			entry["error_message"] = redact.PotentialCredentials(e.GetMessage())
		}
		out = append(out, entry)
	}
	return out, sidesAbsenceNotEmpty
}
