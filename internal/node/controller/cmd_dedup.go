package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/xmapst/xdoc"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/store"
)

// cmd_dedup 集合与文档 ID。单一文档、固定 ID，理由与 appliedState 完全相同：
// store 只有 FindByID（`store.Module.Get`），**没有列举能力**，按
// (pipeline_id, cmd_type) 各开一篇就需要"列出集合下全部文档"这个不存在的能力
// （见 applied_state.go"为什么是单一文档"第 1 条）。
const (
	cmdDedupCollection = "cmd_dedup"
	cmdDedupID         = "node_cmd_dedup"
)

// cmdDedupRecord 保存 (pipeline_id, cmd_type) 的持久化去重槽位。
// xdoc 使用 Go 字段名作为磁盘键；变更字段名需要显式处理存档。
// 流水线 ID 放入记录字段，避免将其直接作为受字符限制的 map 键。
type cmdDedupRecord struct {
	PipelineID    string `json:"pipeline_id"`
	CmdType       int32  `json:"cmd_type"`
	ConfigVersion uint64 `json:"config_version"`
	CmdSeq        uint64 `json:"cmd_seq"`
	// UpdatedAtMs 只供排障与容量观察，**不参与去重判定**。判定用的是
	// (PipelineID, CmdType) 定位槽位、(ConfigVersion, CmdSeq, CmdType) 比对键。
	UpdatedAtMs int64 `json:"updated_at_ms"`
}

// cmdDedupState 是整张去重表的磁盘形态；Records 也是已发布的 xdoc 磁盘键。
type cmdDedupState struct {
	Records []cmdDedupRecord `json:"records"`
}

// toCmdDedupState 把内存表编码成磁盘形态。
//
// CmdType 存枚举数值而不是名字：协议只增不改语义保证数值稳定，名字不保证。
// UNSPECIFIED 仍按未识别指令处理并记录为零值。
func toCmdDedupState(resolved map[cmdDedupSlot]cmdPipelineKey, nowMS int64) cmdDedupState {
	out := cmdDedupState{Records: make([]cmdDedupRecord, 0, len(resolved))}
	for slot, key := range resolved {
		out.Records = append(out.Records, cmdDedupRecord{
			PipelineID:    slot.pipelineID,
			CmdType:       int32(slot.cmdType),
			ConfigVersion: key.ConfigVersion,
			CmdSeq:        key.CmdSeq,
			UpdatedAtMs:   nowMS,
		})
	}
	return out
}

// fromCmdDedupState 把磁盘形态解回内存表。
//
// 槽位键用记录自己的 (PipelineID, CmdType)，而键里的 Type 用同一个 CmdType——
// 二者在写入时必然相同（markCmdResolved 用 key.Type 开槽位），这里不做"两者不一致
// 时不做推断：持久化记录的槽位和记录类型必须来自同一写入点。
func fromCmdDedupState(state cmdDedupState) map[cmdDedupSlot]cmdPipelineKey {
	out := make(map[cmdDedupSlot]cmdPipelineKey, len(state.Records))
	for _, rec := range state.Records {
		t := iproto.PipelineCmdType(rec.CmdType)
		out[cmdDedupSlot{pipelineID: rec.PipelineID, cmdType: t}] = cmdPipelineKey{
			ConfigVersion: rec.ConfigVersion,
			CmdSeq:        rec.CmdSeq,
			Type:          t,
		}
	}
	return out
}

// loadCmdDedup 冷启动读回去重表。
//
// 后续动作相同（都按空表继续、都不阻断启动），区别只在可观测性：
//
// `xdoc.ErrNotFound` 的语义比它的名字宽：实测空 id 也返回 ErrNotFound
// （错误文本 `xdoc: not found:  in "existing"`，id 位置是空的），也就是说它吸收了
// 至少一种参数错误。internal/node/store/store.go 的 Module.Get 有两道前置守卫挡住这一层
// （collectionFiles 白名单 → ErrUnknownCollection；`id == ""` → ErrEmptyID）。
// ⇒ 绕过 store.Get 直连 xdoc.Collection().FindByID 会同时丢掉这两道守卫，
// 而丢掉的后果正是把"id 算错了"读成"没有这条记录"。
//
// 实测：`xdoc.UnmarshalValue` 类型不匹配时目标结构已被填成 `{Records:[]}`，
// 不是完全未触碰。⇒ 忽略 error 只看 `len(Records) == 0` 的话，拿到的是一张
// 与真空表完全不可分辨的空表。磁盘上的坏数据会在内存里变成一张干净的空表。
func loadCmdDedup(ctx context.Context, st iStoreAccess) map[cmdDedupSlot]cmdPipelineKey {
	var state cmdDedupState
	err := st.Get(ctx, cmdDedupCollection, cmdDedupID, &state)
	switch {
	case err == nil:
		slog.Info("cmd dedup table restored", slog.Int("records", len(state.Records)))
		return fromCmdDedupState(state)
	case errors.Is(err, xdoc.ErrNotFound):

		slog.Info("cmd dedup table absent, starting empty")
		return map[cmdDedupSlot]cmdPipelineKey{}
	default:
		// 读到了但解不出来：按空表继续（不阻断启动），但必须留痕。
		slog.Error("cmd dedup table unreadable, dedup unreliable this run",
			slog.Any("error", err))
		return map[cmdDedupSlot]cmdPipelineKey{}
	}
}

// persistCmdDedup 是 cmd_dedup 集合唯一的写入点，用 PutCritical（同步等提交）。
//
// 这张表存在的全部理由是"让去重判据跨进程存活"。用 Put（Cast、不等待）的话，
// 进程在提交前退出就等于没写——而"进程退出"恰恰是这张表要覆盖的那个场景，
// 那样等于什么都没做。这与 persistAppliedState 选 Put 的取舍相反，因为那份快照
// 丢了还有 Manager 重拉兜底（见该函数文档），这张表丢了没有任何补偿路径。
func persistCmdDedup(ctx context.Context, st iStoreAccess, resolved map[cmdDedupSlot]cmdPipelineKey, nowMS int64) error {
	state := toCmdDedupState(resolved, nowMS)
	if err := st.PutCritical(ctx, store.Op{
		Collection: cmdDedupCollection,
		ID:         cmdDedupID,
		Doc:        state,
	}); err != nil {
		return fmt.Errorf("controller: 落盘 cmd_dedup 失败: %w", err)
	}
	return nil
}

// dropCmdDedupFor 删除某条 pipeline 的全部槽位，在它从 appliedState 移除时调用。
//
// cmdDedupSlot 的文档记着"本表没有任何删除路径（pipeline 被删除时不清理这里）"——
// 那在纯内存时代是可接受的取舍（上界 = pipeline 数 × PipelineCmdType 取值数，
// 进程退出即清）。落盘之后不可接受：没有删除路径意味着磁盘单调增长。
//
// 不会。key 含 ConfigVersion，而 Manager 重建 pipeline 用的是新的（严格递增的）
// config_version，旧 cmd_seq 配新 config_version 不会命中被删掉的旧记录——而那种
// 情况下本来就应该执行。
//
// store 没有 Delete，所以"删除"= 过滤后重写整份文档（xdoc 是整篇替换语义）。
func dropCmdDedupFor(resolved map[cmdDedupSlot]cmdPipelineKey, pipelineID string) (removed int) {
	for slot := range resolved {
		if slot.pipelineID == pipelineID {
			delete(resolved, slot)
			removed++
		}
	}
	return removed
}
