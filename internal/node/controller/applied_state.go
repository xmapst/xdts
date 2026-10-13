package controller

import (
	"context"
	"encoding/base64"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/store"
)

const (
	appliedStateCollection = "configs"
	appliedStateID         = "node_pipelines"
)

// appliedKey identifies the independent source or target configuration slot.
type appliedKey struct {
	pipelineID string
	role       iproto.PipelineRole
}

// keyOf 从配置投影本身取出它的记账键——role 的唯一来源是 item.Role（Manager 在
// pipelinespec 组装时按侧填死），本包任何地方都不自行推断或改写它。
func keyOf(item *iproto.PipelineConfigItem) appliedKey {
	return appliedKey{pipelineID: item.GetPipelineId(), role: item.GetRole()}
}

func (k appliedKey) String() string {
	return k.pipelineID + "/" + k.role.String()
}

// appliedPipeline holds actor-owned configuration and runtime intent. Stopped
// or paused roles retain their configuration without assembled modules.
type appliedPipeline struct {
	*iproto.PipelineConfigItem
	running bool

	// paused distinguishes PAUSE from STOP when modules are not running.
	paused bool

	// unloadUncertain means module removal was not confirmed. It prevents a false
	// running/stopped claim and requires conservative cleanup before assembly.
	// This process-local uncertainty is cleared by restarting the process.
	unloadUncertain bool
}

// appliedRecord is the published disk representation of appliedPipeline.
// Configuration is stored as base64 protobuf and rebuilt through normal assembly.
// Stopped uses inverse polarity so a true value records an explicitly stopped role.
// Paused is meaningful only when Stopped is true.
// Process-local unload uncertainty is not persisted.
type appliedRecord struct {
	ItemB64       string `json:"item_b64"`
	ConfigVersion uint64 `json:"config_version"`
	UpdatedAtMs   int64  `json:"updated_at_ms"`
	Role          int32  `json:"role"`
	Stopped       bool   `json:"stopped"`
	Paused        bool   `json:"paused"`
}

// appliedState persists the complete successfully applied set as one document.
// Desired configuration is not persisted separately. Replacing the whole set
// keeps removed pipelines from reappearing after restart.
type appliedState struct {
	// PipelineSides stores source and target applied records independently.
	PipelineSides map[string]appliedSides `json:"pipeline_sides"`
}

// appliedSides 是一条 pipeline 在本节点持有的各角色记录。两个字段都可以为 nil：
// 非自环拓扑下本节点只持有一侧，另一侧恒为 nil；两个都为 nil 的条目不会被写出
// （persistAppliedState 只为实际存在的记录建条目）。
type appliedSides struct {
	Source *appliedRecord `json:"source"`
	Target *appliedRecord `json:"target"`
}

// sideSlot 返回 role 对应的槽位指针，第二个返回值为 false 表示这个 role 没有
// 槽位可放（只有 PIPELINE_ROLE_UNSPECIFIED 会落到这里——协议里 role 是
// SOURCE/TARGET 二值加一个未指定值）。调用方必须检查它并**拒绝**，不能静默丢弃：
// 一条角色未指定的记录既不该被装配、更不该悄悄从快照里消失。
func (s *appliedSides) sideSlot(role iproto.PipelineRole) (**appliedRecord, bool) {
	switch role {
	case iproto.PipelineRole_PIPELINE_ROLE_SOURCE:
		return &s.Source, true
	case iproto.PipelineRole_PIPELINE_ROLE_TARGET:
		return &s.Target, true
	default:
		return nil, false
	}
}

// toAppliedRecord encodes a successfully applied role configuration.
// Stopped preserves inverse polarity; impossible lifecycle combinations are rejected
// before persistence.
func toAppliedRecord(ap appliedPipeline, nowMS int64) (appliedRecord, error) {
	if ap.running && ap.paused {
		return appliedRecord{}, fmt.Errorf(
			"controller: pipeline %q 的已应用状态违反不变式 running ⇒ !paused（running 与 paused 同时为真），拒绝落盘",
			ap.GetPipelineId())
	}
	raw, err := proto.Marshal(ap.PipelineConfigItem)
	if err != nil {
		return appliedRecord{}, fmt.Errorf("controller: 编码 pipeline %q 的已应用快照失败: %w", ap.GetPipelineId(), err)
	}
	return appliedRecord{
		ConfigVersion: ap.GetConfigVersion(),
		Role:          int32(ap.GetRole()),
		ItemB64:       base64.StdEncoding.EncodeToString(raw),
		UpdatedAtMs:   nowMS,
		Stopped:       !ap.running,
		Paused:        ap.paused,
	}, nil
}

// fromAppliedRecord 是 toAppliedRecord 的反向操作，恢复出的 appliedPipeline.
// PipelineConfigItem 用于重新走 buildSourceOptions/buildTargetOptions（见
// appliedRecord 文档）；running 由 Stopped 取反得到，paused 由 Paused 直接
// 透传得到，极性理由分别见 appliedRecord.Stopped/Paused 各自的文档。
func fromAppliedRecord(rec appliedRecord) (appliedPipeline, error) {
	raw, err := base64.StdEncoding.DecodeString(rec.ItemB64)
	if err != nil {
		return appliedPipeline{}, fmt.Errorf("controller: 解码已应用快照的 base64 失败: %w", err)
	}
	var item iproto.PipelineConfigItem
	if err := proto.Unmarshal(raw, &item); err != nil {
		return appliedPipeline{}, fmt.Errorf("controller: 解码已应用快照失败: %w", err)
	}
	if item.GetPipelineId() == "" || item.GetCommon().GetProtocolVersion() != iproto.CurrentProtocolVersion() ||
		(item.GetRole() != iproto.PipelineRole_PIPELINE_ROLE_SOURCE && item.GetRole() != iproto.PipelineRole_PIPELINE_ROLE_TARGET) ||
		int32(item.GetRole()) != rec.Role || item.GetConfigVersion() != rec.ConfigVersion || !rec.Stopped && rec.Paused {
		return appliedPipeline{}, fmt.Errorf("controller: invalid applied snapshot identity or lifecycle")
	}
	return appliedPipeline{PipelineConfigItem: &item, running: !rec.Stopped, paused: rec.Paused}, nil
}

// persistAppliedState writes the complete actor-owned applied set as one document.
// This noncritical restart fallback does not establish a running module or durable
// command success; those guarantees belong to their lifecycle persistence paths.
func persistAppliedState(st iStoreAccess, applied map[appliedKey]appliedPipeline, nowMS int64) error {

	state := appliedState{
		PipelineSides: make(map[string]appliedSides, len(applied)),
	}
	for key, ap := range applied {
		rec, err := toAppliedRecord(ap, nowMS)
		if err != nil {
			return err
		}
		sides := state.PipelineSides[key.pipelineID]
		slot, ok := sides.sideSlot(key.role)
		if !ok {
			// 角色未指定的记录没有槽位可放。拒绝而不是静默丢弃——丢弃会让这条
			// pipeline 的状态凭空从快照里消失，重启后按"从未装配"处理，而
			// 制造它的那个写入点不会收到任何信号（与 toAppliedRecord 拒绝
			// running&&paused 同一条纪律）。
			return fmt.Errorf("controller: pipeline %q 的已应用记录角色未指定（role=%s），拒绝落盘",
				key.pipelineID, key.role)
		}
		*slot = &rec
		state.PipelineSides[key.pipelineID] = sides
	}
	return st.Put(store.Op{
		Collection: appliedStateCollection,
		ID:         appliedStateID,
		Doc:        state,
	})
}

// loadAppliedState restores startup history before any writer or assembly starts.
// Missing state is permitted; malformed records cannot authorize unsafe resume.
func loadAppliedState(ctx context.Context, st iStoreAccess) map[appliedKey]appliedPipeline {
	var state appliedState
	if err := st.Get(ctx, appliedStateCollection, appliedStateID, &state); err != nil {
		return map[appliedKey]appliedPipeline{}
	}

	result := make(map[appliedKey]appliedPipeline, len(state.PipelineSides)*2)
	for id, sides := range state.PipelineSides {
		for role, rec := range map[iproto.PipelineRole]*appliedRecord{
			iproto.PipelineRole_PIPELINE_ROLE_SOURCE: sides.Source,
			iproto.PipelineRole_PIPELINE_ROLE_TARGET: sides.Target,
		} {
			if rec == nil {
				continue
			}
			ap, err := fromAppliedRecord(*rec)
			if err != nil || ap.GetPipelineId() != id || ap.GetRole() != role {
				continue
			}
			result[appliedKey{pipelineID: id, role: role}] = ap
		}
	}
	return result
}

// deleteAppliedPipeline 从内存态 applied 集合中移除一个 pipeline 并立即整体重新
// 持久化——与 persistAppliedState 是同一次整篇覆盖写入，调用方（internal/node/controller/apply.go 的卸载
// 路径）不需要单独调用一次"删除"语义的 store 操作，store 本身也没有这种操作
// （见 op.go：只有整篇 Put，没有按字段删除）。delete 整个 appliedPipeline 值，
// item 与 running 一起消失，不存在"只删配置、留下这一位"的中间态。
func deleteAppliedPipeline(st iStoreAccess, applied map[appliedKey]appliedPipeline, key appliedKey, nowMS int64) error {
	delete(applied, key)
	return persistAppliedState(st, applied, nowMS)
}
