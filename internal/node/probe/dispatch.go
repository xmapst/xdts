package probe

import (
	"context"

	"github.com/xmapst/xdts/internal/iproto"
)

// dispatch 是唯一的 ProbeType 分派点：新增探测类型只需增加分支与执行方法。
// 当前支持 BROKER、CONN_TEST、CATALOG、DEFS、POSITION、PRECHECK、LINK；其余明确返回 UNSUPPORTED。
func (m *Module) dispatch(ctx context.Context, req *iproto.ProbeRequest) *iproto.ProbeResult {
	switch req.Type {
	case iproto.ProbeType_PROBE_TYPE_DATA_CHECK:
		return m.probeDataCheck(ctx, req)
	case iproto.ProbeType_PROBE_TYPE_BROKER:
		return m.probeBroker(ctx, req)
	case iproto.ProbeType_PROBE_TYPE_CONN_TEST:
		return m.probeConnTest(ctx, req)
	case iproto.ProbeType_PROBE_TYPE_CATALOG:
		return m.probeCatalog(ctx, req)
	case iproto.ProbeType_PROBE_TYPE_DEFS:
		return m.probeDefs(ctx, req)
	case iproto.ProbeType_PROBE_TYPE_POSITION:
		return m.probePosition(ctx, req)
	case iproto.ProbeType_PROBE_TYPE_PRECHECK:
		return m.probePrecheck(ctx, req)
	case iproto.ProbeType_PROBE_TYPE_LINK:
		return m.probeLink(ctx, req)
	default:
		// [已认未做登记点] 拒绝：PARAMS 一种
		// ProbeType（NODE-* 无单独条目，属 探测族）。这一支同时兼任"协议新增
		// 本包还不认识的取值"的护栏，两族共用一个出口——**清单只登记前一族**。
		// 清单见 controller/doc.go"已认未做登记点清单"。
		return &iproto.ProbeResult{
			TaskId:  req.TaskId,
			Type:    req.Type,
			Success: false,
			Error: &iproto.Error{
				Code:    "PROBE_TYPE_UNSUPPORTED",
				Message: "该节点尚未实现此探测类型",
			},
		}
	}
}
