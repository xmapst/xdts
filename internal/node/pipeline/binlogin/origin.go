package binlogin

import (
	"strings"
	"uuid"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/reporter"
)

func (m *Module) allowOriginResult(result feedResult) bool {
	txn := result.txn
	if result.originUnknown {
		source, _, _ := strings.Cut(txn.GetGtid(), ":")
		m.Cast(reporter.ModuleName, reporter.EventReport{Event: &iproto.Event{EventId: uuid.New().String(), PipelineId: m.opts.PipelineID, TsMs: m.now().UnixMilli(), Kind: iproto.EventKind_EVENT_KIND_UNKNOWN_ORIGIN, Level: "WARN", Message: "来源 UUID 未登记", Detail: &iproto.Event_UnknownOrigin{UnknownOrigin: &iproto.UnknownOriginEventDetail{Uuid: source, AssumedCenter: m.opts.Center}}}})
		if m.sourceRuntime.DropUnknownOrigin {
			return false
		}
	}
	route := m.sourceRuntime.DatabaseRoute
	if route.TargetCenter == "" {
		route.TargetCenter = m.sourceRuntime.TargetCenter
	}
	if !route.AllowsTxn(txn) {
		m.reportOriginRejected("origin path contains target center or database")
		return false
	}
	if m.sourceRuntime.MaxOriginHops > 0 && txn.GetHops() > m.sourceRuntime.MaxOriginHops {
		m.reportOriginRejected("origin hops exceed configured maximum")
		return false
	}
	if m.sourceRuntime.OriginFilter != nil && !m.sourceRuntime.OriginFilter.AllowOrigin(txn.GetOriginCenter(), txn.GetHops(), txn.GetPath()) {
		return false
	}
	return true
}

// The serial session uses this preview before decoding row images. Observation
// remains at transaction completion so filtered work reports once in order.
func (m *Module) originTxnAllowed(txn *iproto.Txn, unknown bool) bool {
	config := m.sourceRuntime
	if unknown && config.DropUnknownOrigin {
		return false
	}
	route := config.DatabaseRoute
	if route.TargetCenter == "" {
		route.TargetCenter = config.TargetCenter
	}
	return route.AllowsTxn(txn) && (config.MaxOriginHops == 0 || txn.GetHops() <= config.MaxOriginHops) && (config.OriginFilter == nil || config.OriginFilter.AllowOrigin(txn.GetOriginCenter(), txn.GetHops(), txn.GetPath()))
}
func (m *Module) reportOriginRejected(reason string) {
	m.Cast(reporter.ModuleName, reporter.EventReport{Event: &iproto.Event{EventId: uuid.New().String(), PipelineId: m.opts.PipelineID, TsMs: m.now().UnixMilli(), Kind: iproto.EventKind_EVENT_KIND_BACKPRESSURE, Level: "WARN", Message: "来源血缘检查未通过，事务已过滤", Detail: &iproto.Event_Backpressure{Backpressure: &iproto.BackpressureEventDetail{Module: ModuleName(m.opts.PipelineID), Reason: reason}}}})
}
