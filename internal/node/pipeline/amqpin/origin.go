package amqpin

import (
	"uuid"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/reporter"
)

// filterOriginBatch returns a new batch without mutating reassembly ownership.
// A rejected transaction is omitted in its entirety and never reaches Writer.
func (m *Module) filterOriginBatch(batch *iproto.DataBatch) *iproto.DataBatch {
	out := iproto.CloneDataBatch(batch)
	kept := make([]*iproto.Txn, 0, len(out.GetTxns()))
	for _, txn := range out.GetTxns() {
		one, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, txn.GetGtid())
		if err == nil && m.skippedSet != nil && m.skippedSet.Contain(one) {
			continue
		}
		route := m.runtimeDatabaseRoute
		if route.TargetCenter == "" {
			route.TargetCenter = m.opts.Center
		}
		if !route.AllowsTxn(txn) || (m.runtimeMaxOriginHops > 0 && txn.GetHops() > m.runtimeMaxOriginHops) {
			if err := m.rejectedSet.Update(txn.GetGtid()); err == nil {
				rejected := m.rejectedSet.String()
				m.stats.rejectedGTIDSet.Store(&rejected)
			}
			m.Cast(reporter.ModuleName, reporter.EventReport{Event: &iproto.Event{EventId: uuid.New().String(), PipelineId: m.opts.PipelineID, TsMs: m.now().UnixMilli(), Kind: iproto.EventKind_EVENT_KIND_BACKPRESSURE, Level: "WARN", Message: "目标端拒绝回环或超出跳数的事务", Detail: &iproto.Event_Backpressure{Backpressure: &iproto.BackpressureEventDetail{Module: ModuleName(m.opts.PipelineID), Reason: "target lineage check rejected transaction"}}}})
			continue
		}
		kept = append(kept, txn)
	}
	out.Txns = kept
	return out
}

// withoutRejected subtracts interval ranges without enumerating GTID numbers.
func withoutRejected(input, rejected gmysql.GTIDSet) gmysql.GTIDSet {
	out := input.Clone().(*gmysql.MysqlGTIDSet)
	removed := rejected.(*gmysql.MysqlGTIDSet)
	for sid, tags := range *out {
		for tag, intervals := range tags {
			for _, cut := range (*removed)[sid][tag] {
				kept := make(gmysql.IntervalSlice, 0, len(intervals))
				for _, span := range intervals {
					if cut.Stop <= span.Start || cut.Start >= span.Stop {
						kept = append(kept, span)
						continue
					}
					if span.Start < cut.Start {
						kept = append(kept, gmysql.Interval{Start: span.Start, Stop: cut.Start})
					}
					if span.Stop > cut.Stop {
						kept = append(kept, gmysql.Interval{Start: cut.Stop, Stop: span.Stop})
					}
				}
				intervals = kept
			}
			if len(intervals) == 0 {
				delete(tags, tag)
			} else {
				tags[tag] = intervals
			}
		}
		if len(tags) == 0 {
			delete(*out, sid)
		}
	}
	return out
}
