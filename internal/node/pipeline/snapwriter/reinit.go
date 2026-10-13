package snapwriter

import (
	"fmt"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdts/internal/iproto"
)

// checkDeliveryRun distinguishes a superseded valid message from malformed or
// misrouted input, before worker dispatch or completion changes.
func (m *Module) checkDeliveryRun(d delivery) (bool, error) {
	if d.err != nil {
		return false, d.err
	}
	var id string
	var run uint64
	if d.chunk != nil {
		id, run = d.chunk.GetPipelineId(), d.chunk.GetRunId()
		if d.chunk.GetChunkNo() == 0 || d.chunk.GetChunkNo() > 1<<63-2 || len(d.chunk.GetColumnNames()) == 0 {
			return false, fmt.Errorf("snapwriter: invalid chunk identity or columns")
		}
		if err := iproto.ValidateSnapshotChunk(d.chunk); err != nil {
			return false, err
		}
	} else if d.end != nil {
		id, run = d.end.GetPipelineId(), d.end.GetRunId()
		if err := validateEndWatermarks(d.end); err != nil {
			return false, err
		}
	} else if d.begin != nil {
		id, run = d.begin.PipelineId, d.begin.RunId
		if !m.opts.AllowTableRecovery {
			return false, fmt.Errorf("snapwriter: recovery not negotiated")
		}
		if err := iproto.ValidateSnapshotRecoveryBegin(d.begin); err != nil {
			return false, err
		}
	} else if d.probe != nil {
		id, run = d.probe.GetPipelineId(), d.probe.GetRunId()
		if !d.probe.GetSnapshot() || d.probe.GetProbeId() == "" || d.probe.GetConfigVersion() == 0 {
			return false, fmt.Errorf("snapwriter: invalid snapshot probe")
		}
	} else {
		return false, fmt.Errorf("snapwriter: missing snapshot payload")
	}
	if id != m.opts.Run.PipelineID || run > m.opts.Run.RunID {
		return false, fmt.Errorf("snapwriter: message belongs to another pipeline or future run")
	}
	return run < m.opts.Run.RunID, nil
}

func validateEndWatermarks(end *iproto.SnapshotEnd) error {
	if err := iproto.ValidateSnapshotEndPlan(end); err != nil {
		return err
	}
	if end == nil || end.GetTotalChunks() == 0 || end.GetTotalChunks() > 1<<63-2 {
		return fmt.Errorf("snapwriter: invalid end total")
	}
	low, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, end.GetLowGtidSet())
	if err != nil {
		return err
	}
	high, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, end.GetHighGtidSet())
	if err != nil || !high.Contain(low) {
		return fmt.Errorf("snapwriter: invalid end watermarks")
	}
	return nil
}
