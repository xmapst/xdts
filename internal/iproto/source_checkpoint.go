package iproto

import (
	"errors"
	"math"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// ValidateSourceCheckpoint checks a complete, explicitly present observation.
// Empty GTID is valid; missing checkpoint is not a known empty observation.
func ValidateSourceCheckpoint(p *SourceCheckpoint) error {
	if p == nil || p.Epoch == 0 || p.Epoch > math.MaxInt64 || p.BatchSeq > math.MaxInt64 || p.PositionRevision > math.MaxInt64 || p.ObservedTsMs <= 0 || p.ObservedTsMs > 253402300799999 || len(p.PublishedGtidSet) > 1<<20 {
		return errors.New("invalid source checkpoint values")
	}
	for _, raw := range []string{p.PipelineId, p.NodeId} {
		id, err := uuidx.Parse(raw)
		if err != nil || id.IsZero() {
			return errors.New("invalid source checkpoint identity")
		}
	}
	if _, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, p.PublishedGtidSet); err != nil {
		return errors.New("invalid source checkpoint GTID")
	}
	return nil
}
