package amqpin

import (
	"errors"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdts/internal/iproto"
	"google.golang.org/protobuf/proto"
)

// Applied after reassembly and publication ordering, before any SQL. The input
// remains the immutable source publication used for durable ACK proof. H is
// fixed for the entire run; excluded transactions never enter consumed/applied.
func (m *Module) fullOnlyBatch(batch *iproto.DataBatch) (*iproto.DataBatch, error) {
	if !m.opts.FullOnly || m.recoveryPending {
		return batch, nil
	}
	if m.fullOnlyHigh == nil {
		return nil, errors.New("amqpin: full-only boundary is unavailable")
	}
	out := proto.Clone(batch).(*iproto.DataBatch)
	kept := out.Txns[:0]
	for _, txn := range out.Txns {
		included, err := m.fullOnlyTransaction(txn.GetGtid())
		if err != nil {
			return nil, err
		}
		if included {
			kept = append(kept, txn)
		}
	}
	clear(out.Txns[len(kept):])
	out.Txns = kept
	return out, nil
}

func (m *Module) fullOnlyTransaction(gtid string) (bool, error) {
	if m.fullOnlyHigh == nil {
		return false, errors.New("amqpin: full-only boundary is unavailable")
	}
	set, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, gtid)
	if err != nil || set.IsEmpty() || pendingCatchupTransactions(set, emptyGTIDSet()) != 1 {
		return false, errors.New("amqpin: invalid full-only transaction GTID")
	}
	return m.fullOnlyHigh.Contain(set), nil
}
