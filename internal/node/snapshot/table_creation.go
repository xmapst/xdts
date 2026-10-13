package snapshot

import (
	"context"
	"errors"
	"fmt"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdoc"
	"github.com/xmapst/xdts/internal/iproto"
	"google.golang.org/protobuf/proto"
)

type tableCreationRecord struct {
	PipelineID  string `bson:"pipeline_id"`
	RunID       uint64 `bson:"run_id"`
	UUID        string `bson:"uuid"`
	Schema      string `bson:"schema"`
	Table       string `bson:"table"`
	Transaction []byte `bson:"transaction"`
}

func (r *Runner) creationRecordID(schema, table string) string {
	return r.recordID(fmt.Sprintf("creation.%d:%s%s", len(schema), schema, table))
}

func (r *Runner) decodeCreation(saved tableCreationRecord, schema, table string) (*iproto.Txn, error) {
	if !r.opts.EnableTableCreation || saved.PipelineID != r.opts.PipelineID || saved.RunID != r.opts.RunID || saved.UUID != r.opts.SnapshotUUID || saved.Schema != schema || saved.Table != table || len(saved.Transaction) == 0 || len(saved.Transaction) > 1<<20 {
		return nil, fmt.Errorf("snapshot: invalid stored table creation identity")
	}
	txn := new(iproto.Txn)
	if err := proto.Unmarshal(saved.Transaction, txn); err != nil {
		return nil, err
	}
	if err := iproto.ValidateSnapshotCreation(txn, schema, table); err != nil {
		return nil, err
	}
	low, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, r.start.LowGTIDSet)
	if err != nil {
		return nil, err
	}
	one, _ := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, txn.GetGtid())
	if low.Contain(one) {
		return nil, fmt.Errorf("snapshot: creation predates original low watermark")
	}
	return txn, nil
}

// Called by the Runner owner before each read pass. Wait through all DDL
// visible during planning; capture and read back before any corresponding row.
func (r *Runner) prepareTableCreations(ctx context.Context) error {
	if !r.opts.EnableTableCreation {
		return nil
	}
	if r.opts.WaitObserved == nil || r.opts.TableCreation == nil {
		return fmt.Errorf("snapshot: negotiated creation requires ordered source proof")
	}
	high, err := r.source.GTIDSet(ctx)
	if err != nil {
		return err
	}
	wait, cancel := context.WithTimeout(ctx, r.opts.OperationTimeout)
	err = r.opts.WaitObserved(wait, high)
	cancel()
	if err != nil {
		return err
	}
	if r.tableCreations == nil {
		r.tableCreations = make(map[[2]string]*iproto.Txn)
	}
	for _, table := range r.plan.Tables {
		key := [2]string{table.Schema, table.Table}
		if r.tableCreations[key] != nil {
			continue
		}
		id := r.creationRecordID(table.Schema, table.Table)
		var saved tableCreationRecord
		read, cancel := context.WithTimeout(ctx, r.opts.OperationTimeout)
		err := r.store.Get(read, "snapshot_runs", id, &saved)
		cancel()
		if errors.Is(err, xdoc.ErrNotFound) {
			txn := r.opts.TableCreation(table.Schema, table.Table)
			if txn == nil {
				continue
			}
			data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(txn)
			if err != nil {
				return err
			}
			saved = tableCreationRecord{PipelineID: r.opts.PipelineID, RunID: r.opts.RunID, UUID: r.opts.SnapshotUUID, Schema: table.Schema, Table: table.Table, Transaction: data}
			if _, err := r.decodeCreation(saved, table.Schema, table.Table); err != nil {
				return err
			}
			if err := r.createRecord(ctx, fmt.Sprintf("creation.%d:%s%s", len(table.Schema), table.Schema, table.Table), saved); err != nil {
				return err
			}
			read, cancel := context.WithTimeout(ctx, r.opts.OperationTimeout)
			err = r.store.Get(read, "snapshot_runs", id, &saved)
			cancel()
			if err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		txn, err := r.decodeCreation(saved, table.Schema, table.Table)
		if err != nil {
			return err
		}
		r.tableCreations[key] = txn
	}
	return nil
}
