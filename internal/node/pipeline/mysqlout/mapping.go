package mysqlout

import (
	"errors"
	"strings"

	"github.com/xmapst/xdts/internal/iproto"
	"google.golang.org/protobuf/proto"
)

// A destination name can also collide with an unchanged source column. The
// complete row/chunk supplies those columns before any GTID transaction starts.
func validateMappedColumns(columns []string) error {
	seen := make(map[string]bool, len(columns))
	for _, column := range columns {
		key := strings.ToLower(column)
		if key == "" || seen[key] {
			return errors.New("mysqlout: mapping produced ambiguous destination columns")
		}
		seen[key] = true
	}
	return nil
}

func (w *Writer) hasMappings() bool {
	return len(w.opts.Mapping.Schemas)+len(w.opts.Mapping.Tables)+len(w.opts.Mapping.Columns) != 0
}

// Mapping creates independent row ownership before changing names. Value order
// is unchanged; primary and unique keys use the identical source column rules.
func (w *Writer) mapTxn(txn *iproto.Txn) *iproto.Txn {
	if !w.hasMappings() {
		return txn
	}
	out := proto.Clone(txn).(*iproto.Txn)
	for _, row := range out.Rows {
		schema, table := row.Schema, row.Table
		row.Schema, row.Table, row.ColumnNames = w.opts.Mapping.TargetNames(schema, table, row.ColumnNames, w.opts.SchemaOptions.SourceCaseSensitive)
		_, _, row.PrimaryKeyColumns = w.opts.Mapping.TargetNames(schema, table, row.PrimaryKeyColumns, w.opts.SchemaOptions.SourceCaseSensitive)
		for _, key := range row.UniqueKeys {
			_, _, key.Columns = w.opts.Mapping.TargetNames(schema, table, key.Columns, w.opts.SchemaOptions.SourceCaseSensitive)
		}
	}
	return out
}

func (w *Writer) mapSnapshot(chunk *iproto.SnapshotChunk) *iproto.SnapshotChunk {
	if !w.hasMappings() {
		return chunk
	}
	out := proto.Clone(chunk).(*iproto.SnapshotChunk)
	out.Schema, out.Table, out.ColumnNames = w.opts.Mapping.TargetNames(chunk.Schema, chunk.Table, chunk.ColumnNames, w.opts.SchemaOptions.SourceCaseSensitive)
	_, _, out.PrimaryKeyColumns = w.opts.Mapping.TargetNames(chunk.Schema, chunk.Table, chunk.PrimaryKeyColumns, w.opts.SchemaOptions.SourceCaseSensitive)
	for _, key := range out.UniqueKeys {
		_, _, key.Columns = w.opts.Mapping.TargetNames(chunk.Schema, chunk.Table, key.Columns, w.opts.SchemaOptions.SourceCaseSensitive)
	}
	return out
}
