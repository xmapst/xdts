package mysqlout

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"

	"github.com/xmapst/xdts/internal/iproto"
	"google.golang.org/protobuf/proto"
)

type targetColumn struct {
	generated bool
	dataType  string
	extra     string
}

// TIME uses a signed duration and BIT uses unsigned integers in the existing
// wire contract. Convert with the target's actual type before SQL parameters.
func projectTargetValue(value *iproto.ColumnValue, dataType string) (*iproto.ColumnValue, error) {
	if value == nil {
		return value, nil
	}
	if dataType == "bit" {
		if unsigned, ok := value.Value.(*iproto.ColumnValue_U64); ok {
			encoded := make([]byte, 8)
			binary.BigEndian.PutUint64(encoded, unsigned.U64)
			for len(encoded) > 1 && encoded[0] == 0 {
				encoded = encoded[1:]
			}
			return &iproto.ColumnValue{Value: &iproto.ColumnValue_RawBytes{RawBytes: encoded}}, nil
		}
		return value, nil
	}
	temporal, ok := value.Value.(*iproto.ColumnValue_TemporalUsec)
	if !ok {
		return value, nil
	}
	if dataType == "date" && value.ZeroDate {
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_Text{Text: "0000-00-00"}}, nil
	}
	if dataType != "time" {
		return value, nil
	}
	usec := temporal.TemporalUsec
	const maximum = (838*3600+59*60+59)*int64(1000000) + 999999
	if value.ZeroDate || usec < -maximum || usec > maximum {
		return nil, fmt.Errorf("mysqlout: invalid target TIME duration")
	}
	sign := ""
	if usec < 0 {
		sign, usec = "-", -usec
	}
	text := fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, usec/3600000000, usec/60000000%60, usec/1000000%60, usec%1000000)
	return &iproto.ColumnValue{Value: &iproto.ColumnValue_Text{Text: text}}, nil
}

// projectTargetTxn consults each mapped target table once per original GTID.
// Opening the table keeps its metadata lock until commit, including on the
// parallel worker path. No catalog snapshot is shared across transactions.
func (w *Writer) projectTargetTxn(ctx context.Context, tx *dmlTxn, txn *iproto.Txn) (*iproto.Txn, error) {
	prefetch := tx.prefetchTargetColumns
	mode := w.opts.ConflictPolicy.GetMode()
	if w.opts.WritePolicy.GetMergeSameTableRows() && len(txn.Rows) > 1 && w.opts.foreignKeyChecksValue() == 0 &&
		(mode == iproto.ConflictMode_CONFLICT_MODE_UNSPECIFIED || mode == iproto.ConflictMode_CONFLICT_MODE_SOURCE_WINS) {
		prefetch = func(ctx context.Context, rows []*iproto.RowChange) error {
			return tx.prefetchReplayCatalog(ctx, rows, iproto.WriteBatchSize(w.opts.WritePolicy))
		}
	}
	if err := prefetch(ctx, txn.Rows); err != nil {
		return nil, err
	}
	var out *iproto.Txn
	var layout targetProjectionLayout
	for rowIndex, row := range txn.Rows {
		generated, err := tx.targetColumns(ctx, row.Schema, row.Table)
		if err != nil {
			return nil, err
		}
		if !layout.matches(row) {
			layout = buildTargetProjectionLayout(row, generated)
		}
		keep, convert := layout.keep, layout.convert
		if len(keep) == len(row.ColumnNames) && !convert {
			continue
		}
		if len(keep) == 0 {
			return nil, fmt.Errorf("mysqlout: empty incremental target projection")
		}
		for _, name := range row.PrimaryKeyColumns {
			if generated[strings.ToLower(name)].generated {
				return nil, fmt.Errorf("mysqlout: generated target positioning key cannot be projected")
			}
		}
		if timestamp := w.lwwTimestampColumn(row); timestamp != "" && generated[strings.ToLower(timestamp)].generated {
			return nil, fmt.Errorf("mysqlout: generated target conflict timestamp cannot be projected")
		}
		for _, name := range w.transforms.Columns(row.Schema, row.Table) {
			if generated[name].generated {
				return nil, fmt.Errorf("mysqlout: generated target transform column cannot be projected")
			}
		}
		if out == nil {
			out = proto.Clone(txn).(*iproto.Txn)
		}
		projected := out.Rows[rowIndex]
		projected.ColumnNames = make([]string, len(keep))
		for index, original := range keep {
			projected.ColumnNames[index] = row.ColumnNames[original]
		}
		for _, image := range []*iproto.Row{projected.Before, projected.After} {
			if image == nil {
				continue
			}
			values := make([]*iproto.ColumnValue, len(keep))
			for index, original := range keep {
				value, err := projectTargetValue(image.Values[original], layout.types[index])
				if err != nil {
					return nil, err
				}
				values[index] = value
			}
			image.Values = values
		}
	}
	if out != nil {
		return out, nil
	}
	return txn, nil
}

type targetProjectionLayout struct {
	schema, table string
	names         []string
	keep          []int
	types         []string
	convert       bool
}

func (layout targetProjectionLayout) matches(row *iproto.RowChange) bool {
	return layout.names != nil && layout.schema == row.Schema && layout.table == row.Table && slices.Equal(layout.names, row.ColumnNames)
}

func buildTargetProjectionLayout(row *iproto.RowChange, catalog map[string]targetColumn) targetProjectionLayout {
	layout := targetProjectionLayout{schema: row.Schema, table: row.Table, names: slices.Clone(row.ColumnNames), keep: make([]int, 0, len(row.ColumnNames)), types: make([]string, 0, len(row.ColumnNames))}
	for index, name := range row.ColumnNames {
		column := catalog[strings.ToLower(name)]
		if column.generated {
			continue
		}
		layout.keep = append(layout.keep, index)
		layout.types = append(layout.types, column.dataType)
		layout.convert = layout.convert || column.dataType == "time" || column.dataType == "date" || column.dataType == "bit"
	}
	return layout
}

// projectSnapshot drops columns absent from the current target. Missing target
// columns are omitted so MySQL supplies defaults. Keys must survive projection.
func (w *Writer) projectSnapshot(ctx context.Context, chunk *iproto.SnapshotChunk) (*iproto.SnapshotChunk, error) {
	rows, err := w.conn.QueryContext(ctx, "SELECT COLUMN_NAME,GENERATION_EXPRESSION,DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", chunk.Schema, chunk.Table)
	if err != nil {
		return nil, err
	}
	defer func(cleanup func() error) { _ = cleanup() }(rows.Close)
	columns := make(map[string]bool)
	types := make(map[string]string)
	for rows.Next() {
		var name, generated, dataType string
		if err := rows.Scan(&name, &generated, &dataType); err != nil {
			return nil, err
		}
		columns[strings.ToLower(name)] = generated == ""
		types[strings.ToLower(name)] = strings.ToLower(dataType)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var keep []int
	convert := false
	for i, name := range chunk.ColumnNames {
		if columns[strings.ToLower(name)] {
			keep = append(keep, i)
			convert = convert || types[strings.ToLower(name)] == "time" || types[strings.ToLower(name)] == "date" || types[strings.ToLower(name)] == "bit"
		}
	}
	if len(keep) == len(chunk.ColumnNames) && !convert {
		return chunk, nil
	}
	projected := make(map[string]bool)
	for _, i := range keep {
		projected[strings.ToLower(chunk.ColumnNames[i])] = true
	}
	for _, name := range chunk.PrimaryKeyColumns {
		if !projected[strings.ToLower(name)] {
			return nil, fmt.Errorf("mysqlout: snapshot positioning key missing from target")
		}
	}
	if len(keep) == 0 {
		return nil, fmt.Errorf("mysqlout: empty snapshot projection")
	}
	out := proto.Clone(chunk).(*iproto.SnapshotChunk)
	out.ColumnNames = make([]string, len(keep))
	for j, i := range keep {
		out.ColumnNames[j] = chunk.ColumnNames[i]
	}
	for _, row := range out.Rows {
		values := make([]*iproto.ColumnValue, len(keep))
		for j, i := range keep {
			value, err := projectTargetValue(row.Values[i], types[strings.ToLower(chunk.ColumnNames[i])])
			if err != nil {
				return nil, err
			}
			values[j] = value
		}
		row.Values = values
	}
	return out, nil
}
