package mysqlout

import (
	"context"
	"database/sql"
	"maps"
	"slices"
	"strings"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/rule"
	"google.golang.org/protobuf/proto"
)

func compileWriterTransforms(opts Options) (*rule.TransformPlan, error) {
	next := slices.Clone(opts.Transforms)
	for i := range next {
		r := &next[i]
		var columns []string
		r.Schema, r.Table, columns = opts.Mapping.TargetNames(r.Schema, r.Table, []string{r.Column}, opts.SchemaOptions.SourceCaseSensitive)
		r.Column = columns[0]
		r.Params = maps.Clone(r.Params)
	}
	return rule.CompileTransforms(next, opts.SchemaOptions.SourceCaseSensitive)
}

func cloneTransformRules(input []rule.TransformRule) []rule.TransformRule {
	out := slices.Clone(input)
	for i := range out {
		out[i].Params = maps.Clone(out[i].Params)
	}
	return out
}

func (w *Writer) validateTransformKeys(schema, table string, primary []string, unique []*iproto.UniqueKeyColumns, timestamp string) error {
	protected := make(map[string]bool)
	for _, name := range primary {
		protected[strings.ToLower(name)] = true
	}
	for _, key := range unique {
		for _, name := range key.GetColumns() {
			protected[strings.ToLower(name)] = true
		}
	}
	if timestamp != "" {
		protected[strings.ToLower(timestamp)] = true
	}
	for _, name := range w.transforms.Columns(schema, table) {
		if protected[name] {
			return rule.ErrTransformConfig
		}
	}
	return nil
}

// The target owns its actual indexes. Binlog metadata does not contain every
// unique key, and target-only indexes may also exist, so validate them directly.
type iTransformIndexQuery interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func (w *Writer) validateTargetTransformKeys(ctx context.Context, query iTransformIndexQuery, schema, table string) error {
	columns := w.transforms.Columns(schema, table)
	if len(columns) == 0 {
		return nil
	}
	// Hold metadata access for the owning transaction before reading indexes,
	// so a concurrent ALTER cannot introduce a key after this check.
	metadata, err := query.QueryContext(ctx, "SELECT 1 FROM "+quoteIdent(schema)+"."+quoteIdent(table)+" LIMIT 0")
	if err != nil {
		return err
	}
	if err := metadata.Close(); err != nil {
		return err
	}
	rows, err := query.QueryContext(ctx, "SELECT COLUMN_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND NON_UNIQUE=0", schema, table)
	if err != nil {
		return err
	}
	defer func(cleanup func() error) { _ = cleanup() }(rows.Close)
	for rows.Next() {
		var name sql.NullString
		if err := rows.Scan(&name); err != nil {
			return err
		}
		if !name.Valid || slices.Contains(columns, strings.ToLower(name.String)) {
			return rule.ErrTransformConfig
		}
	}
	return rows.Err()
}

func (w *Writer) prepareTransformTxn(ctx context.Context, query iTransformIndexQuery, txn *iproto.Txn) (*iproto.Txn, error) {
	if w.transforms == nil {
		return txn, nil
	}
	checked := make(map[iproto.TargetSourceTable]bool)
	for _, row := range txn.Rows {
		ref := iproto.TargetSourceTable{Schema: row.Schema, Table: row.Table}
		if !checked[ref] {
			if err := w.validateTargetTransformKeys(ctx, query, row.Schema, row.Table); err != nil {
				return nil, err
			}
			checked[ref] = true
		}
	}
	return w.transformTxn(txn)
}

func (w *Writer) transformTxn(txn *iproto.Txn) (*iproto.Txn, error) {
	if w.transforms == nil {
		return txn, nil
	}
	var out *iproto.Txn
	for i, row := range txn.Rows {
		if !w.transforms.HasTable(row.Schema, row.Table) {
			continue
		}
		if err := w.validateTransformKeys(row.Schema, row.Table, row.PrimaryKeyColumns, row.UniqueKeys, w.lwwTimestampColumn(row)); err != nil {
			return nil, err
		}
		if out == nil {
			out = proto.Clone(txn).(*iproto.Txn)
		}
		var err error
		out.Rows[i].Before, err = iproto.TransformRow(w.transforms, row.Schema, row.Table, row.ColumnNames, row.Before)
		if err != nil {
			return nil, err
		}
		out.Rows[i].After, err = iproto.TransformRow(w.transforms, row.Schema, row.Table, row.ColumnNames, row.After)
		if err != nil {
			return nil, err
		}
	}
	if out == nil {
		return txn, nil
	}
	return out, nil
}

func (w *Writer) transformSnapshot(chunk *iproto.SnapshotChunk) (*iproto.SnapshotChunk, error) {
	if w.transforms == nil || !w.transforms.HasTable(chunk.Schema, chunk.Table) {
		return chunk, nil
	}
	if err := w.validateTransformKeys(chunk.Schema, chunk.Table, chunk.PrimaryKeyColumns, chunk.UniqueKeys, ""); err != nil {
		return nil, err
	}
	out := proto.Clone(chunk).(*iproto.SnapshotChunk)
	for i, row := range chunk.Rows {
		var err error
		out.Rows[i], err = iproto.TransformRow(w.transforms, chunk.Schema, chunk.Table, chunk.ColumnNames, row)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
