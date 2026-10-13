package mysqlout

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"uuid"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"google.golang.org/protobuf/proto"

	"github.com/xmapst/xdts/internal/dblineage"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/rule"
)

const maxSnapshotChunk uint64 = 1<<63 - 2

// SnapshotOptions binds the writer to one initialization run. Each worker owns
// one Writer; calls on that Writer, including GTIDSet, remain strictly serial.
type SnapshotOptions struct {
	PipelineID         string
	RunID              uint64
	UUID               string
	MaxRows            uint64
	MaxBytes           uint64
	AllowTableCreation bool
}

func (o SnapshotOptions) Validate() error {
	id, err := uuid.Parse(o.UUID)
	if err != nil || id == uuid.Nil() || id.String() != o.UUID || o.PipelineID == "" || o.MaxRows == 0 || o.MaxBytes == 0 {
		return fmt.Errorf("mysqlout: invalid snapshot run or transaction limits")
	}
	return nil
}

func (w *Writer) GTIDSet(ctx context.Context) (set string, resultErr error) {
	if err := w.ensureConnection(ctx); err != nil {
		return "", err
	}
	defer func() { w.observeConnectionError(resultErr) }()
	err := w.conn.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&set)
	return set, err
}

// ApplySnapshotChunk uses native REPLACE: MySQL deletes every conflicting row
// using ALL actual target unique indexes before inserting each incoming row.
// The source's unique_keys never participate in target conflict resolution.
func (w *Writer) ApplySnapshotChunk(ctx context.Context, opts SnapshotOptions, chunk *iproto.SnapshotChunk) error {
	_, err := w.ApplySnapshotChunkWithResult(ctx, opts, chunk)
	return err
}

// ApplySnapshotChunkWithResult also identifies a newly committed chunk.
// Duplicate GTIDs return false so telemetry never recounts replayed rows.
func (w *Writer) ApplySnapshotChunkWithResult(ctx context.Context, opts SnapshotOptions, chunk *iproto.SnapshotChunk) (result bool, resultErr error) {
	if err := opts.Validate(); err != nil {
		return false, err
	}
	if chunk == nil || chunk.GetPipelineId() != opts.PipelineID || chunk.GetRunId() != opts.RunID ||
		chunk.GetChunkNo() == 0 || chunk.GetChunkNo() > maxSnapshotChunk || len(chunk.GetColumnNames()) == 0 ||
		uint64(len(chunk.GetRows())) > opts.MaxRows || uint64(proto.Size(chunk)) > opts.MaxBytes {
		return false, fmt.Errorf("mysqlout: invalid snapshot identity, chunk number or transaction size")
	}
	if err := iproto.ValidateSnapshotChunk(chunk); err != nil {
		return false, err
	}
	if err := w.ensureConnection(ctx); err != nil {
		return false, err
	}
	defer func() { w.observeConnectionError(resultErr) }()
	if creation := chunk.GetTableCreation(); creation != nil {
		if !opts.AllowTableCreation {
			return false, fmt.Errorf("mysqlout: snapshot table creation not negotiated")
		}
		if err := w.prepareSnapshotCreation(ctx, creation); err != nil {
			return false, err
		}
	}
	chunk = w.mapSnapshot(chunk)
	if dblineage.Reserved(chunk.Schema) {
		return false, fmt.Errorf("mysqlout: reserved metadata cannot be a snapshot destination")
	}
	if err := validateMappedColumns(chunk.GetColumnNames()); err != nil {
		return false, err
	}
	gtid := fmt.Sprintf("%s:%d", opts.UUID, chunk.GetChunkNo())
	set, err := w.GTIDSet(ctx)
	if err != nil {
		return false, err
	}
	executed, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, set)
	if err != nil {
		return false, err
	}
	one, _ := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, gtid)
	if executed.Contain(one) {
		return false, nil
	}
	if err := w.validateTargetTransformKeys(ctx, w.conn, chunk.Schema, chunk.Table); err != nil {
		return false, err
	}
	var transformErr error
	chunk, transformErr = w.transformSnapshot(chunk)
	if transformErr != nil {
		return false, transformErr
	}
	if uint64(proto.Size(chunk)) > opts.MaxBytes {
		return false, rule.ErrTransformEvaluation
	}
	projected, err := w.projectSnapshot(ctx, chunk)
	if err != nil {
		return false, err
	}
	chunk = projected
	statements, err := buildSnapshotStatements(chunk.GetSchema(), chunk.GetTable(), chunk.GetColumnNames(), chunk.GetRows())
	if err != nil {
		return false, err
	}
	if _, err := w.conn.ExecContext(ctx, "SET SESSION foreign_key_checks = 0"); err != nil {
		return false, err
	}
	if err := w.validateTargetColumns(ctx, chunk.GetSchema(), chunk.GetTable(), chunk.GetColumnNames()); err != nil {
		return false, err
	}
	committed := false
	err = withGTIDTxn(ctx, w.conn, gtid, func(ctx context.Context, tx *dmlTxn) error {
		if w.opts.DatabaseRoute.Persistent {
			if err := w.writeTxnLineage(ctx, tx, w.syntheticSourceTxn(gtid)); err != nil {
				return err
			}
		}
		if err := w.validateTargetTransformKeys(ctx, tx.tx, chunk.Schema, chunk.Table); err != nil {
			return err
		}
		committed = true
		for _, statement := range statements {
			if _, err := tx.execDML(ctx, statement.query, statement.args...); err != nil {
				return err
			}
		}
		return nil
	})
	return committed && err == nil, err
}

type snapshotStatement struct {
	query string
	args  []any
}

// Construct every statement before writing any rows. Wide chunks stay within
// MySQL's 65535-placeholder limit without first building an oversized statement;
// the caller still executes all statements under one transaction and GTID.
func buildSnapshotStatements(schema, table string, columns []string, rows []*iproto.Row) ([]snapshotStatement, error) {
	if len(columns) == 0 || len(columns) > 65535 {
		return nil, fmt.Errorf("mysqlout: invalid snapshot column count")
	}
	perStatement := 65535 / len(columns)
	var statements []snapshotStatement
	for first := 0; first < len(rows); first += perStatement {
		last := min(first+perStatement, len(rows))
		query, args, err := buildReplace(schema, table, columns, rows[first:last])
		if err != nil {
			return nil, err
		}
		statements = append(statements, snapshotStatement{query: query, args: args})
	}
	return statements, nil
}

// validateTargetColumns queries the target itself. Generated columns must never
// be explicitly written. Native REPLACE also supports target prefix/expression
// unique indexes without approximating their comparison or collation in Go.
func (w *Writer) validateTargetColumns(ctx context.Context, schema, table string, columns []string) error {
	rows, err := w.conn.QueryContext(ctx, "SELECT COLUMN_NAME, GENERATION_EXPRESSION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", schema, table)
	if err != nil {
		return err
	}
	defer func(cleanup func() error) { _ = cleanup() }(rows.Close)
	allowed := make(map[string]bool)
	for rows.Next() {
		var name, generated string
		if err := rows.Scan(&name, &generated); err != nil {
			return err
		}
		allowed[strings.ToLower(name)] = generated == ""
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, name := range columns {
		if !allowed[strings.ToLower(name)] {
			return fmt.Errorf("mysqlout: target column missing or generated")
		}
	}
	// REPLACE on a keyless target degenerates to INSERT and breaks replay.
	// Require a complete non-null key whose columns are actually projected.
	if err := rows.Close(); err != nil {
		return err
	}
	indexes, err := w.conn.QueryContext(ctx, `SELECT s.INDEX_NAME,s.COLUMN_NAME,s.SUB_PART,c.IS_NULLABLE
FROM information_schema.STATISTICS s LEFT JOIN information_schema.COLUMNS c
ON c.TABLE_SCHEMA=s.TABLE_SCHEMA AND c.TABLE_NAME=s.TABLE_NAME AND c.COLUMN_NAME=s.COLUMN_NAME
WHERE s.TABLE_SCHEMA=? AND s.TABLE_NAME=? AND s.NON_UNIQUE=0 ORDER BY s.INDEX_NAME,s.SEQ_IN_INDEX`, schema, table)
	if err != nil {
		return err
	}
	defer func(cleanup func() error) { _ = cleanup() }(indexes.Close)
	projected := make(map[string]bool, len(columns))
	for _, col := range columns {
		projected[strings.ToLower(col)] = true
	}
	valid := make(map[string]bool)
	for indexes.Next() {
		var name string
		var col, nullable sql.NullString
		var prefix sql.NullInt64
		if err := indexes.Scan(&name, &col, &prefix, &nullable); err != nil {
			return err
		}
		if _, exists := valid[name]; !exists {
			valid[name] = true
		}
		if !col.Valid || prefix.Valid || !nullable.Valid || nullable.String != "NO" || !projected[strings.ToLower(col.String)] {
			valid[name] = false
		}
	}
	if err := indexes.Err(); err != nil {
		return err
	}
	for _, usable := range valid {
		if usable {
			return nil
		}
	}
	return fmt.Errorf("mysqlout: target has no complete projected non-null unique key")
}

func buildReplace(schema, table string, columns []string, rows []*iproto.Row) (string, []any, error) {
	if schema == "" || table == "" || len(columns) == 0 {
		return "", nil, fmt.Errorf("mysqlout: REPLACE target or columns missing")
	}
	seen := make(map[string]bool, len(columns))
	quoted := make([]string, len(columns))
	for i, col := range columns {
		key := strings.ToLower(col)
		if col == "" || seen[key] {
			return "", nil, fmt.Errorf("mysqlout: empty or duplicate REPLACE column")
		}
		seen[key] = true
		quoted[i] = quoteIdent(col)
	}
	args := make([]any, 0, len(rows)*len(columns))
	tuples := make([]string, len(rows))
	tuple := "(" + strings.Repeat("?,", len(columns)-1) + "?)"
	for i, row := range rows {
		if row == nil || len(row.GetValues()) != len(columns) {
			return "", nil, fmt.Errorf("mysqlout: REPLACE row width disagrees with columns")
		}
		for _, value := range row.GetValues() {
			arg, err := columnValueToArg(value)
			if err != nil {
				return "", nil, err
			}
			args = append(args, arg)
		}
		tuples[i] = tuple
	}
	return "REPLACE INTO " + quoteIdent(schema) + "." + quoteIdent(table) + " (" + strings.Join(quoted, ",") + ") VALUES " + strings.Join(tuples, ","), args, nil
}
