package probe

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
)

// This read-only estimate includes query/transfer overhead. Short samples are
// advisory; the explicit multipliers are safety margins, not future guarantees.
func (r *precheckRun) sampleCapacity(ctx context.Context) *iproto.SnapshotCapacityEstimate {
	e := &iproto.SnapshotCapacityEstimate{UnavailableReason: "sample_unavailable"}
	if len(r.args.GetTables()) > 32 {
		e.UnavailableReason = "table_sample_limit"
		return e
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	start := time.Now()
	before, err := readBinlogSizes(ctx, r.conn)
	if err != nil {
		e.UnavailableReason = "binlog_sample_unavailable"
		return e
	}
	var seconds, bytes float64
	for _, table := range r.args.GetTables() {
		schema, name := table.GetSchema(), table.GetTable()
		var estimate sql.NullInt64
		if err := r.conn.QueryRowContext(ctx, "SELECT TABLE_ROWS FROM information_schema.TABLES WHERE BINARY TABLE_SCHEMA=? AND BINARY TABLE_NAME=? AND TABLE_TYPE='BASE TABLE'", schema, name).Scan(&estimate); err != nil || !estimate.Valid || estimate.Int64 < 0 {
			e.UnavailableReason = "row_estimate_unavailable"
			return e
		}
		columns, err := r.conn.QueryContext(ctx, "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE BINARY TABLE_SCHEMA=? AND BINARY TABLE_NAME=? ORDER BY ORDINAL_POSITION LIMIT 257", schema, name)
		if err != nil {
			return e
		}
		var expressions []string
		for columns.Next() {
			var column string
			if err := columns.Scan(&column); err != nil {
				_ = columns.Close()
				return e
			}
			expressions = append(expressions, "LEFT(CAST("+quoteDefinitionName(column)+" AS BINARY),4097)")
		}
		err = columns.Err()
		_ = columns.Close()
		if err != nil || len(expressions) == 0 || len(expressions) > 256 {
			e.UnavailableReason = "column_sample_limit"
			return e
		}
		tableStart := time.Now()
		rows, err := r.conn.QueryContext(ctx, "SELECT "+strings.Join(expressions, ",")+" FROM "+quoteDefinitionName(schema)+"."+quoteDefinitionName(name)+" LIMIT 128")
		if err != nil {
			return e
		}
		var count, payload uint64
		for rows.Next() {
			values := make([][]byte, len(expressions))
			dest := make([]any, len(values))
			for i := range values {
				dest[i] = &values[i]
			}
			if err := rows.Scan(dest...); err != nil {
				_ = rows.Close()
				return e
			}
			count++
			for _, value := range values {
				if len(value) > 4096 {
					_ = rows.Close()
					e.UnavailableReason = "cell_sample_limit"
					return e
				}
				payload += uint64(len(value)) + 32
			}
			if e.SampledBytes+payload > 4<<20 {
				_ = rows.Close()
				e.UnavailableReason = "payload_sample_limit"
				return e
			}
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return e
		}
		if count == 0 && estimate.Int64 != 0 {
			e.UnavailableReason = "row_sample_empty"
			return e
		}
		estimated := max(uint64(estimate.Int64), count)
		if estimated > math.MaxUint64-e.EstimatedRows {
			e.UnavailableReason = "estimate_overflow"
			return e
		}
		e.EstimatedRows += estimated
		e.SampledRows += count
		e.SampledBytes += payload
		if count > 0 {
			seconds += float64(estimated) / float64(count) * max(time.Since(tableStart).Seconds(), .001) * 4
			bytes += float64(estimated) / float64(count) * float64(payload) * 2
		}
	}
	if remaining := time.Second - time.Since(start); remaining > 0 {
		timer := time.NewTimer(remaining)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return e
		}
	}
	after, err := readBinlogSizes(ctx, r.conn)
	if err != nil {
		e.UnavailableReason = "binlog_sample_unavailable"
		return e
	}
	growth, err := binlogGrowth(before, after)
	if err != nil {
		e.UnavailableReason = "binlog_history_changed"
		return e
	}
	if math.IsInf(seconds, 0) || math.IsInf(bytes, 0) || seconds >= float64(math.MaxUint64) || bytes >= float64(math.MaxUint64) {
		e.UnavailableReason = "estimate_overflow"
		return e
	}
	e.EstimatedSeconds = uint64(math.Ceil(max(seconds, 1)))
	e.EstimatedSnapshotBytes = uint64(math.Ceil(bytes))
	if rate := r.args.GetSnapshotRowsPerSecond(); rate > 0 {
		e.EstimatedSeconds = max(e.EstimatedSeconds, e.EstimatedRows/rate+1)
	}
	if rate := r.args.GetSnapshotBytesPerSecond(); rate > 0 {
		e.EstimatedSeconds = max(e.EstimatedSeconds, e.EstimatedSnapshotBytes/rate+1)
	}
	e.BinlogGrowthBytes, e.BinlogSampleMs = growth, uint64(max(time.Since(start).Milliseconds(), 1))
	e.Verified, e.UnavailableReason = true, ""
	return e
}

func readBinlogSizes(ctx context.Context, conn *sql.Conn) (map[string]uint64, error) {
	rows, err := conn.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return nil, err
	}
	defer func(cleanup func() error) { _ = cleanup() }(rows.Close)
	columns, err := rows.Columns()
	if err != nil || len(columns) < 2 {
		return nil, errors.New("binlog columns unavailable")
	}
	out := map[string]uint64{}
	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		dest := make([]any, len(values))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		size, err := strconv.ParseUint(values[1].String, 10, 64)
		if err != nil || !values[0].Valid || values[0].String == "" || len(out) >= 10000 {
			return nil, errors.New("binlog sizes unavailable")
		}
		if _, exists := out[values[0].String]; exists {
			return nil, errors.New("duplicate binlog")
		}
		out[values[0].String] = size
	}
	if len(out) == 0 {
		return nil, errors.New("binlog history empty")
	}
	return out, rows.Err()
}

func binlogGrowth(before, after map[string]uint64) (uint64, error) {
	var growth uint64
	for name, size := range before {
		if current, ok := after[name]; !ok || current < size {
			return 0, errors.New("binlog purged or reset during sample")
		}
	}
	for name, size := range after {
		delta := size - before[name]
		if math.MaxUint64-growth < delta {
			return 0, errors.New("binlog growth overflow")
		}
		growth += delta
	}
	return growth, nil
}
