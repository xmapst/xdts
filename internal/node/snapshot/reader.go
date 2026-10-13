package snapshot

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/resourcequota"
	"github.com/xmapst/xdts/internal/rule"
	"google.golang.org/protobuf/proto"
)

type ReaderOptions struct {
	Budget        *resourcequota.Budget
	ColumnFilter  IColumnFilter
	RowFilter     iproto.ISourceRowPlan
	Transforms    *rule.TransformPlan
	TableFilter   iproto.ISourceTablePlan
	Concurrency   uint32
	MaxRows       uint64
	MaxBytes      uint64
	RowsPerSec    uint64
	BytesPerSec   uint64
	RefreshSchema bool
	Isolated      func(string, string) bool
	WaitSchema    func(context.Context) error
}

type IColumnFilter interface {
	Allow(string, string, string) bool
}

type ChunkData struct {
	buffer  *resourcequota.Buffer
	Chunk   Chunk
	Columns []string
	Rows    []*iproto.Row
	Bytes   uint64
	// Published rows were read from durable progress; no source SELECT ran.
	Published      bool
	PublishedRows  uint64
	releaseRuntime func()
}

// Release returns retained row/encoding capacity. ReadPlan releases after its
// synchronous delivery callback; callers of ReadChunk must release explicitly.
func (d ChunkData) Release() {
	d.buffer.Release()
	if d.releaseRuntime != nil {
		d.releaseRuntime()
	}
}

// Reader is immutable after construction apart from its shared limiter. Each
// read owns its rows and connection. ReadPlan uses bounded workers and serial
// delivery; its callback owns rows for the duration of its synchronous call and
// must finish publishing before returning and releasing their budget.
type Reader struct {
	boundaries     *SQLBoundaryReader
	opts           ReaderOptions
	limiter        *rateLimiter
	runtimeMu      sync.Mutex
	runtime        ReaderOptions
	runtimeActive  uint32
	runtimePaused  bool
	runtimeChanged chan struct{}
}

func NewReader(boundaries *SQLBoundaryReader, opts ReaderOptions) (*Reader, error) {
	if boundaries == nil || opts.Concurrency == 0 || opts.Concurrency > 128 || opts.MaxRows == 0 || opts.MaxBytes == 0 {
		return nil, fmt.Errorf("snapshot: reader, concurrency and chunk limits required")
	}
	return &Reader{boundaries: boundaries, opts: opts,
		runtime: opts, runtimeChanged: make(chan struct{}), limiter: newRateLimiter(opts.RowsPerSec, opts.BytesPerSec)}, nil
}

// ReadChunk returns no partial data on failure. Limits are checked before
// appending each row; it never changes a committed chunk's number to split it.
func (r *Reader) ReadChunk(ctx context.Context, table Table, chunk Chunk) (ChunkData, error) {
	opts, release, err := r.acquireRuntime(ctx)
	if err != nil {
		return ChunkData{}, err
	}
	empty := func() ChunkData {
		names := make([]string, len(chunk.Key.Columns))
		for i, c := range chunk.Key.Columns {
			names[i] = c.Name
		}
		return ChunkData{Chunk: chunk, Columns: names, releaseRuntime: release}
	}
	if opts.Isolated != nil && opts.Isolated(table.Schema, table.Name) {
		return empty(), nil
	}
	data, err := r.readChunk(ctx, table, chunk, opts)
	// Compatible DROP may commit between metadata and SELECT. Retry a bounded
	// number of times from fresh metadata; no partial rows have been delivered.
	for attempt := 0; err != nil && opts.RefreshSchema && attempt < 2; attempt++ {
		var myErr *mysql.MySQLError
		if !errors.As(err, &myErr) || myErr.Number != 1054 {
			break
		}
		data, err = r.readChunk(ctx, table, chunk, opts)
	}
	if err != nil {
		if opts.Isolated != nil && opts.WaitSchema != nil {
			if waitErr := opts.WaitSchema(ctx); waitErr == nil && opts.Isolated(table.Schema, table.Name) {
				return empty(), nil
			}
		}
		release()
		return ChunkData{}, err
	}
	if opts.Isolated != nil && opts.Isolated(table.Schema, table.Name) {
		data.Release()
		return empty(), nil
	}
	data.releaseRuntime = release
	if err := r.limiter.wait(ctx, uint64(len(data.Rows)), data.Bytes); err != nil {
		data.Release()
		return ChunkData{}, err
	}
	return data, nil
}

func (r *Reader) readChunk(ctx context.Context, table Table, chunk Chunk, opts ReaderOptions) (returned ChunkData, readErr error) {
	releaseSlot, err := opts.Budget.AcquireSnapshot(ctx)
	if err != nil {
		return ChunkData{}, err
	}
	defer releaseSlot()
	buffer, err := opts.Budget.Reserve(0)
	if err != nil {
		return ChunkData{}, err
	}
	defer func() {
		if recover() != nil {
			returned, readErr = ChunkData{}, errors.New("snapshot: chunk reader panicked")
		}
		if readErr != nil {
			buffer.Release()
		}
	}()
	if chunk.Schema != table.Schema || chunk.Table != table.Name || chunk.Number == 0 || chunk.Number > maxChunkNumber {
		return ChunkData{}, fmt.Errorf("snapshot: chunk identity disagrees with table")
	}
	if err := validateReadKey(table, chunk.Key); err != nil {
		return ChunkData{}, err
	}
	for _, column := range chunk.Key.Columns {
		if column.Generated {
			return ChunkData{}, fmt.Errorf("snapshot: generated positioning key cannot be projected")
		}
	}
	data := ChunkData{Chunk: chunk, buffer: buffer}
	data.Chunk.Key.Columns = append([]Column(nil), chunk.Key.Columns...)
	data.Chunk.Lower, data.Chunk.Upper = chunk.Lower.clone(), chunk.Upper.clone()
	if opts.TableFilter != nil && !opts.TableFilter.MatchTable(table.Schema, table.Name) {
		for _, key := range chunk.Key.Columns {
			data.Columns = append(data.Columns, key.Name)
		}
		return data, nil
	}
	var columns []Column
	var selected []int
	var filterIndices []int
	var filterNames []string
	var readNames []string
	filterRows := opts.RowFilter != nil && opts.RowFilter.HasRowFilters(table.Schema, table.Name)
	required := map[string]bool{}
	if filterRows {
		for _, name := range opts.RowFilter.RowFilterColumns(table.Schema, table.Name) {
			required[strings.ToLower(name)] = true
		}
	}
	projection := table.Columns
	currentTable := table
	if opts.RefreshSchema {
		current, err := r.boundaries.Table(ctx, table.Schema, table.Name)
		if err != nil {
			return ChunkData{}, err
		}
		projection, err = snapshotProjection(table, current, chunk.Key)
		if err != nil {
			return ChunkData{}, err
		}
		currentTable = current
	}
	if opts.Transforms.HasTable(table.Schema, table.Name) {
		keys := make(map[string]bool)
		for _, name := range currentTable.PrimaryKey.Columns {
			keys[strings.ToLower(name)] = true
		}
		for _, key := range currentTable.UniqueKeys {
			if key.Incomplete {
				return ChunkData{}, rule.ErrTransformConfig
			}
			for _, name := range key.Columns {
				keys[strings.ToLower(name)] = true
			}
		}
		for _, name := range opts.Transforms.Columns(table.Schema, table.Name) {
			if keys[name] {
				return ChunkData{}, rule.ErrTransformConfig
			}
		}
	}
	for _, col := range projection {
		if col.Generated {
			if filterRows {
				if required[strings.ToLower(col.Name)] {
					return ChunkData{}, errors.New("snapshot: generated row filter column is unsupported")
				}
			}
			continue
		}
		keep := opts.ColumnFilter == nil || opts.ColumnFilter.Allow(table.Schema, table.Name, col.Name)
		if !keep && !required[strings.ToLower(col.Name)] {
			continue
		}
		if !supportedSQLType(col.DataType) {
			return ChunkData{}, fmt.Errorf("snapshot: unsupported SQL column type")
		}
		if keep {
			data.Columns = append(data.Columns, col.Name)
			selected = append(selected, len(columns))
		}
		readNames = append(readNames, col.Name)
		if required[strings.ToLower(col.Name)] {
			filterNames = append(filterNames, col.Name)
			filterIndices = append(filterIndices, len(columns))
		}
		columns = append(columns, col)
	}
	if len(filterNames) != len(required) {
		return ChunkData{}, errors.New("snapshot: row filter column is missing")
	}
	for _, key := range chunk.Key.Columns {
		found := slices.Contains(data.Columns, key.Name)
		if !found {
			return ChunkData{}, fmt.Errorf("snapshot: positioning key excluded from projection")
		}
	}
	query, args, err := chunk.Select(readNames)
	if err != nil {
		return ChunkData{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, r.boundaries.timeout)
	defer cancel()
	err = r.boundaries.withReadOnlyConn(ctx, func(ctx context.Context, conn *sql.Conn) error {
		rows, err := conn.QueryContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("snapshot: query chunk failed: %w", err)
		}
		defer func(cleanup func() error) { _ = cleanup() }(rows.Close)
		raw := make([]sql.RawBytes, len(columns))
		dest := make([]any, len(raw))
		for i := range raw {
			dest[i] = &raw[i]
		}
		var readRows, transformedBytes uint64
		for rows.Next() {
			if readRows >= opts.MaxRows {
				return fmt.Errorf("snapshot: chunk exceeds transaction row limit")
			}
			if err := rows.Scan(dest...); err != nil {
				return fmt.Errorf("snapshot: decode chunk row failed")
			}
			readRows++
			rowBytes := uint64(0)
			for _, value := range raw {
				// Include per-column framing in the conservative byte budget; this
				// also bounds allocation for NULL-heavy and empty-value rows.
				cost := uint64(len(value)) + 16
				if cost > opts.MaxBytes-data.Bytes-rowBytes {
					return fmt.Errorf("snapshot: chunk exceeds transaction byte limit")
				}
				rowBytes += cost
			}
			// Acquire the same conservative row framing and four encoding copies
			// once, before allocation, rather than locking the shared node quota
			// separately for every column. Raw SQL buffers remain row-local.
			if err := buffer.Grow(128 + uint64(len(columns))*192 + rowBytes*4); err != nil {
				return err
			}
			data.Bytes += rowBytes
			row := &iproto.Row{Values: make([]*iproto.ColumnValue, len(columns))}
			for i, column := range columns {
				value, err := DecodeSQLValue(raw[i], column)
				if err != nil {
					return err
				}
				row.Values[i] = value
			}
			if filterRows {
				values := make([]any, len(filterIndices))
				for i, index := range filterIndices {
					var err error
					values[i], err = iproto.RowFilterValue(row.Values[index])
					if err != nil {
						return err
					}
				}
				allowed, err := opts.RowFilter.MatchRow(table.Schema, table.Name, filterNames, values)
				if err != nil {
					return err
				}
				if !allowed {
					continue
				}
			}
			projected := make([]*iproto.ColumnValue, len(selected))
			for i, index := range selected {
				projected[i] = row.Values[index]
			}
			row.Values = projected
			row, err = iproto.TransformRow(opts.Transforms, table.Schema, table.Name, data.Columns, row)
			if err != nil {
				return err
			}
			if opts.Transforms.HasTable(table.Schema, table.Name) {
				cost := uint64(proto.Size(row))
				if cost > opts.MaxBytes || transformedBytes > opts.MaxBytes-cost {
					return rule.ErrTransformEvaluation
				}
				transformedBytes += cost
				if err := buffer.Grow(cost * 4); err != nil {
					return err
				}
			}
			data.Rows = append(data.Rows, row)
		}
		if rows.Err() != nil {
			return fmt.Errorf("snapshot: read chunk failed")
		}
		return nil
	})
	if err != nil {
		if ctx.Err() != nil {
			return ChunkData{}, ctx.Err()
		}
		return ChunkData{}, err
	}
	return data, nil
}

// ReadPlan delivers completed chunks without ordering requirements. It validates
// the plan before spawning workers. Cancellation waits for all workers to exit,
// so no source reads survive a stopped/replaced workflow.
func (r *Reader) ReadPlan(ctx context.Context, plan Plan, tables []Table, deliver func(context.Context, ChunkData) error) error {
	return r.ReadPendingPlan(ctx, plan, tables, nil, deliver)
}

func (r *Reader) ReadPendingPlan(ctx context.Context, plan Plan, tables []Table,
	completed func(context.Context, Chunk) (uint64, bool, error), deliver func(context.Context, ChunkData) error) error {
	validator := &Planner{pipelineID: plan.PipelineID, runID: plan.RunID, reinitDigest: plan.ReinitDigest}
	if err := validator.validate(plan); err != nil {
		return err
	}
	if err := matchInputs(plan, tables, plan.ChunkRows); err != nil {
		return err
	}
	orderedTables, err := tablesForPlan(plan, tables)
	if err != nil {
		return err
	}
	if deliver == nil {
		return fmt.Errorf("snapshot: chunk delivery callback required")
	}
	// Only metadata keys are used by matching; clone full metadata and plan before
	// crossing the worker boundary so caller mutations after return cannot leak.
	plan = plan.clone()
	ownedTables := make([]Table, len(orderedTables))
	for i, table := range orderedTables {
		ownedTables[i] = table
		ownedTables[i].Columns = append([]Column(nil), table.Columns...)
		ownedTables[i].PrimaryKey.Columns = append([]string(nil), table.PrimaryKey.Columns...)
		ownedTables[i].UniqueKeys = make([]Index, len(table.UniqueKeys))
		for j, index := range table.UniqueKeys {
			ownedTables[i].UniqueKeys[j] = index
			ownedTables[i].UniqueKeys[j].Columns = append([]string(nil), index.Columns...)
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	type job struct {
		table     Table
		chunk     Chunk
		published bool
		rows      uint64
	}
	type result struct {
		data ChunkData
		err  error
	}
	// Slots enforce the current configured concurrency. Idle workers allow an
	// increase without changing connections or reassigning committed chunks.
	workers := min(uint64(128), plan.Total)
	jobs := make(chan job)
	results := make(chan result, workers)
	var wg sync.WaitGroup
	wg.Go(func() {
		defer close(jobs)
		for i, tablePlan := range plan.Tables {
			for index := uint64(0); index < tablePlan.ChunkCount; index++ {
				chunk, _ := tablePlan.Chunk(index) // validated above
				var rows uint64
				var published bool
				if completed != nil {
					var err error
					rows, published, err = r.completedChunk(ctx, chunk, completed)
					if err != nil {
						select {
						case results <- result{err: err}:
						case <-ctx.Done():
						}
						return
					}
				}
				select {
				case jobs <- job{table: ownedTables[i], chunk: chunk, published: published, rows: rows}:
				case <-ctx.Done():
					return
				}
			}
		}
	})
	for range workers {
		wg.Go(func() {
			for job := range jobs {
				var data ChunkData
				var err error
				if job.published {
					data = ChunkData{Chunk: job.chunk, Published: true, PublishedRows: job.rows}
				} else {
					data, err = r.ReadChunk(ctx, job.table, job.chunk)
				}
				select {
				case results <- result{data, err}:
				case <-ctx.Done():
					data.Release()
					return
				}
				if err != nil {
					return
				}
			}
		})
	}
	closed := make(chan struct{})
	go func() { wg.Wait(); close(results); close(closed) }()
	defer func() {
		cancel()
		<-closed
		for result := range results {
			result.data.Release()
		}
	}()
	for result := range results {
		if result.err != nil {
			return result.err
		}
		if err := ctx.Err(); err != nil {
			result.data.Release()
			return err
		}
		if err := r.deliverChunk(ctx, result.data, deliver); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (r *Reader) completedChunk(ctx context.Context, chunk Chunk, completed func(context.Context, Chunk) (uint64, bool, error)) (rows uint64, published bool, lookupErr error) {
	defer func() {
		if recover() != nil {
			lookupErr = errors.New("snapshot: chunk progress lookup panicked")
		}
	}()
	return completed(ctx, chunk)
}

func (r *Reader) deliverChunk(ctx context.Context, data ChunkData, deliver func(context.Context, ChunkData) error) (deliveryErr error) {
	defer data.Release()
	defer func() {
		if recover() != nil {
			deliveryErr = errors.New("snapshot: chunk delivery panicked")
		}
	}()
	return deliver(ctx, data)
}

// rateLimiter implements two token buckets with one second of initial burst.
// Reservations are serialized across workers, while the waiting is outside the
// mutex and outside database sessions. Cancelled reservations are conservative
// (not refunded), so cancellation cannot increase the allowed throughput.
type rateLimiter struct {
	mu                    sync.Mutex
	rowsRate, bytesRate   float64
	rowTokens, byteTokens float64
	last                  time.Time
}

func newRateLimiter(rows, bytes uint64) *rateLimiter {
	return &rateLimiter{rowsRate: float64(rows), bytesRate: float64(bytes),
		rowTokens: float64(rows), byteTokens: float64(bytes), last: time.Now()}
}

func (l *rateLimiter) wait(ctx context.Context, rows, bytes uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	now := time.Now()
	elapsed := now.Sub(l.last).Seconds()
	l.last = now
	var delay float64
	for _, bucket := range []struct {
		rate, amount float64
		tokens       *float64
	}{{l.rowsRate, float64(rows), &l.rowTokens}, {l.bytesRate, float64(bytes), &l.byteTokens}} {
		if bucket.rate == 0 {
			continue
		}
		*bucket.tokens = min(bucket.rate, *bucket.tokens+elapsed*bucket.rate) - bucket.amount
		delay = max(delay, -*bucket.tokens/bucket.rate)
	}
	l.mu.Unlock()
	if delay <= 0 {
		return ctx.Err()
	}
	if delay >= float64(1<<63-1)/float64(time.Second) {
		return fmt.Errorf("snapshot: rate limit wait exceeds duration range")
	}
	timer := time.NewTimer(time.Duration(delay * float64(time.Second)))
	defer timer.Stop()
	select {
	case <-timer.C:
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}
