package position

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"net"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/xmapst/xdts/internal/mysqlx"
)

const maxTimestampFiles = 256
const maxTimestampBytes uint64 = 512 << 20

var ErrTimestampNotFound = errors.New("position: 截止位点之前没有提交时间符合要求的事务")
var ErrTimestampHistory = errors.New("position: 保留的 binlog 无法证明所需时间点的完整历史")
var ErrTimestampLimit = errors.New("position: 时间点扫描超过文件或字节上限，请缩小保留日志范围")

type TimestampResult struct {
	GTIDSet    string
	File       string
	Offset     uint32
	CommitTSMS int64
}
type timestampFile struct {
	name         string
	size         uint64
	firstEventMS int64
}

// ResolveTimestamp scans a fixed, read-only binlog horizon. Immediate commit
// timestamps refer to this source instance, including locally applied GTIDs.
// The matching transaction is excluded from the returned executed set.
func ResolveTimestamp(ctx context.Context, db *sql.DB, conn Conn, requestedMS int64) (TimestampResult, error) {
	if requestedMS <= 0 || requestedMS > math.MaxInt64/1000 || conn.ServerID == 0 || conn.Host == "" || conn.Port == 0 {
		return TimestampResult{}, fmt.Errorf("position: invalid timestamp probe input")
	}
	if _, ok := ctx.Deadline(); !ok {
		return TimestampResult{}, fmt.Errorf("position: timestamp probe requires a deadline")
	}
	files, err := timestampHorizon(ctx, db)
	if err != nil {
		return TimestampResult{}, err
	}
	if len(files) == 0 {
		return TimestampResult{}, ErrTimestampNotFound
	}
	// Probe each file's first real event before choosing the approximate file.
	// A backwards clock or an unavailable timestamp disables this shortcut.
	for i := range files {
		err := readTimestampFile(ctx, conn, files[i], func(stream *timestampEvents) error {
			for {
				ev, err := stream.GetEvent(ctx)
				if err != nil {
					return err
				}
				if ev.Header.LogPos == 0 {
					continue
				}
				if ev.Header.EventType != replication.FORMAT_DESCRIPTION_EVENT || uint64(ev.Header.LogPos) > files[i].size {
					return ErrTimestampHistory
				}
				files[i].firstEventMS = int64(ev.Header.Timestamp) * 1000
				return nil
			}
		})
		if err != nil {
			return TimestampResult{}, err
		}
	}
	start := timestampCandidate(files, requestedMS)
	scan := &timestampScan{requestedUS: uint64(requestedMS) * 1000, accumulated: gmysql.NewMysqlGTIDSet(), retainedStartKnown: start > 0}
	for _, file := range files[start:] {
		var result TimestampResult
		var found bool
		err := readTimestampFile(ctx, conn, file, func(stream *timestampEvents) error {
			var err error
			result, found, err = scan.readFile(ctx, stream, file)
			return err
		})
		if err != nil {
			return TimestampResult{}, err
		}
		if found {
			return result, nil
		}
	}
	return TimestampResult{}, ErrTimestampNotFound
}

func timestampCandidate(files []timestampFile, requestedMS int64) int {
	start := 0
	for i, file := range files {
		if file.firstEventMS <= 0 || (i > 0 && file.firstEventMS < files[i-1].firstEventMS) {
			return 0
		}
		if file.firstEventMS <= requestedMS && (i == 0 || file.firstEventMS > files[i-1].firstEventMS) {
			start = i
		}
	}
	// First-event timestamps have only second precision. Include the preceding
	// file to cover rotation near the requested transaction's commit boundary.
	if start > 0 {
		start--
	}
	return start
}

func readTimestampFile(ctx context.Context, conn Conn, file timestampFile, read func(*timestampEvents) error) error {
	cfg := mysqlx.NewBinlogSyncerConfig(conn.ServerID, conn.Host, conn.Port, conn.User, conn.Password)
	cfg.DisableRetrySync = true
	cfg.DumpCommandFlag = replication.BINLOG_DUMP_NON_BLOCK
	cfg.ReadTimeout = 5 * time.Second
	cfg.Dialer = func(_ context.Context, network, addr string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}
	cfg.Option = func(c *client.Conn) error { _, err := c.Execute("SET SESSION TRANSACTION READ ONLY"); return err }
	syncer := replication.NewBinlogSyncer(cfg)
	stream, err := syncer.StartSync(gmysql.Position{Name: file.name, Pos: binlogFileStartPos})
	if err != nil {
		syncer.Close()
		return fmt.Errorf("position: open timestamp scan: %w", err)
	}
	err = read(&timestampEvents{stream: stream})
	syncer.Close()
	return err
}

// The upstream streamer selects independently between buffered events and its
// terminal error. Drain events already received before observing that error;
// success still requires reaching the captured horizon or a proved boundary.
type timestampEvents struct {
	stream   *replication.BinlogStreamer
	pending  []*replication.BinlogEvent
	terminal error
}

func (s *timestampEvents) GetEvent(ctx context.Context) (*replication.BinlogEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(s.pending) > 0 {
		event := s.pending[0]
		s.pending[0] = nil
		s.pending = s.pending[1:]
		return event, nil
	}
	if s.terminal != nil {
		return nil, s.terminal
	}
	event, err := s.stream.GetEvent(ctx)
	if err == nil {
		return event, nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	s.terminal = err
	s.pending = s.stream.DumpEvents()
	if len(s.pending) == 0 {
		return nil, err
	}
	return s.GetEvent(ctx)
}

func timestampHorizon(ctx context.Context, db *sql.DB) ([]timestampFile, error) {
	var files []timestampFile
	err := WithReadOnlyConn(ctx, db, func(ctx context.Context, c *sql.Conn) error {
		var version string
		if err := c.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
			return err
		}
		v, err := mysqlx.ParseVersion(version)
		if err != nil {
			return err
		}
		rows, err := c.QueryContext(ctx, mysqlx.ShowBinlogStatusSQL(mysqlx.DetectCaps(v)))
		if err != nil {
			return err
		}
		columns, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			return err
		}
		values := make([]sql.NullString, len(columns))
		dest := make([]any, len(columns))
		for i := range dest {
			dest[i] = &values[i]
		}
		if !rows.Next() {
			_ = rows.Close()
			return ErrTimestampHistory
		}
		if err := rows.Scan(dest...); err != nil {
			_ = rows.Close()
			return err
		}
		var current string
		var end uint64
		for i, col := range columns {
			if col == "File" {
				current = values[i].String
			}
			if col == "Position" {
				if _, err := fmt.Sscan(values[i].String, &end); err != nil {
					_ = rows.Close()
					return err
				}
			}
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if current == "" || end < 4 {
			return ErrTimestampHistory
		}
		rows, err = c.QueryContext(ctx, "SHOW BINARY LOGS")
		if err != nil {
			return err
		}
		defer func(cleanup func() error) { _ = cleanup() }(rows.Close)
		columns, err = rows.Columns()
		if err != nil {
			return err
		}
		var total uint64
		found := false
		for rows.Next() {
			var name string
			var size uint64
			dest = make([]any, len(columns))
			dest[0] = &name
			dest[1] = &size
			for i := 2; i < len(dest); i++ {
				dest[i] = new(sql.RawBytes)
			}
			if err := rows.Scan(dest...); err != nil {
				return err
			}
			if name == current {
				size = end
				found = true
			}
			if size < 4 || size > math.MaxUint32 {
				return ErrTimestampLimit
			}
			total += size
			if len(files) >= maxTimestampFiles || total > maxTimestampBytes {
				return ErrTimestampLimit
			}
			files = append(files, timestampFile{name: name, size: size})
			if found {
				break
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if !found {
			return ErrTimestampHistory
		}
		return nil
	})
	return files, err
}

type timestampScan struct {
	requestedUS         uint64
	accumulated         gmysql.MysqlGTIDSet
	sawFirstTransaction bool
	sawPrevious         bool
	firstEventMS        int64
	retainedStartKnown  bool
}

func (s *timestampScan) readFile(ctx context.Context, stream *timestampEvents, file timestampFile) (TimestampResult, bool, error) {
	s.sawPrevious = false
	for {
		ev, err := stream.GetEvent(ctx)
		if err != nil {
			return TimestampResult{}, false, fmt.Errorf("position: timestamp scan file %s failed: %w", file.name, err)
		}
		if ev.Header.LogPos == 0 {
			continue
		} // initial artificial rotate
		if uint64(ev.Header.LogPos) > file.size {
			return TimestampResult{}, false, ErrTimestampHistory
		}
		if ev.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT {
			// File-selection time is a hint; transaction commit time is decisive.
			s.firstEventMS = int64(ev.Header.Timestamp) * 1000
		}
		result, found, err := s.consume(file.name, ev)
		if err != nil || found {
			return result, found, err
		}
		if uint64(ev.Header.LogPos) == file.size {
			return TimestampResult{}, false, nil
		}
	}
}

func (s *timestampScan) consume(file string, ev *replication.BinlogEvent) (TimestampResult, bool, error) {
	switch ev.Header.EventType {
	case replication.PREVIOUS_GTIDS_EVENT:
		pg, ok := ev.Event.(*replication.PreviousGTIDsEvent)
		if !ok {
			return TimestampResult{}, false, ErrTimestampHistory
		}
		previous, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, pg.GTIDSets)
		if err != nil {
			return TimestampResult{}, false, err
		}
		if s.sawFirstTransaction && !previous.Equal(&s.accumulated) {
			return TimestampResult{}, false, ErrTimestampHistory
		}
		s.accumulated = *previous.(*gmysql.MysqlGTIDSet)
		s.sawPrevious = true
	case replication.ANONYMOUS_GTID_EVENT:
		return TimestampResult{}, false, ErrTimestampHistory
	case replication.GTID_EVENT, replication.GTID_TAGGED_LOG_EVENT:
		g, ok := ev.Event.(*replication.GTIDEvent)
		if !ok || !s.sawPrevious || g.ImmediateCommitTimestamp == 0 || ev.Header.LogPos < ev.Header.EventSize {
			return TimestampResult{}, false, ErrTimestampHistory
		}
		if !s.sawFirstTransaction && !s.retainedStartKnown && g.ImmediateCommitTimestamp >= s.requestedUS && s.accumulated.String() != "" {
			// Purged older files could contain an earlier qualifying transaction.
			return TimestampResult{}, false, ErrTimestampHistory
		}
		s.sawFirstTransaction = true
		if g.ImmediateCommitTimestamp >= s.requestedUS {
			return TimestampResult{GTIDSet: s.accumulated.String(), File: file, Offset: ev.Header.LogPos - ev.Header.EventSize, CommitTSMS: int64(g.ImmediateCommitTimestamp / 1000)}, true, nil
		}
		next, err := g.GTIDNext()
		if err != nil {
			return TimestampResult{}, false, err
		}
		if err := s.accumulated.Update(next.String()); err != nil {
			return TimestampResult{}, false, err
		}
	}
	return TimestampResult{}, false, nil
}
