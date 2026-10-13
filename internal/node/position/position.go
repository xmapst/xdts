package position

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/xmapst/xdts/internal/mysqlx"
)

// connResetTimeout 是归还连接前"重置回可写"这个收尾动作自己的等待上限（不复用
// 调用方的 ctx，见 withReadOnlyConn 的 defer）。一次 SET SESSION 是极轻量的操作，
// 3 秒足够覆盖正常抖动，同时仍然有限——不会让一个真正挂死的数据库连接在这里
// 无限期占住收尾路径。
const connResetTimeout = 3 * time.Second

// binlogFileStartPos 是每个 binlog 文件里第一个事件（FORMAT_DESCRIPTION_EVENT）的
// 起始偏移：文件开头 4 字节是固定的魔数，MySQL 生态（mysqlbinlog、
// CHANGE MASTER TO ... MASTER_LOG_POS=4）统一用 4 作为"从文件最开始读"的位置。
const binlogFileStartPos uint32 = 4

// Conn 是打开一次性 BinlogSyncer 探测连接所需的参数，字段语义与
// binlogin.Options 的同名字段一致，不引入新概念。ResolveFileOffset 需要它来读取
// 文件内容；ResolveLatest/ResolveGTIDSet 只查询变量，不需要。
type Conn struct {
	Host     string
	User     string
	Password string
	// ServerID 是本次一次性探测连接的伪从库 server_id，必须与真正的 binlog_in
	// dump 连接、以及同时运行的其它探测连接都不冲突（MGR-PL-06 同一要求）。调用方
	// 负责分配，本包不做全局唯一性保证。
	ServerID uint32
	Port     uint16
}

// WithReadOnlyConn reserves a session, sets read-only transactions and resets it
// before returning the connection to the pool. A failed reset discards the connection
// so another borrower cannot inherit unintended session state.
func WithReadOnlyConn(ctx context.Context, db *sql.DB, f func(ctx context.Context, conn *sql.Conn) error) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("position: 获取数据库连接失败: %w", err)
	}
	defer func() {
		outerCtxErr := ctx.Err()
		resetCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), connResetTimeout)
		defer cancel()
		if _, resetErr := conn.ExecContext(resetCtx, "SET SESSION TRANSACTION READ WRITE"); resetErr != nil {
			slog.Error("reset read-only state before returning connection failed",
				slog.Any("reset_error", resetErr), slog.Any("outer_ctx_error", outerCtxErr))
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		_ = conn.Close()
	}()
	// 注意：SET SESSION TRANSACTION READ ONLY 设置的是"后续事务"的默认访问模式，
	// 对已经开始的事务不生效。本包目前所有查询都是 autocommit 单条 SELECT，这条
	// SET 起的是"安全带"作用（防止将来有人不小心在这个连接上写）而不是让当前这
	// 一条语句变成只读——如果本包以后要在同一个连接上开显式事务，"先 SET 后
	// BEGIN"这个顺序就会变成有实质意义的约束，不能颠倒。
	if _, err := conn.ExecContext(ctx, "SET SESSION TRANSACTION READ ONLY"); err != nil {
		return fmt.Errorf("position: 设置只读会话失败: %w", err)
	}
	return f(ctx, conn)
}

// ResolveLatest 实现 NODE-POS-01："当前最新"：读取源库 @@GLOBAL.gtid_executed
// 作为起始 GTID 集合。
func ResolveLatest(ctx context.Context, db *sql.DB) (string, error) {
	var gset string
	err := WithReadOnlyConn(ctx, db, func(ctx context.Context, conn *sql.Conn) error {
		return conn.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&gset)
	})
	if err != nil {
		return "", fmt.Errorf("position: 查询 gtid_executed 失败: %w", err)
	}
	return gset, nil
}

var ErrPurgedNotSubset = errors.New("position: 源库 gtid_purged 不是指定集合的子集，所需 binlog 可能已被清理")

// ResolveGTIDSet rejects start sets that omit required purged source GTIDs.
func ResolveGTIDSet(ctx context.Context, db *sql.DB, requested string) (gtidSet string, warning string, err error) {
	var purgedStr, executedStr string
	dbErr := WithReadOnlyConn(ctx, db, func(ctx context.Context, conn *sql.Conn) error {
		if err := conn.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_purged").Scan(&purgedStr); err != nil {
			return fmt.Errorf("查询 gtid_purged 失败: %w", err)
		}
		if err := conn.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&executedStr); err != nil {
			return fmt.Errorf("查询 gtid_executed 失败: %w", err)
		}
		return nil
	})
	if dbErr != nil {
		return "", "", fmt.Errorf("position: %w", dbErr)
	}
	return evaluateGTIDSet(requested, purgedStr, executedStr)
}

// evaluateGTIDSet 是 ResolveGTIDSet 的纯逻辑部分：给定 requested/purged/executed
// 三个 GTID 集合的字符串表示，判断 requested 是否可以安全作为起始位点。不做任何
// I/O，供单测穷举各种组合（见 internal/node/probe/position_test.go）。
func evaluateGTIDSet(requested, purgedStr, executedStr string) (gtidSet string, warning string, err error) {
	requestedSet, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, requested)
	if err != nil {
		return "", "", fmt.Errorf("position: 解析指定的 GTID 集合 %q 失败: %w", requested, err)
	}
	purgedSet, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, purgedStr)
	if err != nil {
		return "", "", fmt.Errorf("position: 解析源库 gtid_purged %q 失败: %w", purgedStr, err)
	}
	if !requestedSet.Contain(purgedSet) {
		return "", "", fmt.Errorf("%w（gtid_purged=%s, requested=%s）", ErrPurgedNotSubset, purgedStr, requested)
	}

	executedSet, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, executedStr)
	if err != nil {
		return "", "", fmt.Errorf("position: 解析源库 gtid_executed %q 失败: %w", executedStr, err)
	}
	if !executedSet.Contain(requestedSet) {
		warning = fmt.Sprintf("指定集合包含源库尚未执行过的 GTID（gtid_executed=%s, requested=%s）", executedStr, requested)
	}

	return requested, warning, nil
}

// BoundaryError 是 ResolveFileOffset 在 offset 未对齐事务边界时返回的错误
// （NODE-POS-03"偏移必须指向事务起点，否则拒绝并给出前后最近的事务边界"）。
type BoundaryError struct {
	File      string
	Requested uint32
	// Preceding 是小于等于 Requested 的最近一个合法事务边界；binlogFileStartPos（4，
	// 文件最开始）总是一个合法边界，因此本字段总是有值，不需要额外的 "found" 标记。
	Preceding uint32
	// FollowingValid 为 false 表示在这个文件范围内没有找到比 Requested 更大的事务
	// 边界（Requested 落在了文件的最后一个事务之后，或文件在此之前就结束/切换）。
	FollowingValid bool
	Following      uint32
}

func (e *BoundaryError) Error() string {
	if e.FollowingValid {
		return fmt.Sprintf("position: 文件 %s 的偏移 %d 未对齐事务边界，最近的边界是 %d（之前）和 %d（之后）",
			e.File, e.Requested, e.Preceding, e.Following)
	}
	return fmt.Sprintf("position: 文件 %s 的偏移 %d 未对齐事务边界，最近的边界是 %d（之前）；该文件范围内没有更靠后的边界",
		e.File, e.Requested, e.Preceding)
}

// ErrBinlogFileNotFound 是 ResolveFileOffset 在源库当前 binlog 文件列表里找不到
// 指定文件时返回的哨兵错误（NODE-POS-03"校验文件存在"）。
var ErrBinlogFileNotFound = errors.New("position: 指定的 binlog 文件在源库当前文件列表中不存在")

// binlogFileExists 用 SHOW BINARY LOGS 校验文件存在（NODE-POS-03）。返回的行列数
// 随版本变化（MySQL 8.0.30+ 多一列 Encrypted），用 sql.RawBytes 占位读取，只关心
// 第一列（Log_name），不假设列数。
func binlogFileExists(ctx context.Context, conn *sql.Conn, file string) (bool, error) {
	rows, err := conn.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return false, fmt.Errorf("SHOW BINARY LOGS 失败: %w", err)
	}
	defer func(cleanup func() error) { _ = cleanup() }(rows.Close)

	cols, err := rows.Columns()
	if err != nil {
		return false, fmt.Errorf("读取 SHOW BINARY LOGS 列信息失败: %w", err)
	}

	for rows.Next() {
		dest := make([]any, len(cols))
		var name string
		dest[0] = &name
		for i := 1; i < len(dest); i++ {
			dest[i] = new(sql.RawBytes)
		}
		if err := rows.Scan(dest...); err != nil {
			return false, fmt.Errorf("扫描 SHOW BINARY LOGS 结果失败: %w", err)
		}
		if name == file {
			return true, nil
		}
	}
	return false, rows.Err()
}

// ResolveFileOffset 实现 NODE-POS-03："指定 binlog 文件 + 偏移"：校验文件存在
// （SHOW BINARY LOGS）；校验 offset 指向事务起点（文件起始 4，或某个 GTID_EVENT
// 的起始字节偏移），不对齐时返回 *BoundaryError 给出前后最近的边界；对齐时按
// "该文件的 Previous_gtids + 文件内该偏移之前的全部 GTID" 换算出等价的 GTID 集合。
//
// 用一次性的 BinlogSyncer（走 replication.BinlogSyncer.StartSync 按文件位点读取，
// 不是 StartSyncGTID）从文件最开始（binlogFileStartPos）顺序读到 offset 为止；
// 探测完成后立即关闭连接，不像 binlog_in 那样长期持有。
func ResolveFileOffset(ctx context.Context, db *sql.DB, conn Conn, file string, offset uint32) (string, error) {
	exists, err := existsCheck(ctx, db, file)
	if err != nil {
		return "", err
	}
	if !exists {
		return "", fmt.Errorf("%w: %s", ErrBinlogFileNotFound, file)
	}
	if offset < binlogFileStartPos {
		return "", fmt.Errorf("position: 偏移 %d 小于文件最小合法位置 %d", offset, binlogFileStartPos)
	}

	cfg := mysqlx.NewBinlogSyncerConfig(conn.ServerID, conn.Host, conn.Port, conn.User, conn.Password)
	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()

	streamer, err := syncer.StartSync(gmysql.Position{Name: file, Pos: binlogFileStartPos})
	if err != nil {
		return "", fmt.Errorf("position: 从文件 %s 起始位置打开探测连接失败: %w", file, err)
	}

	accumulated := gmysql.NewMysqlGTIDSet()
	var currentPos = binlogFileStartPos
	var lastBoundary = binlogFileStartPos // 文件起始总是一个合法边界
	sawPreviousGTIDs := false
	// isFileStart 为 true 时，offset 请求的就是"文件最开始"（binlogFileStartPos）——
	// 这个位置总是合法边界，不管它上面具体落的是哪个事件类型（正常文件里是
	// FORMAT_DESCRIPTION_EVENT）。下面的未对齐判断（default/GTID 分支里的
	// eventStart==offset 检查）必须跳过第一条事件，否则会把"文件最开始"误判成
	// "精确落在了 FORMAT_DESCRIPTION_EVENT 开头，不是事务边界"——那是错的，
	// 位置 4 的真实换算结果要等读到 Previous_gtids 事件才能给出（见下面
	// PREVIOUS_GTIDS_EVENT 分支），不是靠 eventStart==offset 这条通用判断。
	isFileStart := offset == binlogFileStartPos

	for {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return "", fmt.Errorf("position: 探测文件 %s 偏移 %d 时 ctx 结束: %w", file, offset, err)
			}
			return "", fmt.Errorf("position: 读取文件 %s 内容失败: %w", file, err)
		}

		eventStart := currentPos
		currentPos = ev.Header.LogPos

		if isFileStart && !sawPreviousGTIDs && eventStart > binlogFileStartPos && ev.Header.EventType != replication.PREVIOUS_GTIDS_EVENT {
			// 退化场景：这个文件里读到第二条事件了还不是 Previous_gtids（不应该
			// 发生在 GTID 模式下的正常文件，正常应该是紧跟在 FORMAT_DESCRIPTION_EVENT
			// 之后的第二条事件）——排除"这一条正好就是 Previous_gtids"（那是正常
			// 路径，交给下面的 case 处理），必须在这里、在任何分支有机会提前 return
			// 之前就拦下来，明确报错而不是让下面的分支把它误判成一个普通的未对齐/越界。
			return "", fmt.Errorf("position: 文件 %s 在读到 %d 字节前没有出现 Previous_gtids 事件，无法计算文件起始位置的基准 GTID 集合", file, eventStart)
		}

		switch ev.Header.EventType {
		case replication.PREVIOUS_GTIDS_EVENT:
			pg, ok := ev.Event.(*replication.PreviousGTIDsEvent)
			if !ok {
				return "", fmt.Errorf("position: PREVIOUS_GTIDS_EVENT 的 Event 字段类型不是 *replication.PreviousGTIDsEvent（实际 %T）", ev.Event)
			}
			if pg.GTIDSets != "" {
				parsed, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, pg.GTIDSets)
				if err != nil {
					return "", fmt.Errorf("position: 解析文件 %s 的 Previous_gtids 失败: %w", file, err)
				}
				if err := accumulated.Update(parsed.String()); err != nil {
					return "", fmt.Errorf("position: 合并文件 %s 的 Previous_gtids 失败: %w", file, err)
				}
			}
			sawPreviousGTIDs = true
			if offset == binlogFileStartPos {
				// 位置 4 是"文件最开始"，语义上等于"这个文件的 Previous_gtids，
				// 文件内容一概不含"——不是巧合地判 eventStart==offset（这条事件的
				// eventStart 通常不是 4，FORMAT_DESCRIPTION_EVENT 才在 4），而是
				// 直接把 4 当成特殊边界处理，见包文档 BoundaryError.Preceding 的注释。
				return accumulated.String(), nil
			}

		case replication.GTID_EVENT, replication.ANONYMOUS_GTID_EVENT:
			if !isFileStart && eventStart == offset {
				return accumulated.String(), nil // 精确落在这个事务的起点：换算结果就是"这个事务之前的全部"
			}
			if eventStart > offset {
				return "", &BoundaryError{File: file, Requested: offset, Preceding: lastBoundary, FollowingValid: true, Following: eventStart}
			}
			lastBoundary = eventStart
			gtidEv, ok := ev.Event.(*replication.GTIDEvent)
			if !ok {
				return "", fmt.Errorf("position: GTID_EVENT 的 Event 字段类型不是 *replication.GTIDEvent（实际 %T）", ev.Event)
			}
			g, err := gtidEv.GTIDNext()
			if err != nil {
				return "", fmt.Errorf("position: 读取事务 GTID 失败: %w", err)
			}
			if err := accumulated.Update(g.String()); err != nil {
				return "", fmt.Errorf("position: 累积 GTID 集合失败: %w", err)
			}

		case replication.ROTATE_EVENT:
			re, ok := ev.Event.(*replication.RotateEvent)
			if ok && string(re.NextLogName) != file {
				// 真正到达本文件末尾（这是"下一个文件是谁"的标记），offset 始终未匹配：
				// 只能给出 Preceding，本文件范围内没有 Following。
				return "", &BoundaryError{File: file, Requested: offset, Preceding: lastBoundary, FollowingValid: false}
			}
			// NextLogName 等于自己：理论上不会发生在这个场景（这种自指 rotate 只在
			// 极端场景出现），忽略、继续按普通事件推进 currentPos。

		default:
			if !isFileStart && eventStart == offset {
				// 精确落在了一个非事务起点事件的开头（如某条 QUERY_EVENT/ROWS_EVENT
				// 中间）：未对齐，继续扫描找到下一个真正的边界作为 Following。
				following, ferr := findNextBoundary(ctx, streamer, currentPos)
				if ferr != nil {
					return "", ferr
				}
				if following == 0 {
					return "", &BoundaryError{File: file, Requested: offset, Preceding: lastBoundary, FollowingValid: false}
				}
				return "", &BoundaryError{File: file, Requested: offset, Preceding: lastBoundary, FollowingValid: true, Following: following}
			}
			if eventStart > offset {
				return "", &BoundaryError{File: file, Requested: offset, Preceding: lastBoundary, FollowingValid: true, Following: eventStart}
			}
		}
	}
}

// existsCheck 包一层是为了让 ResolveFileOffset 的主体逻辑不必关心 withReadOnlyConn
// 的细节。
func existsCheck(ctx context.Context, db *sql.DB, file string) (bool, error) {
	var exists bool
	err := WithReadOnlyConn(ctx, db, func(ctx context.Context, conn *sql.Conn) error {
		var err error
		exists, err = binlogFileExists(ctx, conn, file)
		return err
	})
	if err != nil {
		return false, fmt.Errorf("position: %w", err)
	}
	return exists, nil
}

// findNextBoundary 从当前流继续往后找下一个事务边界（GTID_EVENT 的起始偏移），
// 用于 ResolveFileOffset 报告 BoundaryError.Following。找到文件末尾仍未出现则
// 返回 0（调用方按 FollowingValid=false 处理）。
func findNextBoundary(ctx context.Context, streamer *replication.BinlogStreamer, fromPos uint32) (uint32, error) {
	pos := fromPos
	for {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			return 0, nil // 文件到此为止，没有更多边界
		}
		start := pos
		pos = ev.Header.LogPos
		switch ev.Header.EventType {
		case replication.GTID_EVENT, replication.ANONYMOUS_GTID_EVENT:
			return start, nil
		case replication.ROTATE_EVENT:
			return 0, nil
		}
	}
}
