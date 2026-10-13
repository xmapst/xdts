package mysqlout

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// singleGTIDPattern 匹配 gtid_next 允许设置的单个事务 GTID（"uuid:序号"），不是
// GTID 集合（不接受区间 "1-5"、逗号分隔多段）——gtid_next 一次只能绑定一个事务。
// 当前 MySQL 最低版本地板是 8.0（O-02），不处理 8.3+ 引入的 tag 语法。
var singleGTIDPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}:[0-9]+$`)

func isSingleGTID(s string) bool {
	return singleGTIDPattern.MatchString(s)
}

// dmlTxn is private to this package and owns one original GTID transaction.
// execDML and ordered INSERT accept only SQL from the package constructors;
// conflict reads lock current rows. DDL uses its independent commit path.
// conn is used exclusively while this transaction owns the physical session.
type dmlTxn struct {
	tx             *sql.Tx
	conn           *sql.Conn
	statements     map[string]*sql.Stmt
	columns        map[string]map[string]targetColumn
	pendingLineage *stagedTxnLineage
	mergeCatalog   map[string]mergeCatalog
	seenDML        map[string]bool
}

// dmlStatementBlocklist 拒绝会隐式提交或改变事务边界的语句，避免破坏 GTID 与数据原子提交。
var dmlStatementBlocklist = []string{
	"CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME",
	"GRANT", "REVOKE", "LOCK TABLES", "UNLOCK TABLES",
	"START TRANSACTION", "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT",
	"ANALYZE TABLE", "OPTIMIZE TABLE", "REPAIR TABLE", "CHECK TABLE",
	"SET GLOBAL", "XA ", "INSTALL ", "UNINSTALL ", "FLUSH ",
}

func assertLooksLikeDML(query string) error {
	upper := strings.ToUpper(strings.TrimSpace(query))
	for _, kw := range dmlStatementBlocklist {
		if strings.HasPrefix(upper, kw) {
			return fmt.Errorf("mysqlout: NODE-OUT-13 违规兜底拦截——execDML 收到疑似会隐式"+
				"提交的语句（命中关键字 %q），gtid_next 事务内不允许执行；这条语句本不应该走到"+
				"这里，说明调用方绕开了本包的语句构造函数，请检查调用来源", kw)
		}
	}
	return nil
}

// execDML 执行一条参数化 DML 语句，语句文本只应来自本包内生成 SQL 的代码
// （internal/node/pipeline/mysqlout/insert.go 等），不接受调用方拼接的任意字符串。
func (t *dmlTxn) execDML(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if err := assertLooksLikeDML(query); err != nil {
		return nil, err
	}
	if len(args) == 0 {
		return t.tx.ExecContext(ctx, query)
	}
	if statement := t.statements[query]; statement != nil {
		return statement.ExecContext(ctx, args...)
	}
	// One-off shapes keep the driver's direct execution path. Prepare only
	// after the same original GTID successfully executed that shape once.
	// Both bounded maps belong exclusively to this transaction.
	if !t.seenDML[query] {
		result, err := t.tx.ExecContext(ctx, query, args...)
		if err == nil && len(t.seenDML) < 32 && len(t.statements) < 32 {
			if t.seenDML == nil {
				t.seenDML = make(map[string]bool)
			}
			t.seenDML[query] = true
		}
		return result, err
	}
	// Only this GTID transaction owns these statements. sql.Tx closes them at
	// commit/rollback; none survives to another transaction or replay worker.
	// Bound distinct SQL shapes even when one source transaction touches many
	// tables. Additional shapes keep the driver's original execution path.
	if len(t.statements) >= 32 {
		return t.tx.ExecContext(ctx, query, args...)
	}
	statement, err := t.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	if t.statements == nil {
		t.statements = make(map[string]*sql.Stmt)
	}
	t.statements[query] = statement
	delete(t.seenDML, query)
	return statement.ExecContext(ctx, args...)
}

// withGTIDTxn 是"以固定 GTID 落地一个事务"这件事本身的唯一入口
// （NODE-OUT-02/13， 写会话初始化模板）：
//
//	SET gtid_next='<gtid>' → BEGIN → fn（只能执行 DML）→ COMMIT/ROLLBACK →
//	SET gtid_next='AUTOMATIC'
//
// 无论 fn 成功、失败，还是 BeginTx/Commit 本身失败，都必须尝试把 gtid_next
// 恢复为 AUTOMATIC：否则这条连接后续的任何语句都会继续绑定在同一个 GTID 上，
// 而同一个 GTID 在同一个会话里不能被提交两次，MySQL 会拒绝，表现为连接卡死。
// 恢复动作用与调用方 ctx 解耦的独立超时执行（理由同 position 包
// withReadOnlyConn 的 defer：上游 ctx 已取消不代表这次收尾也该被取消/被误判成
// "数据库拒绝"）。
func withGTIDTxn(ctx context.Context, conn *sql.Conn, gtid string, fn func(ctx context.Context, tx *dmlTxn) error) error {
	return withGTIDTxnCommit(ctx, conn, gtid, fn, nil)
}

func withGTIDTxnCommit(ctx context.Context, conn *sql.Conn, gtid string, fn func(context.Context, *dmlTxn) error, beforeCommit func(context.Context) error) error {
	if !isSingleGTID(gtid) {
		return fmt.Errorf("mysqlout: gtid 格式非法，拒绝设置 gtid_next: %q", gtid)
	}

	restoreAutomatic := func() error {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), connectTimeout)
		defer cancel()
		_, err := conn.ExecContext(rctx, "SET gtid_next = 'AUTOMATIC'")
		return err
	}

	// SET acquires this session's original GTID ownership before BEGIN. The
	// fixed SELECT observes it without committing or writing another GTID.
	// Keeping both in one request avoids an extra round trip per transaction.
	owned, err := setGTIDAndReadOwnership(ctx, conn, gtid)
	cleanupSetup := func(cause error) error {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), connectTimeout)
		defer cancel()
		_, rollbackErr := conn.ExecContext(rctx, "ROLLBACK")
		restoreErr := restoreAutomatic()
		if rollbackErr != nil || restoreErr != nil {
			return errors.Join(cause, sessionRestoreError{errors.Join(rollbackErr, restoreErr)})
		}
		return cause
	}
	if err != nil {
		return cleanupSetup(fmt.Errorf("mysqlout: set GTID and read session ownership failed: %w", err))
	}
	if owned != "" && !strings.EqualFold(owned, gtid) {
		return cleanupSetup(errors.New("mysqlout: session owns a different transaction GTID"))
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return cleanupSetup(fmt.Errorf("mysqlout: 开启事务失败(gtid=%s): %w", gtid, err))
	}

	// An already executed GTID never owns this session, so its callback must
	// not run and misreport MySQL's skipped writes as fresh conflicts.
	var applyErr error
	if owned != "" {
		applyErr = fn(ctx, &dmlTxn{tx: tx, conn: conn})
	}
	if applyErr == nil && beforeCommit != nil {
		applyErr = beforeCommit(ctx)
	}
	if err := applyErr; err != nil {
		rbErr := tx.Rollback()
		if rbErr != nil {
			slog.Error("rollback transaction failed", slog.String("gtid", gtid), slog.Any("rollback_error", rbErr))
		}
		// Nontransactional tables or trigger side effects can leave changes
		// behind even when ROLLBACK returns nil. Its warnings must be checked
		// before another statement replaces the session diagnostics.
		var sqlErr *mysql.MySQLError
		confirmed := false
		if rbErr == nil && errors.As(err, &sqlErr) && !IsRetryable(err) {
			checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), connectTimeout)
			var warnings uint64
			checkErr := conn.QueryRowContext(checkCtx, "SHOW COUNT(*) WARNINGS").Scan(&warnings)
			cancel()
			confirmed = checkErr == nil && warnings == 0
		}
		restoreErr := restoreAutomatic()
		if restoreErr != nil {
			slog.Error("restore gtid_next=automatic failed after rollback",
				slog.String("gtid", gtid), slog.Any("restore_error", restoreErr))
			err = errors.Join(err, sessionRestoreError{restoreErr})
		}
		if rbErr != nil {
			err = errors.Join(err, rbErr)
		}
		if confirmed && restoreErr == nil {
			return rolledBackSQLFailure{err}
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		if restoreErr := restoreAutomatic(); restoreErr != nil {
			slog.Error("restore gtid_next=automatic failed after commit failed",
				slog.String("gtid", gtid), slog.Any("commit_error", err), slog.Any("restore_error", restoreErr))
			err = errors.Join(err, sessionRestoreError{restoreErr})
		}
		return fmt.Errorf("mysqlout: 提交事务失败(gtid=%s): %w", gtid, err)
	}

	if err := restoreAutomatic(); err != nil {
		return fmt.Errorf("mysqlout: 提交成功但恢复 gtid_next=AUTOMATIC 失败(gtid=%s)，"+
			"该连接后续写入会被这个残留的 gtid_next 卡住，调用方应当丢弃该连接: %w", gtid, sessionRestoreError{err})
	}

	return nil
}

// Only validated UUID:sequence values enter this fixed SQL constructor.
// No caller text, business statement, or driver handle is retained.
func setGTIDAndReadOwnership(ctx context.Context, conn *sql.Conn, gtid string) (string, error) {
	if !isSingleGTID(gtid) {
		return "", errors.New("mysqlout: invalid GTID setup identity")
	}
	rows, err := conn.QueryContext(ctx, "SET gtid_next = '"+gtid+"'; SELECT @@SESSION.gtid_owned")
	if err != nil {
		return "", err
	}
	defer func(cleanup func() error) { _ = cleanup() }(rows.Close)
	if !rows.Next() {
		return "", errors.Join(errors.New("mysqlout: GTID ownership result missing"), rows.Err())
	}
	var owned string
	if err := rows.Scan(&owned); err != nil {
		return "", err
	}
	if rows.Next() || rows.NextResultSet() {
		return "", errors.New("mysqlout: unexpected extra GTID ownership result")
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return owned, rows.Close()
}
