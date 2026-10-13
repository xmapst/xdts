package mysqlout

import (
	"errors"

	"github.com/go-sql-driver/mysql"
)

//   - errnoLockDeadlock (1213)：两个并发事务交叉更新 t_deadlock 的两行，
//     InnoDB 死锁检测器选中一个杀掉，报 "Deadlock found when trying to get
//     lock"。
//   - errnoLockWaitTimeout (1205)：一个事务持锁不放，另一个等到
//     innodb_lock_wait_timeout 超时，报 "Lock wait timeout exceeded"。
//   - errnoReadOnly (1290)：SET GLOBAL super_read_only=1 后尝试写入，报
//     "The MySQL server is running with the --super-read-only option..."；
//     SET GLOBAL read_only=1 对 root 这类拥有 SUPER 权限的账号不生效（这条
//     也是试出来的，不是假设），必须是 super_read_only 才能在用 root 连接时
//     复现只读拒绝。两种 --read-only/--super-read-only 场景报的是同一个错误号。
const (
	errnoLockDeadlock    = 1213
	errnoLockWaitTimeout = 1205
	errnoReadOnly        = 1290
)

// IsRetryable 按 NODE-OUT-08 对写入失败分类：死锁 / 锁等待超时 / 连接断开 / 只读
// 切换归为可重试，其余（结构不一致、语法错误等）归为不可重试。
//
// 连接断开的判定是 Go 标准库/go-sql-driver 层面的启发式，不是逐个实测出来的错误
// 号（连接断开有非常多种触发方式——网络分区、对端主动 RST、TCP keepalive 超时，
// 没有一个统一的错误号可验）：database/sql 用 driver.ErrBadConn 标记"这条连接
// 已经不可信，不该继续用"；go-sql-driver 自己额外用 mysql.ErrInvalidConn 表达
// 同一类状况；net.Error（超时、连接被重置等）与 io.EOF（对端提前关闭连接）是
// 网络层错误的标准信号。四者命中任意一个都归为可重试。
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errWriterClosed) {
		return false
	}
	if errors.Is(err, ErrConflictStopped) {
		return false
	}

	// Unsafe or unsupported AST rewrites cannot be repaired by retrying.
	if errors.Is(err, errDDLNotSupported) {
		return false
	}

	// A failed session cleanup can accompany a SQL error. Replace that
	// connection before retrying; the error must not qualify for SQL skip.
	if isConnectionError(err) {
		return true
	}
	if myErr, ok := errors.AsType[*mysql.MySQLError](err); ok {
		switch myErr.Number {
		case errnoLockDeadlock, errnoLockWaitTimeout, errnoReadOnly:
			return true
		default:
			return false
		}
	}

	return false
}
