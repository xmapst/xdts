package repo

import (
	"errors"

	"github.com/go-sql-driver/mysql"
)

// isDuplicateKeyError 判断错误是否为 MySQL 唯一约束冲突（error 1062）。
// 各仓储方法用它把底层驱动错误翻译成自己的哨兵错误，不把 MySQL 错误码
// 泄露给调用方。
func isDuplicateKeyError(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1062
}
