// Package safeerror maps internal failures to diagnostic text that may leave the node.
package safeerror

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"
)

const hiddenDetail = "操作失败，详细原因请查看节点日志"

// Public exposes only fixed messages and numeric MySQL error codes. Arbitrary
// error text, including wrapped errors, may contain credentials or tokens.
func Public(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "操作超时"
	}
	if errors.Is(err, context.Canceled) {
		return "操作已取消"
	}
	if mysqlErr, ok := errors.AsType[*mysql.MySQLError](err); ok {
		return fmt.Sprintf("MySQL 错误码 %d", mysqlErr.Number)
	}
	return hiddenDetail
}

// PublicText handles error descriptions already combined into free text.
// There is no reliable way to identify an unstructured bare secret in them.
func PublicText(text string) string {
	if text == "" {
		return ""
	}
	return hiddenDetail
}
