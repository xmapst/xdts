package repo

import (
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/go-sql-driver/mysql"
)

var (
	ErrResourceNameInvalid  = errors.New("名称不能为空，且不能超过 128 个字符")
	ErrNodeNameConflict     = errors.New("节点名称已存在")
	ErrCenterNameConflict   = errors.New("中心名称已存在")
	ErrEndpointNameConflict = errors.New("端点名称已存在")
)

func normalizeResourceName(name *string) error {
	*name = strings.TrimSpace(*name)
	if *name == "" || !utf8.ValidString(*name) || utf8.RuneCountInString(*name) > 128 {
		return ErrResourceNameInvalid
	}
	return nil
}

func duplicateIndex(err error, index string) bool {
	var failure *mysql.MySQLError
	return errors.As(err, &failure) && failure.Number == 1062 && strings.Contains(failure.Message, index)
}
