package mysqlout

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
)

// errUnsupportedColumnValue 标记 ColumnValue 落在了本包不认识的 oneof 分支——
// 协议演进"只增字段"意味着未来可能出现本包编译时还不存在的新分支，遇到时必须
// 拒绝而不是当成 NULL 或某个已知分支静默处理。
var errUnsupportedColumnValue = errors.New("mysqlout: 不支持的 ColumnValue 分支")

const epochOffsetMicros = 62135596800 * 1_000_000

// timeFromTemporalUsec 是 mysqlx.temporalUsecFromTime 的反函数。
func timeFromTemporalUsec(usec int64) time.Time {
	return time.UnixMicro(usec - epochOffsetMicros).UTC()
}

// zeroDateLiteral is the DATETIME/TIMESTAMP zero literal. Target projection
// converts DATE zero values and signed TIME durations using actual column
// types before this SQL argument conversion; TIME cannot carry zero_date.
const zeroDateLiteral = "0000-00-00 00:00:00"

// columnValueToArg 把一个 ColumnValue 换算成可以直接作为 database/sql 参数化
// 查询实参的 Go 值。
func columnValueToArg(cv *iproto.ColumnValue) (any, error) {
	if cv == nil {
		return nil, fmt.Errorf("mysqlout: 列值为 nil（下标越界或上游数据缺失）")
	}
	if err := iproto.ValidateColumnValue(cv); err != nil {
		return nil, fmt.Errorf("mysqlout: 列值未通过结构性校验: %w", err)
	}
	if cv.GetZeroDate() {
		return zeroDateLiteral, nil
	}
	switch v := cv.GetValue().(type) {
	case *iproto.ColumnValue_IsNull:
		return nil, nil
	case *iproto.ColumnValue_I64:
		return v.I64, nil
	case *iproto.ColumnValue_U64:

		return strconv.FormatUint(v.U64, 10), nil
	case *iproto.ColumnValue_F64:
		return v.F64, nil
	case *iproto.ColumnValue_Decimal:
		return v.Decimal, nil
	case *iproto.ColumnValue_RawBytes:
		return v.RawBytes, nil
	case *iproto.ColumnValue_Text:
		return v.Text, nil
	case *iproto.ColumnValue_TemporalUsec:
		return timeFromTemporalUsec(v.TemporalUsec), nil
	default:
		return nil, fmt.Errorf("%w: %T", errUnsupportedColumnValue, v)
	}
}
