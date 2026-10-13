package mysqlx

import (
	"fmt"
	"strings"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/xmapst/xdts/internal/iproto"
)

// epochOffsetMicros 是 0001-01-01T00:00:00Z 距 Unix epoch（1970-01-01T00:00:00Z）的
// 微秒数——"temporal_usec: 自 0001-01-01T00:00:00 UTC 起的微秒数"选用
// 这个纪元。
//
// 不能用 time.Time.Sub 计算这个跨度：跨度约 1969 年，超出 time.Duration 能表示的
// 约 ±292 年上限，会静默饱和成错误值。改用纯整数运算：62135596800 是这两个纪元之间的
// 秒数，与 Go 标准库 time 包内部的 unixToInternal 常量一致，可用历法公式独立验证：
// 儒略儒略历（按公历闰年规则往前延拓）下 0001~1969 年的闰年天数 = 1969/4 - 1969/100 +
// 1969/400 = 492-19+4 = 477；总天数 = 1969*365 + 477 = 719162；719162*86400 = 62135596800。
const epochOffsetMicros = 62135596800 * 1_000_000

// temporalUsecFromTime 返回自 0001-01-01T00:00:00Z 起的微秒数。
func temporalUsecFromTime(t time.Time) int64 {
	return t.UTC().UnixMicro() + epochOffsetMicros
}

// ColumnMeta 提供列值解码所需的名称、类型、符号性和 ENUM/SET 成员元数据。
type ColumnMeta struct {
	// Name 供出错时定位问题列；非必需字段，留空不影响解码正确性。
	Name string
	// MySQLType 是 gmysql.MYSQL_TYPE_* 常量。BIT/ENUM/SET/JSON/DECIMAL/DATE/TIME/
	// DATETIME/TIMESTAMP 这几类必须靠它才能正确分流，
	// 其余类型（整数、浮点、二进制、普通字符串）单靠 Go 反射类型即可正确分流，
	// MySQLType 留零值也没关系。
	MySQLType byte
	// Unsigned 目前不参与判定——常规无符号整数在 Go 类型层面已经能区分（go-mysql 按
	// 宽度给 uint8/uint16/uint32/uint64），保留这个字段供调用方完整表达"我知道
	// 这一列的符号性"，便于以后有其它用途时复用，而不需要改函数签名。
	Unsigned bool
	// EnumValues 是 ENUM 列按定义顺序排列的全部成员；索引 i（1-based）对应
	// EnumValues[i-1]。非 ENUM 列留 nil。
	EnumValues []string
	// SetValues 是 SET 列按定义顺序排列的全部成员；位图第 i 位（0-based）对应
	// SetValues[i]。非 SET 列留 nil。
	SetValues []string
}

// BuildColumnMeta 从 TableMapEvent 提取列类型、列名、ENUM/SET 成员和符号性。
// 列名和成员元数据依赖 binlog_row_metadata=FULL；缺失元数据由调用方结合
// 有序 schema history 补齐。
func BuildColumnMeta(tm *replication.TableMapEvent) ([]ColumnMeta, error) {
	if tm == nil {
		return nil, fmt.Errorf("mysqlx: TableMapEvent 为空")
	}
	names := tm.ColumnNameString()
	unsigned := tm.UnsignedMap()
	enumMap := tm.EnumStrValueMap()
	setMap := tm.SetStrValueMap()

	metas := make([]ColumnMeta, tm.ColumnCount)
	for i := range metas {
		// Table-map type bytes may encode a shared storage representation rather than
		// the logical column type; use the accompanying metadata before conversion.
		resolvedType := tm.ColumnType[i]
		switch {
		case tm.IsEnumColumn(i):
			resolvedType = gmysql.MYSQL_TYPE_ENUM
		case tm.IsSetColumn(i):
			resolvedType = gmysql.MYSQL_TYPE_SET
		}
		m := ColumnMeta{MySQLType: resolvedType}
		if i < len(names) {
			m.Name = names[i]
		}
		if u, ok := unsigned[i]; ok {
			m.Unsigned = u
		}
		if vals, ok := enumMap[i]; ok {
			m.EnumValues = vals
		}
		if vals, ok := setMap[i]; ok {
			m.SetValues = vals
		}
		metas[i] = m
	}
	return metas, nil
}

// DecodeColumnValue 将 go-mysql 原始列值转换为 iproto.ColumnValue。
// SQL NULL 对应 is_null=true；其它分支按 MySQLType 和原始值类型选择。
func DecodeColumnValue(raw any, meta ColumnMeta) (*iproto.ColumnValue, error) {
	if raw == nil {
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_IsNull{IsNull: true}}, nil
	}

	// 必须先按列的 MySQL 类型分流的几类：Go 反射类型本身有歧义（BIT vs 真负数 BIGINT
	// 都是 int64）、或者需要额外的元数据翻译（ENUM/SET）、或者同一个 Go 类型（string）
	// 对应好几种不同语义（DECIMAL 文本 / DATE / TIME / DATETIME 零值 / 普通文本）。
	switch meta.MySQLType {
	case gmysql.MYSQL_TYPE_BIT:
		return decodeBitColumn(raw, meta)
	case gmysql.MYSQL_TYPE_ENUM:
		return decodeEnumColumn(raw, meta)
	case gmysql.MYSQL_TYPE_SET:
		return decodeSetColumn(raw, meta)
	case gmysql.MYSQL_TYPE_JSON:
		return decodeJSONColumn(raw, meta)
	}

	// 其余类型单靠 Go 反射类型就能正确分流，不需要 MySQLType 参与判断
	// （"与 BIT 不同，常规无符号整数在 Go 类型层面就能区分"，
	// 这条结论同样适用于浮点、二进制、普通字符串）。
	switch v := raw.(type) {
	case int64:
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_I64{I64: v}}, nil
	case int32:
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_I64{I64: int64(v)}}, nil
	case int16:
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_I64{I64: int64(v)}}, nil
	case int8:
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_I64{I64: int64(v)}}, nil
	case int:
		// MYSQL_TYPE_YEAR 解码给 Go int，见 github.com/go-mysql-org/go-mysql/replication/row_event.go 的 decodeValue YEAR 分支。
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_I64{I64: int64(v)}}, nil
	case uint64:
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_U64{U64: v}}, nil
	case uint32:
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_U64{U64: uint64(v)}}, nil
	case uint16:
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_U64{U64: uint64(v)}}, nil
	case uint8:
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_U64{U64: uint64(v)}}, nil
	case float32:
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_F64{F64: float64(v)}}, nil
	case float64:
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_F64{F64: v}}, nil
	case []byte:
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_RawBytes{RawBytes: v}}, nil
	case string:
		return decodeStringColumn(v, meta)
	case time.Time:
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_TemporalUsec{TemporalUsec: temporalUsecFromTime(v)}}, nil
	default:
		return nil, fmt.Errorf("%w: 列 %s 原始类型 %T（%v）", ErrUnsupportedRawType, meta.Name, raw, raw)
	}
}

// decodeBitColumn 按 BIT 列的无符号位模式转换 go-mysql 返回的 int64。
func decodeBitColumn(raw any, meta ColumnMeta) (*iproto.ColumnValue, error) {
	i, ok := rawToInt64(raw)
	if !ok {
		return nil, fmt.Errorf("%w: BIT 列 %s 原始值类型 %T", ErrUnsupportedRawType, meta.Name, raw)
	}
	return &iproto.ColumnValue{Value: &iproto.ColumnValue_U64{U64: uint64(i)}}, nil
}

// decodeEnumColumn 把 ENUM 列的原始整数索引（1-based，0 表示空字符串/非法值）翻译成
// meta.EnumValues 里对应的字符串。
func decodeEnumColumn(raw any, meta ColumnMeta) (*iproto.ColumnValue, error) {
	idx, ok := rawToInt64(raw)
	if !ok {
		return nil, fmt.Errorf("%w: ENUM 列 %s 原始值类型 %T", ErrUnsupportedRawType, meta.Name, raw)
	}
	if idx == 0 {
		// MySQL ENUM 的既定语义：索引 0 表示空字符串（非法赋值在非严格模式下的结果），
		// 不是 NULL，走 is_null 分支会丢失"这一行确实写入过一个空字符串"这个事实。
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_Text{Text: ""}}, nil
	}
	if idx < 1 || int(idx) > len(meta.EnumValues) {
		return nil, fmt.Errorf("%w: ENUM 列 %s 索引 %d 越界（成员数=%d，"+
			"可能是 binlog_row_metadata 不是 FULL、ColumnMeta.EnumValues 未正确填充）",
			ErrEnumSetMetadataMissing, meta.Name, idx, len(meta.EnumValues))
	}
	return &iproto.ColumnValue{Value: &iproto.ColumnValue_Text{Text: meta.EnumValues[idx-1]}}, nil
}

// SET 值按表定义中的成员顺序用半角逗号连接；空集合为空字符串。
func decodeSetColumn(raw any, meta ColumnMeta) (*iproto.ColumnValue, error) {
	bitmap, ok := rawToInt64(raw)
	if !ok {
		return nil, fmt.Errorf("%w: SET 列 %s 原始值类型 %T", ErrUnsupportedRawType, meta.Name, raw)
	}
	if meta.SetValues == nil && bitmap != 0 {
		return nil, fmt.Errorf("%w: SET 列 %s 位图=%d 但缺少 SetValues 元数据", ErrEnumSetMetadataMissing, meta.Name, bitmap)
	}
	var sb strings.Builder
	first := true
	for i, name := range meta.SetValues {
		if bitmap&(1<<uint(i)) == 0 {
			continue
		}
		if !first {
			sb.WriteByte(',')
		}
		sb.WriteString(name)
		first = false
	}
	return &iproto.ColumnValue{Value: &iproto.ColumnValue_Text{Text: sb.String()}}, nil
}

// decodeJSONColumn 处理完整 JSON 镜像：go-mysql 对非空完整 JSON 给 string，对空文档给
// []byte{}（见 github.com/go-mysql-org/go-mysql/replication/row_event.go 的 decodeValue JSON 分支），两种 Go 类型都要落进 text 分支
// （"text: CHAR/VARCHAR/TEXT/JSON/ENUM/SET"）。
func decodeJSONColumn(raw any, meta ColumnMeta) (*iproto.ColumnValue, error) {
	switch v := raw.(type) {
	case string:
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_Text{Text: v}}, nil
	case []byte:
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_Text{Text: string(v)}}, nil
	default:
		return nil, fmt.Errorf("%w: JSON 列 %s 原始值类型 %T", ErrUnsupportedRawType, meta.Name, raw)
	}
}

// decodeStringColumn 处理原始值是 Go string 的情况：这个 Go 类型本身有歧义，
// 必须靠 meta.MySQLType 区分它到底是 DECIMAL 文本、DATE/TIME/DATETIME/TIMESTAMP 的
// 字符串形态，还是普通 CHAR/VARCHAR/TEXT。
func decodeStringColumn(s string, meta ColumnMeta) (*iproto.ColumnValue, error) {
	switch meta.MySQLType {
	case gmysql.MYSQL_TYPE_DECIMAL, gmysql.MYSQL_TYPE_NEWDECIMAL:
		// UseDecimal=false（NewBinlogSyncerConfig 固定）时 DECIMAL 就是这样原样透传成
		// string：精确保留符号与按列 scale 补齐的尾随零，不需要任何转换。
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_Decimal{Decimal: s}}, nil

	case gmysql.MYSQL_TYPE_DATE, gmysql.MYSQL_TYPE_NEWDATE:
		return decodeDateString(s)

	case gmysql.MYSQL_TYPE_TIME, gmysql.MYSQL_TYPE_TIME2:
		return decodeTimeString(s, meta)

	case gmysql.MYSQL_TYPE_DATETIME, gmysql.MYSQL_TYPE_DATETIME2,
		gmysql.MYSQL_TYPE_TIMESTAMP, gmysql.MYSQL_TYPE_TIMESTAMP2:
		// NewBinlogSyncerConfig 固定 ParseTime=true，因此这个分支在生产路径上只应该
		// 遇到"零值日期退化成 string"这一种情况；非零值都会走
		// DecodeColumnValue 的 time.Time 分支。按前缀而不是逐字节匹配，因为不同小数位数
		// （DATETIME(0)~DATETIME(6)）零值字符串长度不同（"0000-00-00 00:00:00"
		// vs "...000000"）。
		if !strings.HasPrefix(s, "0000-00-00") {
			return nil, fmt.Errorf("%w: 列 %s 收到非零值的 DATETIME/TIMESTAMP 字符串 %q"+
				"（ParseTime 应该恒为 true，检查 BinlogSyncerConfig 是否被改动）",
				ErrUnexpectedNonZeroDatetimeString, meta.Name, s)
		}
		return &iproto.ColumnValue{
			Value:    &iproto.ColumnValue_TemporalUsec{TemporalUsec: 0},
			ZeroDate: true,
		}, nil

	default:
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_Text{Text: s}}, nil
	}
}

// decodeDateString 处理 DATE 列的字符串表示，不受 ParseTime 影响。
func decodeDateString(s string) (*iproto.ColumnValue, error) {
	if s == "0000-00-00" {
		return &iproto.ColumnValue{
			Value:    &iproto.ColumnValue_TemporalUsec{TemporalUsec: 0},
			ZeroDate: true,
		}, nil
	}
	t, err := time.ParseInLocation("2006-01-02", s, time.UTC)
	if err != nil {
		return nil, fmt.Errorf("mysqlx: 解析 DATE 字符串 %q 失败: %w", s, err)
	}
	return &iproto.ColumnValue{Value: &iproto.ColumnValue_TemporalUsec{TemporalUsec: temporalUsecFromTime(t)}}, nil
}

// decodeTimeString 处理恒定以 string 形式到达的 TIME 列。MySQL TIME 语义是"时长/偏移"
// （取值范围 -838:59:59 ~ 838:59:59），不是墙钟时刻，因此不走"自 0001-01-01 起"的日期
// 纪元，而是直接把 [-]HH:MM:SS[.ffffff] 转换成带符号的微秒偏移
// （"时间类型"一行："TIME 按当日内偏移"）。零值 "00:00:00" 是合法的
// 真实零时长，不是 DATE/DATETIME 那种"非法日期"的特判，因此 zero_date 恒为 false。
func decodeTimeString(s string, meta ColumnMeta) (*iproto.ColumnValue, error) {
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")

	main := s
	fracStr := ""
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		main = s[:dot]
		fracStr = s[dot+1:]
	}

	var h, m, sec int
	if _, err := fmt.Sscanf(main, "%d:%d:%d", &h, &m, &sec); err != nil {
		return nil, fmt.Errorf("mysqlx: 解析 TIME 列 %s 的字符串 %q 失败: %w", meta.Name, s, err)
	}

	var fracMicros int64
	if fracStr != "" {
		// 实测零值给 "00:00:00"（没有 .000000 后缀），非零值给到微秒（6 位小数）；
		// 这里按实际长度补零/截断到 6 位，兼容任何 DATETIME/TIME 精度（0~6）配置下
		// 可能出现的不同小数位数，而不是假设固定 6 位。
		for len(fracStr) < 6 {
			fracStr += "0"
		}
		if len(fracStr) > 6 {
			fracStr = fracStr[:6]
		}
		var frac int
		if _, err := fmt.Sscanf(fracStr, "%d", &frac); err != nil {
			return nil, fmt.Errorf("mysqlx: 解析 TIME 列 %s 的小数部分 %q 失败: %w", meta.Name, fracStr, err)
		}
		fracMicros = int64(frac)
	}

	usec := int64(h)*3600_000_000 + int64(m)*60_000_000 + int64(sec)*1_000_000 + fracMicros
	if neg {
		usec = -usec
	}
	return &iproto.ColumnValue{Value: &iproto.ColumnValue_TemporalUsec{TemporalUsec: usec}}, nil
}

// rawToInt64 把 go-mysql 给出的几种可能携带"应该按 int64 处理"的原始值统一成 int64，
// 供 BIT/ENUM/SET 三个分支复用（它们的原始值实测都是 int64，这里保留 int32/int/uint64
// 兜底，是防御性的，不是已知会用到的路径）。
func rawToInt64(raw any) (int64, bool) {
	switch v := raw.(type) {
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case int:
		return int64(v), true
	case uint64:
		return int64(v), true
	default:
		return 0, false
	}
}
