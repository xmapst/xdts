package mysqlx

import "errors"

var (
	// ErrUnsupportedRawType 表示 DecodeColumnValue 收到了一个既不匹配 meta.MySQLType
	// 特殊分支、也不属于已知 Go 类型分流表的原始值。最常见的触发场景是
	// BinlogSyncerConfig.UseDecimal 被误设为 true（会收到 decimal.Decimal 类型，
	// 而不是契约要求的 string，见 internal/mysqlx/columnvalue.go 与 internal/mysqlx/syncer.go）——错误信息里的 %T 会
	// 直接打印出实际类型名，不需要专门为 decimal.Decimal 建一个分支来给出可读的诊断。
	ErrUnsupportedRawType = errors.New("mysqlx: 不支持的原始值类型")

	// ErrEnumSetMetadataMissing 表示 ENUM/SET 索引无法对应到成员字符串。
	// 调用方必须从 TableMap 或有序结构快照提供完整成员元数据。
	ErrEnumSetMetadataMissing = errors.New("mysqlx: 缺少 ENUM/SET 列的字符串取值元数据")

	// ErrUnexpectedNonZeroDatetimeString 表示一个 DATETIME/TIMESTAMP 列以 string 形式
	// 到达、但内容不是零值日期——按 NewBinlogSyncerConfig 固定 ParseTime=true 的前提，
	// 这种情况只应该发生在零值日期退化的场景，非零值必须是 time.Time；出现这个错误
	// 通常说明调用方绕开了 NewBinlogSyncerConfig、自己拼了一个 ParseTime=false 的配置。
	ErrUnexpectedNonZeroDatetimeString = errors.New("mysqlx: 收到非零值的 DATETIME/TIMESTAMP 字符串，预期应为 time.Time")
)
