package mysqlx

import "fmt"

// LowerCaseTableNames represents the MySQL variable's values 0, 1 and 2.
// Zero is case sensitive; 1 and 2 use case-insensitive table comparison.
// The unspecified protocol value is distinct from all three server values.
type LowerCaseTableNames int

const (
	LowerCaseTableNamesUnspecified LowerCaseTableNames = iota
	// LowerCaseTableNamesSensitive 对应 MySQL 值 0：表名在磁盘上原样存储，比较大小写
	// 敏感——Linux 默认值，也是三个取值里唯一需要切换成大小写敏感比较的一个。
	LowerCaseTableNamesSensitive
	// LowerCaseTableNamesLowercaseStored 对应 MySQL 值 1：表名统一转小写存储，比较
	// 大小写不敏感。
	LowerCaseTableNamesLowercaseStored
	// LowerCaseTableNamesLowercaseCompare 对应 MySQL 值 2：表名按原样存储，但比较时
	// 统一按小写归一，效果上大小写不敏感。
	LowerCaseTableNamesLowercaseCompare
)

// String 返回该取值的可读名字，不是 MySQL 原始数值、也不是协议里右移过的整数。
// 供日志与持久化使用——落库该值时应该存这个名字而不是位移后的整数：运维直接对着
// `SHOW VARIABLES LIKE 'lower_case_table_names'` 的原始数值核对时，如果看到的是位移
// 后的整数（例如 LowercaseStored 存成 2，而服务端原始值是 1），会误判"记错了"，
// 实际只是编码不同。落库形态的最终决定权在持久化那一侧，这个方法只是让"存名字"这条
// 路径不需要调用方自己再写一遍映射表。
func (v LowerCaseTableNames) String() string {
	switch v {
	case LowerCaseTableNamesSensitive:
		return "SENSITIVE"
	case LowerCaseTableNamesLowercaseStored:
		return "LOWERCASE_STORED"
	case LowerCaseTableNamesLowercaseCompare:
		return "LOWERCASE_COMPARE"
	default:
		return "UNSPECIFIED"
	}
}

// ParseLowerCaseTableNames 解析 `SELECT @@lower_case_table_names` 的查询结果（MySQL
// 只返回 "0"/"1"/"2" 三种取值之一）。与 ParseVersion 同样的边界：只做字符串→类型值
// 转换，不执行查询、不持有数据库连接。
//
// 空字符串或任何非 "0"/"1"/"2" 的输入一律按错误处理，不折回 Unspecified：Unspecified
// 是"根本没有调用本函数"这件事本身的哨兵，不是"调用了但解析失败"的哨兵——真的探测
// 失败要不要记成"未探测"，是调用方的决定，本函数不替调用方悄悄做这个选择，避免
// "探测失败"和"从未探测"这两种不同的调用方场景被合并成同一种返回值、事后无法区分。
func ParseLowerCaseTableNames(raw string) (LowerCaseTableNames, error) {
	switch raw {
	case "0":
		return LowerCaseTableNamesSensitive, nil
	case "1":
		return LowerCaseTableNamesLowercaseStored, nil
	case "2":
		return LowerCaseTableNamesLowercaseCompare, nil
	default:
		return LowerCaseTableNamesUnspecified, fmt.Errorf("mysqlx: 无法解析 lower_case_table_names 取值 %q（MySQL 只应返回 0/1/2）", raw)
	}
}
