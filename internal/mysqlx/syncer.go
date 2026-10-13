package mysqlx

import (
	"github.com/go-mysql-org/go-mysql/replication"
)

// NewBinlogSyncerConfig 统一 binlog 解码选项：ParseTime=true、UseDecimal=false，
// 确保时间值和 DECIMAL 值满足 DecodeColumnValue 的类型契约。
func NewBinlogSyncerConfig(serverID uint32, host string, port uint16, user, password string) replication.BinlogSyncerConfig {
	return replication.BinlogSyncerConfig{
		ServerID:   serverID,
		Flavor:     "mysql",
		Host:       host,
		Port:       port,
		User:       user,
		Password:   password,
		UseDecimal: false,
		ParseTime:  true,
	}
}
