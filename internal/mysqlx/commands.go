package mysqlx

// 本文件是需求 ENV-01「节点连接后探测版本，通过版本适配层选择命令与变量名」的具体落点：
// 按 Caps/Version 选择正确的 SQL 语句、权限名、认证插件，调用方（预检查、位点探测、
// 结构迁移）不应该自己按版本号写 if，全部通过这里的函数取值。
//
// 范围说明：xdts 的 binlog 读取走 go-mysql 的 BinlogSyncer 二进制复制协议（COM_REGISTER_
// SLAVE / COM_BINLOG_DUMP_GTID），不使用 `CHANGE MASTER TO` / `CHANGE REPLICATION SOURCE
// TO` 这类 SQL 语句，因此本文件不需要对这组 MASTER/REPLICA 术语差异建模；ENV-01 提到的
// "MASTER/SLAVE 与 SOURCE/REPLICA 差异"在本项目实际只体现为下面的 ShowBinlogStatusSQL
// 一处（查询当前位点用的语句名）。

// ShowBinlogStatusSQL 选择位点查询：MySQL 8.4+ 使用 SHOW BINARY LOG STATUS，
// 更早的受支持版本使用 SHOW MASTER STATUS。
func ShowBinlogStatusSQL(caps Caps) string {
	if caps.BinlogStatusStmt {
		return "SHOW BINARY LOG STATUS"
	}
	return "SHOW MASTER STATUS"
}

// ShowReplicasSQL uses the renamed statement where the old name was removed.
func ShowReplicasSQL(caps Caps) string {
	if caps.BinlogStatusStmt {
		return "SHOW REPLICAS"
	}
	return "SHOW SLAVE HOSTS"
}

// SupportedVersion is the project's MySQL 8.x/9.x support boundary.
func SupportedVersion(v Version) bool { return v.Major == 8 || v.Major == 9 }

// GTIDNextPrivileges lists alternatives; the preferred privilege is not mandatory.
func GTIDNextPrivileges(caps Caps) []string {
	privileges := []string{"SUPER"}
	if caps.SessionVarAdmin {
		privileges = append(privileges, "SESSION_VARIABLES_ADMIN")
	}
	if caps.ReplicationApplier {
		privileges = append(privileges, "REPLICATION_APPLIER")
	}
	return privileges
}

// 复制应用权限按版本选择：8.0.18+ 使用 REPLICATION_APPLIER，
// 更早的受支持版本使用 SUPER。
func GTIDNextPrivilege(caps Caps) string {
	switch {
	case caps.ReplicationApplier:
		return "REPLICATION_APPLIER"
	case caps.SessionVarAdmin:
		return "SESSION_VARIABLES_ADMIN"
	default:
		return "SUPER"
	}
}

// DefinerPrivilege 返回结构迁移改写 `DEFINER` 子句（保留非当前账号 DEFINER）所需的
// 权限名：8.2+ 用 `SET_ANY_DEFINER`，更早版本用 `SET_USER_ID`。
func DefinerPrivilege(caps Caps) string {
	if caps.SetAnyDefiner {
		return "SET_ANY_DEFINER"
	}
	return "SET_USER_ID"
}

// verAuthPluginCachingSHA2Only 定义强制使用 caching_sha2_password 的起始版本。
var verAuthPluginCachingSHA2Only = Version{9, 0, 0}

// MySQL 9.x 要求 caching_sha2_password；连接需满足 TLS 或 RSA 公钥交换条件。
func RequiredAuthPlugin(ver Version) string {
	if ver.AtLeast(verAuthPluginCachingSHA2Only) {
		return "caching_sha2_password"
	}
	return ""
}
