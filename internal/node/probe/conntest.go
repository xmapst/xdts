package probe

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/mysqlx"
)

// 连接探测失败通过 Success=false 回执报告；失败结果不包含连接凭据。
func (m *Module) probeConnTest(ctx context.Context, req *iproto.ProbeRequest) *iproto.ProbeResult {
	fail := func(code, message string) *iproto.ProbeResult {
		return &iproto.ProbeResult{
			TaskId:  req.TaskId,
			Type:    req.Type,
			Success: false,
			Error:   &iproto.Error{Code: code, Message: message},
		}
	}

	ep := req.GetEndpoint()
	if ep == nil || strings.TrimSpace(ep.GetHost()) == "" {
		return fail("CONN_TEST_ENDPOINT_MISSING", "请求未提供 endpoint 连接信息")
	}
	if ep.GetPort() == 0 || ep.GetPort() > 65535 {
		return fail("CONN_TEST_PORT_INVALID", fmt.Sprintf("端口号 %d 超出合法范围(1-65535)", ep.GetPort()))
	}
	host := ep.GetHost()
	hostPort := fmt.Sprintf("%s:%d", host, ep.GetPort())

	// DSN 故意不接 TLS/params：与 internal/node/pipeline/mysqlout.Writer 现状一致
	// （该包 internal/node/pipeline/mysqlout/writer.go New() 同样只拼 user:password@tcp(host:port)/，不消费
	// EndpointConn.Tls/Params），是全仓共享的已知边界，不是本函数遗漏——真正打通
	// TLS/params 需要三个消费方（mysqlout/binlogin/probe）一起接，不属于本任务
	// 范围，见交付说明。
	dsn := probeMySQLDSN(ep)

	connCtx, cancel := context.WithTimeout(ctx, m.opts.MySQLConnectTimeout)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		// sql.Open 对 "mysql" driver 只做 DSN 语法解析，不发起网络连接；错误文案见
		// 本函数文档"不转发驱动层原始 err.Error()"一节，此处不含密码，但仍按同一条
		// 纪律不转发。
		slog.Error("conn test dsn parse failed", slog.String("host", hostPort), slog.Any("error", err))
		return fail("CONN_TEST_DSN_INVALID", fmt.Sprintf("连接参数不合法，host=%s", hostPort))
	}
	defer func(cleanup func() error) { _ = cleanup() }(db.Close)

	conn, err := db.Conn(connCtx)
	if err != nil {
		return fail(classifyConnErr(err, hostPort))
	}
	defer func(cleanup func() error) { _ = cleanup() }(conn.Close)

	if err := conn.PingContext(connCtx); err != nil {
		return fail(classifyConnErr(err, hostPort))
	}

	var rawVersion, serverUUID, rawLCTN string
	row := conn.QueryRowContext(connCtx, "SELECT VERSION(), @@server_uuid, @@lower_case_table_names")
	if err := row.Scan(&rawVersion, &serverUUID, &rawLCTN); err != nil {
		slog.Error("conn test query failed", slog.String("host", hostPort), slog.Any("error", err))
		return fail("CONN_TEST_QUERY_FAILED",
			fmt.Sprintf("查询版本/server_uuid/lower_case_table_names 失败，host=%s", hostPort))
	}

	// 参数值使用 mysqlx 的统一解析器校验，避免探测和运行路径判定不同。
	if _, err := mysqlx.ParseVersion(rawVersion); err != nil {
		return fail("CONN_TEST_VERSION_UNSUPPORTED", fmt.Sprintf("host=%s: %v", hostPort, err))
	}
	if _, err := mysqlx.ParseLowerCaseTableNames(rawLCTN); err != nil {
		return fail("CONN_TEST_LOWER_CASE_TABLE_NAMES_INVALID", fmt.Sprintf("host=%s: %v", hostPort, err))
	}

	keyParams := map[string]string{"lower_case_table_names": rawLCTN}
	rows, err := conn.QueryContext(connCtx, `SHOW GLOBAL VARIABLES WHERE Variable_name IN (
		'gtid_mode', 'enforce_gtid_consistency', 'log_bin', 'binlog_format',
		'binlog_row_image', 'binlog_row_metadata', 'binlog_expire_logs_seconds', 'expire_logs_days',
		'binlog_row_value_options', 'binlog_transaction_compression', 'group_replication_group_name')`)
	if err != nil {
		return fail("CONN_TEST_PARAMS_FAILED", "查询实例参数失败")
	}
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			_ = rows.Close()
			return fail("CONN_TEST_PARAMS_FAILED", "读取实例参数失败")
		}
		keyParams[strings.ToLower(name)] = value
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fail("CONN_TEST_PARAMS_FAILED", "读取实例参数失败")
	}
	_ = rows.Close()

	grantRows, err := conn.QueryContext(connCtx, "SHOW GRANTS")
	if err != nil {
		return fail("CONN_TEST_GRANTS_FAILED", "查询账号权限失败")
	}
	var grants []string
	for grantRows.Next() {
		var grant string
		if err := grantRows.Scan(&grant); err != nil {
			_ = grantRows.Close()
			return fail("CONN_TEST_GRANTS_FAILED", "读取账号权限失败")
		}
		grants = append(grants, grant)
	}
	if err := grantRows.Err(); err != nil {
		_ = grantRows.Close()
		return fail("CONN_TEST_GRANTS_FAILED", "读取账号权限失败")
	}
	_ = grantRows.Close()

	return &iproto.ProbeResult{
		TaskId:  req.TaskId,
		Type:    req.Type,
		Success: true,
		Result: &iproto.ProbeResult_ConnTest{
			ConnTest: &iproto.ConnTestResult{
				Ok:         true,
				Version:    rawVersion,
				ServerUuid: serverUUID,
				KeyParams:  keyParams,
				Grants:     grants,
			},
		},
	}
}

// classifyConnErr 区分"服务端响应但拒绝"（鉴权/权限，*mysql.MySQLError）与
// "网络层面完全没连上"（拨号失败/超时），给运维两种不同的、可分别行动的信号——密码
// 错了该改密码，网络不通该查防火墙/host/port，混在一个"CONN_TEST_UNREACHABLE"里会
// 让人两条路都要试一遍。完整 err 只进 slog（见本文件顶部"不转发驱动层原始
// err.Error()"一节），返回值不含它。
func classifyConnErr(err error, hostPort string) (code, message string) {
	if _, ok := errors.AsType[*mysql.MySQLError](err); ok {
		slog.Error("conn test rejected by server",
			slog.String("host", hostPort), slog.Any("error", err))
		return "CONN_TEST_AUTH_FAILED", fmt.Sprintf("目标数据库拒绝连接（鉴权/权限），host=%s", hostPort)
	}
	slog.Error("conn test unreachable", slog.String("host", hostPort), slog.Any("error", err))
	return "CONN_TEST_UNREACHABLE", fmt.Sprintf("无法连接，host=%s", hostPort)
}
