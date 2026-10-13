package probe

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdts/internal/dblineage"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/mysqlx"
	"github.com/xmapst/xdts/internal/schemamig"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// precheckRun owns one connection and one result, entirely inside the probe's
// worker. No mutable run data escapes before execution has finished.
type precheckRun struct {
	conn   *sql.Conn
	args   *iproto.PrecheckProbeArgs
	side   iproto.PipelineRole
	result *iproto.PrecheckResult
	grants []string
	caps   mysqlx.Caps
}

func validPrecheckName(s string) bool {
	return s != "" && utf8.ValidString(s) && utf8.RuneCountInString(s) <= 64 && !strings.ContainsRune(s, 0)
}

func validatePrecheckRequest(req *iproto.ProbeRequest) error {
	ep, args := req.GetEndpoint(), req.GetPrecheck()
	if ep == nil || strings.TrimSpace(ep.GetHost()) == "" || ep.GetPort() == 0 || ep.GetPort() > 65535 || args == nil || len(args.GetTables()) == 0 || len(args.GetTables()) > maxCatalogTables {
		return errors.New("invalid precheck input")
	}
	if req.GetSide() != iproto.PipelineRole_PIPELINE_ROLE_SOURCE && req.GetSide() != iproto.PipelineRole_PIPELINE_ROLE_TARGET {
		return errors.New("invalid precheck side")
	}
	if replica := args.GetSnapshotReplica(); replica != nil {
		if req.GetSide() != iproto.PipelineRole_PIPELINE_ROLE_SOURCE || strings.TrimSpace(replica.GetHost()) == "" || replica.GetPort() == 0 || replica.GetPort() > 65535 || replica.GetTls().GetEnabled() || len(replica.GetParams()) != 0 {
			return errors.New("invalid replica precheck input")
		}
	}
	seen := map[string]bool{}
	for _, t := range args.GetTables() {
		if t == nil || !validPrecheckName(t.GetSchema()) || !validPrecheckName(t.GetTable()) || (t.GetLwwTimestampColumn() != "" && !validPrecheckName(t.GetLwwTimestampColumn())) {
			return errors.New("invalid table scope")
		}
		switch strings.ToLower(t.GetSchema()) {
		case "mysql", "sys", "information_schema", "performance_schema", dblineage.Schema:
			return errors.New("system schema excluded")
		}
		key := t.GetSchema() + "\x00" + t.GetTable()
		if seen[key] {
			return errors.New("duplicate table")
		}
		seen[key] = true
	}
	if args.GetAllowMissingTargetTables() && (!args.GetSchemaMigration() || args.GetDropRecreate()) {
		return errors.New("missing tables require a migration plan")
	}
	if args.GetDropRecreate() && !args.GetSchemaMigration() {
		return errors.New("drop recreate requires schema migration")
	}
	if (args.GetSnapshotRowsPerSecond() != 0 || args.GetSnapshotBytesPerSecond() != 0) && (!args.GetSnapshot() || req.GetSide() != iproto.PipelineRole_PIPELINE_ROLE_SOURCE) {
		return errors.New("snapshot rates outside source snapshot")
	}
	if req.GetSide() == iproto.PipelineRole_PIPELINE_ROLE_SOURCE {
		if args.GetBinlogServerId() == 0 || args.GetSnapshotUuid() != "" || args.GetSnapshotResumeChunkLimitV1() != 0 || args.GetAllowMissingTargetTables() || args.GetCreatesFunctions() {
			return errors.New("invalid source parameters")
		}
		if args.GetStartKind() != iproto.InitPositionKind_INIT_POSITION_KIND_LATEST && args.GetStartKind() != iproto.InitPositionKind_INIT_POSITION_KIND_GTID_SET {
			return errors.New("position must be resolved before precheck")
		}
		if args.GetStartKind() == iproto.InitPositionKind_INIT_POSITION_KIND_LATEST && args.GetStartGtidSet() != "" {
			return errors.New("latest cannot contain a specified set")
		}
		if len(args.GetStartGtidSet()) > 1<<20 {
			return errors.New("GTID set exceeds limit")
		}
		if _, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, args.GetStartGtidSet()); err != nil {
			return errors.New("invalid GTID set")
		}
	} else {
		if args.GetSnapshotResumeChunkLimitV1() > 1<<63-2 || args.GetSnapshotResumeChunkLimitV1() != 0 && !args.GetSnapshot() {
			return errors.New("invalid snapshot resume chunk range")
		}
		if args.GetBinlogServerId() != 0 || args.GetStartKind() != iproto.InitPositionKind_INIT_POSITION_KIND_UNSPECIFIED || args.GetStartGtidSet() != "" || args.GetKeylessIncrementalWarning() {
			return errors.New("source parameters on target")
		}
		if args.GetSnapshot() {
			id, err := uuidx.Parse(args.GetSnapshotUuid())
			if err != nil || id.IsZero() || id.String() != args.GetSnapshotUuid() {
				return errors.New("invalid snapshot UUID")
			}
		} else if args.GetSnapshotUuid() != "" {
			return errors.New("snapshot UUID outside snapshot mode")
		}
		if args.GetCreatesFunctions() && !args.GetSchemaMigration() {
			return errors.New("function creation outside schema migration")
		}
	}
	return nil
}

func (m *Module) probePrecheck(ctx context.Context, req *iproto.ProbeRequest) *iproto.ProbeResult {
	fail := func(code, message string) *iproto.ProbeResult {
		return &iproto.ProbeResult{TaskId: req.GetTaskId(), Type: req.GetType(), Error: &iproto.Error{Code: code, Message: message}}
	}
	if err := validatePrecheckRequest(req); err != nil {
		return fail("PRECHECK_INPUT_INVALID", "预检查需要合法角色、精确业务表范围和已解析的运行参数")
	}
	queryCtx, cancel := context.WithTimeout(ctx, m.opts.MySQLConnectTimeout)
	defer cancel()
	db, err := sql.Open("mysql", probeMySQLDSN(req.GetEndpoint()))
	if err != nil {
		return fail("PRECHECK_CONNECTION_FAILED", "无法建立预检查数据库连接")
	}
	defer func(cleanup func() error) { _ = cleanup() }(db.Close)
	conn, err := db.Conn(queryCtx)
	if err != nil {
		return fail("PRECHECK_CONNECTION_FAILED", "无法建立预检查数据库连接，请检查网络与账号")
	}
	defer func(cleanup func() error) { _ = cleanup() }(conn.Close)
	run := &precheckRun{conn: conn, args: req.GetPrecheck(), side: req.GetSide(), result: &iproto.PrecheckResult{KeyParams: map[string]string{}}}
	if err := run.readEnvironment(queryCtx); err != nil {
		return fail("PRECHECK_ENVIRONMENT_FAILED", "无法完整读取实例参数或 SHOW GRANTS，预检查未通过")
	}
	if req.GetPrecheck().GetSnapshotReplica() != nil {
		m.checkSnapshotReplica(queryCtx, req, run)
	}
	version, err := mysqlx.ParseVersion(run.result.GetVersion())
	supported := err == nil && mysqlx.SupportedVersion(version)
	run.add("PRE-01", supported, false, "MySQL 版本必须属于受支持的 8.x/9.x", map[string]any{"version": run.result.GetVersion()})
	if supported {
		run.caps = mysqlx.DetectCaps(version)
		if _, err := mysqlx.ParseLowerCaseTableNames(run.result.GetKeyParams()["lower_case_table_names"]); err != nil {
			return fail("PRECHECK_ENVIRONMENT_FAILED", "无法确认实例名称大小写规则")
		}
		if run.side == iproto.PipelineRole_PIPELINE_ROLE_SOURCE && run.args.GetSnapshot() {
			sampleConn, err := db.Conn(queryCtx)
			if err != nil {
				run.result.Capacity = &iproto.SnapshotCapacityEstimate{UnavailableReason: "sample_connection_unavailable"}
			} else {
				sample := &precheckRun{conn: sampleConn, args: run.args}
				run.result.Capacity = sample.sampleCapacity(queryCtx)
				_ = sampleConn.Close()
			}
		}
		run.checkParameters()
		run.checkPrivileges()
		if run.args.GetPersistentLineageV1() {
			err := dblineage.New(conn).Validate(queryCtx)
			read := schemamig.GrantsPrivilege(run.grants, dblineage.Schema, dblineage.Table, "SELECT")
			appendAllowed := run.side == iproto.PipelineRole_PIPELINE_ROLE_SOURCE || schemamig.GrantsPrivilege(run.grants, dblineage.Schema, dblineage.Table, "INSERT")
			run.add("PRE-35", err == nil && read && appendAllowed, false, "端点血缘存储需由管理员预建，源端需要 SELECT，目标端需要 SELECT/INSERT", map[string]any{"store_compatible": err == nil, "read_allowed": read, "append_allowed": appendAllowed})
		}
		if err := run.checkInstance(queryCtx); err != nil {
			return fail("PRECHECK_QUERY_FAILED", "无法完整检查复制客户端或 GTID 历史，预检查未通过")
		}
		if err := run.checkTables(queryCtx); err != nil {
			return fail("PRECHECK_TABLES_FAILED", "无法完整检查同步表及触发器，请检查权限或探测期间的结构变更")
		}
	}
	run.result.NodeTimeMs = m.now().UnixMilli()
	return &iproto.ProbeResult{TaskId: req.GetTaskId(), Type: req.GetType(), Success: true, Result: &iproto.ProbeResult_Precheck{Precheck: run.result}}
}

func (r *precheckRun) add(code string, passed, warning bool, message string, detail any) {
	severity := iproto.PrecheckSeverity_PRECHECK_SEVERITY_BLOCKING
	if warning {
		severity = iproto.PrecheckSeverity_PRECHECK_SEVERITY_WARNING
	}
	data, _ := json.Marshal(detail)
	r.result.Items = append(r.result.Items, &iproto.PrecheckItem{Code: code, Passed: passed, Severity: severity, Message: message, DetailJson: data})
}

func (r *precheckRun) readEnvironment(ctx context.Context) error {
	if err := r.conn.QueryRowContext(ctx, "SELECT VERSION(), @@server_uuid").Scan(&r.result.Version, &r.result.ServerUuid); err != nil {
		return err
	}
	rows, err := r.conn.QueryContext(ctx, `SHOW GLOBAL VARIABLES WHERE Variable_name IN (
		'gtid_mode','enforce_gtid_consistency','log_bin','binlog_format','binlog_row_image',
		'binlog_row_metadata','binlog_expire_logs_seconds','expire_logs_days','read_only','super_read_only',
		'server_id','lower_case_table_names','character_set_server','collation_server',
		'time_zone','system_time_zone','sql_mode',
		'log_bin_trust_function_creators','group_replication_group_name','partial_revokes')`)
	if err != nil {
		return err
	}
	metadataBytes := 0
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			_ = rows.Close()
			return err
		}
		metadataBytes += len(name) + len(value)
		if metadataBytes > maxDefsBytes {
			_ = rows.Close()
			return errors.New("precheck metadata exceeds limit")
		}
		r.result.KeyParams[strings.ToLower(name)] = value
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return err
	}
	// SHOW VARIABLES can truncate long GTID histories. Read the complete values
	// directly; never authorize a start from a shortened display value.
	var executed, purged string
	if err := r.conn.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed,@@GLOBAL.gtid_purged").Scan(&executed, &purged); err != nil {
		return err
	}
	if metadataBytes+len(executed)+len(purged) > maxDefsBytes {
		return errors.New("precheck metadata exceeds limit")
	}
	r.result.KeyParams["gtid_executed"], r.result.KeyParams["gtid_purged"] = executed, purged
	env, err := definitionEnvironment(ctx, r.conn)
	if err != nil {
		return err
	}
	r.grants = env.GetGrants()
	return nil
}

func (r *precheckRun) checkParameters() {
	p := r.result.GetKeyParams()
	r.add("PRE-02", strings.EqualFold(p["gtid_mode"], "ON") && strings.EqualFold(p["enforce_gtid_consistency"], "ON"), false,
		"gtid_mode 与 enforce_gtid_consistency 必须为 ON", map[string]string{"gtid_mode": p["gtid_mode"], "enforce_gtid_consistency": p["enforce_gtid_consistency"]})
	r.add("PRE-03", strings.EqualFold(p["log_bin"], "ON") && strings.EqualFold(p["binlog_format"], "ROW") && strings.EqualFold(p["binlog_row_image"], "FULL"), false,
		"log_bin 必须为 ON、binlog_format 为 ROW、binlog_row_image 为 FULL", map[string]string{"log_bin": p["log_bin"], "binlog_format": p["binlog_format"], "binlog_row_image": p["binlog_row_image"]})
	if r.side == iproto.PipelineRole_PIPELINE_ROLE_TARGET {
		r.add("PRE-08", strings.EqualFold(p["read_only"], "OFF") && strings.EqualFold(p["super_read_only"], "OFF"), false,
			"目标库 read_only 和 super_read_only 必须为 OFF", map[string]string{"read_only": p["read_only"], "super_read_only": p["super_read_only"]})
		if r.args.GetCreatesFunctions() {
			r.add("PRE-15", strings.EqualFold(p["log_bin_trust_function_creators"], "ON") || schemamig.GrantsPrivilege(r.grants, "", "", "SUPER"), false,
				"创建函数必须满足开启 binlog 时的函数创建条件", map[string]string{"log_bin_trust_function_creators": p["log_bin_trust_function_creators"]})
		}
	} else if r.args.GetSnapshot() {
		retention, retentionKnown := mysqlx.BinlogRetentionSeconds(p)
		estimate := r.args.GetEstimatedSnapshotSeconds()
		if capacity := r.result.GetCapacity(); capacity != nil {
			if capacity.GetVerified() {
				estimate = max(estimate, capacity.GetEstimatedSeconds())
			} else {
				estimate = 0
			}
		}
		r.add("PRE-21", retentionKnown && (retention == 0 || (estimate != 0 && retention >= estimate)), true,
			"binlog 保留时长应覆盖预计全量耗时；未知耗时需要确认", map[string]any{"retention_seconds": retention, "estimated_snapshot_seconds": estimate, "automatic_expiration_disabled": retentionKnown && retention == 0, "capacity_sample": r.result.GetCapacity(), "duration_multiplier": 4, "snapshot_bytes_multiplier": 2})
	}
}

func (r *precheckRun) checkPrivileges() {
	missing := []map[string]string{}
	require := func(schema, table, privilege string) {
		if !schemamig.GrantsPrivilege(r.grants, schema, table, privilege) {
			missing = append(missing, map[string]string{"schema": schema, "table": table, "privilege": privilege})
		}
	}
	if r.side == iproto.PipelineRole_PIPELINE_ROLE_SOURCE {
		require("", "", "REPLICATION SLAVE")
		require("", "", "REPLICATION CLIENT")
	} else {
		allowed := false
		for _, privilege := range mysqlx.GTIDNextPrivileges(r.caps) {
			allowed = allowed || schemamig.GrantsPrivilege(r.grants, "", "", privilege)
		}
		if !allowed {
			missing = append(missing, map[string]string{"privilege": "gtid_next: " + strings.Join(mysqlx.GTIDNextPrivileges(r.caps), " / ")})
		}
	}
	for _, t := range r.args.GetTables() {
		require(t.GetSchema(), t.GetTable(), "SELECT")
		if r.side == iproto.PipelineRole_PIPELINE_ROLE_TARGET {
			for _, privilege := range []string{"INSERT", "UPDATE", "DELETE"} {
				require(t.GetSchema(), t.GetTable(), privilege)
			}
			if r.args.GetSchemaMigration() || r.args.GetSyncDdl() {
				for _, privilege := range []string{"CREATE", "ALTER", "INDEX", "REFERENCES", "CREATE VIEW", "CREATE ROUTINE", "ALTER ROUTINE", "TRIGGER", "EVENT"} {
					require(t.GetSchema(), t.GetTable(), privilege)
				}
			}
			if r.args.GetDropRecreate() {
				require(t.GetSchema(), t.GetTable(), "DROP")
			}
		} else if r.args.GetSchemaMigration() {
			for _, privilege := range []string{"SHOW VIEW", "TRIGGER", "EVENT"} {
				require(t.GetSchema(), t.GetTable(), privilege)
			}
		}
	}
	if r.side == iproto.PipelineRole_PIPELINE_ROLE_SOURCE && r.args.GetSchemaMigration() {
		allowed := schemamig.GrantsPrivilege(r.grants, "", "", "SELECT") || schemamig.GrantsPrivilege(r.grants, "mysql", "proc", "SELECT") || (r.caps.ShowRoutinePriv && schemamig.GrantsPrivilege(r.grants, "", "", "SHOW_ROUTINE"))
		if !allowed {
			missing = append(missing, map[string]string{"privilege": "读取例程定义"})
		}
	}
	r.add("PRE-04", len(missing) == 0, false, "账号必须具备同步范围的读取、写入和对应运行权限", map[string]any{"missing": missing})
}

func (r *precheckRun) checkInstance(ctx context.Context) error {
	p := r.result.GetKeyParams()
	executed, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, p["gtid_executed"])
	if err != nil {
		return err
	}
	if _, ok := p["gtid_executed"]; !ok {
		return errors.New("executed GTID unavailable")
	}
	if r.side == iproto.PipelineRole_PIPELINE_ROLE_TARGET {
		if r.args.GetSnapshot() {
			empty := true
			limit := r.args.GetSnapshotResumeChunkLimitV1()
			var permitted gmysql.GTIDSet
			if limit != 0 {
				permitted, err = gmysql.ParseGTIDSet(gmysql.MySQLFlavor, fmt.Sprintf("%s:1-%d", r.args.GetSnapshotUuid(), limit))
				if err != nil {
					return err
				}
			}
			for set := range strings.SplitSeq(executed.String(), ",") {
				if strings.EqualFold(strings.SplitN(set, ":", 2)[0], r.args.GetSnapshotUuid()) {
					actual, parseErr := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, set)
					empty = permitted != nil && parseErr == nil && permitted.Contain(actual)
				}
			}
			message := "目标执行历史不得包含本次全量合成 UUID"
			if limit != 0 {
				message = "目标已执行的全量分块必须属于本次恢复运行的原计划范围"
			}
			r.add("PRE-25", empty, false, message, map[string]any{"snapshot_uuid": r.args.GetSnapshotUuid(), "resume_chunk_limit": limit})
		}
		return nil
	}
	if _, ok := p["gtid_purged"]; !ok {
		return errors.New("purged GTID unavailable")
	}
	purged, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, p["gtid_purged"])
	if err != nil {
		return err
	}
	start := executed
	if r.args.GetStartKind() == iproto.InitPositionKind_INIT_POSITION_KIND_GTID_SET {
		start, err = gmysql.ParseGTIDSet(gmysql.MySQLFlavor, r.args.GetStartGtidSet())
		if err != nil {
			return err
		}
	}
	r.add("PRE-06", start.Contain(purged), false, "起始 GTID 集合必须包含源库已经清理的历史", map[string]string{"gtid_purged": p["gtid_purged"], "start_gtid_set": start.String()})
	available, err := r.serverIDAvailable(ctx)
	if err != nil {
		return err
	}
	r.add("PRE-05", available, false, "dump server_id 不得与源实例及已注册复制客户端冲突", map[string]any{"binlog_server_id": r.args.GetBinlogServerId()})
	return nil
}

func (r *precheckRun) serverIDAvailable(ctx context.Context) (bool, error) {
	ownID, err := strconv.ParseUint(r.result.GetKeyParams()["server_id"], 10, 32)
	if err != nil {
		return false, err
	}
	available := ownID != uint64(r.args.GetBinlogServerId())
	rows, err := r.conn.QueryContext(ctx, mysqlx.ShowReplicasSQL(r.caps))
	if err != nil {
		return false, err
	}
	defer func(cleanup func() error) { _ = cleanup() }(rows.Close)
	cols, err := rows.Columns()
	if err != nil {
		return false, err
	}
	idColumn := -1
	for i, col := range cols {
		if strings.EqualFold(col, "Server_id") {
			idColumn = i
		}
	}
	if idColumn < 0 {
		return false, errors.New("replica IDs unavailable")
	}
	clients := 0
	for rows.Next() {
		clients++
		if clients > maxDefsObjects {
			return false, errors.New("replica client count exceeds limit")
		}
		values := make([]sql.NullString, len(cols))
		dest := make([]any, len(cols))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return false, err
		}
		id, err := strconv.ParseUint(values[idColumn].String, 10, 32)
		if err != nil {
			return false, err
		}
		available = available && id != uint64(r.args.GetBinlogServerId())
	}
	return available, rows.Err()
}
