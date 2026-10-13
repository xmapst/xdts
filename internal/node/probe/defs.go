package probe

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/xmapst/xdts/internal/dblineage"
	"github.com/xmapst/xdts/internal/iproto"
)

const maxDefsObjects = 4000
const maxDefsBytes = 4 << 20

var errDefsTooLarge = errors.New("definitions exceed limit")

// Definition scope is explicit: an unrestricted request never enumerates accounts
// or system schemas. SQL identifiers come from metadata and are always quoted.
func (m *Module) probeDefs(ctx context.Context, req *iproto.ProbeRequest) *iproto.ProbeResult {
	fail := func(code, message string) *iproto.ProbeResult {
		return &iproto.ProbeResult{TaskId: req.GetTaskId(), Type: req.GetType(), Error: &iproto.Error{Code: code, Message: message}}
	}
	schemas, err := definitionScope(req.GetDefs().GetSchemas())
	if err != nil {
		return fail("DEFS_SCOPE_INVALID", "请提供有效且不包含系统库的明确业务库范围")
	}
	if err := validDefinerAccounts(req.GetDefs().GetAccounts()); err != nil {
		return fail("DEFS_ACCOUNTS_INVALID", "指定账号列表不合法或超过上限")
	}
	ep := req.GetEndpoint()
	if ep == nil || strings.TrimSpace(ep.GetHost()) == "" || ep.GetPort() == 0 || ep.GetPort() > 65535 {
		return fail("DEFS_ENDPOINT_INVALID", "请求未提供有效的端点连接信息")
	}
	queryCtx, cancel := context.WithTimeout(ctx, m.opts.MySQLConnectTimeout)
	defer cancel()
	db, err := sql.Open("mysql", probeMySQLDSN(ep))
	if err != nil {
		return fail("DEFS_DSN_INVALID", "端点连接参数不合法")
	}
	defer func(cleanup func() error) { _ = cleanup() }(db.Close)
	conn, err := db.Conn(queryCtx)
	if err != nil {
		return fail("DEFS_CONNECT_FAILED", "无法连接端点数据库")
	}
	defer func(cleanup func() error) { _ = cleanup() }(conn.Close)
	entries, err := readDefinitions(queryCtx, conn, schemas, req.GetDefs().GetAllowMissingSchemas())
	if err != nil {
		if errors.Is(err, errDefsTooLarge) {
			return fail("DEFS_TOO_LARGE", "对象数量或定义文本超过结构迁移探测上限，请缩小范围")
		}
		return fail("DEFS_QUERY_FAILED", "无法完整读取对象定义，请检查权限、库名及探测期间的结构变更")
	}
	environment, err := definitionEnvironment(queryCtx, conn)
	if err != nil {
		return fail("DEFS_ENVIRONMENT_FAILED", "无法读取结构迁移账号与环境条件")
	}
	environment.Entries = entries
	environment.Accounts, err = readDefinerAccounts(queryCtx, conn, environment.GetCurrentUser(), req.GetDefs().GetAccounts(), schemas)
	if err != nil {
		return fail("DEFS_ACCOUNTS_FAILED", "无法完成指定账号检查")
	}
	return &iproto.ProbeResult{TaskId: req.GetTaskId(), Type: req.GetType(), Success: true, Result: &iproto.ProbeResult_Defs{Defs: environment}}
}

func definitionEnvironment(ctx context.Context, conn *sql.Conn) (*iproto.DefsResult, error) {
	result := &iproto.DefsResult{KeyParams: map[string]string{}}
	var logBin, trust int
	if err := conn.QueryRowContext(ctx, "SELECT CURRENT_USER(), @@log_bin, @@log_bin_trust_function_creators").Scan(&result.CurrentUser, &logBin, &trust); err != nil {
		return nil, err
	}
	result.KeyParams["log_bin"] = strconv.Itoa(logBin)
	result.KeyParams["log_bin_trust_function_creators"] = strconv.Itoa(trust)
	for key, query := range map[string]string{"version": "SELECT VERSION()", "server_uuid": "SELECT @@server_uuid", "lower_case_table_names": "SELECT @@lower_case_table_names"} {
		var value string
		if err := conn.QueryRowContext(ctx, query).Scan(&value); err != nil {
			return nil, err
		}
		result.KeyParams[key] = value
	}
	for _, capability := range []struct {
		query  string
		values *[]string
	}{{"SELECT CHARACTER_SET_NAME FROM information_schema.CHARACTER_SETS ORDER BY CHARACTER_SET_NAME LIMIT 1001", &result.CharacterSets},
		{"SELECT COLLATION_NAME FROM information_schema.COLLATIONS ORDER BY COLLATION_NAME LIMIT 1001", &result.Collations}} {
		rows, err := conn.QueryContext(ctx, capability.query)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				_ = rows.Close()
				return nil, err
			}
			*capability.values = append(*capability.values, name)
			if len(*capability.values) > 1000 {
				_ = rows.Close()
				return nil, errDefsTooLarge
			}
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return nil, err
		}
	}
	rows, err := conn.QueryContext(ctx, "SHOW GRANTS")
	if err != nil {
		return nil, err
	}
	defer func(cleanup func() error) { _ = cleanup() }(rows.Close)
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return nil, err
		}
		result.Grants = append(result.Grants, grant)
	}
	return result, rows.Err()
}

func definitionScope(input []string) ([]string, error) {
	if len(input) == 0 || len(input) > 100 {
		return nil, errors.New("invalid definition scope")
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(input))
	for _, s := range input {
		if s == "" || !utf8.ValidString(s) || utf8.RuneCountInString(s) > 64 || strings.ContainsRune(s, 0) || strings.TrimSpace(s) != s {
			return nil, errors.New("invalid schema name")
		}
		switch strings.ToLower(s) {
		case "mysql", "sys", "information_schema", "performance_schema", dblineage.Schema:
			return nil, errors.New("system schema excluded")
		}
		if !seen[s] {
			out = append(out, s)
			seen[s] = true
		}
	}
	sort.Strings(out)
	return out, nil
}

func quoteDefinitionName(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

type definitionObject struct {
	schema, name, showKind string
	kind                   iproto.SchemaObjectKind
}

func readDefinitions(ctx context.Context, conn *sql.Conn, schemas []string, allowMissing bool) ([]*iproto.DefsEntry, error) {
	objects := make([]definitionObject, 0)
	var lowerCaseTableNames int
	if err := conn.QueryRowContext(ctx, "SELECT @@lower_case_table_names").Scan(&lowerCaseTableNames); err != nil {
		return nil, err
	}
	for _, schema := range schemas {
		if dblineage.Reserved(schema) {
			return nil, errors.New("reserved lineage schema cannot be migrated")
		}
		var visible string
		predicate := "BINARY SCHEMA_NAME = ?"
		if lowerCaseTableNames != 0 {
			predicate = "LOWER(SCHEMA_NAME) = LOWER(?)"
		}
		if err := conn.QueryRowContext(ctx, "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE "+predicate, schema).Scan(&visible); err != nil {
			if allowMissing && errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return nil, err
		}
		schema = visible
		if len(objects) >= maxDefsObjects {
			return nil, errDefsTooLarge
		}
		objects = append(objects, definitionObject{schema: schema, name: schema, showKind: "DATABASE", kind: iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_DATABASE})
		queries := []string{
			"SELECT TABLE_NAME,TABLE_TYPE FROM information_schema.TABLES WHERE BINARY TABLE_SCHEMA = ? ORDER BY TABLE_NAME LIMIT 4001",
			"SELECT ROUTINE_NAME,ROUTINE_TYPE FROM information_schema.ROUTINES WHERE BINARY ROUTINE_SCHEMA = ? ORDER BY ROUTINE_NAME,ROUTINE_TYPE LIMIT 4001",
			"SELECT TRIGGER_NAME,'TRIGGER' FROM information_schema.TRIGGERS WHERE BINARY TRIGGER_SCHEMA = ? ORDER BY TRIGGER_NAME LIMIT 4001",
			"SELECT EVENT_NAME,'EVENT' FROM information_schema.EVENTS WHERE BINARY EVENT_SCHEMA = ? ORDER BY EVENT_NAME LIMIT 4001",
		}
		for _, query := range queries {
			rows, err := conn.QueryContext(ctx, query, schema)
			if err != nil {
				return nil, err
			}
			for rows.Next() {
				var name, kind string
				if err := rows.Scan(&name, &kind); err != nil {
					_ = rows.Close()
					return nil, err
				}
				var k iproto.SchemaObjectKind
				switch kind {
				case "BASE TABLE":
					kind = "TABLE"
					k = iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_TABLE
				case "VIEW":
					k = iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_VIEW
				case "PROCEDURE":
					k = iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_PROCEDURE
				case "FUNCTION":
					k = iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_FUNCTION
				case "TRIGGER":
					k = iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_TRIGGER
				case "EVENT":
					k = iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_EVENT
				default:
					_ = rows.Close()
					return nil, fmt.Errorf("unsupported object kind")
				}
				if len(objects) >= maxDefsObjects {
					_ = rows.Close()
					return nil, errDefsTooLarge
				}
				objects = append(objects, definitionObject{schema: schema, name: name, showKind: kind, kind: k})
			}
			err = rows.Err()
			_ = rows.Close()
			if err != nil {
				return nil, err
			}
		}
	}
	entries := make([]*iproto.DefsEntry, 0, len(objects))
	bytes := 0
	for _, obj := range objects {
		qualified := quoteDefinitionName(obj.schema) + "." + quoteDefinitionName(obj.name)
		if obj.showKind == "DATABASE" {
			qualified = quoteDefinitionName(obj.schema)
		}
		rows, err := conn.QueryContext(ctx, "SHOW CREATE "+obj.showKind+" "+qualified)
		if err != nil {
			return nil, err
		}
		cols, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		if !rows.Next() {
			err = rows.Err()
			_ = rows.Close()
			if err == nil {
				err = errors.New("definition missing")
			}
			return nil, err
		}
		values := make([]sql.NullString, len(cols))
		dest := make([]any, len(cols))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			_ = rows.Close()
			return nil, err
		}
		ddl := ""
		for i, col := range cols {
			if strings.EqualFold(col, "Create "+obj.showKind) || (obj.showKind == "TRIGGER" && strings.EqualFold(col, "SQL Original Statement")) {
				if values[i].Valid {
					ddl = values[i].String
				}
			}
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return nil, err
		}
		if ddl == "" {
			return nil, errors.New("definition unavailable")
		}
		bytes += len(ddl) + len(obj.schema) + len(obj.name)
		if bytes > maxDefsBytes {
			return nil, errDefsTooLarge
		}
		entry := &iproto.DefsEntry{Schema: obj.schema, ObjectName: obj.name, Kind: obj.kind, Definition: ddl}
		if obj.kind == iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_TABLE {
			if err := conn.QueryRowContext(ctx, "SELECT TABLE_COLLATION FROM information_schema.TABLES WHERE BINARY TABLE_SCHEMA=? AND BINARY TABLE_NAME=? AND TABLE_TYPE='BASE TABLE'", obj.schema, obj.name).Scan(&entry.TableCollationV1); err != nil {
				return nil, err
			}
			bytes += len(entry.TableCollationV1)
			if bytes > maxDefsBytes {
				return nil, errDefsTooLarge
			}
		}
		if obj.kind == iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_VIEW {
			columns, err := conn.QueryContext(ctx, "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE BINARY TABLE_SCHEMA=? AND BINARY TABLE_NAME=? ORDER BY ORDINAL_POSITION", obj.schema, obj.name)
			if err != nil {
				return nil, err
			}
			for columns.Next() {
				var name string
				if err := columns.Scan(&name); err != nil {
					_ = columns.Close()
					return nil, err
				}
				entry.ViewColumns = append(entry.ViewColumns, name)
				bytes += len(name)
			}
			err = columns.Err()
			_ = columns.Close()
			if err != nil || len(entry.ViewColumns) == 0 || bytes > maxDefsBytes {
				return nil, errors.New("view column metadata unavailable or exceeds limit")
			}
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
