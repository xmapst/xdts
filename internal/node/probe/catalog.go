package probe

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/xmapst/xdts/internal/iproto"
)

const (
	maxCatalogTables  = 1000
	maxCatalogColumns = 20000
)

var errCatalogTooLarge = errors.New("catalog exceeds limit")

func probeMySQLDSN(ep *iproto.EndpointConn) string {
	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net = ep.GetUsername(), ep.GetPassword(), "tcp"
	cfg.Addr = net.JoinHostPort(ep.GetHost(), strconv.FormatUint(uint64(ep.GetPort()), 10))
	return cfg.FormatDSN()
}

func (m *Module) probeCatalog(ctx context.Context, req *iproto.ProbeRequest) *iproto.ProbeResult {
	fail := func(code, message string) *iproto.ProbeResult {
		return &iproto.ProbeResult{TaskId: req.TaskId, Type: req.Type, Error: &iproto.Error{Code: code, Message: message}}
	}
	ep := req.GetEndpoint()
	if ep == nil || strings.TrimSpace(ep.GetHost()) == "" || ep.GetPort() == 0 || ep.GetPort() > 65535 {
		return fail("CATALOG_ENDPOINT_INVALID", "请求未提供有效的端点连接信息")
	}
	connCtx, cancel := context.WithTimeout(ctx, m.opts.MySQLConnectTimeout)
	defer cancel()
	db, err := sql.Open("mysql", probeMySQLDSN(ep))
	if err != nil {
		return fail("CATALOG_DSN_INVALID", "端点连接参数不合法")
	}
	defer func(cleanup func() error) { _ = cleanup() }(db.Close)
	conn, err := db.Conn(connCtx)
	if err != nil {
		return fail("CATALOG_CONNECT_FAILED", "无法连接端点数据库")
	}
	defer func(cleanup func() error) { _ = cleanup() }(conn.Close)

	tables, err := readCatalogTables(connCtx, conn)
	if err != nil {
		if errors.Is(err, errCatalogTooLarge) {
			return fail("CATALOG_TOO_LARGE", "可见库表或索引列超过目录浏览上限，请缩小端点账号可见范围")
		}
		return fail("CATALOG_QUERY_FAILED", err.Error())
	}
	return &iproto.ProbeResult{
		TaskId: req.TaskId, Type: req.Type, Success: true,
		Result: &iproto.ProbeResult_Catalog{Catalog: &iproto.CatalogResult{Tables: tables}},
	}
}

func readCatalogTables(ctx context.Context, conn *sql.Conn) ([]*iproto.CatalogTable, error) {
	rows, err := conn.QueryContext(ctx, `SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_ROWS
		FROM information_schema.TABLES
		WHERE TABLE_TYPE = 'BASE TABLE'
		AND TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys', 'xdtsmeta')
		ORDER BY TABLE_SCHEMA, TABLE_NAME LIMIT 1001`)
	if err != nil {
		return nil, fmt.Errorf("查询库表清单失败")
	}
	defer func(cleanup func() error) { _ = cleanup() }(rows.Close)
	tables := make([]*iproto.CatalogTable, 0)
	byName := make(map[string]*iproto.CatalogTable)
	for rows.Next() {
		var schema, table string
		var count sql.NullInt64
		if err := rows.Scan(&schema, &table, &count); err != nil {
			return nil, fmt.Errorf("读取库表清单失败")
		}
		if len(tables) == maxCatalogTables {
			return nil, fmt.Errorf("%w: tables > %d", errCatalogTooLarge, maxCatalogTables)
		}
		entry := &iproto.CatalogTable{Schema: schema, Table: table,
			EstimatedRows: count.Int64, EstimatedRowsKnown: count.Valid}
		tables = append(tables, entry)
		byName[schema+"\x00"+table] = entry
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("读取库表清单失败")
	}
	_ = rows.Close()

	indexRows, err := conn.QueryContext(ctx, `SELECT TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, COLUMN_NAME
		FROM information_schema.STATISTICS
		WHERE NON_UNIQUE = 0
		AND TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys', 'xdtsmeta')
		ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX LIMIT 20001`)
	if err != nil {
		return nil, fmt.Errorf("查询唯一索引失败")
	}
	defer func(cleanup func() error) { _ = cleanup() }(indexRows.Close)
	unique := make(map[string]map[string]*iproto.UniqueKeyColumns)
	columnCount := 0
	for indexRows.Next() {
		columnCount++
		if columnCount > maxCatalogColumns {
			return nil, fmt.Errorf("%w: index columns > %d", errCatalogTooLarge, maxCatalogColumns)
		}
		var schema, table, indexName string
		var column sql.NullString
		if err := indexRows.Scan(&schema, &table, &indexName, &column); err != nil {
			return nil, fmt.Errorf("读取唯一索引失败")
		}
		key := schema + "\x00" + table
		entry := byName[key]
		if entry == nil {
			continue
		}
		if indexName == "PRIMARY" {
			if column.Valid {
				entry.PrimaryKeyColumns = append(entry.PrimaryKeyColumns, column.String)
			}
			continue
		}
		if unique[key] == nil {
			unique[key] = make(map[string]*iproto.UniqueKeyColumns)
		}
		if unique[key][indexName] == nil {
			unique[key][indexName] = &iproto.UniqueKeyColumns{}
		}
		if column.Valid {
			unique[key][indexName].Columns = append(unique[key][indexName].Columns, column.String)
		}
	}
	if err := indexRows.Err(); err != nil {
		return nil, fmt.Errorf("读取唯一索引失败")
	}
	for key, indexes := range unique {
		names := make([]string, 0, len(indexes))
		for name := range indexes {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			byName[key].UniqueKeys = append(byName[key].UniqueKeys, indexes[name])
		}
	}
	return tables, nil
}
