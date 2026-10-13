package binlogin

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	driver "github.com/go-sql-driver/mysql"
	"github.com/xmapst/xdts/internal/dblineage"
	"github.com/xmapst/xdts/internal/node/position"
	"github.com/xmapst/xdts/internal/node/schema"
)

type sessionSchemaCatalog struct {
	module  *Module
	catalog *schema.Catalog
	db      *sql.DB
	ctx     context.Context
}

func (m *Module) newSessionSchemaCatalog(ctx context.Context) (*sessionSchemaCatalog, error) {
	cfg := driver.NewConfig()
	cfg.User, cfg.Passwd = m.opts.User, m.opts.Password
	cfg.Net, cfg.Addr = "tcp", net.JoinHostPort(m.opts.Host, strconv.Itoa(int(m.opts.Port)))
	cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout = 10*time.Second, 10*time.Second, 10*time.Second
	cfg.TLS = m.opts.TLSConfig
	connector, err := driver.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid source catalog connection", errSchemaHistory)
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	catalog, err := schema.NewCatalog(db, m.opts.SourceCaseSensitive, schemaHistoryTimeout)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("%w: create source catalog", errSchemaHistory)
	}
	return &sessionSchemaCatalog{module: m, catalog: catalog, db: db, ctx: ctx}, nil
}

// Capture currently missing selected tables before the first event when the
// current source GTID is contained in the configured start. If the database is
// ahead, the first event must prove a ROW-only gap or use earlier history.
func (c *sessionSchemaCatalog) prepareBaseline(start string) error {
	ctx, cancel := context.WithTimeout(c.ctx, schemaHistoryTimeout)
	defer cancel()
	var names [][2]string
	err := position.WithReadOnlyConn(ctx, c.db, func(ctx context.Context, conn *sql.Conn) error {
		rows, err := conn.QueryContext(ctx, `SELECT TABLE_SCHEMA,TABLE_NAME FROM information_schema.TABLES
			WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA NOT IN ('mysql','information_schema','performance_schema','sys') ORDER BY TABLE_SCHEMA,TABLE_NAME`)
		if err != nil {
			return fmt.Errorf("%w: read source table catalog", errSchemaHistory)
		}
		defer func(cleanup func() error) { _ = cleanup() }(rows.Close)
		for rows.Next() {
			var name [2]string
			if err := rows.Scan(&name[0], &name[1]); err != nil {
				return fmt.Errorf("%w: decode source table catalog", errSchemaHistory)
			}
			if !dblineage.Reserved(name[0]) && c.module.opts.TableFilter.MatchTable(name[0], name[1]) {
				names = append(names, name)
			}
		}
		return rows.Err()
	})
	if err != nil {
		return err
	}
	for _, name := range names {
		if table, ok := c.module.schemaTracker.Lookup(name[0], name[1]); ok && len(table.Columns) > 0 {
			continue
		}
		table, through, err := c.catalog.LoadTable(ctx, name[0], name[1])
		if err != nil {
			if schema.CatalogChanged(err) {
				// Continuous ROW writes need not abort a safe cold start. The
				// first table event performs the bounded binlog history proof.
				continue
			}
			return fmt.Errorf("%w: read baseline table: %w", errSchemaHistory, err)
		}
		if schemaValidThroughStart(start, through) {
			if err := c.module.schemaTracker.Install(table); err != nil {
				return fmt.Errorf("%w: install baseline: %w", errSchemaHistory, err)
			}
		}
	}
	return nil
}

func (c *sessionSchemaCatalog) ensureTable(schemaName, tableName, position string) error {
	return c.ensureTableMetadata(schemaName, tableName, position, false)
}

func (c *sessionSchemaCatalog) ensureTableMetadata(schemaName, tableName, position string, completeRequired bool) error {
	if table, ok := c.module.schemaTracker.Lookup(schemaName, tableName); ok && len(table.Columns) > 0 && (!completeRequired || table.MetadataComplete) {
		return nil
	}
	table, through, err := c.catalog.LoadTable(c.ctx, schemaName, tableName)
	if err != nil && !schema.CatalogChanged(err) {
		return fmt.Errorf("%w: read missing table: %w", errSchemaHistory, err)
	}
	if err != nil || !schemaValidThroughStart(position, through) {
		table, err = c.catalog.LoadTableForReplay(c.ctx, schemaName, tableName, position)
		if err != nil {
			return fmt.Errorf("%w: current source schema is ahead of replay position; restore earlier schema history or reinitialize: %w", errSchemaHistory, err)
		}
	}
	if err := c.module.schemaTracker.Install(table); err != nil {
		return fmt.Errorf("%w: install missing table: %w", errSchemaHistory, err)
	}
	ctx, cancel := context.WithTimeout(c.ctx, schemaHistoryTimeout)
	defer cancel()
	return c.module.commitSchemaHistory(ctx, position)
}

func (a *aggregator) schemaReplayPosition() (string, error) {
	set, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, a.schemaReadPosition())
	if err != nil {
		return "", fmt.Errorf("%w: invalid session schema position", errSchemaHistory)
	}
	if strings.TrimSpace(a.gtid) != "" {
		if err := set.Update(a.gtid); err != nil {
			return "", fmt.Errorf("%w: invalid table event GTID", errSchemaHistory)
		}
	}
	return set.String(), nil
}
