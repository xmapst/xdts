// Package dblineage owns the reserved database metadata contract. Provisioning
// belongs to an administrator; replication sessions only read and append.
package dblineage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
)

const Schema = iproto.ReservedLineageSchema
const Table = "transaction_lineage_v1"
const Marker = "xdts database lineage v1"
const QualifiedTable = "`xdtsmeta`.`transaction_lineage_v1`"
const CreateTable = "CREATE TABLE IF NOT EXISTS " + QualifiedTable + " (gtid VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY, payload LONGBLOB NOT NULL) ENGINE=InnoDB COMMENT='" + Marker + "'"
const Insert = "INSERT INTO " + QualifiedTable + " (gtid,payload) VALUES (?,?)"
const Timeout = 5 * time.Second

func Reserved(schema string) bool { return strings.EqualFold(schema, Schema) }

type IQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// Store borrows the owner's query handle and keeps no mutable business cache.
type Store struct{ query IQueryer }

func New(query IQueryer) Store { return Store{query: query} }

// Validate refuses to adopt an unrelated table sharing the reserved name.
func (store Store) Validate(ctx context.Context) error {
	query := store.query
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	var engine, marker string
	err := query.QueryRowContext(ctx, "SELECT ENGINE,TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", Schema, Table).Scan(&engine, &marker)
	if err != nil || engine != "InnoDB" || marker != Marker {
		return errors.New("dblineage: reserved lineage table missing or incompatible; run administrator provisioning")
	}
	rows, err := query.QueryContext(ctx, "SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,COLUMN_KEY,COLLATION_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION", Schema, Table)
	if err != nil {
		return errors.New("dblineage: cannot inspect lineage columns")
	}
	defer func(cleanup func() error) { _ = cleanup() }(rows.Close)
	index := 0
	for rows.Next() {
		var name, kind, nullable, key string
		var collation sql.NullString
		if err := rows.Scan(&name, &kind, &nullable, &key, &collation); err != nil {
			return err
		}
		valid := index == 0 && name == "gtid" && kind == "varchar(64)" && nullable == "NO" && key == "PRI" && collation.String == "ascii_bin" ||
			index == 1 && name == "payload" && kind == "longblob" && nullable == "NO" && key == "" && !collation.Valid
		if !valid {
			return errors.New("dblineage: incompatible lineage columns")
		}
		index++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if index != 2 {
		return errors.New("dblineage: incomplete lineage columns")
	}
	return nil
}

// Load is bounded even when the pipeline session itself has no deadline. The
// selected expression bounds untrusted blob allocation at the driver boundary.
func (store Store) Load(ctx context.Context, gtid string) (iproto.StoredDatabaseLineage, bool, error) {
	query := store.query
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	var body []byte
	var size int
	err := query.QueryRowContext(ctx, "SELECT LEFT(payload,?),OCTET_LENGTH(payload) FROM "+QualifiedTable+" WHERE gtid=?", iproto.MaxStoredLineageBytes, gtid).Scan(&body, &size)
	if errors.Is(err, sql.ErrNoRows) {
		return iproto.StoredDatabaseLineage{}, false, nil
	}
	if err != nil {
		return iproto.StoredDatabaseLineage{}, false, fmt.Errorf("dblineage: read record failed: %w", err)
	}
	if size > iproto.MaxStoredLineageBytes {
		return iproto.StoredDatabaseLineage{}, false, errors.New("dblineage: record exceeds bound")
	}
	record, err := iproto.DecodeStoredDatabaseLineage(body, gtid)
	return record, err == nil, err
}
