package snapshot

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"uuid"
)

// ReplicaReadPolicy is copied during construction, before the reader starts.
// Every borrowed connection checks its real instance and replication channel;
// a DNS/pool replacement cannot make L, H and chunks refer to different servers.
type ReplicaReadPolicy struct {
	SourceUUID    string
	ReplicaUUID   string
	CaseSensitive bool
}

func (r *SQLBoundaryReader) SetReplicaPolicy(policy ReplicaReadPolicy) error {
	for _, value := range []string{policy.SourceUUID, policy.ReplicaUUID} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil() || id.String() != value {
			return fmt.Errorf("snapshot: replica policy requires probed canonical instance UUIDs")
		}
	}
	if policy.SourceUUID == policy.ReplicaUUID {
		return fmt.Errorf("snapshot: replica and primary must be different instances")
	}
	r.replica = &policy
	return nil
}

func (p ReplicaReadPolicy) verify(ctx context.Context, conn *sql.Conn) error {
	var instance string
	var readOnly, lowerCase int
	if err := conn.QueryRowContext(ctx, "SELECT @@server_uuid, @@global.read_only, @@lower_case_table_names").Scan(&instance, &readOnly, &lowerCase); err != nil {
		return fmt.Errorf("snapshot: verify read instance failed: %w", err)
	}
	if instance != p.ReplicaUUID || readOnly != 1 || lowerCase < 0 || lowerCase > 2 || (lowerCase == 0) != p.CaseSensitive {
		return fmt.Errorf("snapshot: read instance is not the configured read-only replica")
	}
	rows, err := conn.QueryContext(ctx, "SHOW REPLICA STATUS")
	if err != nil {
		return fmt.Errorf("snapshot: inspect replica channel failed: %w", err)
	}
	defer func(cleanup func() error) { _ = cleanup() }(rows.Close)
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	channels := 0
	for rows.Next() {
		channels++
		if channels > 1 {
			return fmt.Errorf("snapshot: replica must have exactly one configured upstream channel")
		}
		values := make([]sql.RawBytes, len(columns))
		args := make([]any, len(columns))
		for i := range values {
			args[i] = &values[i]
		}
		if err := rows.Scan(args...); err != nil {
			return err
		}
		status := make(map[string]string, len(columns))
		for i, column := range columns {
			status[column] = string(values[i])
		}
		if err := p.verifyChannel(status); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if channels != 1 {
		return fmt.Errorf("snapshot: replica has no configured upstream channel")
	}
	return nil
}

func (p ReplicaReadPolicy) verifyChannel(status map[string]string) error {
	if status["Source_UUID"] != p.SourceUUID ||
		!strings.EqualFold(status["Replica_IO_Running"], "Yes") ||
		!strings.EqualFold(status["Replica_SQL_Running"], "Yes") ||
		status["Auto_Position"] != "1" {
		return fmt.Errorf("snapshot: replica channel must actively replicate GTIDs from the configured primary")
	}
	return nil
}
