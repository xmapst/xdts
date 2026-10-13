package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
)

// The SQL event listing never decodes or retains business row values. Each
// page has a fixed bound; the caller's catalog context limits the whole proof.
func verifyRowOnlyHistory(ctx context.Context, conn *sql.Conn, replay, through string) error {
	start, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, replay)
	if err != nil {
		return fmt.Errorf("schema: invalid catalog replay position")
	}
	head, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, through)
	if err != nil || !head.Contain(start) {
		return fmt.Errorf("schema: catalog history positions incomparable")
	}
	if start.Equal(head) {
		return nil
	}
	var difference string
	if conn.QueryRowContext(ctx, "SELECT GTID_SUBTRACT(?,?)", through, replay).Scan(&difference) != nil {
		return fmt.Errorf("schema: invalid catalog history difference")
	}
	needed, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, difference)
	if err != nil || difference == "" {
		return fmt.Errorf("schema: invalid catalog history difference")
	}
	seen, _ := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, "")
	rows, err := conn.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return fmt.Errorf("schema: catalog history logs unavailable")
	}
	columns, err := rows.Columns()
	if err != nil || len(columns) < 2 {
		_ = rows.Close()
		return fmt.Errorf("schema: invalid catalog history log listing")
	}
	var files []string
	for rows.Next() {
		values := make([]sql.RawBytes, len(columns))
		args := make([]any, len(columns))
		for i := range values {
			args[i] = &values[i]
		}
		if rows.Scan(args...) != nil {
			_ = rows.Close()
			return fmt.Errorf("schema: invalid catalog history log row")
		}
		files = append(files, string(values[0]))
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return fmt.Errorf("schema: catalog history log traversal failed")
	}
	for _, file := range files {
		if file == "" || strings.ContainsAny(file, "'\\\x00\r\n") {
			return fmt.Errorf("schema: invalid catalog history log name")
		}
		position := uint64(4)
		eventOffset := uint64(0)
		pending := ""
		active := false
		for {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("schema: catalog history proof timed out")
			}
			query := "SHOW BINLOG EVENTS IN '" + file + "' FROM 4 LIMIT " + strconv.FormatUint(eventOffset, 10) + ",256"
			page, err := conn.QueryContext(ctx, query)
			if err != nil {
				return fmt.Errorf("schema: catalog history events unavailable")
			}
			count := 0
			for page.Next() {
				var name, kind, info string
				var offset, serverID, end uint64
				// MySQL expands compressed transaction payloads into synthetic
				// rows whose start/end positions may be equal. Page by event
				// index, never infer a physical offset from those inner rows.
				if page.Scan(&name, &offset, &kind, &serverID, &end, &info) != nil || name != file || offset < position || end < offset {
					_ = page.Close()
					return fmt.Errorf("schema: invalid catalog history event: type=%s offset=%d end=%d minimum=%d", kind, offset, end, position)
				}
				count++
				position = offset
				switch kind {
				case "Gtid", "Gtid_tagged":
					if active {
						_ = page.Close()
						return fmt.Errorf("schema: catalog history transaction incomplete")
					}
					pending, err = catalogEventGTID(info)
					if err != nil {
						_ = page.Close()
						return err
					}
					set, _ := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, pending)
					active = needed.Contain(set)
				case "Query":
					if active {
						statement := strings.ToUpper(strings.TrimSpace(info))
						if statement == "BEGIN" {
							continue
						}
						if statement == "COMMIT" {
							_ = page.Close()
							return fmt.Errorf("schema: statement transaction prevents catalog history proof")
						}
						_ = page.Close()
						return fmt.Errorf("schema: schema or statement change prevents catalog history proof")
					}
				case "Xid":
					if active {
						if seen.Update(pending) != nil {
							_ = page.Close()
							return fmt.Errorf("schema: invalid catalog committed GTID")
						}
						active = false
						if seen.Contain(needed) {
							_ = page.Close()
							return nil
						}
					}
				case "Table_map", "Write_rows", "Update_rows", "Delete_rows", "Partial_update_rows", "Rows_query":
					// Row-only events cannot change the definition.
				case "Format_desc", "Previous_gtids", "Rotate", "Stop", "Heartbeat":
					if active {
						_ = page.Close()
						return fmt.Errorf("schema: catalog transaction crosses unsupported boundary")
					}
				default:
					if active {
						_ = page.Close()
						return fmt.Errorf("schema: unsupported catalog history event")
					}
				}
			}
			err = page.Err()
			_ = page.Close()
			if err != nil {
				return fmt.Errorf("schema: catalog history event traversal failed")
			}
			if count < 256 {
				break
			}
			eventOffset += uint64(count)
		}
		if active {
			return fmt.Errorf("schema: catalog history transaction incomplete")
		}
	}
	return fmt.Errorf("schema: required catalog history unavailable")
}

func catalogEventGTID(info string) (string, error) {
	_, value, ok := strings.Cut(info, "GTID_NEXT=")
	if !ok {
		return "", fmt.Errorf("schema: catalog event GTID unavailable")
	}
	value = strings.TrimSpace(value)
	if len(value) < 2 || value[0] != '\'' {
		return "", fmt.Errorf("schema: invalid catalog event GTID")
	}
	end := strings.IndexByte(value[1:], '\'')
	if end < 0 {
		return "", fmt.Errorf("schema: invalid catalog event GTID")
	}
	gtid := value[1 : end+1]
	set, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, gtid)
	if err != nil || set.String() != gtid {
		return "", fmt.Errorf("schema: invalid catalog event GTID")
	}
	return gtid, nil
}
