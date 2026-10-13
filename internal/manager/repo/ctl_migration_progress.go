package repo

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

// Engine state is separate from published schema_versions and business models.
const schemaMigrationStatementsDDL = `CREATE TABLE IF NOT EXISTS schema_migration_statements (
  version BIGINT NOT NULL,
  statement_no INT NOT NULL,
  statement_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  state VARCHAR(8) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  PRIMARY KEY (version, statement_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

// A session named lock survives MySQL DDL's implicit commits. The SQL connection
// and lock belong to this workflow until every statement and record completes.
func (m *Migrator) withMigrationLock(ctx context.Context, work func(*Migrator) error) (result error) {
	pool, err := m.db.DB()
	if err != nil {
		return err
	}
	conn, err := pool.Conn(ctx)
	if err != nil {
		return err
	}
	defer func(cleanup func() error) { _ = cleanup() }(conn.Close)
	var schema string
	if err := conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&schema); err != nil {
		return fmt.Errorf("repo: migration requires selected database: %w", err)
	}
	if schema == "" {
		return fmt.Errorf("repo: migration requires selected database")
	}
	name := migrationLockName(schema)
	var acquired sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 30)", name).Scan(&acquired); err != nil {
		// Acquisition may have completed before a cancelled response was read.
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		return fmt.Errorf("repo: acquire migration database lock failed: %w", err)
	}
	if !acquired.Valid || acquired.Int64 != 1 {
		return fmt.Errorf("repo: migration database lock unavailable")
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var released sql.NullInt64
		err := conn.QueryRowContext(releaseCtx, "SELECT RELEASE_LOCK(?)", name).Scan(&released)
		if err != nil || !released.Valid || released.Int64 != 1 {
			// Never return a still-locked session to the reusable pool.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			if err != nil {
				result = errors.Join(result, fmt.Errorf("repo: release migration database lock failed: %w", err))
			} else {
				result = errors.Join(result, fmt.Errorf("repo: migration database lock ownership lost"))
			}
		}
	}()
	db := m.db.Session(&gorm.Session{NewDB: true}).WithContext(ctx)
	db.Statement.ConnPool = conn
	return work(&Migrator{db: db})
}

type migrationStatementProgress struct {
	StatementNo     int
	StatementSHA256 string `gorm:"column:statement_sha256"`
	State           string
}

func (m *Migrator) applyWithProgress(ctx context.Context, mig migration, now time.Time) error {
	var completed int64
	if err := m.db.WithContext(ctx).Raw("SELECT COUNT(*) FROM schema_versions WHERE version=?", mig.Version).Scan(&completed).Error; err != nil {
		return err
	}
	if completed != 0 {
		return nil
	}
	var records []migrationStatementProgress
	if err := m.db.WithContext(ctx).Raw("SELECT statement_no, statement_sha256, state FROM schema_migration_statements WHERE version=? ORDER BY statement_no", mig.Version).Scan(&records).Error; err != nil {
		return err
	}
	progress := make(map[int]string, len(records))
	for i, record := range records {
		if record.StatementNo != i+1 || record.StatementNo > len(mig.Statements) || record.StatementSHA256 != migrationStatementHash(mig.Statements[record.StatementNo-1]) {
			return fmt.Errorf("repo: migration v%d statement %d differs from durable progress; restore matching migration SQL", mig.Version, record.StatementNo)
		}
		if record.State != "done" {
			return fmt.Errorf("repo: migration v%d statement %d outcome uncertain; inspect actual schema and schema_migration_statements before recovery", mig.Version, record.StatementNo)
		}
		progress[record.StatementNo] = record.State
	}
	for i, stmt := range mig.Statements {
		no := i + 1
		if progress[no] == "done" {
			continue
		}
		if err := m.db.WithContext(ctx).Exec("INSERT INTO schema_migration_statements (version,statement_no,statement_sha256,state,updated_at) VALUES(?,?,?,'started',?)", mig.Version, no, migrationStatementHash(stmt), now).Error; err != nil {
			return fmt.Errorf("repo: migration v%d statement %d record start failed: %w", mig.Version, no, err)
		}
		if err := m.db.WithContext(ctx).Exec(stmt).Error; err != nil {
			var serverErr *mysql.MySQLError
			if errors.As(err, &serverErr) && serverErr.Number < 2000 {
				// A server SQL error identifies a failed atomic DDL. Network errors
				// and cancellation cannot prove whether the server committed it.
				cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				removeErr := m.db.WithContext(cleanup).Exec("DELETE FROM schema_migration_statements WHERE version=? AND statement_no=? AND state='started'", mig.Version, no).Error
				cancel()
				if removeErr == nil {
					return fmt.Errorf("repo: migration v%d statement %d failed; repair dependency and retry: %w", mig.Version, no, err)
				}
			}
			return fmt.Errorf("repo: migration v%d statement %d outcome uncertain; inspect actual schema and statement progress: %w", mig.Version, no, err)
		}
		saved := m.db.WithContext(ctx).Exec("UPDATE schema_migration_statements SET state='done',updated_at=? WHERE version=? AND statement_no=? AND state='started'", now, mig.Version, no)
		if saved.Error != nil {
			return fmt.Errorf("repo: migration v%d statement %d committed but progress save failed; inspect actual schema before recovery: %w", mig.Version, no, saved.Error)
		}
		if saved.RowsAffected != 1 {
			return fmt.Errorf("repo: migration v%d statement %d committed but progress record missing; inspect actual schema before recovery", mig.Version, no)
		}
	}
	// Published migrations contain descriptions longer than the original 255
	// character column. The old INSERT IGNORE stored their truncated prefix.
	description := []rune(mig.Description)
	if len(description) > 255 {
		description = description[:255]
	}
	return m.db.WithContext(ctx).Exec("INSERT INTO schema_versions (version,description,applied_at) VALUES (?,?,?)", mig.Version, string(description), now).Error
}

func migrationStatementHash(stmt string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(stmt)))
}

func migrationLockName(schema string) string {
	return fmt.Sprintf("xdts.migration.%x", sha256.Sum256([]byte(schema)))[:63]
}
