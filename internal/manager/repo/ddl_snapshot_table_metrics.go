package repo

func snapshotTableMetricsMigration() migration {
	return migration{Version: 63, Description: "add present-aware snapshot table metrics", Statements: []string{`
ALTER TABLE pipeline_snapshot_tables
 ADD COLUMN total_chunks BIGINT UNSIGNED NULL,
 ADD COLUMN done_chunks BIGINT UNSIGNED NULL,
 ADD COLUMN rows_per_sec DOUBLE NULL,
 ADD COLUMN observed_at_ms BIGINT NOT NULL DEFAULT 0`}}
}
