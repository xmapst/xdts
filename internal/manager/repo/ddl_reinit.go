package repo

func reinitializationsMigration() migration {
	return migration{Version: 54, Description: "create pipeline_reinitializations", Statements: []string{`
CREATE TABLE IF NOT EXISTS pipeline_reinitializations (
 id BINARY(16) NOT NULL, created_at DATETIME(3) NOT NULL, updated_at DATETIME(3) NOT NULL,
 pipeline_id BINARY(16) NOT NULL, from_run_id BIGINT UNSIGNED NOT NULL,
 config_version BIGINT UNSIGNED NOT NULL, digest CHAR(64) NOT NULL, document JSON NOT NULL,
 confirmed_at DATETIME(3) NULL, confirmed_by BINARY(16) NULL,
 new_run_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
 snapshot_uuid BINARY(16) NULL, schema_uuid BINARY(16) NULL, prepare_uuid BINARY(16) NULL,
 cmd_seq BIGINT UNSIGNED NOT NULL DEFAULT 0,
 source_applied BOOLEAN NOT NULL DEFAULT FALSE, target_applied BOOLEAN NOT NULL DEFAULT FALSE,
 PRIMARY KEY(id), UNIQUE KEY uk_pipeline_reinit_from_run(pipeline_id,from_run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`}}
}

func pipelineReinitPendingMigration() migration {
	return migration{Version: 55, Description: "add pipeline REINIT command state", Statements: []string{
		`ALTER TABLE pipelines ADD COLUMN reinit_pending BOOLEAN NOT NULL DEFAULT FALSE`,
	}}
}
