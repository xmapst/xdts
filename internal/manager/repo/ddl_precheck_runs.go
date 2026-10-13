package repo

func precheckRunsMigration() migration {
	return migration{Version: 61, Description: "create pipeline precheck runs", Statements: []string{`
CREATE TABLE IF NOT EXISTS pipeline_precheck_runs (
 id BINARY(16) NOT NULL, created_at DATETIME(3) NOT NULL, updated_at DATETIME(3) NOT NULL,
 pipeline_id BINARY(16) NOT NULL, sequence BIGINT UNSIGNED NOT NULL, config_version BIGINT UNSIGNED NOT NULL, run_id BIGINT UNSIGNED NOT NULL,
 config_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 required JSON NOT NULL, complete BOOLEAN NOT NULL,
 checked_at DATETIME(3) NOT NULL, expires_at DATETIME(3) NOT NULL, created_by BINARY(16) NOT NULL,
 PRIMARY KEY(id), KEY idx_precheck_runs_pipeline(pipeline_id), UNIQUE KEY uk_precheck_runs_sequence(pipeline_id,sequence)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`}}
}
