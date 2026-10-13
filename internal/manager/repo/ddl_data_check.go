package repo

func dataCheckRunsMigration() migration {
	return migration{Version: 85, Description: "create durable pipeline data check runs", Statements: []string{`
CREATE TABLE IF NOT EXISTS pipeline_data_check_runs (
 id BINARY(16) NOT NULL, pipeline_id BINARY(16) NOT NULL, config_version BIGINT UNSIGNED NOT NULL,
 created_by BINARY(16) NOT NULL, status VARCHAR(16) NOT NULL, config JSON NOT NULL,
 sample_modulo BIGINT UNSIGNED NOT NULL, completed_tables INT NOT NULL DEFAULT 0, results JSON NOT NULL,
 error TEXT NULL, expires_at DATETIME(3) NOT NULL, started_at DATETIME(3) NULL, finished_at DATETIME(3) NULL,
 created_at DATETIME(3) NOT NULL, updated_at DATETIME(3) NOT NULL,
 PRIMARY KEY(id), KEY idx_data_checks_pipeline (pipeline_id,created_at), KEY idx_data_checks_pending (status,expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`}}
}
