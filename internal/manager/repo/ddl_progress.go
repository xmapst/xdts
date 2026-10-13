package repo

func pipelineProgressLatestMigration() migration {
	return migration{Version: 64, Description: "create complete pipeline progress latest", Statements: []string{`
CREATE TABLE IF NOT EXISTS pipeline_progress_latest (
  pipeline_id BINARY(16) NOT NULL,
  side VARCHAR(16) NOT NULL,
  node_id BINARY(16) NOT NULL,
  run_id BIGINT UNSIGNED NOT NULL,
  observed_at DATETIME(3) NOT NULL,
  last_sample_at DATETIME(3) NULL,
  values_json JSON NOT NULL,
  PRIMARY KEY (pipeline_id, side)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`}}
}

func pipelineMetricDetailsMigration() migration {
	return migration{Version: 65, Description: "add nullable pipeline metric details", Statements: []string{`
ALTER TABLE pipeline_metrics
  ADD COLUMN side VARCHAR(16) NULL,
  ADD COLUMN run_id BIGINT UNSIGNED NULL,
  ADD COLUMN node_id BINARY(16) NULL,
  ADD COLUMN tps DOUBLE NULL,
  ADD COLUMN conflicts BIGINT NULL,
  ADD COLUMN values_json JSON NULL,
  ADD KEY idx_pipeline_metrics_run_side_ts (pipeline_id, run_id, side, ts)`}}
}
