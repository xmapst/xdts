package repo

func pipelineStatusLatestMigration() migration {
	return migration{Version: 73, Description: "create authenticated pipeline status observations", Statements: []string{`
CREATE TABLE IF NOT EXISTS pipeline_status_latest (
  pipeline_id BINARY(16) NOT NULL,
  side VARCHAR(16) NOT NULL,
  node_id BINARY(16) NOT NULL,
  run_id BIGINT UNSIGNED NOT NULL,
  config_version BIGINT UNSIGNED NOT NULL,
  state VARCHAR(16) NOT NULL,
  last_error TEXT NOT NULL,
  snapshot_status VARCHAR(16) NOT NULL,
  observed_at_ms BIGINT NOT NULL,
  session_id VARCHAR(36) NOT NULL,
  session_started_at_ms BIGINT NOT NULL,
  sequence BIGINT UNSIGNED NOT NULL,
  runtime_instance_id VARCHAR(36) NOT NULL,
  runtime_instance_floor VARCHAR(36) NOT NULL,
  PRIMARY KEY (pipeline_id, side)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`}}
}
