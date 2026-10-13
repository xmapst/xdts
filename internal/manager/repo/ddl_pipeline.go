package repo

// pipelinesMigration 建 pipelines 表，对应
// internal/manager/model.Pipeline（需求 MGR-PL-*）。
func pipelinesMigration() migration {
	return migration{
		Version:     11,
		Description: "create pipelines",
		Statements: []string{`
CREATE TABLE IF NOT EXISTS pipelines (
  id BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  name VARCHAR(128) NOT NULL,
  topology_id BINARY(16) NOT NULL,
  source_datasource_id BINARY(16) NOT NULL,
  source_node_id BINARY(16) NOT NULL,
  target_datasource_id BINARY(16) NOT NULL,
  target_node_id BINARY(16) NOT NULL,
  strategy_id BINARY(16) NOT NULL,
  strategy_version INT UNSIGNED NOT NULL,
  init_mode VARCHAR(32) NOT NULL,
  start_position JSON NULL,
  run_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
  snapshot_uuid BINARY(16) NULL,
  schema_uuid BINARY(16) NULL,
  binlog_server_id INT UNSIGNED NULL,
  desired_state VARCHAR(16) NOT NULL,
  actual_state VARCHAR(16) NOT NULL,
  config_version BIGINT UNSIGNED NOT NULL DEFAULT 0,
  last_error TEXT NULL,
  deleted_at DATETIME(3) NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_pipelines_name (name),
  UNIQUE KEY uk_pipelines_binlog_server_id (binlog_server_id),
  KEY idx_pipelines_topology_id (topology_id),
  KEY idx_pipelines_source_datasource_id (source_datasource_id),
  KEY idx_pipelines_target_datasource_id (target_datasource_id),
  KEY idx_pipelines_desired_actual_state (desired_state, actual_state),
  KEY idx_pipelines_deleted_at (deleted_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
}
