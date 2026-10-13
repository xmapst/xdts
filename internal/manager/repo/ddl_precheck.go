package repo

// pipelinePrechecksMigration 创建 pipeline_prechecks，保存预检查和确认信息。
func pipelinePrechecksMigration() migration {
	return migration{
		Version:     17,
		Description: "create pipeline_prechecks",
		Statements: []string{`
CREATE TABLE IF NOT EXISTS pipeline_prechecks (
  id BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  pipeline_id BINARY(16) NOT NULL,
  config_version BIGINT UNSIGNED NOT NULL,
  item VARCHAR(64) NOT NULL,
  level VARCHAR(16) NOT NULL,
  status VARCHAR(16) NOT NULL,
  detail JSON NULL,
  checked_at DATETIME(3) NOT NULL,
  confirmed_by BINARY(16) NULL,
  PRIMARY KEY (id),
  KEY idx_pipeline_prechecks_pipeline_id (pipeline_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
}
