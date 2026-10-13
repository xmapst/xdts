package repo

// pipelineReleasesMigration 建 pipeline_releases 表，对应
// internal/manager/model.PipelineRelease。
func pipelineReleasesMigration() migration {
	return migration{
		Version:     15,
		Description: "create pipeline_releases",
		Statements: []string{`
CREATE TABLE IF NOT EXISTS pipeline_releases (
  id BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  pipeline_id BINARY(16) NOT NULL,
  config_version BIGINT UNSIGNED NOT NULL,
  side VARCHAR(8) NOT NULL,
  node_id BINARY(16) NOT NULL,
  config_digest VARCHAR(64) NOT NULL DEFAULT '',
  status VARCHAR(16) NOT NULL,
  message TEXT NULL,
  applied_at DATETIME(3) NULL,
  PRIMARY KEY (id),
  KEY idx_pipeline_releases_pipeline_id (pipeline_id),
  KEY idx_pipeline_releases_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
}
