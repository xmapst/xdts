package repo

// datasourcesMigration 建 datasources 表，对应
// internal/manager/model.Datasource（需求 MGR-DS-01）。
func datasourcesMigration() migration {
	return migration{
		Version:     5,
		Description: "create datasources",
		Statements: []string{`
CREATE TABLE IF NOT EXISTS datasources (
  id BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  name VARCHAR(128) NOT NULL,
  center_id BINARY(16) NOT NULL,
  host VARCHAR(255) NOT NULL,
  port SMALLINT UNSIGNED NOT NULL,
  username VARCHAR(128) NOT NULL,
  password VARCHAR(255) NOT NULL DEFAULT '',
  tls JSON NULL,
  params JSON NULL,
  role VARCHAR(16) NOT NULL,
  version VARCHAR(32) NOT NULL DEFAULT '',
  server_uuid VARCHAR(36) NOT NULL DEFAULT '',
  probe_node_id BINARY(16) NULL,
  last_probe_at DATETIME(3) NULL,
  last_probe_result JSON NULL,
  lock_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  deleted_at DATETIME(3) NULL,
  PRIMARY KEY (id),
  KEY idx_datasources_center_id (center_id),
  KEY idx_datasources_deleted_at (deleted_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
}
