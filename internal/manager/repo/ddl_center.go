package repo

// centersMigration 建 centers 表，对应 internal/manager/model.Center
// （需求 C-*）。
func centersMigration() migration {
	return migration{
		Version:     3,
		Description: "create centers",
		Statements: []string{`
CREATE TABLE IF NOT EXISTS centers (
  id BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  code VARCHAR(64) NOT NULL,
  name VARCHAR(128) NOT NULL,
  region VARCHAR(64) NOT NULL DEFAULT '',
  is_manager_center TINYINT(1) NOT NULL DEFAULT 0,
  mq_mgmt_url VARCHAR(255) NOT NULL DEFAULT '',
  mq_mgmt_user VARCHAR(128) NOT NULL DEFAULT '',
  mq_mgmt_password VARCHAR(255) NOT NULL DEFAULT '',
  inspector_node_id BINARY(16) NULL,
  description TEXT NULL,
  lock_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  deleted_at DATETIME(3) NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_centers_code (code),
  KEY idx_centers_deleted_at (deleted_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
}

// centerOriginUUIDsMigration 建 center_origin_uuids 表，对应
// internal/manager/model.CenterOriginUUID（ADR-01、）。
func centerOriginUUIDsMigration() migration {
	return migration{
		Version:     4,
		Description: "create center_origin_uuids",
		Statements: []string{`
CREATE TABLE IF NOT EXISTS center_origin_uuids (
  id BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  center_id BINARY(16) NOT NULL,
  uuid BINARY(16) NOT NULL,
  kind VARCHAR(16) NOT NULL,
  ref_id VARCHAR(128) NOT NULL DEFAULT '',
  PRIMARY KEY (id),
  UNIQUE KEY uk_center_origin_uuids_uuid (uuid),
  KEY idx_center_origin_uuids_center_id (center_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
}
