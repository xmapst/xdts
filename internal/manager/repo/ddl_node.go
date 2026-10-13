package repo

// nodesMigration 建 nodes 表，对应 internal/manager/model.Node
// （需求 MGR-NODE-*）。
func nodesMigration() migration {
	return migration{
		Version:     6,
		Description: "create nodes",
		Statements: []string{`
CREATE TABLE IF NOT EXISTS nodes (
  id BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  name VARCHAR(128) NOT NULL,
  center_id BINARY(16) NOT NULL,
  labels JSON NULL,
  hostname VARCHAR(255) NOT NULL DEFAULT '',
  ips VARCHAR(255) NOT NULL DEFAULT '',
  os_arch VARCHAR(64) NOT NULL DEFAULT '',
  version VARCHAR(32) NOT NULL DEFAULT '',
  token_hash VARCHAR(64) NOT NULL,
  status VARCHAR(16) NOT NULL,
  last_heartbeat_at DATETIME(3) NULL,
  registered_at DATETIME(3) NOT NULL,
  runtime JSON NULL,
  lock_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  deleted_at DATETIME(3) NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_nodes_token_hash (token_hash),
  KEY idx_nodes_center_id (center_id),
  KEY idx_nodes_deleted_at (deleted_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
}

// nodeRegisterCodesMigration 建 node_register_codes 表，对应
// internal/manager/model.NodeRegisterCode（需求 O-04）。
func nodeRegisterCodesMigration() migration {
	return migration{
		Version:     7,
		Description: "create node_register_codes",
		Statements: []string{`
CREATE TABLE IF NOT EXISTS node_register_codes (
  id BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  code_hash VARCHAR(64) NOT NULL,
  expires_at DATETIME(3) NOT NULL,
  max_uses INT NOT NULL DEFAULT 1,
  used_count INT NOT NULL DEFAULT 0,
  center_id BINARY(16) NOT NULL,
  preset_name VARCHAR(128) NOT NULL DEFAULT '',
  preset_labels JSON NULL,
  created_by BINARY(16) NOT NULL,
  revoked_at DATETIME(3) NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_node_register_codes_code_hash (code_hash),
  KEY idx_node_register_codes_center_id (center_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
}
