package repo

// topologiesMigration 建 topologies 表，对应
// internal/manager/model.Topology（需求 MGR-TOPO-*）。
func topologiesMigration() migration {
	return migration{
		Version:     10,
		Description: "create topologies",
		Statements: []string{`
CREATE TABLE IF NOT EXISTS topologies (
  id BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  name VARCHAR(128) NOT NULL,
  type VARCHAR(16) NOT NULL,
  description TEXT NULL,
  deleted_at DATETIME(3) NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_topologies_name (name),
  KEY idx_topologies_deleted_at (deleted_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
}
