package repo

// conflictsMigration 建 conflicts 表，对应 internal/manager/model.Conflict
// （需求 MGR-MON-03）。分区策略说明同 eventsMigration。
func conflictsMigration() migration {
	return migration{
		Version:     19,
		Description: "create conflicts",
		Statements: []string{`
CREATE TABLE IF NOT EXISTS conflicts (
  id BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  ts DATETIME(3) NOT NULL,
  pipeline_id BINARY(16) NOT NULL,
  schema_name VARCHAR(128) NOT NULL,
  table_name VARCHAR(128) NOT NULL,
  pk JSON NOT NULL,
  conflict_type VARCHAR(24) NOT NULL,
  resolution VARCHAR(16) NOT NULL,
  source_row JSON NULL,
  target_row JSON NULL,
  PRIMARY KEY (id),
  KEY idx_conflicts_pipeline_ts (pipeline_id, ts),
  KEY idx_conflicts_type_ts (conflict_type, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
}
