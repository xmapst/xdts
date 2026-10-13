package repo

// eventsMigration 创建事件表；保留期清理使用分批 DELETE。
func eventsMigration() migration {
	return migration{
		Version:     18,
		Description: "create events",
		Statements: []string{`
CREATE TABLE IF NOT EXISTS events (
  id BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  ts DATETIME(3) NOT NULL,
  node_id BINARY(16) NULL,
  pipeline_id BINARY(16) NULL,
  type VARCHAR(32) NOT NULL,
  level VARCHAR(16) NOT NULL,
  payload JSON NULL,
  acked_by BINARY(16) NULL,
  PRIMARY KEY (id),
  KEY idx_events_pipeline_ts (pipeline_id, ts),
  KEY idx_events_type_ts (type, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
}
