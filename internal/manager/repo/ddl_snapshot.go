package repo

// pipelineSnapshotsMigration 建 pipeline_snapshots 表，对应
// internal/manager/model.PipelineSnapshot。
func pipelineSnapshotsMigration() migration {
	return migration{
		Version:     12,
		Description: "create pipeline_snapshots",
		Statements: []string{`
CREATE TABLE IF NOT EXISTS pipeline_snapshots (
  id BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  pipeline_id BINARY(16) NOT NULL,
  run_id BIGINT UNSIGNED NOT NULL,
  snapshot_uuid BINARY(16) NOT NULL,
  low_gtid_set TEXT NULL,
  high_gtid_set TEXT NULL,
  status VARCHAR(16) NOT NULL,
  total_chunks BIGINT UNSIGNED NOT NULL DEFAULT 0,
  done_chunks BIGINT UNSIGNED NOT NULL DEFAULT 0,
  started_at DATETIME(3) NULL,
  finished_at DATETIME(3) NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_pipeline_snapshots_pipeline_run (pipeline_id, run_id),
  KEY idx_pipeline_snapshots_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
}

// pipelineSnapshotTablesMigration 建 pipeline_snapshot_tables 表，对应
// internal/manager/model.PipelineSnapshotTable。
func pipelineSnapshotTablesMigration() migration {
	return migration{
		Version:     13,
		Description: "create pipeline_snapshot_tables",
		Statements: []string{`
CREATE TABLE IF NOT EXISTS pipeline_snapshot_tables (
  id BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  pipeline_id BINARY(16) NOT NULL,
  run_id BIGINT UNSIGNED NOT NULL,
  schema_name VARCHAR(128) NOT NULL,
  table_name VARCHAR(128) NOT NULL,
  est_rows BIGINT NOT NULL DEFAULT 0,
  done_rows BIGINT NOT NULL DEFAULT 0,
  status VARCHAR(16) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_pipeline_snapshot_tables_scope (pipeline_id, run_id, schema_name, table_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
}
