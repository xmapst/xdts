package repo

// pipelineCheckpointsMigration 建 pipeline_checkpoints 表，对应
// internal/manager/model.PipelineCheckpoint（需求
// MGR-MON-08）。
func pipelineCheckpointsMigration() migration {
	return migration{
		Version:     16,
		Description: "create pipeline_checkpoints",
		Statements: []string{`
CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
  id BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  pipeline_id BINARY(16) NOT NULL,
  side VARCHAR(8) NOT NULL,
  read_gtid TEXT NULL,
  published_gtid TEXT NULL,
  applied_gtid TEXT NULL,
  epoch BIGINT UNSIGNED NOT NULL DEFAULT 0,
  batch_seq BIGINT UNSIGNED NOT NULL DEFAULT 0,
  event_time DATETIME(3) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_pipeline_checkpoints_pipeline_side (pipeline_id, side)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
}
