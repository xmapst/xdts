package repo

// v27 is the published cleanup schema; later confirmations use a new migration.
func pipelinesCleanupFieldsMigration() migration {
	return migration{Version: 27, Description: "add pipelines cleanup fields", Statements: []string{`
ALTER TABLE pipelines
  ADD COLUMN cleanup_state VARCHAR(16) NOT NULL DEFAULT 'done',
  ADD COLUMN cleanup_detail JSON NULL,
  ADD COLUMN cleanup_attempts INT NOT NULL DEFAULT 0,
  ADD COLUMN cleanup_last_error TEXT NULL,
  ADD COLUMN cleanup_last_attempt_at DATETIME(3) NULL,
  ADD INDEX idx_pipelines_deleted_cleanup_state (deleted_at, cleanup_state)`}}
}

func pipelineCleanupAcknowledgementsMigration() migration {
	return migration{Version: 70, Description: "persist pipeline role unload confirmations", Statements: []string{`
ALTER TABLE pipelines
 ADD COLUMN source_cleanup_confirmed_at DATETIME(3) NULL,
 ADD COLUMN target_cleanup_confirmed_at DATETIME(3) NULL,
 ADD COLUMN source_cleanup_error TEXT NULL,
 ADD COLUMN target_cleanup_error TEXT NULL`}}
}
