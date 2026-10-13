package repo

func precheckBatchMigration() migration {
	return migration{Version: 62, Description: "bind precheck items to runs", Statements: []string{`
ALTER TABLE pipeline_prechecks
 ADD COLUMN batch_id BINARY(16) NULL,
 ADD COLUMN confirmed_at DATETIME(3) NULL,
 ADD INDEX idx_pipeline_prechecks_batch(batch_id),
 ADD UNIQUE INDEX uk_pipeline_prechecks_batch_item(batch_id,item)`}}
}
