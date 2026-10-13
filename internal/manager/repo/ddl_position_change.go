package repo

func pipelinePositionRevisionMigration() migration {
	return migration{Version: 66, Description: "add explicit pipeline position revision", Statements: []string{`
ALTER TABLE pipelines ADD COLUMN position_revision BIGINT UNSIGNED NOT NULL DEFAULT 0`}}
}

func positionChangesMigration() migration {
	return migration{Version: 67, Description: "create confirmed pipeline position changes", Statements: []string{`
CREATE TABLE IF NOT EXISTS pipeline_position_changes (
 id BINARY(16) NOT NULL PRIMARY KEY,
 pipeline_id BINARY(16) NOT NULL,
 run_id BIGINT UNSIGNED NOT NULL,
 config_version BIGINT UNSIGNED NOT NULL,
 position_revision BIGINT UNSIGNED NOT NULL,
 digest CHAR(64) NOT NULL,
 document JSON NOT NULL,
 created_at DATETIME(3) NOT NULL,
 updated_at DATETIME(3) NOT NULL,
 expires_at DATETIME(3) NOT NULL,
 confirmed_at DATETIME(3) NULL,
 confirmed_by BINARY(16) NULL,
 UNIQUE KEY uk_pipeline_position_change (pipeline_id, run_id, position_revision),
 KEY idx_position_changes_expires_at (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`}}
}
