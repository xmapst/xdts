package repo

func snapshotCatchupProgressMigration() migration {
	return migration{Version: 52, Description: "add snapshot catchup progress", Statements: []string{
		"ALTER TABLE pipeline_snapshots ADD COLUMN catchup_remaining_txns BIGINT UNSIGNED NULL, ADD COLUMN catchup_updated_at_ms BIGINT NOT NULL DEFAULT 0",
	}}
}
