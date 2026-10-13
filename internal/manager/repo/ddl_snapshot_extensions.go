package repo

func snapshotExtensionsMigration() migration {
	return migration{Version: 78, Description: "persist immutable base snapshot counts and extension sequence", Statements: []string{
		`ALTER TABLE pipeline_snapshots ADD COLUMN initial_total_chunks BIGINT UNSIGNED NOT NULL DEFAULT 0, ADD COLUMN plan_extension_sequence BIGINT UNSIGNED NOT NULL DEFAULT 0`,
	}}
}
