package repo

func centerOriginRefreshMigration() migration {
	return migration{Version: 57, Description: "create durable center origin refresh revisions", Statements: []string{`
CREATE TABLE IF NOT EXISTS center_origin_refresh (
 center_id BINARY(16) NOT NULL,
 revision BIGINT UNSIGNED NOT NULL,
 completed_revision BIGINT UNSIGNED NOT NULL DEFAULT 0,
 updated_at DATETIME(3) NOT NULL,
 PRIMARY KEY(center_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`}}
}
