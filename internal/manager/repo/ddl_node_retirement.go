package repo

func nodeRetirementMigration() migration {
	return migration{Version: 58, Description: "retain node retirement authentication material", Statements: []string{`
CREATE TABLE IF NOT EXISTS node_retired_credentials (
 node_id BINARY(16) NOT NULL PRIMARY KEY,
 token_hash VARCHAR(128) NOT NULL,
 retired_at DATETIME(3) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`}}
}
