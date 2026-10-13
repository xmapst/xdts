package repo

func topologyConfirmationsMigration() migration {
	return migration{Version: 50, Description: "create topology_confirmations", Statements: []string{`
CREATE TABLE IF NOT EXISTS topology_confirmations (
 id BINARY(16) NOT NULL,
 created_at DATETIME(3) NOT NULL,
 updated_at DATETIME(3) NOT NULL,
 topology_id BINARY(16) NOT NULL,
 policy_digest CHAR(64) NOT NULL,
 confirmed_by BINARY(16) NOT NULL,
 confirmed_at DATETIME(3) NOT NULL,
 PRIMARY KEY(id),
 UNIQUE KEY uk_topology_confirmation(topology_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`}}
}
