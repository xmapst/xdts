package repo

func nodeRegistrationAttemptsMigration() migration {
	return migration{Version: 56, Description: "create durable node registration attempts", Statements: []string{`
CREATE TABLE IF NOT EXISTS node_registration_attempts (
 register_code_id BINARY(16) NOT NULL,
 instance_id BINARY(16) NOT NULL,
 node_id BINARY(16) NOT NULL,
 center_code VARCHAR(64) NOT NULL,
 token_version INT UNSIGNED NOT NULL,
 created_at DATETIME(3) NOT NULL,
 PRIMARY KEY(register_code_id),
 UNIQUE KEY uk_node_registration_instance(instance_id),
 UNIQUE KEY uk_node_registration_node(node_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`}}
}
