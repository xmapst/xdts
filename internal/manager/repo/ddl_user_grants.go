package repo

func userGrantsMigration() migration {
	return migration{Version: 75, Description: "create fine grained user resource grants", Statements: []string{
		`CREATE TABLE IF NOT EXISTS user_resource_grants (
  id BINARY(16) NOT NULL,
  user_id BINARY(16) NOT NULL,
  scope_type VARCHAR(16) NOT NULL,
  resource_id BINARY(16) NOT NULL,
  can_write BOOLEAN NOT NULL DEFAULT FALSE,
  created_by BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_user_grants_scope (user_id, scope_type, resource_id),
  KEY idx_user_grants_resource (scope_type, resource_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	}}
}

func userScopePolicyMigration() migration {
	return migration{Version: 76, Description: "persist explicit user scope restrictions", Statements: []string{
		`CREATE TABLE IF NOT EXISTS user_scope_policies (
  user_id BINARY(16) NOT NULL PRIMARY KEY,
  restricted BOOLEAN NOT NULL DEFAULT FALSE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	}}
}

func userScopePolicyBackfillMigration() migration {
	return migration{Version: 77, Description: "retain previously configured user scopes", Statements: []string{
		`INSERT INTO user_scope_policies (user_id, restricted)
SELECT DISTINCT user_id, TRUE FROM user_resource_grants
ON DUPLICATE KEY UPDATE user_id = VALUES(user_id)`,
	}}
}
