package repo

// usersMigration 建 users 表，对应 internal/manager/model.User
// （需求 MGR-USR-*）。
func usersMigration() migration {
	return migration{
		Version:     1,
		Description: "create users",
		Statements: []string{`
CREATE TABLE IF NOT EXISTS users (
  id BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  username VARCHAR(128) NOT NULL,
  auth_source VARCHAR(16) NOT NULL,
  external_id VARCHAR(255) NOT NULL DEFAULT '',
  password_hash VARCHAR(255) NOT NULL DEFAULT '',
  role VARCHAR(16) NOT NULL,
  status VARCHAR(16) NOT NULL,
  last_login_at DATETIME(3) NULL,
  deleted_at DATETIME(3) NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_users_username (username),
  KEY idx_users_deleted_at (deleted_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
}

// userSessionsMigration 建 user_sessions 表，对应
// internal/manager/model.UserSession（需求 MGR-USR-04）。
func userSessionsMigration() migration {
	return migration{
		Version:     2,
		Description: "create user_sessions",
		Statements: []string{`
CREATE TABLE IF NOT EXISTS user_sessions (
  id BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  user_id BINARY(16) NOT NULL,
  token_hash VARCHAR(64) NOT NULL,
  expires_at DATETIME(3) NOT NULL,
  client_ip VARCHAR(64) NOT NULL DEFAULT '',
  user_agent VARCHAR(255) NOT NULL DEFAULT '',
  deleted_at DATETIME(3) NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_user_sessions_token_hash (token_hash),
  KEY idx_user_sessions_user_id (user_id),
  KEY idx_user_sessions_expires_at (expires_at),
  KEY idx_user_sessions_deleted_at (deleted_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
}
