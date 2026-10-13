package repo

// loginLockoutsMigration 建 login_lockouts 表，对应
// internal/manager/model.LoginLockout（需求
// MGR-USR-01a/b）。
func loginLockoutsMigration() migration {
	return migration{
		Version:     28,
		Description: "create login_lockouts",
		Statements: []string{`
CREATE TABLE IF NOT EXISTS login_lockouts (
  id BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  scope VARCHAR(16) NOT NULL,
  subject VARCHAR(255) NOT NULL,
  failure_count INT NOT NULL DEFAULT 0,
  window_started_at DATETIME(3) NOT NULL,
  locked_until DATETIME(3) NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_login_lockouts_scope_subject (scope, subject),
  KEY idx_login_lockouts_locked_until (locked_until)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
}
