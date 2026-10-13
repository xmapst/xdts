package repo

// auditLogsMigration 建 audit_logs 表，对应
// internal/manager/model.AuditLog（需求 MGR-USR-06，P1）。
// 不自动清理。
func auditLogsMigration() migration {
	return migration{
		Version:     23,
		Description: "create audit_logs",
		Statements: []string{`
CREATE TABLE IF NOT EXISTS audit_logs (
  id BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  ts DATETIME(3) NOT NULL,
  user_id BINARY(16) NOT NULL,
  action VARCHAR(32) NOT NULL,
  target_type VARCHAR(32) NOT NULL,
  target_id BINARY(16) NULL,
  detail JSON NULL,
  PRIMARY KEY (id),
  KEY idx_audit_logs_ts (ts),
  KEY idx_audit_logs_user_id (user_id),
  KEY idx_audit_logs_action (action)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
}
