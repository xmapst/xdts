package repo

// alertRulesMigration 创建告警规则表。
func alertRulesMigration() migration {
	return migration{
		Version:     21,
		Description: "create alert_rules",
		Statements: []string{`
CREATE TABLE IF NOT EXISTS alert_rules (
  id BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  name VARCHAR(128) NOT NULL,
  dimension VARCHAR(32) NOT NULL,
  enabled TINYINT(1) NOT NULL DEFAULT 1,
  threshold JSON NULL,
  webhook_url VARCHAR(255) NOT NULL DEFAULT '',
  suppress_seconds BIGINT NOT NULL DEFAULT 0,
  created_by BINARY(16) NOT NULL,
  lock_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  deleted_at DATETIME(3) NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_alert_rules_name (name),
  KEY idx_alert_rules_dimension (dimension),
  KEY idx_alert_rules_deleted_at (deleted_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
}

// alertsMigration 建 alerts 表，对应 internal/manager/model.Alert
func alertsMigration() migration {
	return migration{
		Version:     22,
		Description: "create alerts",
		Statements: []string{`
CREATE TABLE IF NOT EXISTS alerts (
  id BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  rule_id BINARY(16) NOT NULL,
  dimension VARCHAR(32) NOT NULL,
  level VARCHAR(16) NOT NULL,
  status VARCHAR(16) NOT NULL,
  pipeline_id BINARY(16) NULL,
  node_id BINARY(16) NULL,
  detail JSON NULL,
  fired_at DATETIME(3) NOT NULL,
  resolved_at DATETIME(3) NULL,
  notified_at DATETIME(3) NULL,
  PRIMARY KEY (id),
  KEY idx_alerts_rule_id (rule_id),
  KEY idx_alerts_dimension (dimension),
  KEY idx_alerts_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
}
