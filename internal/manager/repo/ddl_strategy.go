package repo

// strategiesMigration 建 strategies 表，对应
// internal/manager/model.Strategy（需求 MGR-STR-*）。
func strategiesMigration() migration {
	return migration{
		Version:     8,
		Description: "create strategies",
		Statements: []string{`
CREATE TABLE IF NOT EXISTS strategies (
  id BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  name VARCHAR(128) NOT NULL,
  description TEXT NULL,
  current_version INT UNSIGNED NOT NULL DEFAULT 0,
  deleted_at DATETIME(3) NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_strategies_name (name),
  KEY idx_strategies_deleted_at (deleted_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
}

// strategyVersionsMigration 建 strategy_versions 表，对应
// internal/manager/model.StrategyVersion（需求 MGR-STR-01：
// 版本列表与对比）。不可变，无 updated_at / deleted_at。
func strategyVersionsMigration() migration {
	return migration{
		Version:     9,
		Description: "create strategy_versions",
		Statements: []string{`
CREATE TABLE IF NOT EXISTS strategy_versions (
  id BINARY(16) NOT NULL,
  strategy_id BINARY(16) NOT NULL,
  version INT UNSIGNED NOT NULL,
  rules JSON NOT NULL,
  created_by BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_strategy_versions_strategy_version (strategy_id, version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
}
