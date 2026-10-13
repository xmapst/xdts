package repo

// settingsMigration 建 settings 表，对应 internal/manager/model.Setting
//
// 列名 `key` 是 MySQL 保留字，DDL 与索引定义中用反引号转义
// （internal/manager/model.Setting 的字段注释里也说明了这一点）。
func settingsMigration() migration {
	return migration{
		Version:     24,
		Description: "create settings",
		Statements: []string{"" +
			"CREATE TABLE IF NOT EXISTS settings (\n" +
			"  id BINARY(16) NOT NULL,\n" +
			"  created_at DATETIME(3) NOT NULL,\n" +
			"  updated_at DATETIME(3) NOT NULL,\n" +
			"  `key` VARCHAR(128) NOT NULL,\n" +
			"  value JSON NULL,\n" +
			"  PRIMARY KEY (id),\n" +
			"  UNIQUE KEY uk_settings_key (`key`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		},
	}
}
