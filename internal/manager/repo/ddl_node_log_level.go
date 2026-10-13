package repo

// 幂等性说明：与 v27/v29 相同，依赖 Migrate() 按 schema_versions 记录
// 「一个版本号只应用一次」，不依赖 ALTER TABLE 语句自身可重复执行。
func nodeLogLevelFieldsMigration() migration {
	return migration{
		Version:     30,
		Description: "add nodes log level fields",
		Statements: []string{`
ALTER TABLE nodes
  ADD COLUMN log_level VARCHAR(8) NOT NULL DEFAULT 'info',
  ADD COLUMN log_ship_level VARCHAR(8) NOT NULL DEFAULT 'info'`,
		},
	}
}
