package repo

// schemaMigrationsMigration 建 schema_migrations 表，对应
// internal/manager/model.SchemaMigration。
//
// 注意与本包自身的迁移引擎记录表 schema_versions 是两回事：这张
// schema_migrations 是业务表（结构迁移计划与执行结果），schema_versions
// 是迁移引擎的实现细节。
func schemaMigrationsMigration() migration {
	return migration{
		Version:     14,
		Description: "create schema_migrations",
		Statements: []string{`
CREATE TABLE IF NOT EXISTS schema_migrations (
  id BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  pipeline_id BINARY(16) NOT NULL,
  run_id BIGINT UNSIGNED NOT NULL,
  seq INT UNSIGNED NOT NULL,
  object_type VARCHAR(16) NOT NULL,
  schema_name VARCHAR(128) NOT NULL,
  object_name VARCHAR(128) NOT NULL DEFAULT '',
  action VARCHAR(16) NOT NULL,
  source_ddl MEDIUMTEXT NULL,
  target_ddl MEDIUMTEXT NULL,
  diff JSON NULL,
  status VARCHAR(16) NOT NULL,
  error TEXT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_schema_migrations_seq (pipeline_id, run_id, seq),
  KEY idx_schema_migrations_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
}
