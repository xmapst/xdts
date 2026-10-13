package repo

func schemaPlansMigration() migration {
	return migration{Version: 49, Description: "create schema_plans", Statements: []string{`
CREATE TABLE IF NOT EXISTS schema_plans (
 id BINARY(16) NOT NULL,
 created_at DATETIME(3) NOT NULL,
 updated_at DATETIME(3) NOT NULL,
 pipeline_id BINARY(16) NOT NULL,
 run_id BIGINT UNSIGNED NOT NULL,
 config_version BIGINT UNSIGNED NOT NULL,
 digest CHAR(64) NOT NULL,
 document JSON NOT NULL,
 blocked BOOLEAN NOT NULL,
 confirmed_at DATETIME(3) NULL,
 confirmed_by BINARY(16) NULL,
 PRIMARY KEY(id),
 UNIQUE KEY uk_schema_plans_run(pipeline_id,run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`}}
}
