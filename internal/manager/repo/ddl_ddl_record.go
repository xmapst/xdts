package repo

// ddlRecordsMigration 建 ddl_records 表，对应
// internal/manager/model.DDLRecord。
func ddlRecordsMigration() migration {
	return migration{
		Version:     20,
		Description: "create ddl_records",
		Statements: []string{`
CREATE TABLE IF NOT EXISTS ddl_records (
  id BINARY(16) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  pipeline_id BINARY(16) NOT NULL,
  ts DATETIME(3) NOT NULL,
  gtid VARCHAR(64) NOT NULL DEFAULT '',
  schema_name VARCHAR(128) NOT NULL,
  ddl_sql MEDIUMTEXT NOT NULL,
  status VARCHAR(16) NOT NULL,
  PRIMARY KEY (id),
  KEY idx_ddl_records_pipeline_ts (pipeline_id, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
}
