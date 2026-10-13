package repo

// 不修改 pipelinesMigration（v11）本身：与 v27（cleanup 字段）、v35
// （lower_case_table_names）等既有先例一致，字段变更一律通过追加新
// 版本迁移完成，保持已应用版本可审计、可重放。
//
// 幂等性说明：与本文件其余全部迁移同一个模式——ALTER TABLE ADD COLUMN
// 本身不可重复执行（MySQL 8 不支持 IF NOT EXISTS 子句），但 Migrate()
// 按 schema_versions 记录跳过已应用的版本号，重复调用 Migrate() 依旧
// 幂等。
func pipelineCmdSeqFieldMigration() migration {
	return migration{
		Version:     36,
		Description: "add pipelines last_cmd_seq field",
		Statements: []string{`
ALTER TABLE pipelines
  ADD COLUMN last_cmd_seq BIGINT UNSIGNED NOT NULL DEFAULT 0`,
		},
	}
}
