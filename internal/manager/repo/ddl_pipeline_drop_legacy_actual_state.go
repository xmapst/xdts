package repo

// Drop the aggregate state after the role-state migration has backfilled data.
// Keep migration ordering so source and target observations are retained.
func pipelineDropLegacyActualStateIndexMigration() migration {
	return migration{
		Version:     39,
		Description: "drop degenerate index idx_pipelines_desired_actual_state",
		Statements: []string{
			`ALTER TABLE pipelines DROP INDEX idx_pipelines_desired_actual_state`,
		},
	}
}

// 下线旧列本身。
//
// `pipelines` 表里 NOT NULL 且没有 DEFAULT 的列共 14 个。删掉其中任何
// 一个的 Go 字段而不在同一批 DROP 掉该列，都会以完全相同的方式失败：
// gorm 的 INSERT 不再带那一列 → MySQL 严格模式 `Error 1364 ... doesn't have a
// default value` → 每条新建记录失败，而存量行读写照常，编译期看不见。
//
// 所以：下一次删 pipelines 的任何一个 Go 字段之前，先确认对应列有没有
// DEFAULT。 这条不写在 changelog 里——下一个删字段的人不会去翻当天的
// changelog，但会读到这里。
func pipelineDropLegacyActualStateColumnMigration() migration {
	return migration{
		Version:     40,
		Description: "drop legacy pipelines actual_state and last_error columns",
		Statements: []string{
			`ALTER TABLE pipelines DROP COLUMN actual_state, DROP COLUMN last_error`,
		},
	}
}
