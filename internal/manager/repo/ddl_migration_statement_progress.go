package repo

// Bootstrap creates this engine table before older outstanding migrations.
// Recording v72 also rejects older Managers that cannot honor its recovery state.
func migrationStatementProgressMigration() migration {
	return migration{Version: 72, Description: "enable durable migration statement progress", Statements: []string{schemaMigrationStatementsDDL}}
}
