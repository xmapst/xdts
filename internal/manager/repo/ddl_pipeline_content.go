package repo

func automaticTopologyMigration() migration {
	return migration{Version: 80, Description: "reserve automatic pipeline topology", Statements: []string{
		`INSERT INTO topologies (id, name, type, description, created_at, updated_at) VALUES (UNHEX('00000000000040008000000000000001'), '__xdts_pipeline_topology__', 'custom', 'Generated from pipelines', UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))`,
	}}
}

func pipelineRuleOwnershipMigration() migration {
	return migration{Version: 81, Description: "assign immutable rules to pipelines", Statements: []string{
		`ALTER TABLE strategies ADD COLUMN owner_pipeline_id BINARY(16) NULL, ADD UNIQUE INDEX uk_strategies_owner_pipeline (owner_pipeline_id)`,
	}}
}
