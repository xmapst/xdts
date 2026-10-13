package repo

func pipelineStateBundlesMigration() migration {
	return migration{Version: 79, Description: "persist stopped snapshot role handovers with assignment", Statements: []string{
		`CREATE TABLE pipeline_state_bundles (pipeline_id BINARY(16) NOT NULL, side VARCHAR(16) NOT NULL, bundle MEDIUMBLOB NOT NULL, PRIMARY KEY (pipeline_id,side)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	}}
}
