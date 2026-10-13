package repo

// Rename the datasource table and dependent pipeline references together.
func dataEndpointRenameMigration() migration {
	return migration{
		Version:     32,
		Description: "rename datasources to data_endpoints, drop role column, rename pipelines source/target datasource_id columns",
		Statements: []string{
			"ALTER TABLE datasources RENAME TO data_endpoints",
			"ALTER TABLE data_endpoints DROP COLUMN role",
			"ALTER TABLE data_endpoints RENAME INDEX idx_datasources_center_id TO idx_data_endpoints_center_id",
			"ALTER TABLE data_endpoints RENAME INDEX idx_datasources_deleted_at TO idx_data_endpoints_deleted_at",
			"ALTER TABLE pipelines RENAME COLUMN source_datasource_id TO source_data_endpoint_id",
			"ALTER TABLE pipelines RENAME COLUMN target_datasource_id TO target_data_endpoint_id",
			"ALTER TABLE pipelines RENAME INDEX idx_pipelines_source_datasource_id TO idx_pipelines_source_data_endpoint_id",
			"ALTER TABLE pipelines RENAME INDEX idx_pipelines_target_datasource_id TO idx_pipelines_target_data_endpoint_id",
		},
	}
}
