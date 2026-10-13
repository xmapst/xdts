package repo

// endpointRenameMigration renames data_endpoints and its dependent columns and indexes.
// It preserves existing records and supports databases already migrated by earlier versions.
func endpointRenameMigration() migration {
	return migration{
		Version:     34,
		Description: "rename data_endpoints to endpoints, rename pipelines source/target data_endpoint_id columns, backfill audit_logs enum values",
		Statements: []string{
			"ALTER TABLE data_endpoints RENAME TO endpoints",
			"ALTER TABLE endpoints RENAME INDEX idx_data_endpoints_center_id TO idx_endpoints_center_id",
			"ALTER TABLE endpoints RENAME INDEX idx_data_endpoints_deleted_at TO idx_endpoints_deleted_at",
			"ALTER TABLE pipelines RENAME COLUMN source_data_endpoint_id TO source_endpoint_id",
			"ALTER TABLE pipelines RENAME COLUMN target_data_endpoint_id TO target_endpoint_id",
			"ALTER TABLE pipelines RENAME INDEX idx_pipelines_source_data_endpoint_id TO idx_pipelines_source_endpoint_id",
			"ALTER TABLE pipelines RENAME INDEX idx_pipelines_target_data_endpoint_id TO idx_pipelines_target_endpoint_id",
			"UPDATE audit_logs SET action = 'endpoint_change' WHERE action IN ('datasource_change', 'data_endpoint_change')",
			"UPDATE audit_logs SET target_type = 'endpoint' WHERE target_type IN ('datasource', 'data_endpoint')",
		},
	}
}
