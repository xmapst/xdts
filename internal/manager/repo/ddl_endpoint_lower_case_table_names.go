package repo

// Store the probed server policy as text; empty means unknown and zero is valid.
// Keep the migration's published column representation unchanged.
func endpointLowerCaseTableNamesMigration() migration {
	return migration{
		Version:     35,
		Description: "add endpoints.lower_case_table_names",
		Statements: []string{
			"ALTER TABLE endpoints ADD COLUMN lower_case_table_names VARCHAR(32) NOT NULL DEFAULT '' AFTER server_uuid",
		},
	}
}
