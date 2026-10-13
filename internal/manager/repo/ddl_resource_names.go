package repo

func resourceNamesMigration() migration {
	return migration{Version: 82, Description: "enforce unique node names", Statements: []string{
		"ALTER TABLE nodes ADD UNIQUE INDEX uk_nodes_name (name)",
	}}
}

func centerNamesMigration() migration {
	return migration{Version: 83, Description: "enforce unique center names", Statements: []string{
		"ALTER TABLE centers ADD UNIQUE INDEX uk_centers_name (name)",
	}}
}

func endpointNamesMigration() migration {
	return migration{Version: 84, Description: "enforce unique endpoint names", Statements: []string{
		"ALTER TABLE endpoints ADD UNIQUE INDEX uk_endpoints_name (name)",
	}}
}
