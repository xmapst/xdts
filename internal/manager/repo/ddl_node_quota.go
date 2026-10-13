package repo

func nodePipelineQuotaMigration() migration {
	return migration{Version: 71, Description: "add node pipeline allocation quota", Statements: []string{
		"ALTER TABLE nodes ADD COLUMN max_pipelines INT UNSIGNED NOT NULL DEFAULT 0",
	}}
}
