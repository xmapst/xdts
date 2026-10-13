package repo

func nodeCapabilitiesMigration() migration {
	return migration{Version: 74, Description: "store authenticated node feature capabilities", Statements: []string{
		"ALTER TABLE nodes ADD COLUMN capabilities JSON NULL",
	}}
}
