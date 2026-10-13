package repo

func userMustChangePasswordMigration() migration {
	return migration{
		Version:     48,
		Description: "add user must change password flag",
		Statements:  []string{`ALTER TABLE users ADD COLUMN must_change_password BOOLEAN NOT NULL DEFAULT FALSE`},
	}
}
