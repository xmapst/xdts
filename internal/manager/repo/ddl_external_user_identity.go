package repo

// 空外部身份为 NULL，允许多个本地账号；删除仍保留外部身份预约。
func externalUserIdentityMigration() migration {
	return migration{Version: 59, Description: "unique external user identity", Statements: []string{`
ALTER TABLE users
 ADD COLUMN external_identity_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin GENERATED ALWAYS AS (NULLIF(external_id, '')) STORED,
 ADD UNIQUE INDEX uk_users_external_identity (auth_source, external_identity_key)`}}
}
