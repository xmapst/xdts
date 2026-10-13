package repo

func oidcLoginStatesMigration() migration {
	return migration{Version: 60, Description: "create single use OIDC login states", Statements: []string{`
CREATE TABLE IF NOT EXISTS oidc_login_states (
 state_hash VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 browser_hash VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 nonce VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 verifier VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 config_hash VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 expires_at DATETIME(3) NOT NULL,
 PRIMARY KEY (state_hash), KEY idx_oidc_login_states_expiry (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`}}
}
