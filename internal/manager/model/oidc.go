package model

import "time"

// OIDCLoginState 仅保存在服务端；令牌与授权码均不落此表。
type OIDCLoginState struct {
	StateHash   string    `gorm:"column:state_hash;primaryKey" json:"-"`
	BrowserHash string    `gorm:"column:browser_hash" json:"-"`
	Nonce       string    `gorm:"column:nonce" json:"-"`
	Verifier    string    `gorm:"column:verifier" json:"-"`
	ConfigHash  string    `gorm:"column:config_hash" json:"-"`
	ExpiresAt   time.Time `gorm:"column:expires_at" json:"expires_at"`
}

func (OIDCLoginState) TableName() string { return "oidc_login_states" }
