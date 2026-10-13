package model

import (
	"fmt"

	"github.com/xmapst/xdts/pkg/uuidx"
)

// GrantScope identifies the resource boundary of a fine-grained permission.
type GrantScope string

const (
	GrantScopeCenter   GrantScope = "center"
	GrantScopePipeline GrantScope = "pipeline"
)

func (s GrantScope) Valid() bool { return s == GrantScopeCenter || s == GrantScopePipeline }

// UserGrant is an explicit permission for a user. Administrators bypass grants;
// grants only narrow non-admin access and never elevate a readonly account.
type UserGrant struct {
	Base
	UserID     uuidx.UUID `gorm:"column:user_id;type:binary(16);not null;index:idx_user_grants_user" json:"user_id"`
	Scope      GrantScope `gorm:"column:scope_type;type:varchar(16);not null" json:"scope"`
	ResourceID uuidx.UUID `gorm:"column:resource_id;type:binary(16);not null" json:"resource_id"`
	CanWrite   bool       `gorm:"column:can_write;not null;default:false" json:"can_write"`
	CreatedBy  uuidx.UUID `gorm:"column:created_by;type:binary(16);not null" json:"created_by"`
}

func (UserGrant) TableName() string { return "user_resource_grants" }

func (g UserGrant) Validate() error {
	if g.UserID.IsZero() || g.ResourceID.IsZero() || g.CreatedBy.IsZero() {
		return fmt.Errorf("model: user grant ids must be non-zero")
	}
	if !g.Scope.Valid() {
		return fmt.Errorf("model: invalid grant scope %q", g.Scope)
	}
	if g.CanWrite {
		return fmt.Errorf("model: resource grants cannot elevate readonly users")
	}
	return nil
}

// UserScopePolicy distinguishes unrestricted reads from an explicitly empty
// scope. Missing rows use the global readonly policy.
type UserScopePolicy struct {
	UserID     uuidx.UUID `gorm:"column:user_id;type:binary(16);primaryKey"`
	Restricted bool       `gorm:"column:restricted;not null"`
}

func (UserScopePolicy) TableName() string { return "user_scope_policies" }
