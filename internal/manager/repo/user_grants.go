package repo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrInvalidUserScope = errors.New("repo: invalid user resource scope")

const MaxUserScopes = 1000

func UserScopeRestricted(ctx context.Context, db *gorm.DB, userID uuidx.UUID) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var policy model.UserScopePolicy
	err := db.WithContext(ctx).First(&policy, "user_id = ?", userID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("repo: read user scope policy: %w", err)
	}
	return policy.Restricted, nil
}

func ListUserGrants(ctx context.Context, db *gorm.DB, userID uuidx.UUID) ([]model.UserGrant, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var grants []model.UserGrant
	if err := db.WithContext(ctx).Where("user_id = ?", userID).Order("scope_type, resource_id").Find(&grants).Error; err != nil {
		return nil, fmt.Errorf("repo: list user grants: %w", err)
	}
	return grants, nil
}

// ReplaceUserGrants activates the explicit scope, including an empty deny-all scope.
func ReplaceUserGrants(ctx context.Context, db *gorm.DB, userID, createdBy uuidx.UUID, grants []model.UserGrant) error {
	return ReplaceUserScopes(ctx, db, userID, createdBy, true, grants)
}

// ReplaceUserScopes serializes replacements on the user row. The input belongs
// to the caller and is never mutated; validation and policy updates are atomic.
func ReplaceUserScopes(ctx context.Context, db *gorm.DB, userID, createdBy uuidx.UUID, restricted bool, grants []model.UserGrant) error {
	if userID.IsZero() || createdBy.IsZero() || len(grants) > MaxUserScopes || (!restricted && len(grants) > 0) {
		return ErrInvalidUserScope
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var user model.User
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&user, "id = ?", userID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrUserNotFound
			}
			return err
		}
		actor, err := GetUser(ctx, tx, createdBy)
		if err != nil {
			return err
		}
		if actor.Role != model.UserRoleAdmin {
			return ErrInvalidUserScope
		}
		copies := make([]model.UserGrant, len(grants))
		seen := make(map[string]bool, len(grants))
		for i, input := range grants {
			// Generate all persisted identity and ownership fields at this boundary.
			g := model.UserGrant{UserID: userID, CreatedBy: createdBy, Scope: input.Scope, ResourceID: input.ResourceID, CanWrite: input.CanWrite}
			if err := g.Validate(); err != nil {
				return fmt.Errorf("%w: %v", ErrInvalidUserScope, err)
			}
			key := string(g.Scope) + ":" + g.ResourceID.String()
			if seen[key] {
				return ErrInvalidUserScope
			}
			seen[key] = true
			if g.Scope == model.GrantScopeCenter {
				_, err = GetCenter(ctx, tx, g.ResourceID)
				if errors.Is(err, ErrCenterNotFound) {
					return ErrInvalidUserScope
				}
			} else {
				_, err = GetPipeline(ctx, tx, g.ResourceID)
				if errors.Is(err, ErrPipelineNotFound) {
					return ErrInvalidUserScope
				}
			}
			if err != nil {
				return err
			}
			copies[i] = g
		}
		if err := tx.Where("user_id = ?", userID).Delete(&model.UserGrant{}).Error; err != nil {
			return err
		}
		if len(copies) > 0 {
			if err := tx.Create(&copies).Error; err != nil {
				return err
			}
		}
		return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "user_id"}}, DoUpdates: clause.AssignmentColumns([]string{"restricted"})}).Create(&model.UserScopePolicy{UserID: userID, Restricted: restricted}).Error
	})
}

// The policy and grants are read in one statement so a concurrent replacement
// cannot mix unrestricted policy with a different committed grant generation.
const userPipelineScopePredicate = `(
 NOT EXISTS (SELECT 1 FROM user_scope_policies policy WHERE policy.user_id = ? AND policy.restricted = TRUE)
 OR EXISTS (SELECT 1 FROM user_resource_grants g WHERE g.user_id = ? AND (
  (g.scope_type = 'pipeline' AND g.resource_id = p.id) OR
  (g.scope_type = 'center' AND (g.resource_id = se.center_id OR g.resource_id = te.center_id))
 ))
)`

func UserCanAccessPipeline(ctx context.Context, db *gorm.DB, userID, pipelineID uuidx.UUID, write bool) (bool, error) {
	// Administrators bypass in HTTP; these grants only apply to readonly users.
	if write {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var allowed bool
	q := `SELECT EXISTS(SELECT 1 FROM pipelines p
 LEFT JOIN endpoints se ON se.id = p.source_endpoint_id AND se.deleted_at IS NULL
 LEFT JOIN endpoints te ON te.id = p.target_endpoint_id AND te.deleted_at IS NULL
 WHERE p.id = ? AND p.deleted_at IS NULL AND ` + userPipelineScopePredicate + `)`
	if err := db.WithContext(ctx).Raw(q, pipelineID, userID, userID).Scan(&allowed).Error; err != nil {
		return false, fmt.Errorf("repo: check pipeline scope: %w", err)
	}
	return allowed, nil
}

func FilterPipelinesByUser(ctx context.Context, db *gorm.DB, userID uuidx.UUID, pipelines []model.Pipeline) ([]model.Pipeline, error) {
	if len(pipelines) == 0 {
		return pipelines, nil
	}
	ids := make([]uuidx.UUID, 0, len(pipelines))
	for _, p := range pipelines {
		ids = append(ids, p.ID)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var allowed []struct{ ID uuidx.UUID }
	q := `SELECT p.id FROM pipelines p
 LEFT JOIN endpoints se ON se.id = p.source_endpoint_id AND se.deleted_at IS NULL
 LEFT JOIN endpoints te ON te.id = p.target_endpoint_id AND te.deleted_at IS NULL
 WHERE p.id IN ? AND p.deleted_at IS NULL AND ` + userPipelineScopePredicate
	if err := db.WithContext(ctx).Raw(q, ids, userID, userID).Scan(&allowed).Error; err != nil {
		return nil, fmt.Errorf("repo: filter pipelines by scope: %w", err)
	}
	set := make(map[uuidx.UUID]bool, len(allowed))
	for _, row := range allowed {
		set[row.ID] = true
	}
	out := make([]model.Pipeline, 0, len(allowed))
	for _, p := range pipelines {
		if set[p.ID] {
			out = append(out, p)
		}
	}
	return out, nil
}
