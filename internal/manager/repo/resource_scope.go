package repo

import (
	"context"
	"fmt"

	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

type readScopeKey struct{}

// WithUserReadScope attaches the authenticated user to a read request. Each
// statement reads the current policy and grants together, including requests
// whose policy was unrestricted at the time authentication finished.
func WithUserReadScope(ctx context.Context, userID uuidx.UUID) context.Context {
	return context.WithValue(ctx, readScopeKey{}, userID)
}

func ReadScopeUser(ctx context.Context) (uuidx.UUID, bool) {
	id, ok := ctx.Value(readScopeKey{}).(uuidx.UUID)
	return id, ok
}

type ReadResource uint8

const (
	ReadCenter ReadResource = iota
	ReadEndpoint
	ReadNode
	ReadPipeline
	ReadTopology
	ReadStrategy
	ReadStrategyVersion
	ReadLog
	ReadEvent
	ReadConflict
	ReadAlert
	ReadDashboardPipeline
	ReadDashboardSnapshot
	ReadCenterOrigin
)

// ReadResourceScope only accepts a fixed resource/alias pair. SQL identifiers
// are selected by this code; clients cannot provide an identifier or predicate.
func ReadResourceScope(ctx context.Context, tx *gorm.DB, resource ReadResource) *gorm.DB {
	id, scoped := ReadScopeUser(ctx)
	if !scoped {
		return tx
	}
	if id.IsZero() {
		return tx.Where("1=0")
	}
	pipeline := `SELECT p.id FROM pipelines p
 LEFT JOIN endpoints se ON se.id=p.source_endpoint_id AND se.deleted_at IS NULL
 LEFT JOIN endpoints te ON te.id=p.target_endpoint_id AND te.deleted_at IS NULL
 WHERE p.deleted_at IS NULL AND EXISTS (SELECT 1 FROM user_resource_grants ug WHERE ug.user_id=? AND (
 (ug.scope_type='pipeline' AND ug.resource_id=p.id) OR
 (ug.scope_type='center' AND (ug.resource_id=se.center_id OR ug.resource_id=te.center_id))))`
	centerGrant := func(column string) string {
		return `EXISTS (SELECT 1 FROM user_resource_grants cg WHERE cg.user_id=? AND cg.scope_type='center' AND cg.resource_id=` + column + `)`
	}
	var predicate string
	args := []any{id}
	switch resource {
	case ReadCenter, ReadCenterOrigin:
		column := "centers.id"
		if resource == ReadCenterOrigin {
			column = "center_origin_uuids.center_id"
		}
		predicate = centerGrant(column)
		args = append(args, id)
	case ReadEndpoint:
		predicate = centerGrant("endpoints.center_id") + ` OR EXISTS (SELECT 1 FROM pipelines visible WHERE visible.id IN (` + pipeline + `) AND (visible.source_endpoint_id=endpoints.id OR visible.target_endpoint_id=endpoints.id))`
		args = append(args, id, id)
	case ReadNode:
		predicate = centerGrant("nodes.center_id") + ` OR EXISTS (SELECT 1 FROM pipelines visible WHERE visible.id IN (` + pipeline + `) AND (visible.source_node_id=nodes.id OR visible.target_node_id=nodes.id))`
		args = append(args, id, id)
	case ReadPipeline, ReadDashboardPipeline, ReadDashboardSnapshot:
		column := "pipelines.id"
		if resource == ReadDashboardPipeline {
			column = "p.id"
		}
		if resource == ReadDashboardSnapshot {
			column = "s.pipeline_id"
		}
		predicate = column + ` IN (` + pipeline + `)`
		args = append(args, id)
	case ReadTopology, ReadStrategy, ReadStrategyVersion:
		binding := "visible.topology_id=topologies.id"
		if resource == ReadStrategy {
			binding = "visible.strategy_id=strategies.id"
		}
		if resource == ReadStrategyVersion {
			binding = "visible.strategy_id=strategy_versions.strategy_id AND visible.strategy_version=strategy_versions.version"
		}
		predicate = `EXISTS (SELECT 1 FROM pipelines visible WHERE visible.id IN (` + pipeline + `) AND ` + binding + `)`
		args = append(args, id)
	case ReadLog, ReadEvent, ReadConflict, ReadAlert:
		table := map[ReadResource]string{ReadLog: "logs", ReadEvent: "events", ReadConflict: "conflicts", ReadAlert: "alerts"}[resource]
		predicate = table + `.pipeline_id IN (` + pipeline + `)`
		args = append(args, id)
		if resource != ReadConflict {
			// A visible shared Node never grants access to another pipeline's
			// records. Only node-level records may use a direct center grant.
			predicate += ` OR (` + table + `.pipeline_id IS NULL AND EXISTS (SELECT 1 FROM nodes rn WHERE rn.id=` + table + `.node_id AND rn.deleted_at IS NULL AND ` + centerGrant("rn.center_id") + `))`
			args = append(args, id)
		}
	default:
		return tx.Where("1=0")
	}
	return tx.Where(`(NOT EXISTS (SELECT 1 FROM user_scope_policies policy WHERE policy.user_id=? AND policy.restricted=TRUE) OR (`+predicate+`))`, args...)
}

// RequireFullCenterScope protects catalogs, origin identities and other center
// metadata that cannot be narrowed by a pipeline's selected table rules.
func UserHasFullCenterScope(ctx context.Context, db *gorm.DB, centerID uuidx.UUID) (bool, error) {
	var n int64
	err := ReadResourceScope(ctx, db.WithContext(ctx).Model(&struct{}{}).Table("centers"), ReadCenter).Where("centers.id=? AND centers.deleted_at IS NULL", centerID).Count(&n).Error
	if err != nil {
		return false, fmt.Errorf("repo: check center metadata scope: %w", err)
	}
	return n > 0, nil
}
