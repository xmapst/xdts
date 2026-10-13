package topologycheck

import (
	"errors"
	"fmt"
)

var ErrBidirectionalInitialization = errors.New("invalid bidirectional initialization")

type InitializationEdge struct {
	ID              string
	Source          string
	Target          string
	Mode            string
	SchemaMigration bool
}

func (e InitializationEdge) initializes() bool {
	return e.SchemaMigration || e.Mode == "full_and_incremental" || e.Mode == "full_only" || e.Mode == "schema_only"
}

// ValidateInitialization applies NODE-SNAP-16 to the reverse center direction.
// Two purely incremental directions can retain their independently chosen starts.
func ValidateInitialization(candidate InitializationEdge, edges []InitializationEdge) error {
	for _, other := range edges {
		if other.ID == candidate.ID || other.Source != candidate.Target || other.Target != candidate.Source {
			continue
		}
		if !candidate.initializes() && !other.initializes() {
			continue
		}
		if candidate.initializes() && other.initializes() {
			return fmt.Errorf("%w: %s and %s both initialize", ErrBidirectionalInitialization, candidate.ID, other.ID)
		}
		reverse := other
		if other.initializes() {
			reverse = candidate
		}
		if reverse.Mode != "incremental_only_latest" {
			return fmt.Errorf("%w: reverse %s must start at latest", ErrBidirectionalInitialization, reverse.ID)
		}
	}
	return nil
}
