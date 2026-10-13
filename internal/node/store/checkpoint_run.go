package store

import (
	"encoding/json"
	"math"
	"strconv"
)

// MatchesCheckpointRun validates the run identity in a persisted checkpoint.
// Missing or malformed identities never match.
func MatchesCheckpointRun(doc map[string]any, run uint64) bool {
	value, valid := CheckpointRun(doc)
	return valid && value == run
}

// CheckpointRun reads the required run identity from a persisted checkpoint.
func CheckpointRun(doc map[string]any) (uint64, bool) {
	switch n := doc["run_id"].(type) {
	case nil:
		return 0, false
	case uint64:
		return n, true
	case int64:
		return uint64(n), n >= 0
	case int:
		return uint64(n), n >= 0
	case float64:
		return uint64(n), n >= 0 && n <= 1<<53-1 && n == math.Trunc(n)
	case json.Number:
		value, err := strconv.ParseUint(string(n), 10, 64)
		return value, err == nil
	default:
		return 0, false
	}
}
