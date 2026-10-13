package store

import (
	"encoding/json"
	"math"
	"strconv"
)

// CheckpointPositionRevision reads the required position revision. Invalid or
// missing persisted values never authorize a checkpoint restore.
func CheckpointPositionRevision(doc map[string]any) (uint64, bool) {
	v, ok := doc["position_revision"]
	if !ok || v == nil {
		return 0, false
	}
	return checkpointUnsigned(v)
}

func CheckpointEpoch(doc map[string]any) (uint64, bool) {
	return checkpointUnsigned(doc["epoch"])
}

func CheckpointEpochCeiling(doc map[string]any) (uint64, bool) {
	return checkpointUnsigned(doc["epoch_ceiling"])
}

func CheckpointBatchSequence(doc map[string]any) (uint64, bool) {
	return checkpointUnsigned(doc["batch_seq"])
}

// xdoc omits empty strings. The explicit known marker preserves an empty GTID
// without interpreting a missing field as a known empty position.
func CheckpointPublishedGTID(doc map[string]any) (string, bool) {
	if value, present := doc["published_gtid_set"]; present {
		set, valid := value.(string)
		return set, valid
	}
	known, valid := doc["published_gtid_set_known"].(bool)
	return "", valid && known
}

func checkpointUnsigned(v any) (uint64, bool) {
	switch n := v.(type) {
	case nil:
		return 0, false
	case uint64:
		return n, n <= math.MaxInt64
	case int64:
		return uint64(n), n >= 0
	case int:
		return uint64(n), n >= 0
	case float64:
		return uint64(n), n >= 0 && n <= 1<<53-1 && n == math.Trunc(n)
	case json.Number:
		x, err := strconv.ParseUint(string(n), 10, 63)
		return x, err == nil
	default:
		return 0, false
	}
}
