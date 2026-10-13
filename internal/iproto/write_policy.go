package iproto

import "errors"

const (
	MaxReplayDegree   = 128
	MaxWriteBatchSize = 10000
)

func ValidateWritePolicy(policy *WritePolicy) error {
	if policy.GetParallelReplayDegree() > MaxReplayDegree || policy.GetBatchSize() > MaxWriteBatchSize {
		return errors.New("iproto: write batch size must not exceed 10000 and replay degree must not exceed 128")
	}
	return nil
}

func ReplayDegree(policy *WritePolicy) int { return max(1, int(policy.GetParallelReplayDegree())) }

func WriteBatchSize(policy *WritePolicy) int {
	if policy.GetBatchSize() == 0 {
		return 500
	}
	return int(policy.GetBatchSize())
}
