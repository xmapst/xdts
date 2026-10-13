package iproto

import (
	"errors"
	"time"

	"google.golang.org/protobuf/proto"
)

type PublicationWindowLimits struct {
	Batches int
	Bytes   uint64
	Timeout time.Duration
}

func ClonePublicationWindow(policy *IncrementalWindowPolicy) *IncrementalWindowPolicy {
	if policy == nil {
		return nil
	}
	return proto.Clone(policy).(*IncrementalWindowPolicy)
}

func NormalizePublicationWindow(policy *IncrementalWindowPolicy) (PublicationWindowLimits, error) {
	if policy.GetMaxBatches() > 128 {
		return PublicationWindowLimits{}, errors.New("iproto: publication window exceeds batch bounds")
	}
	limits := PublicationWindowLimits{Batches: max(1, int(policy.GetMaxBatches())), Bytes: policy.GetMaxBytes(), Timeout: time.Duration(policy.GetReorderTimeoutMs()) * time.Millisecond}
	if limits.Batches > 128 || limits.Bytes > 1<<30 || (limits.Timeout != 0 && (limits.Timeout < 100*time.Millisecond || limits.Timeout > 10*time.Minute)) {
		return PublicationWindowLimits{}, errors.New("iproto: publication window exceeds batch, byte or timeout bounds")
	}
	if limits.Bytes == 0 {
		limits.Bytes = 64 << 20
	}
	if limits.Timeout == 0 {
		limits.Timeout = time.Minute
	}
	return limits, nil
}

func ValidatePublicationWindowMetadata(batch *DataBatch) error {
	first, policy := batch.GetPublicationWindowFirstSeq(), batch.GetPublicationWindowPolicy()
	if first == 0 && policy == nil {
		return nil
	}
	limits, err := NormalizePublicationWindow(policy)
	if err != nil || first == 0 || policy == nil || limits.Batches <= 1 || first > batch.GetBatchSeq() || batch.GetBatchSeq() >= 1<<63 || batch.GetEpoch() == 0 || batch.GetEpoch() >= 1<<63 || batch.GetChunk() != nil {
		return errors.New("iproto: invalid publication window generation metadata")
	}
	for _, txn := range batch.GetTxns() {
		if txn.GetDdl() != nil {
			return errors.New("iproto: publication window cannot contain DDL")
		}
	}
	return nil
}
