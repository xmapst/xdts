package iproto

import (
	"errors"
	"time"
)

// TargetErrorPolicy is an independently owned local value. Retries excludes
// the initial attempt. Configured distinguishes explicit zero from defaults.
type TargetErrorPolicy struct {
	Configured                 bool
	Retries                    uint32
	InitialBackoff, MaxBackoff time.Duration
	SkipUnretryable            bool
}

func TargetErrorPolicyFromProto(policy *ErrorPolicy) (TargetErrorPolicy, error) {
	if policy == nil {
		return TargetErrorPolicy{}, nil
	}
	value := TargetErrorPolicy{Configured: true, Retries: policy.GetRetryableMaxRetries(), InitialBackoff: time.Duration(policy.GetRetryBackoffMs()) * time.Millisecond, MaxBackoff: time.Duration(policy.GetRetryBackoffMaxMs()) * time.Millisecond, SkipUnretryable: policy.GetSkipAndLogUnretryable()}
	if value.InitialBackoff == 0 {
		value.InitialBackoff = 500 * time.Millisecond
	}
	if value.MaxBackoff == 0 {
		value.MaxBackoff = 30 * time.Second
	}
	return value, ValidateTargetErrorPolicy(value)
}

func ValidateTargetErrorPolicy(policy TargetErrorPolicy) error {
	if !policy.Configured {
		if policy != (TargetErrorPolicy{}) {
			return errors.New("target: unconfigured error policy must be empty")
		}
		return nil
	}
	if policy.InitialBackoff <= 0 || policy.MaxBackoff < policy.InitialBackoff || policy.MaxBackoff > time.Duration(^uint32(0))*time.Millisecond {
		return errors.New("target: invalid error retry policy")
	}
	return nil
}
