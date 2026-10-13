package iproto

import "context"

// PipelineRuntimeStatus is an immutable local lifecycle observation. InstanceID
// identifies one construction, including reconstructions within the same run.
type PipelineRuntimeStatus struct {
	PipelineID    string
	RunID         uint64
	ConfigVersion uint64
	Role          PipelineRole
	InstanceID    string
	State         PipelineState
}

type runtimeInstanceKey struct{}

func WithPipelineInstance(ctx context.Context, instanceID string) context.Context {
	return context.WithValue(ctx, runtimeInstanceKey{}, instanceID)
}

func PipelineInstance(ctx context.Context) string {
	value, _ := ctx.Value(runtimeInstanceKey{}).(string)
	return value
}
