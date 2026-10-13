package model

import (
	"errors"
	"fmt"
)

var ErrPipelineTransition = errors.New("pipeline lifecycle transition is not allowed")

// ValidateDesiredTransition validates an intent, independently of asynchronous
// node progress. Running still requires the HTTP precheck/topology gates.
func (p Pipeline) ValidateDesiredTransition(want DesiredState) error {
	if !want.Valid() {
		return fmt.Errorf("%w: invalid desired state %q", ErrPipelineTransition, want)
	}
	if want != DesiredStatePaused || p.DesiredState == DesiredStatePaused ||
		p.DesiredState == DesiredStateRunning || p.LastCmdSeq != 0 {
		return nil
	}
	neverStarted := func(s ActualState) bool {
		return s == ActualStateDraft || s == ActualStatePrechecking
	}
	if neverStarted(p.SourceActualState) && neverStarted(p.TargetActualState) {
		return fmt.Errorf("%w: pipeline has never been authorized to run", ErrPipelineTransition)
	}
	return nil
}

// PipelineTransitionSuccessors describes direct lifecycle edges. Returning a
// fresh slice gives callers independent ownership of the immutable rules.
func PipelineTransitionSuccessors(from ActualState) []ActualState {
	switch from {
	case ActualStateDraft:
		return []ActualState{ActualStatePrechecking, ActualStateDeploying}
	case ActualStatePrechecking:
		return []ActualState{ActualStateDraft, ActualStateDeploying}
	case ActualStateDeploying:
		return []ActualState{ActualStateMigrating, ActualStateSnapshotting, ActualStateRunning, ActualStateError}
	case ActualStateMigrating:
		return []ActualState{ActualStateSnapshotting, ActualStateRunning, ActualStateStopped, ActualStateError}
	case ActualStateSnapshotting:
		return []ActualState{ActualStateCatchingUp, ActualStatePaused, ActualStateStopped, ActualStateError}
	case ActualStateCatchingUp:
		return []ActualState{ActualStateRunning, ActualStatePaused, ActualStateStopped, ActualStateError}
	case ActualStateRunning:
		return []ActualState{ActualStatePaused, ActualStateStopped, ActualStateError}
	case ActualStatePaused:
		return []ActualState{ActualStateDeploying, ActualStateSnapshotting, ActualStateCatchingUp, ActualStateRunning, ActualStateStopped, ActualStateError}
	case ActualStateError:
		return []ActualState{ActualStateDeploying, ActualStateSnapshotting, ActualStateCatchingUp, ActualStateRunning, ActualStatePaused, ActualStateStopped}
	case ActualStateStopped:
		return []ActualState{ActualStateDeploying}
	default:
		return nil
	}
}

// CanObservePipelineTransition permits coalesced forward phases. Terminal or
// externally commanded states cannot be intermediate steps: Running cannot
// reach Snapshotting by silently inventing a stop and another start.
func CanObservePipelineTransition(from, to ActualState) bool {
	if !from.Valid() || !to.Valid() || to == ActualStateUnknown {
		return false
	}
	if from == to || from == ActualStateUnknown {
		return true
	}
	seen := map[ActualState]bool{from: true}
	queue := []ActualState{from}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, next := range PipelineTransitionSuccessors(current) {
			if next == to {
				return true
			}
			if seen[next] {
				continue
			}
			seen[next] = true
			switch next {
			case ActualStateDeploying, ActualStateMigrating, ActualStateSnapshotting, ActualStateCatchingUp:
				queue = append(queue, next)
			}
		}
	}
	return false
}
