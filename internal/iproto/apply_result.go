package iproto

// BatchApplyResult distinguishes MySQL-committed GTIDs from transactions
// intentionally rolled back by a conflict policy. Ownership belongs to caller.
type BatchApplyResult struct {
	AppliedGTIDs  []string
	RejectedGTIDs []string
	// Durably held whole transactions; never counted as MySQL-applied GTIDs.
	DeferredGTIDs []string
}
