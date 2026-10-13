// Package pipelinespec assembles independent source and target configurations.
// Persisted pipelines, endpoints, strategies and topology are the input authority.
// Rules, capabilities, endpoint identity and publication limits are validated
// before projecting role-specific configurations and computing their digests.
//
// Both assigned nodes must support optional contracts. Snapshot and recovery
// identities remain immutable within a run; confirmed reinitialization and signed
// migration state are bound to endpoint, role, configuration and run identities.
package pipelinespec
