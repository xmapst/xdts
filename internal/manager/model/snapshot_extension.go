package model

// A source/target observation owns this independent immutable value.
type SnapshotPlanExtensionProgress struct{ Sequence, TotalChunks, DoneChunks uint64 }
