// Package schema maintains source table structure from DDL and read-only catalog
// metadata. Tracker state belongs to the source session; Snapshot returns deep
// copies for persistence and Restore validates the identifier comparison policy.
//
// Generated columns, keys, renames and drop operations update the same table model.
// Persisted snapshots carry the GTID boundary supplied by the caller. Disk keys
// are part of the published state contract.
package schema
