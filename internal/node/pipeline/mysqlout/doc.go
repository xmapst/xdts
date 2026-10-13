// Package mysqlout applies source transactions and snapshot chunks to MySQL.
// Each Writer owns a physical connection and is used serially. Parallel workers
// have independent connections; the caller owns their scheduling and lifetime.
//
// Native GTIDs provide commit deduplication. DML and DDL preserve original GTIDs,
// while snapshot chunks and recovery cleanup use disjoint synthetic ranges.
// Persistent endpoint lineage is committed with copied data to prevent loops.
//
// Mappings, generated-column projection, conflict policy, trigger guards and
// session settings are checked on the actual target. Snapshot catchup uses source
// replacement semantics. Recovery journals retain complete cross-table transactions;
// ACK and checkpoint decisions belong to the incremental consumer.
package mysqlout
