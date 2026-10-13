// Package rule compiles source filtering and transformation policy.
// Compiled plans are immutable and reusable. Schema/table comparison follows the
// source lower_case_table_names setting; column names are case insensitive.
//
// Exclusions take precedence over inclusions. Row filters run before projection,
// and updates crossing a filter boundary become inserts or deletes. Expressions
// use bounded evaluation and cannot mutate source data or call external services.
// Snapshot and incremental readers share the same policy semantics.
package rule
