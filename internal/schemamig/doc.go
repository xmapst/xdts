// Package schemamig rewrites source schema definitions for the target.
// Tables and views use TiDB ASTs; stored programs use the MySQL grammar. Mappings,
// DDL policy, definers, trigger guards and dependency order are applied explicitly.
//
// Unsupported or ambiguous rewrites fail closed. Source definitions and endpoint
// case sensitivity remain authoritative. Parser registration preserves MySQL
// charset/collation names, including utf8mb3 fidelity markers.
package schemamig
