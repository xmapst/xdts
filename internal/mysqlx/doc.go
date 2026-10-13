// Package mysqlx centralizes MySQL version capabilities and binlog compatibility.
// Capability probes determine supported commands and session behavior. Row decoding
// preserves integer widths, generated-column metadata and partial JSON updates;
// unsupported event shapes fail rather than silently losing changes.
package mysqlx
