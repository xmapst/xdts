// Package buildinfo exposes immutable linker metadata for release artifacts.
package buildinfo

import "fmt"

// Set only by the Go linker; never changed by runtime application code.
var version = "dev"
var revision = "unknown"

func Version() string  { return version }
func Revision() string { return revision }
func String() string   { return fmt.Sprintf("%s (%s)", version, revision) }
