// Package schemaplan generates migration plans from authenticated source and
// target definitions. Build is pure: it only reads caller-owned inputs and
// returns independent plan data. It does not confirm or execute a plan.
//
// Current behavior includes whole-scope mapped-name collision checks, source
// definition identity validation, mapped table column/key comparison, stable
// view dependency sorting, incompatible-target blocking, and SQL export with
// delimiter selection that cannot collide with stored bodies.
//
// Cyclic views and views with CTE dependencies require further placeholder or
// scope binding support and currently block. Column projection for views and
// routines also blocks. Table foreign keys are retained by the existing
// per-object rewriter; splitting them for execution after snapshot convergence
// belongs to the execution-plan integration. Preview persistence, confirmation,
// per-run UUID registration and Node execution are added by the orchestration
// layer; these capabilities are not implied by Build succeeding.
package schemaplan
