// Package probe executes authenticated inspection requests on behalf of Manager.
// Controller transfers each verified request and delivery handle asynchronously;
// probe owns execution, result publication and acknowledgement after that transfer.
//
// Requests use endpoint-local credentials and bounded operations. Database probes
// read metadata without changing global settings. Replies carry current identity,
// configuration and request IDs; unavailable or unsupported operations return
// explicit errors. Shared transport and store handles belong to the process.
package probe
