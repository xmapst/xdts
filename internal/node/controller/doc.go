// Package controller owns Node configuration and pipeline lifecycle.
// Control messages are verified before dispatch. Configuration is pulled, compared
// with the last successfully applied state, then assembled through background
// workflows; completion returns to the actor before authoritative state changes.
//
// Applied configuration, command identities and lifecycle intent are persisted
// before reporting success. Startup reads those records before enabling writes.
// Source and target roles have separate ownership and migration state. Runtime
// updates drain complete transactions or snapshot reads before changing policy.
//
// Shared connection, publisher and store handles belong to the process. Shutdown
// joins pipeline work before releasing them. Probe requests transfer delivery
// ownership through asynchronous messages.
package controller
