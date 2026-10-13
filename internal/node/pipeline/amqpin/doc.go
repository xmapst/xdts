// Package amqpin consumes a pipeline's incremental queue.
// A bounded transport callback transfers deliveries to one input coordinator,
// which owns decoding, continuity, reassembly, runtime barriers, positions and ACKs.
// Each original GTID commits independently; partial failure records actual commits
// and retains unconfirmed deliveries. Decode buffers remain owned until completion.
//
// DDL, fragments, watermarks, probes and runtime updates drain pending replay first.
// Snapshot catchup separates consumed and applied GTIDs. Table recovery durably
// retains whole dependent transactions and pauses SQL behind the replacement
// barrier; completion replays the journal before reporting convergence.
//
// Actor updates acquire ownership only between complete transactions or fragments.
// Shutdown cancels and joins the coordinator before releasing writers and buffers.
package amqpin
