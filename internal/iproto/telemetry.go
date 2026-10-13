package iproto

// PipelineTelemetry is an internal cumulative observation, never a wire rate.
// Each component owns its counters; every message and nested value is immutable
// after Cast. InstanceID changes when a component is reconstructed in the same run.
type PipelineTelemetry struct {
	PipelineID             string
	RunID                  uint64
	Role                   PipelineRole
	Component              string
	InstanceID             string
	ObservedTSMS           int64
	Rows                   uint64
	Txns                   uint64
	Bytes                  uint64
	Errors                 uint64
	Conflicts              uint64
	InflightBatches        uint32
	InflightBytes          uint64
	ReadGTIDSet            string
	PublishedGTIDSet       string
	AppliedGTIDSet         string
	SourceEventTSMS        int64
	LagMS                  float64
	QueueModule            string
	QueueLen               uint32
	QueueCap               uint32
	SourceNodeID           string
	SourceEpoch            uint64
	SourceBatchSeq         uint64
	SourcePositionRevision uint64
	HasSourceCheckpoint    bool
}

const (
	TelemetrySourceReader    = "source_reader"
	TelemetrySourcePublisher = "source_publisher"
	TelemetryTargetConsumer  = "target_consumer"
	TelemetrySourceSnapshot  = "source_snapshot"
	TelemetryTargetSnapshot  = "target_snapshot"
)
