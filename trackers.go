package synclet

// MessageTracker is used to encap State tracking, Record tracking and Log tracking
// It's thread safe
type MessageTracker struct {
	// State saves typed state (STREAM, GLOBAL, or LEGACY) - protocol v0.5.2
	State StateWriter
	// Record will emit a record (data point) out to airbyte to sync with appropriate timestamps
	Record RecordWriter
	// Log logs out to airbyte
	Log LogWriter
	// Trace emits runtime metadata (errors, estimates, analytics) - protocol v0.5.2
	Trace TraceWriter
	// Control allows connectors to update configuration mid-sync - protocol v0.5.2
	Control ControlWriter
}

// LogTracker is a single struct which holds a tracker which can be used for logs
type LogTracker struct {
	Log LogWriter
}
