package synclet

import "io"

// Destination is the interface to implement for creating a destination connector.
type Destination interface {
	// Spec returns the connector specification.
	Spec(logTracker LogTracker) (*ConnectorSpecification, error)
	// Check verifies the destination connectivity.
	Check(dstCfgPath string, logTracker LogTracker) error
	// Write receives messages from the source via inputReader and writes them to the destination.
	// Use tracker.State() to emit STATE messages confirming committed records.
	// Return nil for successful completion, error to signal failure.
	Write(dstCfgPath string, catalogPath string, inputReader io.Reader, tracker MessageTracker) error
}
