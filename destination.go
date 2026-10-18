package synclet

import "context"

// Destination is a connector that writes data. C is its configuration
// struct; the runner decodes the configuration JSON into it before every call.
type Destination[C any] interface {
	// Spec returns the connector's capabilities and configuration schema.
	Spec(ctx context.Context) (*ConnectorSpecification, error)
	// Check verifies that cfg reaches the target. A non-nil error becomes a
	// FAILED connection status whose message is err.Error().
	Check(ctx context.Context, cfg C) error
	// Write consumes the record stream from in, echoes each STATE message
	// through em once the records before it are committed, and returns nil
	// at EOF after its last flush.
	Write(ctx context.Context, cfg C, catalog *ConfiguredCatalog, in *MessageReader, em *Emitter) error
}
