package synclet

import "context"

// Source is a connector that reads data. C is its configuration struct; the
// runner decodes the configuration JSON into it before every call.
type Source[C any] interface {
	// Spec returns the connector's capabilities and configuration schema.
	Spec(ctx context.Context) (*ConnectorSpecification, error)
	// Check verifies that cfg reaches the data. A non-nil error becomes a
	// FAILED connection status whose message is err.Error().
	Check(ctx context.Context, cfg C) error
	// Discover returns the streams cfg gives access to.
	Discover(ctx context.Context, cfg C) (*Catalog, error)
	// Read emits the configured streams' records and state through em and
	// returns nil when every stream is done. state is never nil.
	Read(ctx context.Context, cfg C, catalog *ConfiguredCatalog, state *InputState, em *Emitter) error
}
