// Command minimal is the smallest source connector built on the SDK: one stream, full refresh.
package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/cli"
)

type config struct{}

type greetingSource struct{}

func (greetingSource) Spec(context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		ProtocolVersion:               "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{synclet.DestinationSyncModeOverwrite},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title: "Greeting source",
			Type:  "object",
		},
	}, nil
}

func (greetingSource) Check(context.Context, config) error {
	return nil
}

func (greetingSource) Discover(context.Context, config) (*synclet.Catalog, error) {
	return &synclet.Catalog{Streams: []synclet.Stream{{
		Name: "greetings",
		JSONSchema: synclet.ObjectSchema(map[synclet.PropertyName]synclet.PropertySpec{
			"text": {PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.String}}},
		}),
		SupportedSyncModes: []synclet.SyncMode{synclet.SyncModeFullRefresh},
	}}}, nil
}

func (greetingSource) Read(_ context.Context, _ config, catalog *synclet.ConfiguredCatalog, _ *synclet.InputState, em *synclet.Emitter) error {
	for _, stream := range catalog.Streams {
		if err := em.Record(stream.Stream.Descriptor(), map[string]any{"text": "hello"}); err != nil {
			return err
		}
	}
	return nil
}

func main() { cli.RunSource(greetingSource{}) }
