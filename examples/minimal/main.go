// Command minimal is the smallest source connector built on the SDK: one stream, full refresh.
package main

import (
	"log"
	"os"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

type greetingSource struct{}

func (greetingSource) Spec(synclet.LogTracker) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		ProtocolVersion:               "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{synclet.DestinationSyncModeOverwrite},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title: "Greeting source",
			Type:  "object",
		},
	}, nil
}

func (greetingSource) Check(string, synclet.LogTracker) error {
	return nil
}

func (greetingSource) Discover(string, synclet.LogTracker) (*synclet.Catalog, error) {
	return &synclet.Catalog{Streams: []synclet.Stream{{
		Name: "greetings",
		JSONSchema: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
			"text": {PropertyType: synclet.PropertyType{Type: []synclet.PropType{synclet.String}}},
		}},
		SupportedSyncModes: []synclet.SyncMode{synclet.SyncModeFullRefresh},
	}}}, nil
}

func (greetingSource) Read(_, _ string, catalog *synclet.ConfiguredCatalog, tracker synclet.MessageTracker) error {
	for _, stream := range catalog.Streams {
		if err := tracker.Record(map[string]any{"text": "hello"}, stream.Stream.Name, stream.Stream.Namespace); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	if err := synclet.NewSourceRunner(greetingSource{}, os.Stdout).Start(); err != nil {
		log.Fatal(err)
	}
}
