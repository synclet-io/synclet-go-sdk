# synclet-go-sdk

A small Go SDK for writing connectors that speak the Airbyte protocol (v0.5.2). You implement a typed
`Source` or `Destination` interface; the runner handles the `spec`, `check`, `discover`, `read` and `write`
commands and writes protocol messages to stdout. The SDK is used by [Synclet](https://github.com/synclet-io/synclet)
connectors and works with any orchestrator that runs Airbyte protocol connectors as Docker images.

This module supersedes [`github.com/saturn4er/airbyte-go-sdk`](https://github.com/saturn4er/airbyte-go-sdk).
To migrate, replace the import path with `github.com/synclet-io/synclet-go-sdk`; the package is named `synclet`.

## Install

```bash
go get github.com/synclet-io/synclet-go-sdk@v0.1.0
```

Requires Go 1.26 or newer.

## Minimal source

A source with one stream that emits one record per sync. The same program lives in
[`examples/minimal`](examples/minimal/main.go) and is compiled by CI.

```go
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
```

Build and run it like any connector:

```bash
go build -o greeting ./examples/minimal
./greeting spec
./greeting check --config config.json
./greeting discover --config config.json
./greeting read --config config.json --catalog catalog.json
```

## What is in the module

- `Source` and `Destination` interfaces, run by `NewSourceRunner` and `NewDestinationRunner`.
- `MessageTracker`: thread-safe writers for records, state (STREAM, GLOBAL, LEGACY), logs, traces and control messages.
- Protocol types: connector specification, catalog, configured catalog, stream state, trace and estimate messages.
- `InferSchemaFromStruct` and the `schema` package to derive a stream's JSON schema from a Go struct.
- `LoadStreamStates` and `UnmarshalFromPath` to read the input state and config files.

[`examples/httpsource`](examples/httpsource) is a fuller source that reads an HTTP API, emits estimates and state.

## Development

```bash
go vet ./...
go test -race -count=1 ./...
golangci-lint run
```

## License

MIT, see [LICENSE](LICENSE).

Airbyte is a trademark of Airbyte, Inc.; this project is not affiliated with or endorsed by Airbyte, Inc.
