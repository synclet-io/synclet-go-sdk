# synclet-go-sdk

A small Go SDK for writing connectors that speak the Airbyte protocol (v0.5.2). You implement a typed
`Source` or `Destination` interface; package `cli` runs it as a process that handles the `spec`, `check`,
`discover`, `read` and `write` commands and writes protocol messages to stdout. The SDK is used by
[Synclet](https://github.com/synclet-io/synclet) connectors and works with any orchestrator that runs Airbyte
protocol connectors as Docker images.

The SDK covers only what the protocol needs: context, check result, record and state emission, logging and
the transport. HTTP clients, pagination, SQL and retries are ordinary Go libraries.

This module supersedes [`github.com/saturn4er/airbyte-go-sdk`](https://github.com/saturn4er/airbyte-go-sdk).
To migrate, replace the import path with `github.com/synclet-io/synclet-go-sdk` (package `synclet`) and
follow [Migrating from v0.1](#migrating-from-v01).

## Install

```bash
go get github.com/synclet-io/synclet-go-sdk@v0.3.0
```

Requires Go 1.26 or newer.

## Interfaces

```go
type Source[C any] interface {
	Spec(ctx context.Context) (*ConnectorSpecification, error)
	Check(ctx context.Context, cfg C) error
	Discover(ctx context.Context, cfg C) (*Catalog, error)
	Read(ctx context.Context, cfg C, catalog *ConfiguredCatalog, state *InputState, em *Emitter) error
}

type Destination[C any] interface {
	Spec(ctx context.Context) (*ConnectorSpecification, error)
	Check(ctx context.Context, cfg C) error
	Write(ctx context.Context, cfg C, catalog *ConfiguredCatalog, in *MessageReader, em *Emitter) error
}
```

- `C` is the connector's own config struct. The runner decodes the config JSON into it before every call
  (unknown keys are ignored); a config that does not decode never reaches the connector.
- Every method takes a `context.Context`. SIGTERM and SIGINT cancel it; the runner then emits a TRACE ERROR
  and the process exits 1.
- `Check` returning nil is `CONNECTION_STATUS SUCCEEDED`. An error is `CONNECTION_STATUS FAILED` with
  `err.Error()` as the message the user sees, and exit 0.
- `Read` gets the previous state (never nil) and emits records and state through the `Emitter`. `Write` reads
  the incoming stream with `in.Next()` until `io.EOF` and echoes each STATE once the records before it are
  committed.
- `Emitter` is the only writer to the transport: `Record`, `StreamState`, `GlobalState`, `State`, `Log`,
  `Trace` and `Control`, safe from any number of goroutines, one unbuffered write per message.
- `ServeSource` and `ServeDestination` run one command over any `io.Writer`; they are the transport seam.
  Tests and other transports call them with a `Request` and a buffer.
- `Stream.JSONSchema` is raw JSON; `ObjectSchema(props)` builds `{"type":"object","properties":...}`.

## main

A connector's `main` hands the connector to `cli` and nothing else. `cli` is the only package that touches
`os.Args`, stdin, stdout and files. On Linux, macOS and the BSDs it moves the protocol stream to a
duplicate of descriptor 1 and points descriptor 1 at stderr before calling the connector, so a stray
`fmt.Println`, a logger holding `os.Stdout` or a raw write to descriptor 1 cannot corrupt the protocol
stream; elsewhere it points the `os.Stdout` variable at stderr. The smallest source, compiled by CI as
[`examples/minimal`](examples/minimal/main.go):

```go
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
```

```bash
go build -o greeting ./examples/minimal
./greeting spec
./greeting check --config config.json
./greeting discover --config config.json
./greeting read --config config.json --catalog catalog.json [--state state.json]
```

A destination built with `cli.RunDestination` runs `write --config config.json --catalog catalog.json`
with the record stream on stdin. Flags may come in any order. An unknown command, a missing flag or an unreadable file prints usage on stderr
and exits 2; a failed command, a cancellation or a panic exits 1 after one TRACE ERROR.
[`examples/httpsource`](examples/httpsource) is a fuller source that reads an HTTP API with the context,
emits estimates and per-stream state.

## State

`state.Stream(desc)` returns a stream's saved state as raw JSON (nil when there is none); decode your own
cursor struct from it. `InputState` accepts every shape Synclet writes: no state, STREAM entries, one GLOBAL
state (`Shared` plus `Streams`) and LEGACY state (`Legacy`), with type names in any case.

```go
for _, cs := range catalog.Streams {
	desc := cs.Stream.Descriptor()
	var cur cursor
	if raw := state.Stream(desc); raw != nil {
		if err := json.Unmarshal(raw, &cur); err != nil {
			return err
		}
	}
	// em.Record(desc, row) for each row, then:
	if err := em.StreamState(desc, cur); err != nil {
		return err
	}
}
```

Emit a state only after the records it covers. Synclet persists a state when the destination echoes it, so a
checkpoint every N rows bounds what a crash replays. CDC sources emit `em.GlobalState(position, perStream)`.

The Emitter counts the records it writes and sets `sourceStats.recordCount` on every STATE, as destinations on
the current Airbyte CDK require: a STREAM state carries its stream's records since that stream's previous STATE,
a GLOBAL or LEGACY state every record since the previous STATE.

## Logging

`synclet.Logger(ctx)` is a `*slog.Logger` whose lines reach Synclet as LOG messages (Debug, Info, Warn and
Error map to DEBUG, INFO, WARN and ERROR; attributes are appended as `key=value`). Outside a runner it is
`slog.Default()`. The cli also sets `slog.Default()` to the protocol logger, so libraries that log through
slog reach Synclet too; the `log` package writes to stderr.

## Errors

An error returned from a call becomes one TRACE ERROR with `err.Error()` as its message and exit 1. Wrap it
with `synclet.ConfigError(err)` when the configuration is at fault, which sets `failure_type: config_error`;
anything else is `system_error`. A panic becomes a TRACE ERROR with a stack trace; a cancelled context gives
`cancelled: <ctx error>`. A panic in a goroutine the connector started is not recovered.

## Testing

Package `testkit` runs a connector in memory through `ServeSource` and `ServeDestination` and parses what it
writes. One call checks the protocol contract of a source:

```go
func TestProtocol(t *testing.T) {
	var catalog synclet.ConfiguredCatalog
	raw, err := os.ReadFile("testdata/catalog.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &catalog))

	testkit.RunSourceSuite(t, mysource.New(), testkit.SourceSuite{
		Config:       json.RawMessage(`{"host":"localhost","port":5432}`),
		BrokenConfig: json.RawMessage(`{"host":"localhost","port":1}`),
		Catalog:      &catalog,
	})
}
```

The suite reports through `t.Errorf` when:

- the spec's `connectionSpecification` is not a valid JSON schema;
- check fails with `Config`, or does not fail with a message with `BrokenConfig`;
- discover returns no streams, or a stream whose `json_schema` is invalid or has no properties;
- read emits a record of a stream the catalog does not configure, or one that does not validate against its
  stream's `json_schema` or has a top-level field the schema does not declare (unless it sets
  `additionalProperties`);
- a record follows its stream's last STATE, or an incremental stream emits records and no STATE;
- a STATE lacks `sourceStats.recordCount`, or its count differs from the records it covers;
- an incremental stream emits records none of which has a value at its cursor field, or a second read from
  the first read's last STATE emits a record whose cursor is older than the greatest cursor of the first
  read. The cursor is `default_cursor_field` for a source-defined cursor and the configured `cursor_field`
  otherwise; numbers, RFC 3339 timestamps and strings compare out of the box, and
  `SourceSuite.CompareCursor` orders anything else. A source that re-reads a window before its saved
  cursor sets `SourceSuite.Lookback` to return the oldest cursor value the second read may emit;
- read keeps running 5 seconds after its context is cancelled at its first message. This catches a read that
  blocks, sleeps or retries past cancellation. A read that ignores its context but finishes its fixtures
  sooner passes, so to exercise the check, make the fixtures outlast the first message, for example with a
  test server that holds a response until the request's context ends.

The connector brings its own fixtures (an HTTP test server, a database container) with ordinary libraries;
`testkit` only drives the protocol. For checks of its own, a test runs one command and inspects the output:

```go
out, err := testkit.RunSource(ctx, src, synclet.Request{Command: synclet.CommandRead, Config: cfg, Catalog: cat})
require.NoError(t, err)
require.NoError(t, out.Err)
users := out.Records(synclet.StreamDescriptor{Name: "users", Namespace: "public"})
states, logs := out.States(), out.Logs()
next := out.NextState() // the state file of the following read
```

`testkit.RunDestination` does the same for a destination with the record stream in `Request.Input`.

## Migrating from v0.1

v0.2.0 removes the v0.1 runner API with no shims.

| v0.1 | v0.2 |
|---|---|
| `synclet.NewSourceRunner(src, os.Stdout).Start()` | `cli.RunSource(src)` |
| `synclet.NewDestinationRunner(dst, os.Stdout).Start()` | `cli.RunDestination(dst)` |
| `Check(path string, logTracker)` with `UnmarshalFromPath` | `Check(ctx, cfg Config)`; the config arrives decoded |
| `logTracker.Log(level, msg)` | `synclet.Logger(ctx).Info(msg, "key", value)` |
| `context.Background()` | the `ctx` parameter |
| `tracker.Record(data, name, ns)` | `em.Record(desc, data)` |
| `tracker.State(StateTypeStream, ...)`, `LoadStreamStates` | `em.StreamState(desc, v)`, `state.Stream(desc)` |
| GLOBAL state by hand | `em.GlobalState(shared, perStream)`, `state.Shared` |
| `EmitEstimate`, `EmitError` | `em.Trace(TraceMessage{...})`; return the error for a fatal failure |
| `bufio.Scanner` over stdin in `Write` | `in.Next()` |
| `JSONSchema: synclet.Properties{...}` | `JSONSchema: synclet.ObjectSchema(props)` |
| `FormatType` `"datetime"` | `synclet.DateTime` (`"date-time"`), `Date`, `Time` |
| `AnalyticsTrace.Value` map | string |
| `InferSchemaFromStruct(v, logTracker) Properties` | `InferSchemaFromStruct(v) (Properties, error)` |

## Development

```bash
go vet ./...
go test -race -count=1 ./...
golangci-lint run
```

## License

MIT, see [LICENSE](LICENSE).

Airbyte is a trademark of Airbyte, Inc.; this project is not affiliated with or endorsed by Airbyte, Inc.
