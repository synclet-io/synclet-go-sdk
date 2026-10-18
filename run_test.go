package synclet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testConfig struct {
	Host string `json:"host"`
}

type fakeSource struct {
	check func(ctx context.Context, cfg testConfig) error
	read  func(ctx context.Context, cfg testConfig, cat *ConfiguredCatalog, st *InputState, em *Emitter) error
}

func (fakeSource) Spec(context.Context) (*ConnectorSpecification, error) {
	return &ConnectorSpecification{ProtocolVersion: "0.5.2", ConnectionSpecification: ConnectionSpecification{Title: "fake", Type: "object"}}, nil
}

func (s fakeSource) Check(ctx context.Context, cfg testConfig) error {
	if s.check != nil {
		return s.check(ctx, cfg)
	}
	return nil
}

func (fakeSource) Discover(_ context.Context, cfg testConfig) (*Catalog, error) {
	return &Catalog{Streams: []Stream{{Name: cfg.Host, JSONSchema: ObjectSchema(nil), SupportedSyncModes: []SyncMode{SyncModeFullRefresh}}}}, nil
}

func (s fakeSource) Read(ctx context.Context, cfg testConfig, cat *ConfiguredCatalog, st *InputState, em *Emitter) error {
	return s.read(ctx, cfg, cat, st, em)
}

const (
	testCfg     = `{"host":"db"}`
	testCatalog = `{"streams":[{"stream":{"name":"users","json_schema":{"type":"object"}},"sync_mode":"incremental","destination_sync_mode":"append"}]}`
)

func serveSource(t *testing.T, ctx context.Context, src fakeSource, req Request) ([]Message, error) {
	t.Helper()
	var out bytes.Buffer
	err := ServeSource[testConfig](ctx, src, req, &out)
	return decodeMessages(t, out.Bytes()), err
}

func traceErrors(msgs []Message) []*ErrorTrace {
	var errs []*ErrorTrace
	for _, m := range msgs {
		if m.Type == MessageTypeTrace && m.Trace.Type == TraceTypeError {
			errs = append(errs, m.Trace.Error)
		}
	}
	return errs
}

func TestServeSource(t *testing.T) {
	ctx := context.Background()

	t.Run("spec", func(t *testing.T) {
		msgs, err := serveSource(t, ctx, fakeSource{}, Request{Command: CommandSpec})
		require.NoError(t, err)
		require.Len(t, msgs, 1)
		assert.Equal(t, MessageTypeSpec, msgs[0].Type)
		assert.Equal(t, "fake", msgs[0].Spec.ConnectionSpecification.Title)
	})

	t.Run("check ok", func(t *testing.T) {
		var got testConfig
		src := fakeSource{check: func(_ context.Context, cfg testConfig) error { got = cfg; return nil }}
		msgs, err := serveSource(t, ctx, src, Request{Command: CommandCheck, Config: json.RawMessage(testCfg)})
		require.NoError(t, err)
		require.Len(t, msgs, 1)
		assert.Equal(t, MessageTypeConnectionStatus, msgs[0].Type)
		assert.Equal(t, ConnectionStatus{Status: ConnectionStatusSucceeded}, *msgs[0].ConnectionStatus)
		assert.Equal(t, testConfig{Host: "db"}, got)
	})

	t.Run("check failed", func(t *testing.T) {
		src := fakeSource{check: func(context.Context, testConfig) error { return errors.New("bad password") }}
		msgs, err := serveSource(t, ctx, src, Request{Command: CommandCheck, Config: json.RawMessage(testCfg)})
		require.NoError(t, err)
		require.Len(t, msgs, 1)
		assert.Equal(t, ConnectionStatus{Status: ConnectionStatusFailed, Message: "bad password"}, *msgs[0].ConnectionStatus)
	})

	t.Run("check undecodable config", func(t *testing.T) {
		called := false
		src := fakeSource{check: func(context.Context, testConfig) error { called = true; return nil }}
		msgs, err := serveSource(t, ctx, src, Request{Command: CommandCheck, Config: json.RawMessage(`{"host":5}`)})
		require.NoError(t, err)
		require.Len(t, msgs, 1)
		assert.Equal(t, ConnectionStatusFailed, msgs[0].ConnectionStatus.Status)
		assert.True(t, strings.HasPrefix(msgs[0].ConnectionStatus.Message, "invalid configuration"), msgs[0].ConnectionStatus.Message)
		assert.False(t, called)
	})

	t.Run("check cancelled", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		src := fakeSource{check: func(ctx context.Context, _ testConfig) error { cancel(); return ctx.Err() }}
		msgs, err := serveSource(t, cctx, src, Request{Command: CommandCheck, Config: json.RawMessage(testCfg)})
		require.Error(t, err)
		require.Len(t, msgs, 1)
		errs := traceErrors(msgs)
		require.Len(t, errs, 1)
		assert.Equal(t, "cancelled: context canceled", errs[0].Message)
	})

	t.Run("discover", func(t *testing.T) {
		msgs, err := serveSource(t, ctx, fakeSource{}, Request{Command: CommandDiscover, Config: json.RawMessage(testCfg)})
		require.NoError(t, err)
		require.Len(t, msgs, 1)
		assert.Equal(t, MessageTypeCatalog, msgs[0].Type)
		assert.Equal(t, "db", msgs[0].Catalog.Streams[0].Name)
	})

	t.Run("read", func(t *testing.T) {
		src := fakeSource{read: func(_ context.Context, _ testConfig, cat *ConfiguredCatalog, st *InputState, em *Emitter) error {
			desc := cat.Streams[0].Stream.Descriptor()
			var cur struct{ Cursor int }
			if err := json.Unmarshal(st.Stream(desc), &cur); err != nil {
				return err
			}
			for i := 1; i <= 3; i++ {
				if err := em.Record(desc, map[string]int{"id": cur.Cursor + i}); err != nil {
					return err
				}
			}
			return em.StreamState(desc, map[string]int{"Cursor": cur.Cursor + 3})
		}}
		msgs, err := serveSource(t, ctx, src, Request{
			Command: CommandRead,
			Config:  json.RawMessage(testCfg),
			Catalog: json.RawMessage(testCatalog),
			State:   json.RawMessage(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"users"},"stream_state":{"Cursor":10}}}]`),
		})
		require.NoError(t, err)
		require.Len(t, msgs, 4)
		for i, m := range msgs[:3] {
			assert.Equal(t, MessageTypeRecord, m.Type)
			assert.JSONEq(t, fmt.Sprintf(`{"id":%d}`, 11+i), string(m.Record.Data))
		}
		assert.Equal(t, MessageTypeState, msgs[3].Type)
		assert.JSONEq(t, `{"Cursor":13}`, string(msgs[3].State.Stream.StreamState))
	})

	t.Run("read without state", func(t *testing.T) {
		var got *InputState
		src := fakeSource{read: func(_ context.Context, _ testConfig, _ *ConfiguredCatalog, st *InputState, _ *Emitter) error {
			got = st
			return nil
		}}
		_, err := serveSource(t, ctx, src, Request{Command: CommandRead, Config: json.RawMessage(testCfg), Catalog: json.RawMessage(testCatalog)})
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, StateType(""), got.Type)
	})

	t.Run("read config error", func(t *testing.T) {
		src := fakeSource{read: func(context.Context, testConfig, *ConfiguredCatalog, *InputState, *Emitter) error {
			return ConfigError(errors.New("unknown table"))
		}}
		msgs, err := serveSource(t, ctx, src, Request{Command: CommandRead, Config: json.RawMessage(testCfg), Catalog: json.RawMessage(testCatalog)})
		require.Error(t, err)
		errs := traceErrors(msgs)
		require.Len(t, errs, 1)
		assert.Equal(t, FailureTypeConfigError, errs[0].FailureType)
		assert.Equal(t, "unknown table", errs[0].Message)
		assert.Empty(t, errs[0].StackTrace)
	})

	t.Run("read system error", func(t *testing.T) {
		src := fakeSource{read: func(context.Context, testConfig, *ConfiguredCatalog, *InputState, *Emitter) error {
			return errors.New("connection reset")
		}}
		msgs, err := serveSource(t, ctx, src, Request{Command: CommandRead, Config: json.RawMessage(testCfg), Catalog: json.RawMessage(testCatalog)})
		require.Error(t, err)
		errs := traceErrors(msgs)
		require.Len(t, errs, 1)
		assert.Equal(t, FailureTypeSystemError, errs[0].FailureType)
	})

	t.Run("read panics", func(t *testing.T) {
		src := fakeSource{read: func(_ context.Context, _ testConfig, cat *ConfiguredCatalog, _ *InputState, em *Emitter) error {
			_ = em.Record(cat.Streams[0].Stream.Descriptor(), map[string]int{"id": 1})
			panic("nil map")
		}}
		msgs, err := serveSource(t, ctx, src, Request{Command: CommandRead, Config: json.RawMessage(testCfg), Catalog: json.RawMessage(testCatalog)})
		require.Error(t, err)
		require.Len(t, msgs, 2)
		errs := traceErrors(msgs)
		require.Len(t, errs, 1)
		assert.Contains(t, errs[0].Message, "nil map")
		assert.Contains(t, errs[0].StackTrace, "goroutine")
		assert.Equal(t, FailureTypeSystemError, errs[0].FailureType)
	})

	t.Run("read cancelled", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		src := fakeSource{read: func(ctx context.Context, _ testConfig, cat *ConfiguredCatalog, _ *InputState, em *Emitter) error {
			desc := cat.Streams[0].Stream.Descriptor()
			for i := 0; ; i++ {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := em.Record(desc, map[string]int{"id": i}); err != nil {
					return err
				}
				if i == 2 {
					cancel()
				}
			}
		}}
		msgs, err := serveSource(t, cctx, src, Request{Command: CommandRead, Config: json.RawMessage(testCfg), Catalog: json.RawMessage(testCatalog)})
		require.Error(t, err)
		require.Len(t, msgs, 4)
		assert.Equal(t, MessageTypeRecord, msgs[2].Type)
		require.Equal(t, MessageTypeTrace, msgs[3].Type)
		assert.Equal(t, "cancelled: context canceled", msgs[3].Trace.Error.Message)
	})

	t.Run("read logs through context", func(t *testing.T) {
		src := fakeSource{read: func(ctx context.Context, _ testConfig, _ *ConfiguredCatalog, _ *InputState, _ *Emitter) error {
			Logger(ctx).Info("reading", "stream", "users")
			return nil
		}}
		msgs, err := serveSource(t, ctx, src, Request{Command: CommandRead, Config: json.RawMessage(testCfg), Catalog: json.RawMessage(testCatalog)})
		require.NoError(t, err)
		require.Len(t, msgs, 1)
		assert.Equal(t, LogMessage{Level: LogLevelInfo, Message: "reading stream=users"}, *msgs[0].Log)
	})

	t.Run("read bad catalog", func(t *testing.T) {
		msgs, err := serveSource(t, ctx, fakeSource{}, Request{Command: CommandRead, Config: json.RawMessage(testCfg), Catalog: json.RawMessage(`{`)})
		require.Error(t, err)
		errs := traceErrors(msgs)
		require.Len(t, errs, 1)
		assert.Equal(t, FailureTypeSystemError, errs[0].FailureType)
		assert.True(t, strings.HasPrefix(errs[0].Message, "invalid catalog"), errs[0].Message)
	})

	t.Run("read undecodable config", func(t *testing.T) {
		msgs, err := serveSource(t, ctx, fakeSource{}, Request{Command: CommandRead, Config: json.RawMessage(`[]`), Catalog: json.RawMessage(testCatalog)})
		require.Error(t, err)
		errs := traceErrors(msgs)
		require.Len(t, errs, 1)
		assert.Equal(t, FailureTypeConfigError, errs[0].FailureType)
		assert.True(t, strings.HasPrefix(errs[0].Message, "invalid configuration"), errs[0].Message)
	})

	t.Run("unsupported command", func(t *testing.T) {
		msgs, err := serveSource(t, ctx, fakeSource{}, Request{Command: CommandWrite})
		require.Error(t, err)
		assert.Len(t, traceErrors(msgs), 1)
	})
}

type fakeDestination struct {
	got []Message
}

func (*fakeDestination) Spec(context.Context) (*ConnectorSpecification, error) {
	return &ConnectorSpecification{}, nil
}

func (*fakeDestination) Check(context.Context, testConfig) error { return nil }

func (d *fakeDestination) Write(_ context.Context, _ testConfig, _ *ConfiguredCatalog, in *MessageReader, em *Emitter) error {
	for {
		m, err := in.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		d.got = append(d.got, *m)
		if m.Type == MessageTypeState {
			if err := em.State(*m.State); err != nil {
				return err
			}
		}
	}
}

func TestServeDestination(t *testing.T) {
	t.Run("write", func(t *testing.T) {
		input := `{"type":"RECORD","record":{"stream":"users","data":{"id":1},"emitted_at":1}}
{"type":"RECORD","record":{"stream":"users","data":{"id":2},"emitted_at":2}}
{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"users"},"stream_state":{"cursor":2}}}}
`
		dst := &fakeDestination{}
		var out bytes.Buffer
		err := ServeDestination[testConfig](context.Background(), dst, Request{
			Command: CommandWrite, Config: json.RawMessage(testCfg), Catalog: json.RawMessage(testCatalog), Input: strings.NewReader(input),
		}, &out)
		require.NoError(t, err)
		require.Len(t, dst.got, 3)
		assert.Equal(t, `{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"users"},"stream_state":{"cursor":2}}}}`+"\n", out.String())
	})

	t.Run("check", func(t *testing.T) {
		var out bytes.Buffer
		err := ServeDestination[testConfig](context.Background(), &fakeDestination{}, Request{Command: CommandCheck, Config: json.RawMessage(testCfg)}, &out)
		require.NoError(t, err)
		assert.Equal(t, `{"type":"CONNECTION_STATUS","connectionStatus":{"status":"SUCCEEDED"}}`+"\n", out.String())
	})

	t.Run("discover is not a destination command", func(t *testing.T) {
		var out bytes.Buffer
		err := ServeDestination[testConfig](context.Background(), &fakeDestination{}, Request{Command: CommandDiscover, Config: json.RawMessage(testCfg)}, &out)
		require.Error(t, err)
		assert.Len(t, traceErrors(decodeMessages(t, out.Bytes())), 1)
	})
}
