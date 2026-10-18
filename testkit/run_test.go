package testkit_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func TestRunSource(t *testing.T) {
	ctx := context.Background()
	src := newTableSource()
	users := synclet.StreamDescriptor{Name: "users", Namespace: "app"}
	orders := synclet.StreamDescriptor{Name: "orders", Namespace: "app"}

	t.Run("spec", func(t *testing.T) {
		out, err := testkit.RunSource(ctx, src, synclet.Request{Command: synclet.CommandSpec})
		require.NoError(t, err)
		require.NoError(t, out.Err)
		require.NotNil(t, out.Spec())
		assert.Equal(t, "Table source", out.Spec().ConnectionSpecification.Title)
	})

	t.Run("check", func(t *testing.T) {
		out, err := testkit.RunSource(ctx, src, synclet.Request{Command: synclet.CommandCheck, Config: json.RawMessage(`{"token":""}`)})
		require.NoError(t, err)
		require.NotNil(t, out.ConnectionStatus())
		assert.Equal(t, synclet.ConnectionStatus{Status: synclet.ConnectionStatusFailed, Message: "token is required"}, *out.ConnectionStatus())
	})

	t.Run("discover", func(t *testing.T) {
		out, err := testkit.RunSource(ctx, src, synclet.Request{Command: synclet.CommandDiscover, Config: json.RawMessage(validConfig)})
		require.NoError(t, err)
		require.NotNil(t, out.Catalog())
		assert.Len(t, out.Catalog().Streams, 2)
	})

	t.Run("read", func(t *testing.T) {
		out, err := testkit.RunSource(ctx, src, synclet.Request{Command: synclet.CommandRead, Config: json.RawMessage(validConfig), Catalog: json.RawMessage(incrementalCatalog)})
		require.NoError(t, err)
		require.NoError(t, out.Err)
		assert.Len(t, out.Records(users), 3)
		assert.Len(t, out.Records(orders), 2)
		assert.JSONEq(t, `{"id":1,"name":"ann","updated_at":"2026-01-01T00:00:00Z"}`, string(out.Records(users)[0].Data))
		require.Len(t, out.States(), 2)
		assert.Equal(t, users, out.States()[0].Stream.StreamDescriptor)
		require.NotEmpty(t, out.Logs())
		assert.Equal(t, synclet.LogLevelInfo, out.Logs()[0].Level)
		assert.JSONEq(t, `[
			{"type":"STREAM","stream":{"stream_descriptor":{"name":"users","namespace":"app"},"stream_state":{"cursor":"2026-01-03T00:00:00Z"}}},
			{"type":"STREAM","stream":{"stream_descriptor":{"name":"orders","namespace":"app"},"stream_state":{"cursor":20}}}
		]`, string(out.NextState()))

		next, err := testkit.RunSource(ctx, src, synclet.Request{Command: synclet.CommandRead, Config: json.RawMessage(validConfig), Catalog: json.RawMessage(incrementalCatalog), State: out.NextState()})
		require.NoError(t, err)
		require.NoError(t, next.Err)
		assert.Len(t, next.Records(users), 1)
	})

	t.Run("failed read", func(t *testing.T) {
		broken := newTableSource()
		broken.readErr = errors.New("connection reset")
		out, err := testkit.RunSource(ctx, broken, synclet.Request{Command: synclet.CommandRead, Config: json.RawMessage(validConfig), Catalog: json.RawMessage(incrementalCatalog)})
		require.NoError(t, err)
		require.EqualError(t, out.Err, "connection reset")
		require.Len(t, out.ErrorTraces(), 1)
		assert.Equal(t, "connection reset", out.ErrorTraces()[0].Message)
	})
}

func TestNextState(t *testing.T) {
	tests := []struct {
		name string
		msgs []synclet.Message
		want string
	}{
		{name: "no state", want: ``},
		{
			name: "last global",
			msgs: []synclet.Message{
				{Type: synclet.MessageTypeState, State: &synclet.StateMessage{Type: synclet.StateTypeGlobal, Global: &synclet.GlobalState{SharedState: json.RawMessage(`1`), StreamStates: []synclet.StreamState{}}}},
				{Type: synclet.MessageTypeState, State: &synclet.StateMessage{Type: synclet.StateTypeGlobal, Global: &synclet.GlobalState{SharedState: json.RawMessage(`2`), StreamStates: []synclet.StreamState{}}}},
			},
			want: `[{"type":"GLOBAL","global":{"shared_state":2,"stream_states":[]}}]`,
		},
		{
			name: "last legacy",
			msgs: []synclet.Message{
				{Type: synclet.MessageTypeState, State: &synclet.StateMessage{Type: synclet.StateTypeLegacy, Data: json.RawMessage(`{"a":1}`)}},
				{Type: synclet.MessageTypeState, State: &synclet.StateMessage{Type: synclet.StateTypeLegacy, Data: json.RawMessage(`{"a":2}`)}},
			},
			want: `[{"type":"LEGACY","data":{"a":2}}]`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := (&testkit.Output{Messages: tt.msgs}).NextState()
			if tt.want == "" {
				assert.Nil(t, got)
				return
			}
			assert.JSONEq(t, tt.want, string(got))
		})
	}
}

func TestRunDestination(t *testing.T) {
	in := `{"type":"RECORD","record":{"stream":"users","data":{"id":1},"emitted_at":1}}
{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"users"},"stream_state":{"cursor":1}}}}
`
	out, err := testkit.RunDestination(context.Background(), echoDestination{}, synclet.Request{
		Command: synclet.CommandWrite,
		Config:  json.RawMessage(`{}`),
		Catalog: json.RawMessage(`{"streams":[]}`),
		Input:   strings.NewReader(in),
	})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	require.Len(t, out.States(), 1)
	assert.JSONEq(t, `{"cursor":1}`, string(out.States()[0].Stream.StreamState))
	require.Len(t, out.Logs(), 1)
	assert.Equal(t, "wrote 1 records", out.Logs()[0].Message)
}

type echoDestination struct{}

func (echoDestination) Spec(context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{}, nil
}

func (echoDestination) Check(context.Context, struct{}) error { return nil }

func (echoDestination) Write(_ context.Context, _ struct{}, _ *synclet.ConfiguredCatalog, in *synclet.MessageReader, em *synclet.Emitter) error {
	n := 0
	for {
		m, err := in.Next()
		if errors.Is(err, io.EOF) {
			return em.Log(synclet.LogLevelInfo, fmt.Sprintf("wrote %d records", n))
		}
		if err != nil {
			return err
		}
		switch m.Type {
		case synclet.MessageTypeRecord:
			n++
		case synclet.MessageTypeState:
			if err := em.State(*m.State); err != nil {
				return err
			}
		}
	}
}
