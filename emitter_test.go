package synclet

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func decodeMessages(t *testing.T, out []byte) []Message {
	t.Helper()
	var msgs []Message
	for _, line := range bytes.Split(bytes.TrimRight(out, "\n"), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var m Message
		require.NoError(t, json.Unmarshal(line, &m), string(line))
		msgs = append(msgs, m)
	}
	return msgs
}

func emitOne(t *testing.T, emit func(em *Emitter) error) (Message, string) {
	t.Helper()
	var out bytes.Buffer
	require.NoError(t, emit(NewEmitter(&out)))
	msgs := decodeMessages(t, out.Bytes())
	require.Len(t, msgs, 1, "want one line, got %q", out.String())
	return msgs[0], strings.TrimSpace(out.String())
}

func TestEmitterRecord(t *testing.T) {
	t.Run("struct", func(t *testing.T) {
		row := struct {
			ID int `json:"id"`
		}{ID: 1}
		m, line := emitOne(t, func(em *Emitter) error {
			return em.Record(StreamDescriptor{Name: "users", Namespace: "public"}, row)
		})
		assert.Equal(t, MessageTypeRecord, m.Type)
		require.NotNil(t, m.Record)
		assert.Equal(t, "users", m.Record.Stream)
		assert.Equal(t, "public", m.Record.Namespace)
		assert.JSONEq(t, `{"id":1}`, string(m.Record.Data))
		assert.Greater(t, m.Record.EmittedAt, int64(1_700_000_000_000))
		assert.NotContains(t, line, `"emitted_at":0`)
	})

	t.Run("no namespace", func(t *testing.T) {
		_, line := emitOne(t, func(em *Emitter) error {
			return em.Record(StreamDescriptor{Name: "users"}, map[string]any{"id": 1})
		})
		assert.NotContains(t, line, `"namespace"`)
	})

	t.Run("raw message passes through", func(t *testing.T) {
		_, line := emitOne(t, func(em *Emitter) error {
			return em.Record(StreamDescriptor{Name: "users"}, json.RawMessage(`{"id":12345678901234567890,"amount":1.10}`))
		})
		assert.Contains(t, line, `"data":{"id":12345678901234567890,"amount":1.10}`)
	})
}

func TestEmitterStreamState(t *testing.T) {
	m, _ := emitOne(t, func(em *Emitter) error {
		return em.StreamState(StreamDescriptor{Name: "users", Namespace: "public"}, map[string]any{"cursor": 5})
	})
	assert.Equal(t, MessageTypeState, m.Type)
	require.NotNil(t, m.State)
	assert.Equal(t, StateTypeStream, m.State.Type)
	require.NotNil(t, m.State.Stream)
	assert.Equal(t, StreamDescriptor{Name: "users", Namespace: "public"}, m.State.Stream.StreamDescriptor)
	assert.JSONEq(t, `{"cursor":5}`, string(m.State.Stream.StreamState))
}

func TestEmitterGlobalState(t *testing.T) {
	m, _ := emitOne(t, func(em *Emitter) error {
		return em.GlobalState(map[string]any{"binlog": "000001:4"}, map[StreamDescriptor]any{
			{Name: "users", Namespace: "shop"}:  map[string]any{"pk": 3},
			{Name: "orders", Namespace: "shop"}: map[string]any{"pk": 1},
			{Name: "zones"}:                     map[string]any{"pk": 2},
		})
	})
	assert.Equal(t, MessageTypeState, m.Type)
	require.NotNil(t, m.State)
	assert.Equal(t, StateTypeGlobal, m.State.Type)
	require.NotNil(t, m.State.Global)
	assert.JSONEq(t, `{"binlog":"000001:4"}`, string(m.State.Global.SharedState))
	var order []StreamDescriptor
	for _, s := range m.State.Global.StreamStates {
		order = append(order, s.StreamDescriptor)
	}
	assert.Equal(t, []StreamDescriptor{{Name: "zones"}, {Name: "orders", Namespace: "shop"}, {Name: "users", Namespace: "shop"}}, order)
	assert.JSONEq(t, `{"pk":2}`, string(m.State.Global.StreamStates[0].StreamState))

	_, line := emitOne(t, func(em *Emitter) error { return em.GlobalState(nil, nil) })
	assert.Contains(t, line, `"stream_states":[]`)
}

func TestEmitterState(t *testing.T) {
	m, line := emitOne(t, func(em *Emitter) error {
		return em.State(StateMessage{Type: StateTypeLegacy, Data: json.RawMessage(`{"timestamp":42}`)})
	})
	assert.Equal(t, MessageTypeState, m.Type)
	assert.Equal(t, `{"type":"STATE","state":{"type":"LEGACY","data":{"timestamp":42},"sourceStats":{"recordCount":0}}}`, line)
}

func TestEmitterLog(t *testing.T) {
	m, _ := emitOne(t, func(em *Emitter) error { return em.Log(LogLevelWarn, "slow") })
	assert.Equal(t, MessageTypeLog, m.Type)
	require.NotNil(t, m.Log)
	assert.Equal(t, LogMessage{Level: LogLevelWarn, Message: "slow"}, *m.Log)
}

func TestEmitterTrace(t *testing.T) {
	rows := int64(10)
	m, _ := emitOne(t, func(em *Emitter) error {
		return em.Trace(TraceMessage{Type: TraceTypeEstimate, Estimate: &EstimateTrace{Name: "users", Type: EstimateTypeStream, RowEstimate: &rows}})
	})
	assert.Equal(t, MessageTypeTrace, m.Type)
	require.NotNil(t, m.Trace)
	assert.Equal(t, TraceTypeEstimate, m.Trace.Type)
	assert.Greater(t, m.Trace.EmittedAt, float64(1_700_000_000_000))
	require.NotNil(t, m.Trace.Estimate)
	assert.Equal(t, int64(10), *m.Trace.Estimate.RowEstimate)
}

func TestEmitterControl(t *testing.T) {
	m, _ := emitOne(t, func(em *Emitter) error { return em.Control(map[string]any{"token": "new"}) })
	assert.Equal(t, MessageTypeControl, m.Type)
	require.NotNil(t, m.Control)
	assert.Equal(t, ControlTypeConnectorConfig, m.Control.Type)
	assert.Greater(t, m.Control.EmittedAt, float64(1_700_000_000_000))
	require.NotNil(t, m.Control.ConnectorConfig)
	assert.JSONEq(t, `{"token":"new"}`, string(m.Control.ConnectorConfig.Config))
}

func TestEmitterConcurrent(t *testing.T) {
	var out bytes.Buffer
	em := NewEmitter(&out)
	var wg sync.WaitGroup
	for g := 0; g < 100; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				assert.NoError(t, em.Record(StreamDescriptor{Name: "users"}, map[string]any{"g": g, "i": i}))
			}
		}()
	}
	wg.Wait()
	msgs := decodeMessages(t, out.Bytes())
	assert.Len(t, msgs, 10_000)
}
