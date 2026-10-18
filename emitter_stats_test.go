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

func stateCounts(t *testing.T, out []byte) []*float64 {
	t.Helper()
	var counts []*float64
	for _, m := range decodeMessages(t, out) {
		if m.Type != MessageTypeState {
			continue
		}
		require.NotNil(t, m.State)
		if m.State.SourceStats == nil {
			counts = append(counts, nil)
			continue
		}
		n := m.State.SourceStats.RecordCount
		counts = append(counts, &n)
	}
	return counts
}

func TestEmitterSourceStats(t *testing.T) {
	a := StreamDescriptor{Name: "a"}
	b := StreamDescriptor{Name: "b"}
	ax := StreamDescriptor{Name: "a", Namespace: "x"}
	ay := StreamDescriptor{Name: "a", Namespace: "y"}
	records := func(desc StreamDescriptor, n int) func(em *Emitter) error {
		return func(em *Emitter) error {
			for i := 0; i < n; i++ {
				if err := em.Record(desc, map[string]any{"i": i}); err != nil {
					return err
				}
			}
			return nil
		}
	}
	stream := func(desc StreamDescriptor) func(em *Emitter) error {
		return func(em *Emitter) error { return em.StreamState(desc, map[string]any{"c": 1}) }
	}
	global := func(em *Emitter) error {
		return em.GlobalState(map[string]any{"lsn": 1}, map[StreamDescriptor]any{a: 1, b: 2})
	}
	legacy := func(em *Emitter) error {
		return em.State(StateMessage{Type: StateTypeLegacy, Data: json.RawMessage(`{"t":1}`)})
	}
	tests := []struct {
		name   string
		script []func(em *Emitter) error
		want   []float64
	}{
		{"stream state takes its stream's records", []func(em *Emitter) error{records(a, 3), stream(a)}, []float64{3}},
		{"per-stream counts reset on their own state", []func(em *Emitter) error{records(a, 2), records(b, 1), stream(a), stream(b), records(a, 1), stream(a)}, []float64{2, 1, 1}},
		{"state without records", []func(em *Emitter) error{stream(a)}, []float64{0}},
		{"namespace tells streams apart", []func(em *Emitter) error{records(ax, 2), records(ay, 1), stream(ax), stream(ay)}, []float64{2, 1}},
		{"global takes every record since the previous state", []func(em *Emitter) error{records(a, 2), records(b, 3), global, records(a, 1), global}, []float64{5, 1}},
		{"global excludes records a stream state took", []func(em *Emitter) error{records(a, 2), records(b, 3), stream(a), global}, []float64{2, 3}},
		{"legacy takes every record since the previous state", []func(em *Emitter) error{records(a, 2), records(b, 1), legacy}, []float64{3}},
		{"global resets per-stream counts", []func(em *Emitter) error{records(a, 2), global, records(a, 1), stream(a)}, []float64{2, 1}},
		{"caller's sourceStats is replaced by the count", []func(em *Emitter) error{records(a, 2), func(em *Emitter) error {
			return em.State(StateMessage{Type: StateTypeStream, Stream: &StreamState{StreamDescriptor: a}, SourceStats: &StateStats{RecordCount: 99}})
		}}, []float64{2}},
		{"lower-case stream type counts per stream", []func(em *Emitter) error{records(a, 2), records(b, 1), func(em *Emitter) error {
			return em.State(StateMessage{Type: "stream", Stream: &StreamState{StreamDescriptor: a}})
		}}, []float64{2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			em := NewEmitter(&out)
			for _, step := range tt.script {
				require.NoError(t, step(em))
			}
			counts := stateCounts(t, out.Bytes())
			require.Len(t, counts, len(tt.want))
			for i, want := range tt.want {
				require.NotNil(t, counts[i], "state %d has no sourceStats", i)
				assert.Equal(t, want, *counts[i], "state %d", i)
			}
		})
	}
}

func TestEmitterSourceStatsConcurrent(t *testing.T) {
	var out bytes.Buffer
	em := NewEmitter(&out)
	desc := StreamDescriptor{Name: "users"}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				assert.NoError(t, em.Record(desc, map[string]any{"g": g, "i": i}))
				if i%50 == 0 {
					assert.NoError(t, em.StreamState(desc, map[string]any{"i": i}))
				}
			}
		}()
	}
	wg.Wait()
	require.NoError(t, em.StreamState(desc, nil))

	var since float64
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		var m Message
		require.NoError(t, json.Unmarshal([]byte(line), &m))
		switch m.Type {
		case MessageTypeRecord:
			since++
		case MessageTypeState:
			require.NotNil(t, m.State.SourceStats)
			assert.Equal(t, since, m.State.SourceStats.RecordCount)
			since = 0
		}
	}
}
