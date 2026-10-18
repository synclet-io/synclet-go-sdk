package testkit

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

type errRecorder struct {
	testing.TB
	errs []string
}

func (r *errRecorder) Helper() {}

func (r *errRecorder) Errorf(format string, args ...any) {
	r.errs = append(r.errs, fmt.Sprintf(format, args...))
}

func TestCheckSourceStats(t *testing.T) {
	users := synclet.StreamDescriptor{Name: "users", Namespace: "app"}
	orders := synclet.StreamDescriptor{Name: "orders", Namespace: "app"}
	rec := func(desc synclet.StreamDescriptor) synclet.Message {
		return synclet.Message{Type: synclet.MessageTypeRecord, Record: &synclet.RecordMessage{Stream: desc.Name, Namespace: desc.Namespace, Data: json.RawMessage(`{}`)}}
	}
	stats := func(n float64) *synclet.StateStats { return &synclet.StateStats{RecordCount: n} }
	stream := func(desc synclet.StreamDescriptor, st *synclet.StateStats) synclet.Message {
		return synclet.Message{Type: synclet.MessageTypeState, State: &synclet.StateMessage{
			Type: synclet.StateTypeStream, Stream: &synclet.StreamState{StreamDescriptor: desc}, SourceStats: st,
		}}
	}
	global := func(st *synclet.StateStats) synclet.Message {
		return synclet.Message{Type: synclet.MessageTypeState, State: &synclet.StateMessage{Type: synclet.StateTypeGlobal, Global: &synclet.GlobalState{}, SourceStats: st}}
	}
	legacy := func(st *synclet.StateStats) synclet.Message {
		return synclet.Message{Type: synclet.MessageTypeState, State: &synclet.StateMessage{Type: synclet.StateTypeLegacy, Data: json.RawMessage(`{}`), SourceStats: st}}
	}
	tests := []struct {
		name string
		msgs []synclet.Message
		want []string
	}{
		{"counts match", []synclet.Message{
			rec(users), rec(users), rec(orders), stream(users, stats(2)), stream(orders, stats(1)), stream(users, stats(0)),
			rec(users), rec(orders), global(stats(2)), rec(orders), legacy(stats(1)),
		}, nil},
		{"global after a stream state", []synclet.Message{rec(users), rec(orders), stream(users, stats(1)), global(stats(1))}, nil},
		{"missing sourceStats", []synclet.Message{rec(users), stream(users, nil)}, []string{
			"state: STATE 1 (stream app.users) has no sourceStats.recordCount",
		}},
		{"stream count differs", []synclet.Message{rec(users), rec(users), rec(orders), stream(users, stats(3))}, []string{
			"state: STATE 1 (stream app.users): sourceStats.recordCount 3, want 2, the records since the stream's previous STATE",
		}},
		{"global count differs", []synclet.Message{rec(users), rec(orders), global(stats(1))}, []string{
			"state: STATE 1 (GLOBAL): sourceStats.recordCount 1, want 2, the records since the previous STATE",
		}},
		{"global resets per-stream counts", []synclet.Message{rec(users), global(stats(1)), rec(users), stream(users, stats(2))}, []string{
			"state: STATE 2 (stream app.users): sourceStats.recordCount 2, want 1, the records since the stream's previous STATE",
		}},
		{"legacy count differs", []synclet.Message{rec(users), legacy(stats(0))}, []string{
			"state: STATE 1 (LEGACY): sourceStats.recordCount 0, want 1, the records since the previous STATE",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &errRecorder{TB: t}
			checkSourceStats(r, &Output{Messages: tt.msgs})
			assert.Equal(t, tt.want, r.errs)
		})
	}
}
