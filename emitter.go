package synclet

import (
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"
)

// Emitter writes protocol messages to the transport, one Write per message,
// from any number of goroutines.
type Emitter struct {
	mu sync.Mutex
	w  io.Writer
}

// NewEmitter returns an Emitter writing to w.
func NewEmitter(w io.Writer) *Emitter {
	return &Emitter{w: w}
}

// Record emits one row of a stream with the current time as emitted_at. data
// is a struct or a map; a json.RawMessage is written verbatim.
func (e *Emitter) Record(desc StreamDescriptor, data any) error {
	raw, err := marshalValue(data)
	if err != nil {
		return fmt.Errorf("marshal record of %s: %w", desc.Name, err)
	}
	return e.write(&Message{Type: MessageTypeRecord, Record: &RecordMessage{
		Stream:    desc.Name,
		Namespace: desc.Namespace,
		Data:      raw,
		EmittedAt: time.Now().UnixMilli(),
	}})
}

// StreamState emits a STREAM state checkpoint for one stream.
func (e *Emitter) StreamState(desc StreamDescriptor, state any) error {
	raw, err := marshalValue(state)
	if err != nil {
		return fmt.Errorf("marshal state of %s: %w", desc.Name, err)
	}
	return e.State(StateMessage{Type: StateTypeStream, Stream: &StreamState{StreamDescriptor: desc, StreamState: raw}})
}

// GlobalState emits a GLOBAL state checkpoint: shared is the state common to
// every stream, streams the per-stream state, written sorted by namespace
// then name.
func (e *Emitter) GlobalState(shared any, streams map[StreamDescriptor]any) error {
	var sharedRaw json.RawMessage
	if shared != nil {
		raw, err := marshalValue(shared)
		if err != nil {
			return fmt.Errorf("marshal shared state: %w", err)
		}
		sharedRaw = raw
	}
	states := make([]StreamState, 0, len(streams))
	for desc, st := range streams {
		raw, err := marshalValue(st)
		if err != nil {
			return fmt.Errorf("marshal state of %s: %w", desc.Name, err)
		}
		states = append(states, StreamState{StreamDescriptor: desc, StreamState: raw})
	}
	slices.SortFunc(states, func(a, b StreamState) int {
		return cmp.Or(
			cmp.Compare(a.StreamDescriptor.Namespace, b.StreamDescriptor.Namespace),
			cmp.Compare(a.StreamDescriptor.Name, b.StreamDescriptor.Name),
		)
	})
	return e.State(StateMessage{Type: StateTypeGlobal, Global: &GlobalState{SharedState: sharedRaw, StreamStates: states}})
}

// State emits a state message as given.
func (e *Emitter) State(msg StateMessage) error {
	return e.write(&Message{Type: MessageTypeState, State: &msg})
}

// Log emits a LOG message.
func (e *Emitter) Log(level LogLevel, msg string) error {
	return e.write(&Message{Type: MessageTypeLog, Log: &LogMessage{Level: level, Message: msg}})
}

// Trace emits a trace message with the current time as emitted_at.
func (e *Emitter) Trace(msg TraceMessage) error {
	msg.EmittedAt = nowMillis()
	return e.write(&Message{Type: MessageTypeTrace, Trace: &msg})
}

// Control emits a CONNECTOR_CONFIG control message carrying config.
func (e *Emitter) Control(config any) error {
	raw, err := marshalValue(config)
	if err != nil {
		return fmt.Errorf("marshal connector config: %w", err)
	}
	return e.write(&Message{Type: MessageTypeControl, Control: &ControlMessage{
		Type:            ControlTypeConnectorConfig,
		EmittedAt:       nowMillis(),
		ConnectorConfig: &ControlConnectorConfig{Config: raw},
	}})
}

func (e *Emitter) write(m *Message) error {
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal %s message: %w", m.Type, err)
	}
	b = append(b, '\n')
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, err := e.w.Write(b); err != nil {
		return fmt.Errorf("write %s message: %w", m.Type, err)
	}
	return nil
}

func marshalValue(v any) (json.RawMessage, error) {
	if raw, ok := v.(json.RawMessage); ok {
		return raw, nil
	}
	return json.Marshal(v)
}

func nowMillis() float64 {
	return float64(time.Now().UnixMilli())
}
