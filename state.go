package synclet

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// StateType is the shape of a StateMessage.
type StateType string

const (
	StateTypeStream StateType = "STREAM"
	StateTypeGlobal StateType = "GLOBAL"
	StateTypeLegacy StateType = "LEGACY"
)

// StateMessage is a checkpoint: Stream for STREAM, Global for GLOBAL, Data
// for LEGACY.
type StateMessage struct {
	Type             StateType       `json:"type"`
	Stream           *StreamState    `json:"stream,omitempty"`
	Global           *GlobalState    `json:"global,omitempty"`
	Data             json.RawMessage `json:"data,omitempty"`
	SourceStats      *StateStats     `json:"sourceStats,omitempty"`
	DestinationStats *StateStats     `json:"destinationStats,omitempty"`
}

// StreamState is the checkpoint of one stream.
type StreamState struct {
	StreamDescriptor StreamDescriptor `json:"stream_descriptor"`
	StreamState      json.RawMessage  `json:"stream_state,omitempty"`
}

// GlobalState is a checkpoint shared by every stream, such as a CDC log
// position, with the per-stream progress next to it.
type GlobalState struct {
	SharedState  json.RawMessage `json:"shared_state,omitempty"`
	StreamStates []StreamState   `json:"stream_states"`
}

// StateStats counts the records a checkpoint covers.
type StateStats struct {
	RecordCount float64 `json:"recordCount"`
}

// InputState is the state of the previous sync as the platform hands it to a
// source. Type is "" when the sync has none.
type InputState struct {
	Type    StateType
	Streams []StreamState   // STREAM entries, or a GLOBAL state's stream_states
	Shared  json.RawMessage // a GLOBAL state's shared_state
	Legacy  json.RawMessage // a LEGACY state's data
}

// ParseInputState decodes the state file the platform writes: nothing, null
// or [] for an empty state; an array of state messages (STREAM entries, one
// GLOBAL or one LEGACY); or a bare object, which is a LEGACY state. Type names
// are matched regardless of case.
func ParseInputState(b []byte) (*InputState, error) {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || bytes.Equal(b, []byte("null")) {
		return &InputState{}, nil
	}
	if b[0] == '{' {
		if !json.Valid(b) {
			return nil, fmt.Errorf("decode legacy state: invalid JSON")
		}
		return &InputState{Type: StateTypeLegacy, Legacy: json.RawMessage(b)}, nil
	}
	var msgs []StateMessage
	if err := json.Unmarshal(b, &msgs); err != nil {
		return nil, fmt.Errorf("decode state: %w", err)
	}
	st := &InputState{}
	for i, m := range msgs {
		typ := StateType(strings.ToUpper(string(m.Type)))
		if st.Type != "" && typ != st.Type {
			return nil, fmt.Errorf("state entry %d: %s mixed with %s", i, typ, st.Type)
		}
		st.Type = typ
		switch typ {
		case StateTypeStream:
			if m.Stream == nil {
				return nil, fmt.Errorf("state entry %d: STREAM without stream", i)
			}
			st.Streams = append(st.Streams, *m.Stream)
		case StateTypeGlobal:
			if m.Global == nil {
				return nil, fmt.Errorf("state entry %d: GLOBAL without global", i)
			}
			st.Shared = m.Global.SharedState
			st.Streams = append(st.Streams, m.Global.StreamStates...)
		case StateTypeLegacy:
			st.Legacy = m.Data
		default:
			return nil, fmt.Errorf("state entry %d: unknown state type %q", i, m.Type)
		}
	}
	return st, nil
}

// Stream returns the saved state of one stream, or nil when there is none.
func (s *InputState) Stream(desc StreamDescriptor) json.RawMessage {
	for _, st := range s.Streams {
		if st.StreamDescriptor == desc {
			if bytes.Equal(st.StreamState, []byte("null")) {
				return nil
			}
			return st.StreamState
		}
	}
	return nil
}
