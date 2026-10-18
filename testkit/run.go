// Package testkit runs connectors in memory through the SDK's transport and
// checks that a source keeps the protocol contract.
package testkit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Output is the protocol stream of one command and the error the connector
// returned.
type Output struct {
	Messages []synclet.Message
	// Err is what ServeSource or ServeDestination returned; when it is non-nil
	// the stream ends with its TRACE ERROR.
	Err error
}

// RunSource runs one command of src through ServeSource and parses the
// stream it writes. The error reports a stream that is not protocol messages.
func RunSource[C any](ctx context.Context, src synclet.Source[C], req synclet.Request) (*Output, error) {
	var buf bytes.Buffer
	serr := synclet.ServeSource(ctx, src, req, &buf)
	return parse(&buf, serr)
}

// RunDestination runs one command of dst through ServeDestination, with
// req.Input as the record stream, and parses the stream it writes.
func RunDestination[C any](ctx context.Context, dst synclet.Destination[C], req synclet.Request) (*Output, error) {
	var buf bytes.Buffer
	serr := synclet.ServeDestination(ctx, dst, req, &buf)
	return parse(&buf, serr)
}

func parse(r io.Reader, serr error) (*Output, error) {
	out := &Output{Err: serr}
	msgs, err := ReadMessages(r)
	out.Messages = msgs
	return out, err
}

// ReadMessages parses a protocol stream until its end.
func ReadMessages(r io.Reader) ([]synclet.Message, error) {
	mr := synclet.NewMessageReader(r)
	var msgs []synclet.Message
	for {
		m, err := mr.Next()
		if errors.Is(err, io.EOF) {
			return msgs, nil
		}
		if err != nil {
			return msgs, err
		}
		msgs = append(msgs, *m)
	}
}

// Records returns the records of one stream in the order they were emitted.
func (o *Output) Records(desc synclet.StreamDescriptor) []synclet.RecordMessage {
	var recs []synclet.RecordMessage
	for _, m := range o.Messages {
		if m.Type == synclet.MessageTypeRecord && recordDescriptor(m.Record) == desc {
			recs = append(recs, *m.Record)
		}
	}
	return recs
}

// States returns every STATE message in the order they were emitted.
func (o *Output) States() []synclet.StateMessage {
	var states []synclet.StateMessage
	for _, m := range o.Messages {
		if m.Type == synclet.MessageTypeState {
			states = append(states, *m.State)
		}
	}
	return states
}

// Logs returns every LOG message in the order they were emitted.
func (o *Output) Logs() []synclet.LogMessage {
	var logs []synclet.LogMessage
	for _, m := range o.Messages {
		if m.Type == synclet.MessageTypeLog {
			logs = append(logs, *m.Log)
		}
	}
	return logs
}

// ErrorTraces returns the payloads of every TRACE ERROR message.
func (o *Output) ErrorTraces() []synclet.ErrorTrace {
	var errs []synclet.ErrorTrace
	for _, m := range o.Messages {
		if m.Type == synclet.MessageTypeTrace && m.Trace.Type == synclet.TraceTypeError && m.Trace.Error != nil {
			errs = append(errs, *m.Trace.Error)
		}
	}
	return errs
}

// Spec returns the payload of the first SPEC message, or nil.
func (o *Output) Spec() *synclet.ConnectorSpecification {
	for _, m := range o.Messages {
		if m.Type == synclet.MessageTypeSpec {
			return m.Spec
		}
	}
	return nil
}

// ConnectionStatus returns the payload of the first CONNECTION_STATUS
// message, or nil.
func (o *Output) ConnectionStatus() *synclet.ConnectionStatus {
	for _, m := range o.Messages {
		if m.Type == synclet.MessageTypeConnectionStatus {
			return m.ConnectionStatus
		}
	}
	return nil
}

// Catalog returns the payload of the first CATALOG message, or nil.
func (o *Output) Catalog() *synclet.Catalog {
	for _, m := range o.Messages {
		if m.Type == synclet.MessageTypeCatalog {
			return m.Catalog
		}
	}
	return nil
}

// NextState returns the state the platform would hand the next read: the
// last STREAM state of each stream, or the last GLOBAL or LEGACY state, as a
// JSON array of state messages. It is nil when the stream has no state.
func (o *Output) NextState() json.RawMessage {
	var next []synclet.StateMessage
	streams := map[synclet.StreamDescriptor]int{}
	for _, st := range o.States() {
		st.SourceStats, st.DestinationStats = nil, nil
		if st.Type != synclet.StateTypeStream || st.Stream == nil {
			next, streams = []synclet.StateMessage{st}, map[synclet.StreamDescriptor]int{}
			continue
		}
		if i, ok := streams[st.Stream.StreamDescriptor]; ok {
			next[i] = st
			continue
		}
		if len(next) > 0 && next[0].Type != synclet.StateTypeStream {
			next = nil
		}
		streams[st.Stream.StreamDescriptor] = len(next)
		next = append(next, st)
	}
	if len(next) == 0 {
		return nil
	}
	b, err := json.Marshal(next)
	if err != nil {
		return nil
	}
	return b
}

func recordDescriptor(r *synclet.RecordMessage) synclet.StreamDescriptor {
	return synclet.StreamDescriptor{Name: r.Stream, Namespace: r.Namespace}
}
