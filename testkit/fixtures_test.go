package testkit_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

type tableConfig struct {
	Token string `json:"token"`
}

const (
	validConfig  = `{"token":"secret"}`
	brokenConfig = `{"token":""}`
)

type fault string

const (
	faultInvalidSpec        fault = "invalid spec"
	faultCheckIgnoresConfig fault = "check ignores config"
	faultEmptySchema        fault = "empty schema"
	faultUnconfiguredStream fault = "record of an unconfigured stream"
	faultOutsideSchema      fault = "record outside the schema"
	faultWrongType          fault = "record with a wrong type"
	faultStateFirst         fault = "state before records"
	faultIgnoresCursor      fault = "second read ignores the cursor"
	faultIgnoresCancel      fault = "read ignores cancellation"
	faultCheckRejectsValid  fault = "check rejects a valid config"
	faultCheckNoMessage     fault = "check fails without a message"
	faultNoStreams          fault = "discover returns no streams"
	faultInvalidSchema      fault = "invalid stream schema"
	faultNoRecords          fault = "read emits no records"
	faultNoState            fault = "incremental read emits no state"
	faultNoCursorValue      fault = "records without a cursor value"
	faultLookback           fault = "second read re-reads a lookback window"
)

type tableStream struct {
	stream synclet.Stream
	cursor string
	rows   []map[string]any
}

// tableSource reads fixed rows of two incremental streams: users with a
// source-defined RFC 3339 cursor and orders with a user-chosen integer cursor.
type tableSource struct {
	streams []tableStream
	fault   fault
	readErr error
	release chan struct{}
}

func newTableSource() *tableSource {
	return &tableSource{streams: []tableStream{
		{
			stream: synclet.Stream{
				Name:      "users",
				Namespace: "app",
				JSONSchema: synclet.ObjectSchema(map[synclet.PropertyName]synclet.PropertySpec{
					"id":         {PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.Integer}}},
					"name":       {PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.String}}},
					"updated_at": {PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.String}}, Format: synclet.DateTime},
				}),
				SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental},
				SourceDefinedCursor:     true,
				DefaultCursorField:      []string{"updated_at"},
				SourceDefinedPrimaryKey: [][]string{{"id"}},
			},
			cursor: "updated_at",
			rows: []map[string]any{
				{"id": 1, "name": "ann", "updated_at": "2026-01-01T00:00:00Z"},
				{"id": 2, "name": "bob", "updated_at": "2026-01-02T00:00:00Z"},
				{"id": 3, "name": "cid", "updated_at": "2026-01-03T00:00:00Z"},
			},
		},
		{
			stream: synclet.Stream{
				Name:      "orders",
				Namespace: "app",
				JSONSchema: synclet.ObjectSchema(map[synclet.PropertyName]synclet.PropertySpec{
					"id":     {PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.Integer}}},
					"seq":    {PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.Integer}}},
					"amount": {PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.Number, synclet.Null}}},
				}),
				SupportedSyncModes: []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental},
			},
			cursor: "seq",
			rows: []map[string]any{
				{"id": 1, "seq": 10, "amount": 9.5},
				{"id": 2, "seq": 20, "amount": nil},
			},
		},
	}}
}

func (s *tableSource) Spec(context.Context) (*synclet.ConnectorSpecification, error) {
	typ := "object"
	if s.fault == faultInvalidSpec {
		typ = "objekt"
	}
	return &synclet.ConnectorSpecification{
		ProtocolVersion:               "0.5.2",
		SupportsIncremental:           true,
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{synclet.DestinationSyncModeAppend},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:    "Table source",
			Type:     typ,
			Required: []synclet.PropertyName{"token"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"token": {PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.String}}, IsSecret: true},
			}},
		},
	}, nil
}

func (s *tableSource) Check(_ context.Context, cfg tableConfig) error {
	switch {
	case s.fault == faultCheckRejectsValid:
		return errors.New("token rejected")
	case cfg.Token == "" && s.fault == faultCheckNoMessage:
		return errors.New(" ")
	case cfg.Token == "" && s.fault != faultCheckIgnoresConfig:
		return errors.New("token is required")
	}
	return nil
}

func (s *tableSource) Discover(context.Context, tableConfig) (*synclet.Catalog, error) {
	cat := &synclet.Catalog{}
	if s.fault == faultNoStreams {
		return cat, nil
	}
	for _, ts := range s.streams {
		st := ts.stream
		switch {
		case s.fault == faultEmptySchema && st.Name == "users":
			st.JSONSchema = json.RawMessage(`{}`)
		case s.fault == faultInvalidSchema && st.Name == "users":
			st.JSONSchema = json.RawMessage(`{"type":"objekt","properties":{"id":{}}}`)
		}
		cat.Streams = append(cat.Streams, st)
	}
	return cat, nil
}

type tableState struct {
	Cursor any `json:"cursor"`
}

func (s *tableSource) Read(ctx context.Context, _ tableConfig, cat *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	if s.readErr != nil {
		return s.readErr
	}
	for _, cs := range cat.Streams {
		desc := cs.Stream.Descriptor()
		i := slices.IndexFunc(s.streams, func(ts tableStream) bool { return ts.stream.Descriptor() == desc })
		if i < 0 {
			return synclet.ConfigError(fmt.Errorf("unknown stream %q", desc.Name))
		}
		ts := s.streams[i]
		var from tableState
		if raw := state.Stream(desc); raw != nil && s.fault != faultIgnoresCursor {
			if err := json.Unmarshal(raw, &from); err != nil {
				return err
			}
		}
		if n, ok := from.Cursor.(float64); ok && s.fault == faultLookback {
			from.Cursor = n - 10
		}
		synclet.Logger(ctx).Info("reading", "stream", desc.Name)
		rows, err := s.fetch(ctx, ts)
		if err != nil {
			return err
		}
		var emit []map[string]any
		last := from.Cursor
		for _, row := range rows {
			if s.fault == faultNoRecords || from.Cursor != nil && less(row[ts.cursor], from.Cursor) {
				continue
			}
			emit = append(emit, s.mutate(desc, row))
			last = row[ts.cursor]
		}
		if s.fault == faultStateFirst {
			if err := em.StreamState(desc, tableState{Cursor: last}); err != nil {
				return err
			}
		}
		for _, row := range emit {
			if err := em.Record(desc, row); err != nil {
				return err
			}
		}
		if s.fault == faultUnconfiguredStream {
			if err := em.Record(synclet.StreamDescriptor{Name: "ghosts"}, map[string]any{"id": 1}); err != nil {
				return err
			}
		}
		if s.fault != faultStateFirst && s.fault != faultNoState {
			if err := em.StreamState(desc, tableState{Cursor: last}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *tableSource) fetch(ctx context.Context, ts tableStream) ([]map[string]any, error) {
	for {
		if err := ctx.Err(); err == nil {
			return ts.rows, nil
		} else if s.fault != faultIgnoresCancel {
			return nil, err
		}
		select {
		case <-s.release:
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (s *tableSource) mutate(desc synclet.StreamDescriptor, row map[string]any) map[string]any {
	if desc.Name != "users" {
		return row
	}
	out := maps.Clone(row)
	switch s.fault {
	case faultOutsideSchema:
		out["nickname"] = "x"
	case faultWrongType:
		out["id"] = fmt.Sprint(row["id"])
	case faultNoCursorValue:
		delete(out, "updated_at")
	}
	return out
}

func less(a, b any) bool {
	switch av := a.(type) {
	case int:
		switch bv := b.(type) {
		case int:
			return av < bv
		case float64:
			return float64(av) < bv
		}
	case string:
		if bv, ok := b.(string); ok {
			return av < bv
		}
	}
	return false
}

func configuredCatalog(src *tableSource) *synclet.ConfiguredCatalog {
	cat := &synclet.ConfiguredCatalog{}
	for _, ts := range src.streams {
		cs := synclet.ConfiguredStream{Stream: ts.stream, SyncMode: synclet.SyncModeIncremental, DestinationSyncMode: synclet.DestinationSyncModeAppend}
		if !ts.stream.SourceDefinedCursor {
			cs.CursorField = []string{ts.cursor}
		}
		cat.Streams = append(cat.Streams, cs)
	}
	return cat
}

var incrementalCatalog = func() string {
	b, err := json.Marshal(configuredCatalog(newTableSource()))
	if err != nil {
		panic(err)
	}
	return string(b)
}()
