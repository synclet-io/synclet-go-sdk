package testkit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// CancelTimeout is how long a read may keep running after its context is
// cancelled. The cancel check catches a read that blocks, sleeps or retries
// past cancellation; a read that ignores its context but finishes its
// fixtures sooner passes, so fixtures that exercise it make the read outlast
// its first message, such as a test server that holds a response until the
// request's context ends.
const CancelTimeout = 5 * time.Second

// SourceSuite is what RunSourceSuite needs from a connector's test: inputs
// that reach the connector's fixtures, in the form the platform sends them.
type SourceSuite struct {
	// Config is a configuration that passes check and reads Catalog.
	Config json.RawMessage
	// BrokenConfig is a configuration check must reject.
	BrokenConfig json.RawMessage
	// Catalog is the configured catalog every read runs with. An incremental
	// stream's cursor is its stream's default_cursor_field when the cursor is
	// source-defined, its cursor_field otherwise.
	Catalog *synclet.ConfiguredCatalog
	// State is the state of the first read; nil starts from scratch.
	State json.RawMessage
	// CompareCursor orders two cursor values as decoded from record JSON
	// (json.Number, string, bool, map or slice) and returns -1, 0 or 1. Nil
	// compares numbers numerically, two RFC 3339 timestamps as times and
	// other strings lexically.
	CompareCursor func(a, b any) (int, error)
	// Lookback returns the oldest cursor value the second read may emit for
	// a stream, given the greatest cursor of the first read, for sources that
	// re-read a window before their saved cursor. Nil allows nothing older
	// than that cursor.
	Lookback func(desc synclet.StreamDescriptor, cursor any) (any, error)
}

// RunSourceSuite checks that src keeps the protocol contract and reports
// every violation through t.Errorf:
//
//   - spec returns a connectionSpecification that is a valid JSON schema;
//   - check succeeds with Config and fails with a message with BrokenConfig;
//   - discover returns streams whose json_schema is a valid JSON schema
//     with properties;
//   - read emits records only of the configured streams, each valid against
//     its stream's json_schema with no undeclared top-level field; a
//     date-time or time property with airbyte_type timestamp_without_timezone
//     or time_without_timezone takes values without a zone;
//   - no record follows its stream's last STATE, and an incremental stream
//     with records emits a STATE;
//   - every STATE carries sourceStats.recordCount equal to the records it
//     covers: a STREAM state its stream's records since that stream's
//     previous STATE, a GLOBAL or LEGACY state every record since the
//     previous STATE;
//   - every incremental stream with records has a value at its cursor field,
//     and a second read from the first read's last STATE emits no record
//     whose cursor is older than the first read's greatest cursor, or than
//     the bound Lookback returns for it;
//   - read returns within CancelTimeout after its context is cancelled at
//     its first message (see CancelTimeout for what this catches).
func RunSourceSuite[C any](t testing.TB, src synclet.Source[C], s SourceSuite) {
	t.Helper()
	if s.Config == nil || s.BrokenConfig == nil || s.Catalog == nil {
		t.Errorf("testkit: SourceSuite needs Config, BrokenConfig and Catalog")
		return
	}
	catalog, err := json.Marshal(s.Catalog)
	if err != nil {
		t.Errorf("testkit: marshal catalog: %v", err)
		return
	}
	ctx := t.Context()
	readReq := synclet.Request{Command: synclet.CommandRead, Config: s.Config, Catalog: catalog, State: s.State}

	checkSpec(ctx, t, src)
	checkCheck(ctx, t, src, s)
	checkDiscover(ctx, t, src, s.Config)
	if first := checkRead(ctx, t, src, s.Catalog, readReq); first != nil {
		checkStateOrder(t, s.Catalog, first)
		checkSourceStats(t, first)
		next := readReq
		next.State = first.NextState()
		checkIncremental(ctx, t, src, s, first, next)
	}
	checkCancel(ctx, t, src, readReq)
}

func checkSpec[C any](ctx context.Context, t testing.TB, src synclet.Source[C]) {
	t.Helper()
	out, ok := run(ctx, t, "spec", src, synclet.Request{Command: synclet.CommandSpec})
	if !ok {
		return
	}
	spec := out.Spec()
	if spec == nil {
		t.Errorf("spec: no SPEC message")
		return
	}
	raw, err := json.Marshal(spec.ConnectionSpecification)
	if err != nil {
		t.Errorf("spec: marshal connectionSpecification: %v", err)
		return
	}
	if _, err := compileSchema(raw); err != nil {
		t.Errorf("spec: connectionSpecification is not a valid JSON schema: %v", err)
	}
}

func checkCheck[C any](ctx context.Context, t testing.TB, src synclet.Source[C], s SourceSuite) {
	t.Helper()
	if out, ok := run(ctx, t, "check: valid config", src, synclet.Request{Command: synclet.CommandCheck, Config: s.Config}); ok {
		switch st := out.ConnectionStatus(); {
		case st == nil:
			t.Errorf("check: valid config: no CONNECTION_STATUS message")
		case st.Status != synclet.ConnectionStatusSucceeded:
			t.Errorf("check: valid config: status %s: %s", st.Status, st.Message)
		}
	}
	if out, ok := run(ctx, t, "check: broken config", src, synclet.Request{Command: synclet.CommandCheck, Config: s.BrokenConfig}); ok {
		switch st := out.ConnectionStatus(); {
		case st == nil:
			t.Errorf("check: broken config: no CONNECTION_STATUS message")
		case st.Status != synclet.ConnectionStatusFailed:
			t.Errorf("check: broken config: status %s, want FAILED", st.Status)
		case strings.TrimSpace(st.Message) == "":
			t.Errorf("check: broken config: FAILED without a message")
		}
	}
}

func checkDiscover[C any](ctx context.Context, t testing.TB, src synclet.Source[C], cfg json.RawMessage) {
	t.Helper()
	out, ok := run(ctx, t, "discover", src, synclet.Request{Command: synclet.CommandDiscover, Config: cfg})
	if !ok {
		return
	}
	cat := out.Catalog()
	if cat == nil {
		t.Errorf("discover: no CATALOG message")
		return
	}
	if len(cat.Streams) == 0 {
		t.Errorf("discover: no streams")
	}
	for _, st := range cat.Streams {
		name := streamName(st.Descriptor())
		if st.Name == "" {
			t.Errorf("discover: stream without a name")
		}
		if _, err := compileSchema(st.JSONSchema); err != nil {
			t.Errorf("discover: stream %s: json_schema is not a valid JSON schema: %v", name, err)
			continue
		}
		if props, _ := schemaProperties(st.JSONSchema); len(props) == 0 {
			t.Errorf("discover: stream %s: json_schema has no properties", name)
		}
	}
}

func checkRead[C any](ctx context.Context, t testing.TB, src synclet.Source[C], cat *synclet.ConfiguredCatalog, req synclet.Request) *Output {
	t.Helper()
	out, ok := run(ctx, t, "read", src, req)
	if !ok {
		return nil
	}
	configured := map[synclet.StreamDescriptor]synclet.ConfiguredStream{}
	for _, cs := range cat.Streams {
		configured[cs.Stream.Descriptor()] = cs
	}
	var order []synclet.StreamDescriptor
	records := map[synclet.StreamDescriptor][]synclet.RecordMessage{}
	for _, m := range out.Messages {
		if m.Type != synclet.MessageTypeRecord {
			continue
		}
		desc := recordDescriptor(m.Record)
		if _, seen := records[desc]; !seen {
			order = append(order, desc)
		}
		records[desc] = append(records[desc], *m.Record)
	}
	if len(records) == 0 {
		t.Errorf("read: no records")
	}
	for _, desc := range order {
		recs := records[desc]
		cs, ok := configured[desc]
		if !ok {
			t.Errorf("read: %d record(s) of stream %s, which the catalog does not configure", len(recs), streamName(desc))
			continue
		}
		v, err := newRecordValidator(cs.Stream.JSONSchema)
		if err != nil {
			t.Errorf("read: stream %s: json_schema: %v", streamName(desc), err)
			continue
		}
		var bad int
		var first error
		for _, r := range recs {
			if err := v.validate(r.Data); err != nil {
				bad++
				if first == nil {
					first = err
				}
			}
		}
		if bad > 0 {
			t.Errorf("read: stream %s: %d record(s) outside json_schema; first: %v", streamName(desc), bad, first)
		}
	}
	return out
}

func checkStateOrder(t testing.TB, cat *synclet.ConfiguredCatalog, out *Output) {
	t.Helper()
	for _, cs := range cat.Streams {
		desc := cs.Stream.Descriptor()
		lastState := -1
		for i, m := range out.Messages {
			if m.Type == synclet.MessageTypeState && stateCovers(m.State, desc) {
				lastState = i
			}
		}
		var records, after int
		for i, m := range out.Messages {
			if m.Type == synclet.MessageTypeRecord && recordDescriptor(m.Record) == desc {
				records++
				if i > lastState {
					after++
				}
			}
		}
		switch {
		case lastState >= 0 && after > 0:
			t.Errorf("state: stream %s: %d record(s) after its last STATE", streamName(desc), after)
		case lastState < 0 && records > 0 && cs.SyncMode == synclet.SyncModeIncremental:
			t.Errorf("state: incremental stream %s emitted %d record(s) and no STATE", streamName(desc), records)
		}
	}
}

func checkSourceStats(t testing.TB, out *Output) {
	t.Helper()
	perStream := map[synclet.StreamDescriptor]int64{}
	var total int64
	var n int
	for _, m := range out.Messages {
		switch {
		case m.Type == synclet.MessageTypeRecord && m.Record != nil:
			perStream[recordDescriptor(m.Record)]++
			total++
		case m.Type == synclet.MessageTypeState && m.State != nil:
			n++
			st := m.State
			isStream := strings.EqualFold(string(st.Type), string(synclet.StateTypeStream)) && st.Stream != nil
			want, what, covers := total, strings.ToUpper(string(st.Type)), "the records since the previous STATE"
			if isStream {
				desc := st.Stream.StreamDescriptor
				want, what, covers = perStream[desc], "stream "+streamName(desc), "the records since the stream's previous STATE"
				total -= want
				delete(perStream, desc)
			} else {
				total = 0
				clear(perStream)
			}
			switch {
			case st.SourceStats == nil:
				t.Errorf("state: STATE %d (%s) has no sourceStats.recordCount", n, what)
			case st.SourceStats.RecordCount != float64(want):
				t.Errorf("state: STATE %d (%s): sourceStats.recordCount %v, want %d, %s", n, what, st.SourceStats.RecordCount, want, covers)
			}
		}
	}
}

func stateCovers(st *synclet.StateMessage, desc synclet.StreamDescriptor) bool {
	if st == nil {
		return false
	}
	if st.Type == synclet.StateTypeStream {
		return st.Stream != nil && st.Stream.StreamDescriptor == desc
	}
	return true
}

func checkIncremental[C any](ctx context.Context, t testing.TB, src synclet.Source[C], s SourceSuite, first *Output, req synclet.Request) {
	t.Helper()
	compare := s.CompareCursor
	if compare == nil {
		compare = compareCursor
	}
	type cursor struct {
		path []string
		top  any
	}
	cursors := map[synclet.StreamDescriptor]cursor{}
	for _, cs := range s.Catalog.Streams {
		if cs.SyncMode != synclet.SyncModeIncremental {
			continue
		}
		desc := cs.Stream.Descriptor()
		path := cursorField(cs)
		if len(path) == 0 {
			t.Errorf("incremental: stream %s has no cursor_field and no source-defined cursor", streamName(desc))
			continue
		}
		recs := first.Records(desc)
		top, err := maxCursor(recs, path, compare)
		if err != nil {
			t.Errorf("incremental: stream %s: %v", streamName(desc), err)
			continue
		}
		if top == nil {
			if len(recs) > 0 {
				t.Errorf("incremental: stream %s: %d record(s) and none has a value at cursor field %s", streamName(desc), len(recs), strings.Join(path, "."))
			}
			continue
		}
		if s.Lookback != nil {
			if top, err = s.Lookback(desc, top); err != nil {
				t.Errorf("incremental: stream %s: lookback: %v", streamName(desc), err)
				continue
			}
		}
		cursors[desc] = cursor{path: path, top: top}
	}
	if len(cursors) == 0 {
		return
	}
	if req.State == nil {
		t.Errorf("incremental: the first read emitted no STATE to resume from")
		return
	}
	out, ok := run(ctx, t, "incremental: second read", src, req)
	if !ok {
		return
	}
	for _, cs := range s.Catalog.Streams {
		desc := cs.Stream.Descriptor()
		c, ok := cursors[desc]
		if !ok {
			continue
		}
		older, err := countOlder(out.Records(desc), c.path, c.top, compare)
		if err != nil {
			t.Errorf("incremental: stream %s: %v", streamName(desc), err)
			continue
		}
		if older > 0 {
			t.Errorf("incremental: stream %s: %d record(s) older than the cursor %v", streamName(desc), older, c.top)
		}
	}
}

func cursorField(cs synclet.ConfiguredStream) []string {
	if cs.Stream.SourceDefinedCursor && len(cs.Stream.DefaultCursorField) > 0 {
		return cs.Stream.DefaultCursorField
	}
	if len(cs.CursorField) > 0 {
		return cs.CursorField
	}
	return cs.Stream.DefaultCursorField
}

func maxCursor(recs []synclet.RecordMessage, path []string, compare func(a, b any) (int, error)) (any, error) {
	var top any
	for _, r := range recs {
		v, err := cursorValue(r.Data, path)
		if err != nil {
			return nil, err
		}
		if v == nil {
			continue
		}
		if top == nil {
			top = v
			continue
		}
		cmp, err := compare(v, top)
		if err != nil {
			return nil, err
		}
		if cmp > 0 {
			top = v
		}
	}
	return top, nil
}

func countOlder(recs []synclet.RecordMessage, path []string, cursor any, compare func(a, b any) (int, error)) (int, error) {
	var older int
	for _, r := range recs {
		v, err := cursorValue(r.Data, path)
		if err != nil {
			return 0, err
		}
		if v == nil {
			continue
		}
		cmp, err := compare(v, cursor)
		if err != nil {
			return 0, err
		}
		if cmp < 0 {
			older++
		}
	}
	return older, nil
}

func cursorValue(data json.RawMessage, path []string) (any, error) {
	v, err := decodeJSON(data)
	if err != nil {
		return nil, fmt.Errorf("decode record: %w", err)
	}
	for _, key := range path {
		obj, ok := v.(map[string]any)
		if !ok {
			return nil, nil
		}
		v = obj[key]
	}
	return v, nil
}

func compareCursor(a, b any) (int, error) {
	switch av := a.(type) {
	case json.Number:
		bv, ok := b.(json.Number)
		if !ok {
			break
		}
		x, okx := new(big.Rat).SetString(av.String())
		y, oky := new(big.Rat).SetString(bv.String())
		if !okx || !oky {
			return 0, fmt.Errorf("cannot compare cursor values %s and %s", av, bv)
		}
		return x.Cmp(y), nil
	case string:
		bv, ok := b.(string)
		if !ok {
			break
		}
		x, errx := time.Parse(time.RFC3339Nano, av)
		y, erry := time.Parse(time.RFC3339Nano, bv)
		if errx == nil && erry == nil {
			return x.Compare(y), nil
		}
		return strings.Compare(av, bv), nil
	}
	return 0, fmt.Errorf("cannot compare cursor values %v (%T) and %v (%T); set SourceSuite.CompareCursor", a, a, b, b)
}

// cancelAtFirstWrite discards the stream; its first Write closes first and
// returns only once cancelled is closed, so the read goes on under a
// cancelled context.
type cancelAtFirstWrite struct {
	once      sync.Once
	first     chan struct{}
	cancelled chan struct{}
}

func (w *cancelAtFirstWrite) Write(p []byte) (int, error) {
	w.once.Do(func() {
		close(w.first)
		<-w.cancelled
	})
	return len(p), nil
}

// checkCancel cancels a read at its first message, or after 100ms when it
// writes nothing, and waits CancelTimeout for it to return.
func checkCancel[C any](ctx context.Context, t testing.TB, src synclet.Source[C], req synclet.Request) {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w := &cancelAtFirstWrite{first: make(chan struct{}), cancelled: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = synclet.ServeSource(ctx, src, req, w)
	}()
	select {
	case <-w.first:
	case <-done:
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	close(w.cancelled)
	select {
	case <-done:
	case <-time.After(CancelTimeout):
		t.Errorf("cancel: read still running %s after its context was cancelled", CancelTimeout)
	}
}

func run[C any](ctx context.Context, t testing.TB, step string, src synclet.Source[C], req synclet.Request) (*Output, bool) {
	t.Helper()
	out, err := RunSource(ctx, src, req)
	if err != nil {
		t.Errorf("%s: %v", step, err)
		return nil, false
	}
	if out.Err != nil {
		t.Errorf("%s: %v", step, out.Err)
		return nil, false
	}
	return out, true
}

func compileSchema(raw json.RawMessage) (*jsonschema.Schema, error) {
	doc, err := decodeJSON(raw)
	if err != nil {
		return nil, err
	}
	localizeFormats(doc)
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft7)
	c.RegisterFormat(localDateTime)
	c.RegisterFormat(localTime)
	const url = "mem://schema.json"
	if err := c.AddResource(url, doc); err != nil {
		return nil, err
	}
	return c.Compile(url)
}

func schemaProperties(raw json.RawMessage) (props map[string]json.RawMessage, additional bool) {
	var s struct {
		Properties           map[string]json.RawMessage `json:"properties"`
		AdditionalProperties json.RawMessage            `json:"additionalProperties"`
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, false
	}
	return s.Properties, s.AdditionalProperties != nil
}

// recordValidator checks a record against its stream's json_schema. A
// top-level field the schema does not declare is an error unless the schema
// sets additionalProperties.
type recordValidator struct {
	schema   *jsonschema.Schema
	declared map[string]json.RawMessage
	strict   bool
}

func newRecordValidator(raw json.RawMessage) (*recordValidator, error) {
	sch, err := compileSchema(raw)
	if err != nil {
		return nil, err
	}
	props, additional := schemaProperties(raw)
	return &recordValidator{schema: sch, declared: props, strict: props != nil && !additional}, nil
}

func (v *recordValidator) validate(data json.RawMessage) error {
	doc, err := decodeJSON(data)
	if err != nil {
		return fmt.Errorf("decode record: %w", err)
	}
	if err := v.schema.Validate(doc); err != nil {
		return err
	}
	obj, ok := doc.(map[string]any)
	if !ok || !v.strict {
		return nil
	}
	var undeclared []string
	for name := range obj {
		if _, ok := v.declared[name]; !ok {
			undeclared = append(undeclared, name)
		}
	}
	if len(undeclared) > 0 {
		slices.Sort(undeclared)
		return fmt.Errorf("fields not declared in json_schema: %s", strings.Join(undeclared, ", "))
	}
	return nil
}

func decodeJSON(raw json.RawMessage) (any, error) {
	return jsonschema.UnmarshalJSON(bytes.NewReader(raw))
}

func streamName(desc synclet.StreamDescriptor) string {
	if desc.Namespace == "" {
		return desc.Name
	}
	return desc.Namespace + "." + desc.Name
}
