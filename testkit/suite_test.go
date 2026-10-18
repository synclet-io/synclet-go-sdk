package testkit_test

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

// recorder is a testing.TB that collects the suite's failures instead of
// failing the test that runs it.
type recorder struct {
	testing.TB
	mu   sync.Mutex
	errs []string
}

func (r *recorder) Helper() {}

func (r *recorder) Errorf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, fmt.Sprintf(format, args...))
}

func (r *recorder) Error(args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, fmt.Sprint(args...))
}

func (r *recorder) Fatalf(format string, args ...any) {
	r.Errorf(format, args...)
	r.FailNow()
}

func (r *recorder) Fatal(args ...any) {
	r.Error(args...)
	r.FailNow()
}

func runSuite(t *testing.T, src *tableSource) []string {
	t.Helper()
	rec := &recorder{TB: t}
	testkit.RunSourceSuite(rec, src, testkit.SourceSuite{
		Config:       json.RawMessage(validConfig),
		BrokenConfig: json.RawMessage(brokenConfig),
		Catalog:      configuredCatalog(src),
	})
	return rec.errs
}

func TestSourceSuitePasses(t *testing.T) {
	src := newTableSource()
	assert.Empty(t, runSuite(t, src))
}

func TestSourceSuiteDetects(t *testing.T) {
	tests := []struct {
		fault fault
		want  string
	}{
		{faultInvalidSpec, "spec: connectionSpecification is not a valid JSON schema"},
		{faultCheckIgnoresConfig, "check: broken config: status SUCCEEDED, want FAILED"},
		{faultEmptySchema, "discover: stream app.users: json_schema has no properties"},
		{faultUnconfiguredStream, "read: 2 record(s) of stream ghosts, which the catalog does not configure"},
		{faultOutsideSchema, "read: stream app.users: 3 record(s) outside json_schema"},
		{faultWrongType, "read: stream app.users: 3 record(s) outside json_schema"},
		{faultStateFirst, "state: stream app.users: 3 record(s) after its last STATE"},
		{faultCheckRejectsValid, "check: valid config: status FAILED: token rejected"},
		{faultCheckNoMessage, "check: broken config: FAILED without a message"},
		{faultNoStreams, "discover: no streams"},
		{faultInvalidSchema, "discover: stream app.users: json_schema is not a valid JSON schema"},
		{faultNoRecords, "read: no records"},
		{faultNoState, "state: incremental stream app.users emitted 3 record(s) and no STATE"},
		{faultNoCursorValue, "incremental: stream app.users: 3 record(s) and none has a value at cursor field updated_at"},
		{faultLookback, "incremental: stream app.orders: 1 record(s) older than the cursor 20"},
		{faultIgnoresCursor, "incremental: stream app.users: 2 record(s) older than the cursor 2026-01-03T00:00:00Z"},
	}
	for _, tt := range tests {
		t.Run(string(tt.fault), func(t *testing.T) {
			src := newTableSource()
			src.fault = tt.fault
			src.release = make(chan struct{})
			t.Cleanup(func() { close(src.release) })
			errs := runSuite(t, src)
			require.NotEmpty(t, errs, "suite reported no failure")
			assert.True(t, containsLine(errs, tt.want), "want a failure containing %q, got:\n%s", tt.want, strings.Join(errs, "\n"))
		})
	}
}

func TestSourceSuiteNeedsCursorField(t *testing.T) {
	src := newTableSource()
	cat := configuredCatalog(src)
	cat.Streams[1].CursorField = nil
	rec := &recorder{TB: t}
	testkit.RunSourceSuite(rec, src, testkit.SourceSuite{Config: json.RawMessage(validConfig), BrokenConfig: json.RawMessage(brokenConfig), Catalog: cat})
	assert.True(t, containsLine(rec.errs, "incremental: stream app.orders has no cursor_field and no source-defined cursor"), strings.Join(rec.errs, "\n"))
}

func TestSourceSuiteCancelIsTimed(t *testing.T) {
	src := newTableSource()
	src.fault = faultIgnoresCancel
	src.release = make(chan struct{})
	t.Cleanup(func() { close(src.release) })
	start := time.Now()
	errs := runSuite(t, src)
	elapsed := time.Since(start)
	assert.True(t, containsLine(errs, "cancel: read still running 5s after its context was cancelled"), strings.Join(errs, "\n"))
	assert.GreaterOrEqual(t, elapsed, 5*time.Second)
	assert.Less(t, elapsed, 7*time.Second)
}

func TestCompareCursor(t *testing.T) {
	src := newTableSource()
	src.fault = faultIgnoresCursor
	var calls int
	rec := &recorder{TB: t}
	testkit.RunSourceSuite(rec, src, testkit.SourceSuite{
		Config:       json.RawMessage(validConfig),
		BrokenConfig: json.RawMessage(brokenConfig),
		Catalog:      configuredCatalog(src),
		CompareCursor: func(a, b any) (int, error) {
			calls++
			return 0, nil
		},
	})
	assert.Positive(t, calls)
	assert.Empty(t, rec.errs)
}

func TestSourceSuiteLookback(t *testing.T) {
	src := newTableSource()
	src.fault = faultLookback
	var streams []string
	rec := &recorder{TB: t}
	testkit.RunSourceSuite(rec, src, testkit.SourceSuite{
		Config:       json.RawMessage(validConfig),
		BrokenConfig: json.RawMessage(brokenConfig),
		Catalog:      configuredCatalog(src),
		Lookback: func(desc synclet.StreamDescriptor, cursor any) (any, error) {
			streams = append(streams, desc.Name)
			n, ok := cursor.(json.Number)
			if !ok {
				return cursor, nil
			}
			v, err := n.Int64()
			if err != nil {
				return nil, err
			}
			return json.Number(strconv.FormatInt(v-10, 10)), nil
		},
	})
	assert.Empty(t, rec.errs)
	assert.ElementsMatch(t, []string{"users", "orders"}, streams)
}

func TestSourceSuiteInitialState(t *testing.T) {
	src := newTableSource()
	state := `[{"type":"STREAM","stream":{"stream_descriptor":{"name":"orders","namespace":"app"},"stream_state":{"cursor":20}}}]`
	rec := &recorder{TB: t}
	testkit.RunSourceSuite(rec, src, testkit.SourceSuite{
		Config:       json.RawMessage(validConfig),
		BrokenConfig: json.RawMessage(brokenConfig),
		Catalog:      configuredCatalog(src),
		State:        json.RawMessage(state),
	})
	assert.Empty(t, rec.errs)
}

func containsLine(errs []string, want string) bool {
	for _, e := range errs {
		if strings.Contains(e, want) {
			return true
		}
	}
	return false
}
