package synclet

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Each line is one Synclet's pkg/protocol reads or writes; decoding it into
// Message and encoding it again gives the same JSON.
func TestMessageWire(t *testing.T) {
	lines := []string{
		`{"type":"LOG","log":{"level":"INFO","message":"Starting connector..."}}`,
		`{"type":"SPEC","spec":{"documentationUrl":"https://example.com","changelogUrl":"https://example.com/changelog","supportsIncremental":true,"supportsNormalization":false,"supportsDBT":false,"supported_destination_sync_modes":["overwrite","append"],"connectionSpecification":{"title":"Example","description":"Example source","type":"object","required":["host"],"properties":{"host":{"type":"string","description":"Host","order":0},"port":{"type":"integer","default":5432},"since":{"type":"string","format":"date-time"},"password":{"type":"string","airbyte_secret":true}}},"protocol_version":"0.5.2"}}`,
		`{"type":"CONNECTION_STATUS","connectionStatus":{"status":"SUCCEEDED","message":"Connected"}}`,
		`{"type":"CONNECTION_STATUS","connectionStatus":{"status":"FAILED","message":"access denied for user 'x'"}}`,
		`{"type":"CATALOG","catalog":{"streams":[{"name":"users","json_schema":{"type":"object","properties":{"id":{"type":"integer"}}},"supported_sync_modes":["full_refresh","incremental"],"source_defined_cursor":true,"default_cursor_field":["updated_at"],"source_defined_primary_key":[["id"]],"namespace":"public"}]}}`,
		`{"type":"RECORD","record":{"stream":"users","data":{"id":1,"name":"Alice","email":"alice@example.com"},"emitted_at":1700000000000}}`,
		`{"type":"RECORD","record":{"stream":"users","namespace":"public","data":{"id":12345678901234567890,"price":1.10},"emitted_at":1700000000001}}`,
		`{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"users"},"stream_state":{"cursor":2}}}}`,
		`{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"users","namespace":"public"},"stream_state":{"cursor":"2023-01-01T00:00:00Z"}},"sourceStats":{"recordCount":2},"destinationStats":{"recordCount":2}}}`,
		`{"type":"STATE","state":{"type":"GLOBAL","global":{"shared_state":{"binlog":"mysql-bin.000001:4"},"stream_states":[{"stream_descriptor":{"name":"orders","namespace":"shop"},"stream_state":{"pk":10}},{"stream_descriptor":{"name":"users","namespace":"shop"}}]}}}`,
		`{"type":"STATE","state":{"type":"LEGACY","data":{"timestamp":1700000000000}}}`,
		`{"type":"TRACE","trace":{"type":"STREAM_STATUS","emitted_at":1700000000002,"stream_status":{"stream_descriptor":{"name":"users"},"status":"COMPLETE"}}}`,
		`{"type":"TRACE","trace":{"type":"ERROR","emitted_at":1700000000003,"error":{"message":"boom","internal_message":"details","stack_trace":"goroutine 1 [running]:","failure_type":"config_error","stream_descriptor":{"name":"users","namespace":"public"}}}}`,
		`{"type":"TRACE","trace":{"type":"ESTIMATE","emitted_at":1700000000004,"estimate":{"name":"users","namespace":"public","type":"STREAM","row_estimate":100,"byte_estimate":2048}}}`,
		`{"type":"TRACE","trace":{"type":"ANALYTICS","emitted_at":1700000000005,"analytics":{"type":"rows_read","value":"100"}}}`,
		`{"type":"CONTROL","control":{"type":"CONNECTOR_CONFIG","emitted_at":1700000000006,"connectorConfig":{"config":{"token":"new"}}}}`,
	}
	for _, line := range lines {
		var m Message
		require.NoError(t, json.Unmarshal([]byte(line), &m), line)
		got, err := json.Marshal(m)
		require.NoError(t, err, line)
		assert.JSONEq(t, line, string(got))
	}
}

func TestRecordDataKeepsLargeIntegers(t *testing.T) {
	line := `{"type":"RECORD","record":{"stream":"users","data":{"id":12345678901234567890},"emitted_at":1}}`
	var m Message
	require.NoError(t, json.Unmarshal([]byte(line), &m))
	got, err := json.Marshal(m)
	require.NoError(t, err)
	assert.True(t, bytes.Contains(got, []byte(`"id":12345678901234567890`)), string(got))
}

func TestObjectSchema(t *testing.T) {
	got := ObjectSchema(map[PropertyName]PropertySpec{
		"id":   {PropertyType: PropertyType{Type: PropTypes{Integer}}},
		"name": {PropertyType: PropertyType{Type: PropTypes{Null, String}}, Format: DateTime},
	})
	assert.JSONEq(t, `{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":["null","string"],"format":"date-time"}}}`, string(got))
	assert.JSONEq(t, `{"type":"object","properties":{}}`, string(ObjectSchema(nil)))
}

func TestStreamDescriptor(t *testing.T) {
	s := Stream{Name: "users", Namespace: "public"}
	assert.Equal(t, StreamDescriptor{Name: "users", Namespace: "public"}, s.Descriptor())
}
