package testkit

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordValidatorZoneLessTypes(t *testing.T) {
	v, err := newRecordValidator(json.RawMessage(`{"type":"object","properties":{
		"local_ts":{"type":["null","string"],"format":"date-time","airbyte_type":"timestamp_without_timezone"},
		"utc_ts":{"type":["null","string"],"format":"date-time","airbyte_type":"timestamp_with_timezone"},
		"plain_ts":{"type":["null","string"],"format":"date-time"},
		"local_time":{"type":["null","string"],"format":"time","airbyte_type":"time_without_timezone"},
		"nested":{"type":"object","properties":{"ts":{"type":"string","format":"date-time","airbyte_type":"timestamp_without_timezone"}}}
	}}`))
	require.NoError(t, err)
	tests := []struct {
		name   string
		record string
		valid  bool
	}{
		{"zone-less timestamp", `{"local_ts":"2026-10-16T10:00:00"}`, true},
		{"zone-less timestamp with fraction", `{"local_ts":"2026-10-16T10:00:00.123456"}`, true},
		{"null timestamp", `{"local_ts":null}`, true},
		{"timestamp with a zone where none belongs", `{"local_ts":"2026-10-16T10:00:00Z"}`, false},
		{"not a timestamp", `{"local_ts":"16.10.2026 10:00"}`, false},
		{"nested zone-less timestamp", `{"nested":{"ts":"2026-10-16T10:00:00"}}`, true},
		{"zone-less time", `{"local_time":"10:00:00"}`, true},
		{"time with a zone where none belongs", `{"local_time":"10:00:00+02:00"}`, false},
		{"with_timezone keeps requiring a zone", `{"utc_ts":"2026-10-16T10:00:00"}`, false},
		{"with_timezone with a zone", `{"utc_ts":"2026-10-16T10:00:00+00:00"}`, true},
		{"no airbyte_type keeps requiring a zone", `{"plain_ts":"2026-10-16T10:00:00"}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.validate(json.RawMessage(tt.record))
			if tt.valid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
