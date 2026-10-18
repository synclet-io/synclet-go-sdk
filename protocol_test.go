package synclet

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestTraceMessageTypes tests trace message marshaling with different subtypes
func TestTraceMessageErrorType(t *testing.T) {
	failureType := failureTypeSystem
	internalMsg := "internal error details"
	stackTrace := "stack trace here"

	msg := &message{
		Type: msgTypeTrace,
		traceMessage: &traceMessage{
			EmittedAt: 1234567890.0,
			Type:      traceTypeError,
			Error: &errorTraceMessage{
				Message:         "Connection failed",
				FailureType:     &failureType,
				InternalMessage: &internalMsg,
				StackTrace:      &stackTrace,
			},
		},
	}

	data, err := json.Marshal(msg)
	assert.NoError(t, err, "Failed to marshal trace error message")

	expectedJSON := `{
		"type": "TRACE",
		"trace": {
			"emitted_at": 1234567890.0,
			"type": "ERROR",
			"error": {
				"message": "Connection failed",
				"failure_type": "system_error",
				"internal_message": "internal error details",
				"stack_trace": "stack trace here"
			}
		}
	}`

	assert.JSONEq(t, expectedJSON, string(data))
}

func TestTraceMessageEstimateType(t *testing.T) {
	namespace := "test_namespace"
	rowEstimate := int64(1000)
	byteEstimate := int64(50000)

	msg := &message{
		Type: msgTypeTrace,
		traceMessage: &traceMessage{
			EmittedAt: 1234567890.0,
			Type:      traceTypeEstimate,
			Estimate: &estimateTraceMessage{
				Name:         "users",
				Namespace:    &namespace,
				Type:         EstimateTypeStream,
				RowEstimate:  &rowEstimate,
				ByteEstimate: &byteEstimate,
			},
		},
	}

	data, err := json.Marshal(msg)
	assert.NoError(t, err, "Failed to marshal trace estimate message")

	expectedJSON := `{
		"type": "TRACE",
		"trace": {
			"emitted_at": 1234567890.0,
			"type": "ESTIMATE",
			"estimate": {
				"name": "users",
				"namespace": "test_namespace",
				"type": "STREAM",
				"row_estimate": 1000,
				"byte_estimate": 50000
			}
		}
	}`

	assert.JSONEq(t, expectedJSON, string(data))
}

func TestTraceMessageAnalyticsType(t *testing.T) {
	msg := &message{
		Type: msgTypeTrace,
		traceMessage: &traceMessage{
			EmittedAt: 1234567890.0,
			Type:      traceTypeAnalytics,
			Analytics: &analyticsTraceMessage{
				Type: "custom_event",
				Value: map[string]interface{}{
					"event": "sync_started",
					"count": 42,
				},
			},
		},
	}

	data, err := json.Marshal(msg)
	assert.NoError(t, err, "Failed to marshal trace analytics message")

	expectedJSON := `{
		"type": "TRACE",
		"trace": {
			"emitted_at": 1234567890.0,
			"type": "ANALYTICS",
			"analytics": {
				"type": "custom_event",
				"value": {
					"event": "sync_started",
					"count": 42
				}
			}
		}
	}`

	assert.JSONEq(t, expectedJSON, string(data))
}

// TestControlMessage tests control message marshaling
func TestControlMessage(t *testing.T) {
	msg := &message{
		Type: msgTypeControl,
		controlMessage: &controlMessage{
			EmittedAt: 1234567890.0,
			Type:      controlTypeConnectorConfig,
			ConnectorConfig: &controlConnectorConfigMessage{
				Config: map[string]interface{}{
					"apiKey":  "new-api-key",
					"timeout": 30,
				},
			},
		},
	}

	data, err := json.Marshal(msg)
	assert.NoError(t, err, "Failed to marshal control message")

	expectedJSON := `{
		"type": "CONTROL",
		"control": {
			"emitted_at": 1234567890.0,
			"type": "CONNECTOR_CONFIG",
			"connectorConfig": {
				"config": {
					"apiKey": "new-api-key",
					"timeout": 30
				}
			}
		}
	}`

	assert.JSONEq(t, expectedJSON, string(data))
}

// TestEnhancedStateMessages tests the new state types
func TestStateMessageLegacy(t *testing.T) {
	msg := &message{
		Type: msgTypeState,
		state: &state{
			Type: StateTypeLegacy,
			Data: map[string]interface{}{
				"lastSync": 1234567890,
			},
		},
	}

	data, err := json.Marshal(msg)
	assert.NoError(t, err, "Failed to marshal legacy state message")

	expectedJSON := `{
		"type": "STATE",
		"state": {
			"type": "LEGACY",
			"data": {
				"lastSync": 1234567890
			}
		}
	}`

	assert.JSONEq(t, expectedJSON, string(data))
}

func TestStateMessageStream(t *testing.T) {
	namespace := "test_namespace"

	msg := &message{
		Type: msgTypeState,
		state: &state{
			Type: StateTypeStream,
			Stream: &streamState{
				StreamDescriptor: streamDescriptor{
					Name:      "users",
					Namespace: &namespace,
				},
				StreamState: map[string]interface{}{
					"cursor": "2023-01-01T00:00:00Z",
				},
			},
		},
	}

	data, err := json.Marshal(msg)
	assert.NoError(t, err, "Failed to marshal stream state message")

	expectedJSON := `{
		"type": "STATE",
		"state": {
			"type": "STREAM",
			"stream": {
				"stream_descriptor": {
					"name": "users",
					"namespace": "test_namespace"
				},
				"stream_state": {
					"cursor": "2023-01-01T00:00:00Z"
				}
			}
		}
	}`

	assert.JSONEq(t, expectedJSON, string(data))
}

func TestStateMessageGlobal(t *testing.T) {
	namespace := "test_namespace"

	msg := &message{
		Type: msgTypeState,
		state: &state{
			Type: StateTypeGlobal,
			Global: &globalState{
				SharedState: map[string]interface{}{
					"lastFullRefresh": "2023-01-01T00:00:00Z",
				},
				StreamStates: []streamState{
					{
						StreamDescriptor: streamDescriptor{
							Name:      "users",
							Namespace: &namespace,
						},
						StreamState: map[string]interface{}{
							"cursor": "2023-01-01T00:00:00Z",
						},
					},
				},
			},
		},
	}

	data, err := json.Marshal(msg)
	assert.NoError(t, err, "Failed to marshal global state message")

	expectedJSON := `{
		"type": "STATE",
		"state": {
			"type": "GLOBAL",
			"global": {
				"shared_state": {
					"lastFullRefresh": "2023-01-01T00:00:00Z"
				},
				"stream_states": [
					{
						"stream_descriptor": {
							"name": "users",
							"namespace": "test_namespace"
						},
						"stream_state": {
							"cursor": "2023-01-01T00:00:00Z"
						}
					}
				]
			}
		}
	}`

	assert.JSONEq(t, expectedJSON, string(data))
}

// TestMessageValidation tests the MarshalJSON validation logic
func TestMessageValidationTraceOnly(t *testing.T) {
	// Valid trace message
	msg := &message{
		Type: msgTypeTrace,
		traceMessage: &traceMessage{
			EmittedAt: 1234567890.0,
			Type:      traceTypeError,
			Error: &errorTraceMessage{
				Message: "Test error",
			},
		},
	}

	_, err := json.Marshal(msg)
	assert.NoError(t, err, "Valid trace message should marshal without error")

	// Invalid: trace message with record also set
	msg.record = &record{
		EmittedAt: 1234567890,
		Stream:    "test",
		Data:      map[string]interface{}{"test": "data"},
	}

	_, err = json.Marshal(msg)
	assert.Error(t, err, "Message with both trace and record should fail validation")
}

func TestMessageValidationControlOnly(t *testing.T) {
	// Valid control message
	msg := &message{
		Type: msgTypeControl,
		controlMessage: &controlMessage{
			EmittedAt: 1234567890.0,
			Type:      controlTypeConnectorConfig,
			ConnectorConfig: &controlConnectorConfigMessage{
				Config: map[string]interface{}{"key": "value"},
			},
		},
	}

	_, err := json.Marshal(msg)
	assert.NoError(t, err, "Valid control message should marshal without error")

	// Invalid: control message with state also set
	msg.state = &state{
		Type: StateTypeLegacy,
		Data: map[string]interface{}{"test": "data"},
	}

	_, err = json.Marshal(msg)
	assert.Error(t, err, "Message with both control and state should fail validation")
}

// TestWriterFunctions tests the writer functions
func TestTraceWriter(t *testing.T) {
	var buf bytes.Buffer
	writer := newTraceWriter(&buf)

	failureType := failureTypeConfig
	err := writer(traceTypeError, &errorTraceMessage{
		Message:     "Test error",
		FailureType: &failureType,
	})

	assert.NoError(t, err, "TraceWriter should not fail")

	// Unmarshal to normalize timestamp
	var result map[string]interface{}
	err = json.Unmarshal(buf.Bytes(), &result)
	assert.NoError(t, err)

	// Normalize emitted_at to fixed value for comparison
	if trace, ok := result["trace"].(map[string]interface{}); ok {
		trace["emitted_at"] = float64(0)
	}

	normalizedJSON, err := json.Marshal(result)
	assert.NoError(t, err)

	expectedJSON := `{
		"type": "TRACE",
		"trace": {
			"emitted_at": 0,
			"type": "ERROR",
			"error": {
				"message": "Test error",
				"failure_type": "config_error"
			}
		}
	}`

	assert.JSONEq(t, expectedJSON, string(normalizedJSON))
}

func TestControlWriter(t *testing.T) {
	var buf bytes.Buffer
	writer := newControlWriter(&buf)

	err := writer(controlTypeConnectorConfig, &controlConnectorConfigMessage{
		Config: map[string]interface{}{
			"apiKey": "test-key",
		},
	})

	assert.NoError(t, err, "ControlWriter should not fail")

	// Unmarshal to normalize timestamp
	var result map[string]interface{}
	err = json.Unmarshal(buf.Bytes(), &result)
	assert.NoError(t, err)

	// Normalize emitted_at to fixed value for comparison
	if control, ok := result["control"].(map[string]interface{}); ok {
		control["emitted_at"] = float64(0)
	}

	normalizedJSON, err := json.Marshal(result)
	assert.NoError(t, err)

	expectedJSON := `{
		"type": "CONTROL",
		"control": {
			"emitted_at": 0,
			"type": "CONNECTOR_CONFIG",
			"connectorConfig": {
				"config": {
					"apiKey": "test-key"
				}
			}
		}
	}`

	assert.JSONEq(t, expectedJSON, string(normalizedJSON))
}

func TestStateWriterLegacy(t *testing.T) {
	var buf bytes.Buffer
	writer := newStateWriter(&buf)

	err := writer(StateTypeLegacy, map[string]interface{}{"lastSync": 1234567890})
	assert.NoError(t, err, "StateWriter should not fail")

	expectedJSON := `{
		"type": "STATE",
		"state": {
			"type": "LEGACY",
			"data": {
				"lastSync": 1234567890
			}
		}
	}`

	assert.JSONEq(t, expectedJSON, buf.String())
}

func TestStateWriterStream(t *testing.T) {
	var buf bytes.Buffer
	writer := newStateWriter(&buf)

	namespace := "test_namespace"
	err := writer(StateTypeStream, &streamState{
		StreamDescriptor: streamDescriptor{
			Name:      "users",
			Namespace: &namespace,
		},
		StreamState: map[string]interface{}{"cursor": "2023-01-01"},
	})

	assert.NoError(t, err, "StateWriter should not fail")

	expectedJSON := `{
		"type": "STATE",
		"state": {
			"type": "STREAM",
			"stream": {
				"stream_descriptor": {
					"name": "users",
					"namespace": "test_namespace"
				},
				"stream_state": {
					"cursor": "2023-01-01"
				}
			}
		}
	}`

	assert.JSONEq(t, expectedJSON, buf.String())
}

// TestHelperFunctions tests the convenience helper functions
func TestEmitError(t *testing.T) {
	var buf bytes.Buffer
	writer := newTraceWriter(&buf)

	failureType := failureTypeSystem
	internalMsg := "internal details"
	stackTrace := "stack trace"

	err := EmitError(writer, "Test error", &failureType, &internalMsg, &stackTrace)
	assert.NoError(t, err, "EmitError should not fail")

	// Unmarshal to normalize timestamp
	var result map[string]interface{}
	err = json.Unmarshal(buf.Bytes(), &result)
	assert.NoError(t, err)

	// Normalize emitted_at to fixed value for comparison
	if trace, ok := result["trace"].(map[string]interface{}); ok {
		trace["emitted_at"] = float64(0)
	}

	normalizedJSON, err := json.Marshal(result)
	assert.NoError(t, err)

	expectedJSON := `{
		"type": "TRACE",
		"trace": {
			"emitted_at": 0,
			"type": "ERROR",
			"error": {
				"message": "Test error",
				"failure_type": "system_error",
				"internal_message": "internal details",
				"stack_trace": "stack trace"
			}
		}
	}`

	assert.JSONEq(t, expectedJSON, string(normalizedJSON))
}

func TestEmitEstimate(t *testing.T) {
	var buf bytes.Buffer
	writer := newTraceWriter(&buf)

	namespace := "test_namespace"
	rowEstimate := int64(1000)
	byteEstimate := int64(50000)

	err := EmitEstimate(writer, "users", &namespace, EstimateTypeStream, &rowEstimate, &byteEstimate)
	assert.NoError(t, err, "EmitEstimate should not fail")

	// Unmarshal to normalize timestamp
	var result map[string]interface{}
	err = json.Unmarshal(buf.Bytes(), &result)
	assert.NoError(t, err)

	// Normalize emitted_at to fixed value for comparison
	if trace, ok := result["trace"].(map[string]interface{}); ok {
		trace["emitted_at"] = float64(0)
	}

	normalizedJSON, err := json.Marshal(result)
	assert.NoError(t, err)

	expectedJSON := `{
		"type": "TRACE",
		"trace": {
			"emitted_at": 0,
			"type": "ESTIMATE",
			"estimate": {
				"name": "users",
				"namespace": "test_namespace",
				"type": "STREAM",
				"row_estimate": 1000,
				"byte_estimate": 50000
			}
		}
	}`

	assert.JSONEq(t, expectedJSON, string(normalizedJSON))
}

// TestDestinationSyncModes tests the new sync mode
func TestDestinationSyncModeAppendDedup(t *testing.T) {
	assert.Equal(t, DestinationSyncMode("append_dedup"), DestinationSyncModeAppendDedup)
}

// TestProtocolVersion tests that protocol version can be set
func TestProtocolVersion(t *testing.T) {
	spec := &ConnectorSpecification{
		ProtocolVersion: "0.5.2",
		ConnectionSpecification: ConnectionSpecification{
			Title: "Test",
			Type:  "object",
		},
	}

	data, err := json.Marshal(spec)
	assert.NoError(t, err, "Failed to marshal spec")

	expectedJSON := `{
		"protocol_version": "0.5.2",
		"connectionSpecification": {
			"title": "Test",
			"type": "object",
			"description": "",
			"required": null,
			"properties": null
		},
		"supportsIncremental": false,
		"supportsNormalization": false,
		"supportsDBT": false,
		"supported_destination_sync_modes": null,
		"changeLogUrl": ""
	}`

	assert.JSONEq(t, expectedJSON, string(data))
}
