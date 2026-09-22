package airbyte

import (
	"encoding/json"
	"errors"
	"io"
	"time"
)

type cmd string

const (
	cmdSpec     cmd = "spec"
	cmdCheck    cmd = "check"
	cmdDiscover cmd = "discover"
	cmdRead     cmd = "read"
	cmdWrite    cmd = "write"
)

type msgType string

const (
	msgTypeRecord         msgType = "RECORD"
	msgTypeState          msgType = "STATE"
	msgTypeLog            msgType = "LOG"
	msgTypeConnectionStat msgType = "CONNECTION_STATUS"
	msgTypeCatalog        msgType = "CATALOG"
	msgTypeSpec           msgType = "SPEC"
	msgTypeTrace          msgType = "TRACE"
	msgTypeControl        msgType = "CONTROL"
)

var errInvalidTypePayload = errors.New("message type and payload are invalid")

type message struct {
	Type                    msgType `json:"type"`
	*record                 `json:"record,omitempty"`
	*state                  `json:"state,omitempty"`
	*logMessage             `json:"log,omitempty"`
	*ConnectorSpecification `json:"spec,omitempty"`
	*connectionStatus       `json:"connectionStatus,omitempty"`
	*Catalog                `json:"catalog,omitempty"`
	*traceMessage           `json:"trace,omitempty"`
	*controlMessage         `json:"control,omitempty"`
}

// message MarshalJSON is a custom marshaller which validates the messageType with the sub-struct
func (m *message) MarshalJSON() ([]byte, error) {
	switch m.Type {
	case msgTypeRecord:
		if m.record == nil ||
			m.state != nil ||
			m.logMessage != nil ||
			m.connectionStatus != nil ||
			m.Catalog != nil ||
			m.ConnectorSpecification != nil ||
			m.traceMessage != nil ||
			m.controlMessage != nil {
			return nil, errInvalidTypePayload
		}
	case msgTypeState:
		if m.state == nil ||
			m.record != nil ||
			m.logMessage != nil ||
			m.connectionStatus != nil ||
			m.Catalog != nil ||
			m.ConnectorSpecification != nil ||
			m.traceMessage != nil ||
			m.controlMessage != nil {
			return nil, errInvalidTypePayload
		}
	case msgTypeLog:
		if m.logMessage == nil ||
			m.record != nil ||
			m.state != nil ||
			m.connectionStatus != nil ||
			m.Catalog != nil ||
			m.ConnectorSpecification != nil ||
			m.traceMessage != nil ||
			m.controlMessage != nil {
			return nil, errInvalidTypePayload
		}
	case msgTypeTrace:
		if m.traceMessage == nil ||
			m.record != nil ||
			m.state != nil ||
			m.logMessage != nil ||
			m.connectionStatus != nil ||
			m.Catalog != nil ||
			m.ConnectorSpecification != nil ||
			m.controlMessage != nil {
			return nil, errInvalidTypePayload
		}
	case msgTypeControl:
		if m.controlMessage == nil ||
			m.record != nil ||
			m.state != nil ||
			m.logMessage != nil ||
			m.connectionStatus != nil ||
			m.Catalog != nil ||
			m.ConnectorSpecification != nil ||
			m.traceMessage != nil {
			return nil, errInvalidTypePayload
		}
	case msgTypeSpec:
		if m.ConnectorSpecification == nil ||
			m.record != nil ||
			m.state != nil ||
			m.logMessage != nil ||
			m.connectionStatus != nil ||
			m.Catalog != nil ||
			m.traceMessage != nil ||
			m.controlMessage != nil {
			return nil, errInvalidTypePayload
		}
	case msgTypeCatalog:
		if m.Catalog == nil ||
			m.record != nil ||
			m.state != nil ||
			m.logMessage != nil ||
			m.connectionStatus != nil ||
			m.ConnectorSpecification != nil ||
			m.traceMessage != nil ||
			m.controlMessage != nil {
			return nil, errInvalidTypePayload
		}
	case msgTypeConnectionStat:
		if m.connectionStatus == nil ||
			m.record != nil ||
			m.state != nil ||
			m.logMessage != nil ||
			m.Catalog != nil ||
			m.ConnectorSpecification != nil ||
			m.traceMessage != nil ||
			m.controlMessage != nil {
			return nil, errInvalidTypePayload
		}
	}

	type m2 message
	return json.Marshal(m2(*m))
}

// write emits data outbound from your source/destination to airbyte workers
func write(w io.Writer, m *message) error {
	return json.NewEncoder(w).Encode(m)
}

// record defines a record as per airbyte - a "data point"
type record struct {
	EmittedAt int64       `json:"emitted_at"`
	Namespace string      `json:"namespace"`
	Data      interface{} `json:"data"`
	Stream    string      `json:"stream"`
}

// StateType defines the type of state (STREAM, GLOBAL, or LEGACY)
type StateType string

const (
	StateTypeStream StateType = "STREAM"
	StateTypeGlobal StateType = "GLOBAL"
	StateTypeLegacy StateType = "LEGACY"
)

// StreamDescriptor identifies a specific stream by name and optional namespace
type StreamDescriptor struct {
	Name      string  `json:"name"`
	Namespace *string `json:"namespace,omitempty"`
}

// streamDescriptor is an alias kept for internal JSON compatibility.
type streamDescriptor = StreamDescriptor

// StreamState represents state for a single stream (output format for Airbyte coordinator).
type StreamState struct {
	StreamDescriptor StreamDescriptor       `json:"stream_descriptor"`
	StreamState      map[string]interface{} `json:"stream_state,omitempty"`
}

// streamState is an alias kept for internal JSON compatibility.
type streamState = StreamState

// InputState represents a single state entry from the Airbyte input state file.
// The input file is a JSON array of these objects.
type InputState = state

// LoadStreamStates parses the Airbyte input state file and returns a map
// keyed by "namespace:name" (or just "name" if no namespace) to stream state data.
func LoadStreamStates(path string) (map[string]map[string]interface{}, error) {
	var entries []InputState
	if err := UnmarshalFromPath(path, &entries); err != nil {
		return nil, err
	}

	result := make(map[string]map[string]interface{}, len(entries))
	for _, entry := range entries {
		if entry.Type != StateTypeStream || entry.Stream == nil {
			continue
		}
		key := entry.Stream.StreamDescriptor.Name
		if entry.Stream.StreamDescriptor.Namespace != nil {
			key = *entry.Stream.StreamDescriptor.Namespace + ":" + key
		}
		result[key] = entry.Stream.StreamState
	}
	return result, nil
}

// globalState represents state shared across multiple streams
type globalState struct {
	SharedState  map[string]interface{} `json:"shared_state,omitempty"`
	StreamStates []StreamState          `json:"stream_states"`
}

// state is used to store data between syncs - useful for incremental syncs and state storage
type state struct {
	Type   StateType    `json:"type"`             // State type: STREAM, GLOBAL, or LEGACY
	Stream *streamState `json:"stream,omitempty"` // Used when Type is STREAM
	Global *globalState `json:"global,omitempty"` // Used when Type is GLOBAL
	Data   interface{}  `json:"data,omitempty"`   // Used when Type is LEGACY
}

// LogLevel defines the log levels that can be emitted with airbyte logs
type LogLevel string

const (
	LogLevelFatal LogLevel = "FATAL"
	LogLevelError LogLevel = "ERROR"
	LogLevelWarn  LogLevel = "WARN"
	LogLevelInfo  LogLevel = "INFO"
	LogLevelDebug LogLevel = "DEBUG"
	LogLevelTrace LogLevel = "TRACE"
)

type logMessage struct {
	Level   LogLevel `json:"level"`
	Message string   `json:"message"`
}

// traceType defines the type of trace message
type traceType string

const (
	traceTypeError     traceType = "ERROR"
	traceTypeEstimate  traceType = "ESTIMATE"
	traceTypeAnalytics traceType = "ANALYTICS"
)

// traceMessage is used to emit runtime metadata (errors, estimates, analytics)
type traceMessage struct {
	EmittedAt float64                `json:"emitted_at"`
	Type      traceType              `json:"type"`
	Error     *errorTraceMessage     `json:"error,omitempty"`
	Estimate  *estimateTraceMessage  `json:"estimate,omitempty"`
	Analytics *analyticsTraceMessage `json:"analytics,omitempty"`
}

// failureType defines the type of failure for error trace messages
type failureType string

const (
	failureTypeSystem failureType = "system_error"
	failureTypeConfig failureType = "config_error"
)

// errorTraceMessage provides details about failures with user-friendly messaging
type errorTraceMessage struct {
	Message         string       `json:"message"`
	InternalMessage *string      `json:"internal_message,omitempty"`
	StackTrace      *string      `json:"stack_trace,omitempty"`
	FailureType     *failureType `json:"failure_type,omitempty"`
}

// EstimateType defines the scope of an estimate (per stream or entire sync)
type EstimateType string

const (
	EstimateTypeStream EstimateType = "STREAM"
	EstimateTypeSync   EstimateType = "SYNC"
)

// estimateTraceMessage provides row/byte count predictions for syncs
type estimateTraceMessage struct {
	Name         string       `json:"name"`
	Namespace    *string      `json:"namespace,omitempty"`
	Type         EstimateType `json:"type"`
	RowEstimate  *int64       `json:"row_estimate,omitempty"`
	ByteEstimate *int64       `json:"byte_estimate,omitempty"`
}

// analyticsTraceMessage is used for emitting custom analytics events
type analyticsTraceMessage struct {
	Type  string                 `json:"type"`
	Value map[string]interface{} `json:"value"`
}

type checkStatus string

const (
	checkStatusSuccess checkStatus = "SUCCEEDED"
	checkStatusFailed  checkStatus = "FAILED"
)

type connectionStatus struct {
	Status checkStatus `json:"status"`
}

// controlType defines the type of control message
type controlType string

const (
	controlTypeConnectorConfig controlType = "CONNECTOR_CONFIG"
)

// controlMessage allows connectors to update configuration mid-sync
type controlMessage struct {
	EmittedAt       float64                        `json:"emitted_at"`
	Type            controlType                    `json:"type"`
	ConnectorConfig *controlConnectorConfigMessage `json:"connectorConfig,omitempty"`
}

// controlConnectorConfigMessage contains updated connector configuration
type controlConnectorConfigMessage struct {
	Config map[string]interface{} `json:"config"`
}

// Catalog defines the complete available schema you can sync with a source
// This should not be mistaken with ConfiguredCatalog which is the "selected" schema you want to sync
type Catalog struct {
	Streams []Stream `json:"streams"`
}

// Stream defines a single "schema" you'd like to sync - think of this as a table, collection, topic, etc. In airbyte terminology these are "streams"
type Stream struct {
	Name                    string     `json:"name"`
	JSONSchema              Properties `json:"json_schema"`
	SupportedSyncModes      []SyncMode `json:"supported_sync_modes,omitempty"`
	SourceDefinedCursor     bool       `json:"source_defined_cursor,omitempty"`
	DefaultCursorField      []string   `json:"default_cursor_field,omitempty"`
	SourceDefinedPrimaryKey [][]string `json:"source_defined_primary_key,omitempty"`
	Namespace               string     `json:"namespace"`
}

// ConfiguredCatalog is the "selected" schema you want to sync
// This should not be mistaken with Catalog which represents the complete available schema to sync
type ConfiguredCatalog struct {
	Streams []ConfiguredStream `json:"streams"`
}

// ConfiguredStream defines a single selected stream to sync
type ConfiguredStream struct {
	Stream              Stream              `json:"stream"`
	SyncMode            SyncMode            `json:"sync_mode"`
	CursorField         []string            `json:"cursor_field"`
	DestinationSyncMode DestinationSyncMode `json:"destination_sync_mode"`
	PrimaryKey          [][]string          `json:"primary_key"`
}

// SyncMode defines the modes that your source is able to sync in
type SyncMode string

const (
	// SyncModeFullRefresh means the data will be wiped and fully synced on each run
	SyncModeFullRefresh SyncMode = "full_refresh"
	// SyncModeIncremental is used for incremental syncs
	SyncModeIncremental SyncMode = "incremental"
)

// DestinationSyncMode represents how the destination should interpret your data
type DestinationSyncMode string

var (
	// DestinationSyncModeAppend is used for the destination to know it needs to append data
	DestinationSyncModeAppend DestinationSyncMode = "append"
	// DestinationSyncModeOverwrite is used to indicate the destination should overwrite data
	DestinationSyncModeOverwrite DestinationSyncMode = "overwrite"
	// DestinationSyncModeAppendDedup is used to append and deduplicate data by primary key
	DestinationSyncModeAppendDedup DestinationSyncMode = "append_dedup"
)

// ConnectorSpecification is used to define the connector wide settings. Every connection using your connector will comply to these settings
type ConnectorSpecification struct {
	DocumentationURL              string                  `json:"documentationUrl,omitempty"`
	ChangeLogURL                  string                  `json:"changeLogUrl"`
	SupportsIncremental           bool                    `json:"supportsIncremental"`
	SupportsNormalization         bool                    `json:"supportsNormalization"`
	SupportsDBT                   bool                    `json:"supportsDBT"`
	SupportedDestinationSyncModes []DestinationSyncMode   `json:"supported_destination_sync_modes"`
	ConnectionSpecification       ConnectionSpecification `json:"connectionSpecification"`
	ProtocolVersion               string                  `json:"protocol_version,omitempty"` // Protocol version (e.g., "0.5.2"), defaults to "0.2.0" if omitted
}

// https://json-schema.org/learn/getting-started-step-by-step.html

// Properties defines the property map which is used to define any single "field name" along with its specification
type Properties struct {
	Properties map[PropertyName]PropertySpec `json:"properties"`
}

// PropertyName is a alias for a string to make it clear to the user that the "key" in the map is the name of the property
type PropertyName string

// ConnectionSpecification is used to define the settings that are configurable "per" instance of your connector
type ConnectionSpecification struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Properties
	Type     string         `json:"type"` // should always be "object"
	Required []PropertyName `json:"required"`
}

// PropType defines the property types any field can take. See more here:  https://docs.airbyte.com/understanding-airbyte/supported-data-types
type PropType string

const (
	String  PropType = "string"
	Number  PropType = "number"
	Integer PropType = "integer"
	Object  PropType = "object"
	Array   PropType = "array"
	Null    PropType = "null"
)

// AirbytePropType is used to define airbyte specific property types. See more here: https://docs.airbyte.com/understanding-airbyte/supported-data-types
type AirbytePropType string

const (
	TimestampWithTZ AirbytePropType = "timestamp_with_timezone"
	TimestampWOTZ   AirbytePropType = "timestamp_without_timezone"
	BigInteger      AirbytePropType = "big_integer"
	BigNumber       AirbytePropType = "big_number"
)

// FormatType is used to define data type formats supported by airbyte where needed (usually for strings formatted as dates). See more here: https://docs.airbyte.com/understanding-airbyte/supported-data-types
type FormatType string

const (
	Date     FormatType = "date"
	DateTime FormatType = "datetime"
)

type PropertyType struct {
	Type        PropTypes       `json:"type,omitempty"`
	AirbyteType AirbytePropType `json:"airbyte_type,omitempty"`
}
type PropertySpec struct {
	Title        string `json:"title,omitempty"`
	Description  string `json:"description,omitempty"`
	PropertyType `json:",omitempty"`
	Const        interface{}                   `json:"const,omitempty"`
	Default      interface{}                   `json:"default,omitempty"`
	Examples     []string                      `json:"examples,omitempty"`
	Items        map[string]interface{}        `json:"items,omitempty"`
	Properties   map[PropertyName]PropertySpec `json:"properties,omitempty"`
	Required     []PropertyName                `json:"required,omitempty"`
	OneOf        []PropertySpec                `json:"oneOf,omitempty"`
	Order        *int                          `json:"order,omitempty"`
	IsSecret     bool                          `json:"airbyte_secret,omitempty"`
}

// LogWriter is exported for documentation purposes - only use this through LogTracker or MessageTracker
// to ensure thread-safe behavior with the writer
type LogWriter func(level LogLevel, s string) error

// StateWriter is exported for documentation purposes - only use this through MessageTracker
// This writer supports protocol v0.5.2 typed states (STREAM, GLOBAL, LEGACY)
type StateWriter func(sType StateType, stateData interface{}) error

// RecordWriter is exported for documentation purposes - only use this through MessageTracker
type RecordWriter func(v interface{}, streamName string, namespace string) error

// TraceWriter is exported for documentation purposes - only use this through MessageTracker
type TraceWriter func(tType traceType, trace interface{}) error

// ControlWriter is exported for documentation purposes - only use this through MessageTracker
type ControlWriter func(cType controlType, control interface{}) error

func newLogWriter(w io.Writer) LogWriter {
	return func(lvl LogLevel, s string) error {
		return write(w, &message{
			Type: msgTypeLog,
			logMessage: &logMessage{
				Level:   lvl,
				Message: s,
			},
		})
	}
}

func newStateWriter(w io.Writer) StateWriter {
	return func(sType StateType, stateData interface{}) error {
		s := &state{
			Type: sType,
		}

		switch sType {
		case StateTypeStream:
			s.Stream = stateData.(*streamState)
		case StateTypeGlobal:
			s.Global = stateData.(*globalState)
		case StateTypeLegacy:
			s.Data = stateData
		default:
			return errors.New("unsupported state type")
		}

		return write(w, &message{
			Type:  msgTypeState,
			state: s,
		})
	}
}

func newRecordWriter(w io.Writer) RecordWriter {
	return func(s interface{}, stream string, namespace string) error {
		return write(w, &message{
			Type: msgTypeRecord,
			record: &record{
				EmittedAt: time.Now().UnixMilli(),
				Data:      s,
				Namespace: namespace,
				Stream:    stream,
			},
		})
	}
}

func newTraceWriter(w io.Writer) TraceWriter {
	return func(tType traceType, trace interface{}) error {
		tm := &traceMessage{
			EmittedAt: float64(time.Now().UnixMilli()),
			Type:      tType,
		}

		switch tType {
		case traceTypeError:
			tm.Error = trace.(*errorTraceMessage)
		case traceTypeEstimate:
			tm.Estimate = trace.(*estimateTraceMessage)
		case traceTypeAnalytics:
			tm.Analytics = trace.(*analyticsTraceMessage)
		default:
			return errors.New("unsupported trace type")
		}

		return write(w, &message{
			Type:         msgTypeTrace,
			traceMessage: tm,
		})
	}
}

func newControlWriter(w io.Writer) ControlWriter {
	return func(cType controlType, control interface{}) error {
		cm := &controlMessage{
			EmittedAt: float64(time.Now().UnixMilli()),
			Type:      cType,
		}

		if cType == controlTypeConnectorConfig {
			cm.ConnectorConfig = control.(*controlConnectorConfigMessage)
		}

		return write(w, &message{
			Type:           msgTypeControl,
			controlMessage: cm,
		})
	}
}

// EmitError is a convenience function for emitting error trace messages
func EmitError(w TraceWriter, msg string, failureType *failureType, internalMsg, stackTrace *string) error {
	return w(traceTypeError, &errorTraceMessage{
		Message:         msg,
		FailureType:     failureType,
		InternalMessage: internalMsg,
		StackTrace:      stackTrace,
	})
}

// EmitEstimate is a convenience function for emitting estimate trace messages
func EmitEstimate(w TraceWriter, streamName string, namespace *string, eType EstimateType, rowEstimate, byteEstimate *int64) error {
	return w(traceTypeEstimate, &estimateTraceMessage{
		Name:         streamName,
		Namespace:    namespace,
		Type:         eType,
		RowEstimate:  rowEstimate,
		ByteEstimate: byteEstimate,
	})
}
