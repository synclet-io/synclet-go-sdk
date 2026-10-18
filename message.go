package synclet

import "encoding/json"

// MessageType says which payload field of a Message is set.
type MessageType string

const (
	MessageTypeRecord           MessageType = "RECORD"
	MessageTypeState            MessageType = "STATE"
	MessageTypeLog              MessageType = "LOG"
	MessageTypeSpec             MessageType = "SPEC"
	MessageTypeConnectionStatus MessageType = "CONNECTION_STATUS"
	MessageTypeCatalog          MessageType = "CATALOG"
	MessageTypeTrace            MessageType = "TRACE"
	MessageTypeControl          MessageType = "CONTROL"
)

// Message is one line of the protocol stream.
type Message struct {
	Type             MessageType             `json:"type"`
	Record           *RecordMessage          `json:"record,omitempty"`
	State            *StateMessage           `json:"state,omitempty"`
	Log              *LogMessage             `json:"log,omitempty"`
	Spec             *ConnectorSpecification `json:"spec,omitempty"`
	ConnectionStatus *ConnectionStatus       `json:"connectionStatus,omitempty"`
	Catalog          *Catalog                `json:"catalog,omitempty"`
	Trace            *TraceMessage           `json:"trace,omitempty"`
	Control          *ControlMessage         `json:"control,omitempty"`
}

// StreamDescriptor identifies a stream by name and optional namespace.
type StreamDescriptor struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
}

// RecordMessage carries one row of a stream.
type RecordMessage struct {
	Stream    string          `json:"stream"`
	Namespace string          `json:"namespace,omitempty"`
	Data      json.RawMessage `json:"data"`
	EmittedAt int64           `json:"emitted_at"`
}

// LogLevel is the severity of a LogMessage.
type LogLevel string

const (
	LogLevelFatal LogLevel = "FATAL"
	LogLevelError LogLevel = "ERROR"
	LogLevelWarn  LogLevel = "WARN"
	LogLevelInfo  LogLevel = "INFO"
	LogLevelDebug LogLevel = "DEBUG"
	LogLevelTrace LogLevel = "TRACE"
)

// LogMessage is a log line routed through the protocol stream.
type LogMessage struct {
	Level   LogLevel `json:"level"`
	Message string   `json:"message"`
}

// ConnectionStatusType is the outcome of a check.
type ConnectionStatusType string

const (
	ConnectionStatusSucceeded ConnectionStatusType = "SUCCEEDED"
	ConnectionStatusFailed    ConnectionStatusType = "FAILED"
)

// ConnectionStatus is the result of the check command; Message is shown to
// the user when the check failed.
type ConnectionStatus struct {
	Status  ConnectionStatusType `json:"status"`
	Message string               `json:"message,omitempty"`
}

// ControlTypeConnectorConfig marks a ControlMessage that carries a new
// connector configuration.
const ControlTypeConnectorConfig = "CONNECTOR_CONFIG"

// ControlMessage lets a connector hand a changed configuration back to the
// platform mid-sync.
type ControlMessage struct {
	Type            string                  `json:"type"`
	EmittedAt       float64                 `json:"emitted_at"`
	ConnectorConfig *ControlConnectorConfig `json:"connectorConfig,omitempty"`
}

// ControlConnectorConfig holds the configuration a ControlMessage carries.
type ControlConnectorConfig struct {
	Config json.RawMessage `json:"config"`
}
