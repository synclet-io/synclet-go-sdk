package synclet

// TraceType says which payload field of a TraceMessage is set.
type TraceType string

const (
	TraceTypeError        TraceType = "ERROR"
	TraceTypeEstimate     TraceType = "ESTIMATE"
	TraceTypeStreamStatus TraceType = "STREAM_STATUS"
	TraceTypeAnalytics    TraceType = "ANALYTICS"
)

// FailureType classifies an ErrorTrace for the platform's retry policy.
type FailureType string

const (
	FailureTypeSystemError    FailureType = "system_error"
	FailureTypeConfigError    FailureType = "config_error"
	FailureTypeTransientError FailureType = "transient_error"
)

// EstimateType is the scope of an EstimateTrace: one stream or the sync.
type EstimateType string

const (
	EstimateTypeStream EstimateType = "STREAM"
	EstimateTypeSync   EstimateType = "SYNC"
)

// StreamStatus is the progress of a stream reported by a StreamStatusTrace.
type StreamStatus string

const (
	StreamStatusStarted    StreamStatus = "STARTED"
	StreamStatusRunning    StreamStatus = "RUNNING"
	StreamStatusComplete   StreamStatus = "COMPLETE"
	StreamStatusIncomplete StreamStatus = "INCOMPLETE"
)

// TraceMessage carries runtime metadata: an error, an estimate, a stream
// status or an analytics event.
type TraceMessage struct {
	Type         TraceType          `json:"type"`
	EmittedAt    float64            `json:"emitted_at"`
	Error        *ErrorTrace        `json:"error,omitempty"`
	Estimate     *EstimateTrace     `json:"estimate,omitempty"`
	StreamStatus *StreamStatusTrace `json:"stream_status,omitempty"`
	Analytics    *AnalyticsTrace    `json:"analytics,omitempty"`
}

// ErrorTrace reports a failure; Message is shown to the user.
type ErrorTrace struct {
	Message          string            `json:"message"`
	InternalMessage  string            `json:"internal_message,omitempty"`
	StackTrace       string            `json:"stack_trace,omitempty"`
	FailureType      FailureType       `json:"failure_type,omitempty"`
	StreamDescriptor *StreamDescriptor `json:"stream_descriptor,omitempty"`
}

// EstimateTrace predicts the size of a stream or of the whole sync.
type EstimateTrace struct {
	Name         string       `json:"name"`
	Namespace    string       `json:"namespace,omitempty"`
	Type         EstimateType `json:"type"`
	RowEstimate  *int64       `json:"row_estimate,omitempty"`
	ByteEstimate *int64       `json:"byte_estimate,omitempty"`
}

// StreamStatusTrace reports where a stream is in the sync.
type StreamStatusTrace struct {
	StreamDescriptor StreamDescriptor `json:"stream_descriptor"`
	Status           StreamStatus     `json:"status"`
}

// AnalyticsTrace is a named event with a string value.
type AnalyticsTrace struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}
