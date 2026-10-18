package synclet

import (
	"encoding/json"
	"fmt"
)

// Catalog lists every stream a source can sync.
type Catalog struct {
	Streams []Stream `json:"streams"`
}

// Stream describes one syncable stream: a table, a collection, an endpoint.
type Stream struct {
	Name                    string          `json:"name"`
	JSONSchema              json.RawMessage `json:"json_schema"`
	SupportedSyncModes      []SyncMode      `json:"supported_sync_modes,omitempty"`
	SourceDefinedCursor     bool            `json:"source_defined_cursor,omitempty"`
	DefaultCursorField      []string        `json:"default_cursor_field,omitempty"`
	SourceDefinedPrimaryKey [][]string      `json:"source_defined_primary_key,omitempty"`
	Namespace               string          `json:"namespace,omitempty"`
}

// Descriptor returns the stream's name and namespace as a StreamDescriptor.
func (s Stream) Descriptor() StreamDescriptor {
	return StreamDescriptor{Name: s.Name, Namespace: s.Namespace}
}

// ConfiguredCatalog is the subset of a Catalog the platform asked to sync,
// with the sync settings chosen for each stream.
type ConfiguredCatalog struct {
	Streams []ConfiguredStream `json:"streams"`
}

// ConfiguredStream is one stream of a ConfiguredCatalog.
type ConfiguredStream struct {
	Stream              Stream              `json:"stream"`
	SyncMode            SyncMode            `json:"sync_mode"`
	CursorField         []string            `json:"cursor_field,omitempty"`
	DestinationSyncMode DestinationSyncMode `json:"destination_sync_mode"`
	PrimaryKey          [][]string          `json:"primary_key,omitempty"`
}

// SyncMode is how a source reads a stream.
type SyncMode string

const (
	SyncModeFullRefresh SyncMode = "full_refresh"
	SyncModeIncremental SyncMode = "incremental"
)

// DestinationSyncMode is how a destination applies a stream's records.
type DestinationSyncMode string

const (
	DestinationSyncModeAppend      DestinationSyncMode = "append"
	DestinationSyncModeOverwrite   DestinationSyncMode = "overwrite"
	DestinationSyncModeAppendDedup DestinationSyncMode = "append_dedup"
)

// ObjectSchema returns {"type":"object","properties":{...}} for a stream's
// JSONSchema. It panics on a value that cannot be marshaled; property specs
// hold JSON values only.
func ObjectSchema(props map[PropertyName]PropertySpec) json.RawMessage {
	if props == nil {
		props = map[PropertyName]PropertySpec{}
	}
	b, err := json.Marshal(struct {
		Type       string                        `json:"type"`
		Properties map[PropertyName]PropertySpec `json:"properties"`
	}{Type: "object", Properties: props})
	if err != nil {
		panic(fmt.Sprintf("synclet.ObjectSchema: %v", err))
	}
	return b
}
