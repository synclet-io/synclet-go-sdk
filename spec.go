package synclet

// ConnectorSpecification is what the spec command returns: the connector's
// capabilities and the JSON schema of its configuration.
type ConnectorSpecification struct {
	DocumentationURL              string                  `json:"documentationUrl,omitempty"`
	ChangeLogURL                  string                  `json:"changelogUrl"`
	SupportsIncremental           bool                    `json:"supportsIncremental"`
	SupportsNormalization         bool                    `json:"supportsNormalization"`
	SupportsDBT                   bool                    `json:"supportsDBT"`
	SupportedDestinationSyncModes []DestinationSyncMode   `json:"supported_destination_sync_modes"`
	ConnectionSpecification       ConnectionSpecification `json:"connectionSpecification"`
	ProtocolVersion               string                  `json:"protocol_version,omitempty"`
}

// Properties is the "properties" keyword of a JSON schema object.
type Properties struct {
	Properties map[PropertyName]PropertySpec `json:"properties,omitempty"`
}

// PropertyName is the key of a property in a JSON schema object.
type PropertyName string

// ConnectionSpecification is the JSON schema of a connector's configuration.
type ConnectionSpecification struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Properties
	Type     string         `json:"type"`
	Required []PropertyName `json:"required,omitempty"`
}

// PropType is a JSON schema type. See https://docs.airbyte.com/understanding-airbyte/supported-data-types
type PropType string

const (
	String  PropType = "string"
	Number  PropType = "number"
	Integer PropType = "integer"
	Object  PropType = "object"
	Array   PropType = "array"
	Null    PropType = "null"
)

// AirbytePropType refines a PropType. See https://docs.airbyte.com/understanding-airbyte/supported-data-types
type AirbytePropType string

const (
	TimestampWithTZ AirbytePropType = "timestamp_with_timezone"
	TimestampWOTZ   AirbytePropType = "timestamp_without_timezone"
	BigInteger      AirbytePropType = "big_integer"
	BigNumber       AirbytePropType = "big_number"
)

// FormatType is a JSON schema "format" for string properties.
type FormatType string

const (
	Date     FormatType = "date"
	DateTime FormatType = "date-time"
	Time     FormatType = "time"
)

// PropertyType is the "type" and "airbyte_type" keywords of a property.
type PropertyType struct {
	Type        PropTypes       `json:"type,omitempty"`
	AirbyteType AirbytePropType `json:"airbyte_type,omitempty"`
}

// PropertySpec is the JSON schema of one property.
type PropertySpec struct {
	Title        string `json:"title,omitempty"`
	Description  string `json:"description,omitempty"`
	PropertyType `json:",omitempty"`
	Format       FormatType                    `json:"format,omitempty"`
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
