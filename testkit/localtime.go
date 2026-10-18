package testkit

import (
	"errors"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// Airbyte types whose values carry no zone. JSON Schema's date-time and time
// formats require an offset, so a property refined by one of them is checked
// against the zone-less form instead.
const (
	airbyteTimestampWOTZ = "timestamp_without_timezone"
	airbyteTimeWOTZ      = "time_without_timezone"

	formatLocalDateTime = "synclet-local-date-time"
	formatLocalTime     = "synclet-local-time"
)

var (
	localDateTime = &jsonschema.Format{Name: formatLocalDateTime, Validate: zoneLess("2006-01-02T15:04:05", "YYYY-MM-DDTHH:MM:SS[.fraction] without a zone")}
	localTime     = &jsonschema.Format{Name: formatLocalTime, Validate: zoneLess("15:04:05", "HH:MM:SS[.fraction] without a zone")}
)

// zoneLess accepts a string that is layout with an optional fraction of a
// second and nothing after it; other JSON types are not its concern.
func zoneLess(layout, want string) func(v any) error {
	return func(v any) error {
		s, ok := v.(string)
		if !ok {
			return nil
		}
		for _, l := range []string{layout, layout + ".999999999"} {
			if _, err := time.Parse(l, s); err == nil {
				return nil
			}
		}
		return errors.New("not " + want)
	}
}

// localizeFormats walks a decoded schema and replaces the format of every
// property refined to a zone-less airbyte_type with its zone-less
// counterpart: date-time under timestamp_without_timezone, time under
// time_without_timezone.
func localizeFormats(doc any) {
	switch v := doc.(type) {
	case map[string]any:
		switch {
		case v["airbyte_type"] == airbyteTimestampWOTZ && v["format"] == "date-time":
			v["format"] = formatLocalDateTime
		case v["airbyte_type"] == airbyteTimeWOTZ && v["format"] == "time":
			v["format"] = formatLocalTime
		}
		for _, child := range v {
			localizeFormats(child)
		}
	case []any:
		for _, child := range v {
			localizeFormats(child)
		}
	}
}
