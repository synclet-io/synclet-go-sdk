package airbyte

import (
	"encoding/json"
	"fmt"
)

// PropTypes is the JSON Schema "type" keyword of a property. A single type is
// written as a plain string, which the Airbyte UI needs in order to render an
// array of objects as a list of forms; several types are written as an array.
// Both forms are accepted when reading.
type PropTypes []PropType

func (t PropTypes) MarshalJSON() ([]byte, error) {
	if len(t) == 1 {
		return json.Marshal(t[0])
	}
	return json.Marshal([]PropType(t))
}

func (t *PropTypes) UnmarshalJSON(data []byte) error {
	var single PropType
	if err := json.Unmarshal(data, &single); err == nil {
		*t = PropTypes{single}
		return nil
	}
	var many []PropType
	if err := json.Unmarshal(data, &many); err != nil {
		return fmt.Errorf("type must be a string or an array of strings: %w", err)
	}
	*t = many
	return nil
}
