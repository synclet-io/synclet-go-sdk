package synclet

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/synclet-io/synclet-go-sdk/schema"
)

// InferSchemaFromStruct derives a stream's properties from the fields of a
// Go struct.
func InferSchemaFromStruct(i any) (Properties, error) {
	var prop Properties

	s, err := schema.Generate(reflect.TypeOf(i))
	if err != nil {
		return prop, fmt.Errorf("generate schema: %w", err)
	}

	b, err := json.Marshal(s)
	if err != nil {
		return prop, fmt.Errorf("marshal schema: %w", err)
	}

	if err := json.Unmarshal(b, &prop); err != nil {
		return prop, fmt.Errorf("decode schema into properties: %w", err)
	}

	return prop, nil
}
