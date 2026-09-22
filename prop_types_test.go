package airbyte

import (
	"encoding/json"
	"testing"
)

func TestPropTypesJSON(t *testing.T) {
	cases := []struct {
		types PropTypes
		json  string
	}{
		{PropTypes{String}, `{"type":"string"}`},
		{PropTypes{Null, String}, `{"type":["null","string"]}`},
		{nil, `{}`},
	}

	for _, c := range cases {
		got, err := json.Marshal(PropertyType{Type: c.types})
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != c.json {
			t.Errorf("marshal %v: got %s, want %s", c.types, got, c.json)
		}

		var back PropertyType
		if err := json.Unmarshal([]byte(c.json), &back); err != nil {
			t.Fatalf("unmarshal %s: %v", c.json, err)
		}
		if len(back.Type) != len(c.types) {
			t.Errorf("unmarshal %s: got %v, want %v", c.json, back.Type, c.types)
		}
		for i := range c.types {
			if back.Type[i] != c.types[i] {
				t.Errorf("unmarshal %s: got %v, want %v", c.json, back.Type, c.types)
			}
		}
	}
}
