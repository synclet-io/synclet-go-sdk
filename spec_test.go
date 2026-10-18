package synclet

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionSpecificationOmitsUnsetKeywords(t *testing.T) {
	got, err := json.Marshal(ConnectionSpecification{Title: "Greeting source", Type: "object"})
	require.NoError(t, err)
	assert.JSONEq(t, `{"title":"Greeting source","description":"","type":"object"}`, string(got))
}
