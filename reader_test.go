package synclet

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessageReader(t *testing.T) {
	in := "\n" + `{"type":"RECORD","record":{"stream":"users","data":{"id":1},"emitted_at":1}}` + "\n\n  \n" +
		`{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"users"}}}}`
	r := NewMessageReader(strings.NewReader(in))
	m, err := r.Next()
	require.NoError(t, err)
	assert.Equal(t, MessageTypeRecord, m.Type)
	assert.JSONEq(t, `{"id":1}`, string(m.Record.Data))
	m, err = r.Next()
	require.NoError(t, err)
	assert.Equal(t, MessageTypeState, m.Type)
	_, err = r.Next()
	assert.ErrorIs(t, err, io.EOF)
	_, err = r.Next()
	assert.ErrorIs(t, err, io.EOF)
}

func TestMessageReaderLongLine(t *testing.T) {
	big := strings.Repeat("x", 5<<20)
	in := `{"type":"RECORD","record":{"stream":"s","data":{"v":"` + big + `"},"emitted_at":1}}` + "\n"
	m, err := NewMessageReader(strings.NewReader(in)).Next()
	require.NoError(t, err)
	assert.Len(t, m.Record.Data, len(big)+8)
}

func TestMessageReaderNotProtocol(t *testing.T) {
	for _, line := range []string{"hello world", `{"record":{}}`, `[1,2]`} {
		_, err := NewMessageReader(strings.NewReader(line + "\n")).Next()
		assert.Error(t, err, line)
		assert.NotErrorIs(t, err, io.EOF, line)
	}
}
