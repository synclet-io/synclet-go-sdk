package synclet

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// MessageReader reads protocol messages line by line from a destination's
// input.
type MessageReader struct {
	r *bufio.Reader
}

// NewMessageReader returns a MessageReader over r.
func NewMessageReader(r io.Reader) *MessageReader {
	return &MessageReader{r: bufio.NewReader(r)}
}

// Next returns the next message, io.EOF at the end of the input, or an error
// for a line that is not a protocol message.
func (r *MessageReader) Next() (*Message, error) {
	for {
		line, err := r.r.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			var m Message
			if derr := json.Unmarshal(line, &m); derr != nil {
				return nil, fmt.Errorf("decode protocol message: %w", derr)
			}
			if m.Type == "" {
				return nil, errors.New("decode protocol message: no type")
			}
			return &m, nil
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("read protocol message: %w", err)
		}
	}
}
