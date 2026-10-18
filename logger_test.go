package synclet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoggerLevels(t *testing.T) {
	var out bytes.Buffer
	l := NewLogger(NewEmitter(&out), slog.LevelDebug)
	l.Debug("d")
	l.Info("i")
	l.Warn("w")
	l.Error("e")
	msgs := decodeMessages(t, out.Bytes())
	require.Len(t, msgs, 4)
	var got []LogMessage
	for _, m := range msgs {
		require.Equal(t, MessageTypeLog, m.Type)
		got = append(got, *m.Log)
	}
	assert.Equal(t, []LogMessage{
		{Level: LogLevelDebug, Message: "d"},
		{Level: LogLevelInfo, Message: "i"},
		{Level: LogLevelWarn, Message: "w"},
		{Level: LogLevelError, Message: "e"},
	}, got)
}

func TestLoggerMinLevel(t *testing.T) {
	var out bytes.Buffer
	l := NewLogger(NewEmitter(&out), slog.LevelInfo)
	l.Debug("hidden")
	l.Info("shown")
	msgs := decodeMessages(t, out.Bytes())
	require.Len(t, msgs, 1)
	assert.Equal(t, "shown", msgs[0].Log.Message)
}

func TestLoggerAttributes(t *testing.T) {
	var out bytes.Buffer
	l := NewLogger(NewEmitter(&out), slog.LevelDebug).With("stream", "users")
	l.WithGroup("db").Info("reading", "from", 5, "table", "a b")
	msgs := decodeMessages(t, out.Bytes())
	require.Len(t, msgs, 1)
	assert.Equal(t, `reading stream=users db.from=5 db.table="a b"`, msgs[0].Log.Message)
}

func TestLoggerContext(t *testing.T) {
	assert.Same(t, slog.Default(), Logger(context.Background()))
	l := NewLogger(NewEmitter(&bytes.Buffer{}), slog.LevelInfo)
	assert.Same(t, l, Logger(WithLogger(context.Background(), l)))
}

func TestConfigError(t *testing.T) {
	base := errors.New("bad host")
	err := fmt.Errorf("check: %w", ConfigError(base))
	assert.True(t, isConfigError(err))
	assert.ErrorIs(t, err, base)
	assert.Equal(t, "check: bad host", err.Error())
	assert.False(t, isConfigError(base))
	assert.Nil(t, ConfigError(nil))
}
