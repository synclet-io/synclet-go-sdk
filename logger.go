package synclet

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
)

type loggerKey struct{}

// NewLogger returns a logger that emits records at level and above as LOG
// messages through em. Attributes are appended to the message as key=value.
func NewLogger(em *Emitter, level slog.Leveler) *slog.Logger {
	return slog.New(&logHandler{em: em, level: level})
}

// WithLogger returns a context carrying l for Logger.
func WithLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, l)
}

// Logger returns the logger the runner attached to ctx, or slog.Default()
// outside a runner.
func Logger(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}

type logHandler struct {
	em     *Emitter
	level  slog.Leveler
	prefix string
	attrs  string
}

func (h *logHandler) Enabled(_ context.Context, l slog.Level) bool {
	return l >= h.level.Level()
}

func (h *logHandler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	b.WriteString(r.Message)
	b.WriteString(h.attrs)
	r.Attrs(func(a slog.Attr) bool {
		appendAttr(&b, h.prefix, a)
		return true
	})
	return h.em.Log(logLevel(r.Level), b.String())
}

func (h *logHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	var b strings.Builder
	b.WriteString(h.attrs)
	for _, a := range attrs {
		appendAttr(&b, h.prefix, a)
	}
	c := *h
	c.attrs = b.String()
	return &c
}

func (h *logHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	c := *h
	c.prefix = h.prefix + name + "."
	return &c
}

func appendAttr(b *strings.Builder, prefix string, a slog.Attr) {
	a.Value = a.Value.Resolve()
	if a.Equal(slog.Attr{}) {
		return
	}
	if a.Value.Kind() == slog.KindGroup {
		p := prefix
		if a.Key != "" {
			p += a.Key + "."
		}
		for _, ga := range a.Value.Group() {
			appendAttr(b, p, ga)
		}
		return
	}
	v := a.Value.String()
	if v == "" || strings.ContainsAny(v, " \t\n\"=") {
		v = strconv.Quote(v)
	}
	fmt.Fprintf(b, " %s%s=%s", prefix, a.Key, v)
}

func logLevel(l slog.Level) LogLevel {
	switch {
	case l >= slog.LevelError:
		return LogLevelError
	case l >= slog.LevelWarn:
		return LogLevelWarn
	case l >= slog.LevelInfo:
		return LogLevelInfo
	default:
		return LogLevelDebug
	}
}
