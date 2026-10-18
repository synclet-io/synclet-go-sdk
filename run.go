package synclet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
	"strings"
)

// Command is one of the five protocol commands.
type Command string

const (
	CommandSpec     Command = "spec"
	CommandCheck    Command = "check"
	CommandDiscover Command = "discover"
	CommandRead     Command = "read"
	CommandWrite    Command = "write"
)

// Request is one connector invocation: the command and the inputs the
// transport collected for it.
type Request struct {
	Command Command
	Config  json.RawMessage // check, discover, read, write
	Catalog json.RawMessage // read, write
	State   json.RawMessage // read; nil when the sync has no state
	Input   io.Reader       // write: the record stream
}

// ServeSource runs one command of src and writes the protocol stream to out.
// A non-nil error has been emitted as exactly one TRACE ERROR and means the
// process should exit non-zero.
func ServeSource[C any](ctx context.Context, src Source[C], req Request, out io.Writer) error {
	return serve(ctx, NewEmitter(out), func(ctx context.Context, em *Emitter) error {
		switch req.Command {
		case CommandSpec:
			return emitSpec(ctx, em, src.Spec)
		case CommandCheck:
			return check(ctx, em, req, src.Check)
		case CommandDiscover:
			cfg, err := decodeConfig[C](req.Config)
			if err != nil {
				return err
			}
			cat, err := src.Discover(ctx, cfg)
			if err != nil {
				return err
			}
			return em.write(&Message{Type: MessageTypeCatalog, Catalog: cat})
		case CommandRead:
			cfg, err := decodeConfig[C](req.Config)
			if err != nil {
				return err
			}
			cat, err := decodeCatalog(req.Catalog)
			if err != nil {
				return err
			}
			st, err := ParseInputState(req.State)
			if err != nil {
				return fmt.Errorf("invalid state: %w", err)
			}
			return src.Read(ctx, cfg, cat, st, em)
		default:
			return fmt.Errorf("unsupported source command %q", req.Command)
		}
	})
}

// ServeDestination runs one command of dst and writes the protocol stream to
// out. A non-nil error has been emitted as exactly one TRACE ERROR and means
// the process should exit non-zero.
func ServeDestination[C any](ctx context.Context, dst Destination[C], req Request, out io.Writer) error {
	return serve(ctx, newEchoEmitter(out), func(ctx context.Context, em *Emitter) error {
		switch req.Command {
		case CommandSpec:
			return emitSpec(ctx, em, dst.Spec)
		case CommandCheck:
			return check(ctx, em, req, dst.Check)
		case CommandWrite:
			cfg, err := decodeConfig[C](req.Config)
			if err != nil {
				return err
			}
			cat, err := decodeCatalog(req.Catalog)
			if err != nil {
				return err
			}
			in := req.Input
			if in == nil {
				in = strings.NewReader("")
			}
			return dst.Write(ctx, cfg, cat, NewMessageReader(in), em)
		default:
			return fmt.Errorf("unsupported destination command %q", req.Command)
		}
	})
}

func serve(ctx context.Context, em *Emitter, call func(ctx context.Context, em *Emitter) error) error {
	ctx = WithLogger(ctx, NewLogger(em, slog.LevelInfo))
	trace, err := runRecovered(ctx, em, call)
	if err == nil {
		return nil
	}
	if trace == nil {
		trace = &ErrorTrace{Message: err.Error(), FailureType: FailureTypeSystemError}
		if isConfigError(err) {
			trace.FailureType = FailureTypeConfigError
		}
	}
	if werr := em.Trace(TraceMessage{Type: TraceTypeError, Error: trace}); werr != nil {
		return errors.Join(err, werr)
	}
	return err
}

func runRecovered(ctx context.Context, em *Emitter, call func(ctx context.Context, em *Emitter) error) (trace *ErrorTrace, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
			trace = &ErrorTrace{Message: err.Error(), StackTrace: string(debug.Stack()), FailureType: FailureTypeSystemError}
		}
	}()
	err = call(ctx, em)
	if cerr := ctx.Err(); cerr != nil {
		return nil, fmt.Errorf("cancelled: %w", cerr)
	}
	return nil, err
}

func emitSpec(ctx context.Context, em *Emitter, spec func(context.Context) (*ConnectorSpecification, error)) error {
	s, err := spec(ctx)
	if err != nil {
		return err
	}
	return em.write(&Message{Type: MessageTypeSpec, Spec: s})
}

func check[C any](ctx context.Context, em *Emitter, req Request, check func(context.Context, C) error) error {
	status := ConnectionStatus{Status: ConnectionStatusSucceeded}
	cfg, err := decodeConfig[C](req.Config)
	if err == nil {
		err = check(ctx, cfg)
		if ctx.Err() != nil {
			return err
		}
	}
	if err != nil {
		status = ConnectionStatus{Status: ConnectionStatusFailed, Message: err.Error()}
	}
	return em.write(&Message{Type: MessageTypeConnectionStatus, ConnectionStatus: &status})
}

func decodeConfig[C any](raw json.RawMessage) (C, error) {
	var cfg C
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, ConfigError(fmt.Errorf("invalid configuration: %w", err))
	}
	return cfg, nil
}

func decodeCatalog(raw json.RawMessage) (*ConfiguredCatalog, error) {
	var cat ConfiguredCatalog
	if err := json.Unmarshal(raw, &cat); err != nil {
		return nil, fmt.Errorf("invalid catalog: %w", err)
	}
	return &cat, nil
}
