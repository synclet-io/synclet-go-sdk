// Package cli runs a connector as a process: it parses the command line,
// reads the config, catalog and state files, cancels on SIGTERM and SIGINT
// and writes the protocol stream to stdout. It is the only part of the SDK
// that touches os.Args, stdin, stdout and files.
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const usage = `usage:
  spec
  check --config FILE
  discover --config FILE
  read --config FILE --catalog FILE [--state FILE]
  write --config FILE --catalog FILE < records`

type serveFunc func(ctx context.Context, req synclet.Request, out io.Writer) error

// RunSource runs src as a process and exits: 0 on success, 1 when the
// command failed, was cancelled or panicked, 2 on a usage error.
func RunSource[C any](src synclet.Source[C]) {
	os.Exit(runProcess(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr,
		func(ctx context.Context, req synclet.Request, out io.Writer) error {
			return synclet.ServeSource(ctx, src, req, out)
		}))
}

// RunDestination runs dst as a process and exits like RunSource.
func RunDestination[C any](dst synclet.Destination[C]) {
	os.Exit(runProcess(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr,
		func(ctx context.Context, req synclet.Request, out io.Writer) error {
			return synclet.ServeDestination(ctx, dst, req, out)
		}))
}

// runProcess points os.Stdout at stderr so that only the protocol stream
// reaches stdout, and cancels ctx on SIGTERM and SIGINT.
func runProcess(ctx context.Context, args []string, stdin io.Reader, stdout, stderr *os.File, serve serveFunc) int {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	saved := os.Stdout
	os.Stdout = stderr
	defer func() { os.Stdout = saved }()
	return run(ctx, args, stdin, stdout, stderr, serve)
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, serve serveFunc) int {
	req, err := parse(args, stdin)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "%v\n%s\n", err, usage)
		return 2
	}
	out := &lockedWriter{w: stdout}
	saved := slog.Default()
	slog.SetDefault(synclet.NewLogger(synclet.NewEmitter(out), slog.LevelInfo))
	defer slog.SetDefault(saved)
	if err := serve(ctx, req, out); err != nil {
		return 1
	}
	return 0
}

func parse(args []string, stdin io.Reader) (synclet.Request, error) {
	if len(args) == 0 {
		return synclet.Request{}, errors.New("missing command")
	}
	req := synclet.Request{Command: synclet.Command(args[0])}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var config, catalog, state string
	switch req.Command {
	case synclet.CommandSpec:
	case synclet.CommandCheck, synclet.CommandDiscover:
		fs.StringVar(&config, "config", "", "")
	case synclet.CommandRead:
		fs.StringVar(&config, "config", "", "")
		fs.StringVar(&catalog, "catalog", "", "")
		fs.StringVar(&state, "state", "", "")
	case synclet.CommandWrite:
		fs.StringVar(&config, "config", "", "")
		fs.StringVar(&catalog, "catalog", "", "")
		req.Input = stdin
	default:
		return req, fmt.Errorf("unknown command %q", args[0])
	}
	if err := fs.Parse(args[1:]); err != nil {
		return req, fmt.Errorf("%s: %w", args[0], err)
	}
	if fs.NArg() > 0 {
		return req, fmt.Errorf("%s: unexpected argument %q", args[0], fs.Arg(0))
	}
	var err error
	if fs.Lookup("config") != nil {
		if req.Config, err = readFile("config", config); err != nil {
			return req, err
		}
	}
	if fs.Lookup("catalog") != nil {
		if req.Catalog, err = readFile("catalog", catalog); err != nil {
			return req, err
		}
	}
	if state != "" {
		if req.State, err = readFile("state", state); err != nil {
			return req, err
		}
	}
	return req, nil
}

func readFile(flagName, path string) ([]byte, error) {
	if path == "" {
		return nil, fmt.Errorf("missing --%s", flagName)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read --%s: %w", flagName, err)
	}
	return b, nil
}

// lockedWriter lets the connector's Emitter and the slog.Default logger share
// stdout without interleaving lines.
type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}
