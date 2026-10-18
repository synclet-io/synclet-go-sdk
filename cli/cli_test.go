package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

type config struct {
	Mode string `json:"mode"`
}

var initStdout = os.Stdout

type testSource struct{}

func (testSource) Spec(context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{ProtocolVersion: "0.5.2"}, nil
}

func (testSource) Check(context.Context, config) error { return nil }

func (testSource) Discover(context.Context, config) (*synclet.Catalog, error) {
	return &synclet.Catalog{Streams: []synclet.Stream{{Name: "users", JSONSchema: synclet.ObjectSchema(nil)}}}, nil
}

func (testSource) Read(ctx context.Context, cfg config, cat *synclet.ConfiguredCatalog, _ *synclet.InputState, em *synclet.Emitter) error {
	desc := cat.Streams[0].Stream.Descriptor()
	switch cfg.Mode {
	case "print":
		fmt.Println("stray print")
		return em.Record(desc, map[string]int{"id": 1})
	case "slow":
		if err := em.Record(desc, map[string]int{"id": 1}); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	case "captured":
		_, _ = fmt.Fprint(initStdout, "captured at init")
		return em.Record(desc, map[string]int{"id": 1})
	case "panic":
		panic("boom")
	}
	return nil
}

type testDestination struct{}

func (testDestination) Spec(context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{}, nil
}

func (testDestination) Check(context.Context, config) error { return nil }

func (testDestination) Write(_ context.Context, _ config, _ *synclet.ConfiguredCatalog, in *synclet.MessageReader, em *synclet.Emitter) error {
	for {
		m, err := in.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if m.Type == synclet.MessageTypeState {
			if err := em.State(*m.State); err != nil {
				return err
			}
		}
	}
}

func writeFiles(t *testing.T, mode string) (cfg, cat, state string) {
	t.Helper()
	dir := t.TempDir()
	cfg = filepath.Join(dir, "config.json")
	cat = filepath.Join(dir, "catalog.json")
	state = filepath.Join(dir, "state.json")
	require.NoError(t, os.WriteFile(cfg, []byte(`{"mode":"`+mode+`"}`), 0o600))
	require.NoError(t, os.WriteFile(cat, []byte(`{"streams":[{"stream":{"name":"users","json_schema":{}},"sync_mode":"full_refresh","destination_sync_mode":"append"}]}`), 0o600))
	require.NoError(t, os.WriteFile(state, []byte(`[]`), 0o600))
	return cfg, cat, state
}

type recorded struct {
	req synclet.Request
}

func (r *recorded) serve(_ context.Context, req synclet.Request, out io.Writer) error {
	r.req = req
	if req.Input != nil {
		b, err := io.ReadAll(req.Input)
		if err != nil {
			return err
		}
		r.req.Input = bytes.NewReader(b)
	}
	_, err := io.WriteString(out, `{"type":"LOG","log":{"level":"INFO","message":"ok"}}`+"\n")
	return err
}

func TestCLIFlags(t *testing.T) {
	cfg, cat, state := writeFiles(t, "")
	cases := []struct {
		args    []string
		command synclet.Command
		state   bool
	}{
		{[]string{"spec"}, synclet.CommandSpec, false},
		{[]string{"check", "--config", cfg}, synclet.CommandCheck, false},
		{[]string{"discover", "--config", cfg}, synclet.CommandDiscover, false},
		{[]string{"read", "--config", cfg, "--catalog", cat}, synclet.CommandRead, false},
		{[]string{"read", "--config", cfg, "--catalog", cat, "--state", state}, synclet.CommandRead, true},
		{[]string{"read", "--state", state, "--catalog", cat, "--config", cfg}, synclet.CommandRead, true},
		{[]string{"read", "--catalog=" + cat, "-config", cfg}, synclet.CommandRead, false},
		{[]string{"write", "--config", cfg, "--catalog", cat}, synclet.CommandWrite, false},
		{[]string{"write", "--catalog", cat, "--config", cfg}, synclet.CommandWrite, false},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			var rec recorded
			var stdout, stderr bytes.Buffer
			code := run(context.Background(), tc.args, strings.NewReader("input"), &stdout, &stderr, rec.serve)
			require.Equal(t, 0, code, stderr.String())
			assert.Equal(t, tc.command, rec.req.Command)
			assert.Empty(t, stderr.String())
			assert.Equal(t, `{"type":"LOG","log":{"level":"INFO","message":"ok"}}`+"\n", stdout.String())
			if tc.command != synclet.CommandSpec {
				assert.JSONEq(t, `{"mode":""}`, string(rec.req.Config))
			}
			if tc.command == synclet.CommandRead || tc.command == synclet.CommandWrite {
				assert.Contains(t, string(rec.req.Catalog), `"users"`)
			}
			if tc.state {
				assert.Equal(t, "[]", string(rec.req.State))
			} else {
				assert.Nil(t, rec.req.State)
			}
			if tc.command == synclet.CommandWrite {
				require.NotNil(t, rec.req.Input)
				b, _ := io.ReadAll(rec.req.Input)
				assert.Equal(t, "input", string(b))
			} else {
				assert.Nil(t, rec.req.Input)
			}
		})
	}
}

func TestCLIUsage(t *testing.T) {
	cfg, _, _ := writeFiles(t, "")
	for _, args := range [][]string{
		nil,
		{"sync"},
		{"check"},
		{"discover", "--catalog", cfg},
		{"read", "--config", cfg},
		{"write", "--catalog", cfg},
		{"check", "--config", filepath.Join(t.TempDir(), "missing.json")},
		{"check", "--config", cfg, "extra"},
		{"spec", "--config", cfg},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var rec recorded
			var stdout, stderr bytes.Buffer
			code := run(context.Background(), args, strings.NewReader(""), &stdout, &stderr, rec.serve)
			assert.Equal(t, 2, code)
			assert.Contains(t, stderr.String(), "usage:")
			assert.Empty(t, stdout.String())
			assert.Empty(t, rec.req.Command)
		})
	}
}

func TestCLIServeError(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"spec"}, strings.NewReader(""), &stdout, &stderr,
		func(context.Context, synclet.Request, io.Writer) error { return errors.New("boom") })
	assert.Equal(t, 1, code)
}

func TestCLIStrayStdout(t *testing.T) {
	cfg, cat, _ := writeFiles(t, "print")
	outR, outW, err := os.Pipe()
	require.NoError(t, err)
	errR, errW, err := os.Pipe()
	require.NoError(t, err)

	saved := os.Stdout
	os.Stdout = outW
	t.Cleanup(func() { os.Stdout = saved })

	code := runProcess(context.Background(), []string{"read", "--config", cfg, "--catalog", cat}, strings.NewReader(""), outW, errW,
		func(ctx context.Context, req synclet.Request, out io.Writer) error {
			return synclet.ServeSource[config](ctx, testSource{}, req, out)
		})
	require.NoError(t, outW.Close())
	require.NoError(t, errW.Close())
	stdout, err := io.ReadAll(outR)
	require.NoError(t, err)
	stderr, err := io.ReadAll(errR)
	require.NoError(t, err)

	assert.Equal(t, 0, code, string(stderr))
	assertProtocol(t, stdout)
	assert.Contains(t, string(stderr), "stray print")
}

func TestCLILogPackage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"spec"}, strings.NewReader(""), &stdout, &stderr,
		func(context.Context, synclet.Request, io.Writer) error {
			log.Print("from log pkg")
			return nil
		})
	assert.Equal(t, 0, code)
	assert.NotContains(t, stdout.String(), "from log pkg")
	assert.Contains(t, stderr.String(), "from log pkg")
}

func TestCLICapturedStdout(t *testing.T) {
	cfg, cat, _ := writeFiles(t, "captured")
	cmd := helper(t, "source", "read", "--config", cfg, "--catalog", cat)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	require.NoError(t, err, stderr.String())
	msgs := assertProtocol(t, out)
	assert.Len(t, msgs, 1)
	assert.Contains(t, stderr.String(), "captured at init")
}

func assertProtocol(t *testing.T, out []byte) []synclet.Message {
	t.Helper()
	var msgs []synclet.Message
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		var m synclet.Message
		require.NoError(t, json.Unmarshal(sc.Bytes(), &m), "not a protocol line: %q", sc.Text())
		require.NotEmpty(t, m.Type, "not a protocol line: %q", sc.Text())
		msgs = append(msgs, m)
	}
	require.NotEmpty(t, msgs)
	return msgs
}

func TestHelperProcess(t *testing.T) {
	kind := os.Getenv("SYNCLET_CLI_HELPER")
	if kind == "" {
		return
	}
	for i, a := range os.Args {
		if a == "--" {
			os.Args = append([]string{os.Args[0]}, os.Args[i+1:]...)
			break
		}
	}
	switch kind {
	case "source":
		RunSource(testSource{})
	case "destination":
		RunDestination(testDestination{})
	}
}

func helper(t *testing.T, kind string, args ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], append([]string{"-test.run=^TestHelperProcess$", "--"}, args...)...)
	cmd.Env = append(os.Environ(), "SYNCLET_CLI_HELPER="+kind)
	return cmd
}

func lastTraceError(t *testing.T, msgs []synclet.Message) *synclet.ErrorTrace {
	t.Helper()
	var errs []*synclet.ErrorTrace
	for _, m := range msgs {
		if m.Type == synclet.MessageTypeTrace && m.Trace.Type == synclet.TraceTypeError {
			errs = append(errs, m.Trace.Error)
		}
	}
	require.Len(t, errs, 1)
	last := msgs[len(msgs)-1]
	require.Equal(t, synclet.MessageTypeTrace, last.Type, "TRACE ERROR is the last line")
	return errs[0]
}

func TestCLISignal(t *testing.T) {
	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGINT} {
		t.Run(sig.String(), func(t *testing.T) {
			cfg, cat, _ := writeFiles(t, "slow")
			cmd := helper(t, "source", "read", "--config", cfg, "--catalog", cat)
			stdout, err := cmd.StdoutPipe()
			require.NoError(t, err)
			cmd.Stderr = os.Stderr
			require.NoError(t, cmd.Start())

			r := bufio.NewReader(stdout)
			first, err := r.ReadBytes('\n')
			require.NoError(t, err)
			require.NoError(t, cmd.Process.Signal(sig))
			rest, err := io.ReadAll(r)
			require.NoError(t, err)

			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			select {
			case err = <-done:
			case <-time.After(10 * time.Second):
				_ = cmd.Process.Kill()
				t.Fatal("connector did not exit after the signal")
			}
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit)
			assert.Equal(t, 1, exit.ExitCode())

			msgs := assertProtocol(t, append(first, rest...))
			assert.Equal(t, synclet.MessageTypeRecord, msgs[0].Type)
			assert.Equal(t, "cancelled: context canceled", lastTraceError(t, msgs).Message)
		})
	}
}

func TestCLIPanic(t *testing.T) {
	cfg, cat, _ := writeFiles(t, "panic")
	cmd := helper(t, "source", "read", "--config", cfg, "--catalog", cat)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit, stderr.String())
	assert.Equal(t, 1, exit.ExitCode())
	trace := lastTraceError(t, assertProtocol(t, stdout.Bytes()))
	assert.Equal(t, "panic: boom", trace.Message)
	assert.Contains(t, trace.StackTrace, "goroutine")
}

func TestCLIDestinationProcess(t *testing.T) {
	cfg, cat, _ := writeFiles(t, "")
	cmd := helper(t, "destination", "write", "--catalog", cat, "--config", cfg)
	cmd.Stdin = strings.NewReader(`{"type":"RECORD","record":{"stream":"users","data":{"id":1},"emitted_at":1}}
{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"users"},"stream_state":{"cursor":1}}}}
`)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	require.NoError(t, err, stderr.String())
	assert.Equal(t, `{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"users"},"stream_state":{"cursor":1}}}}`+"\n", string(out))
}
