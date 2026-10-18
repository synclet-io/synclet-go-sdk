package synclet

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockDestination struct {
	specFn  func(LogTracker) (*ConnectorSpecification, error)
	checkFn func(string, LogTracker) error
	writeFn func(string, string, io.Reader, MessageTracker) error
}

func (m *mockDestination) Spec(lt LogTracker) (*ConnectorSpecification, error) {
	if m.specFn != nil {
		return m.specFn(lt)
	}
	return nil, nil
}

func (m *mockDestination) Check(cfgPath string, lt LogTracker) error {
	if m.checkFn != nil {
		return m.checkFn(cfgPath, lt)
	}
	return nil
}

func (m *mockDestination) Write(cfgPath string, catPath string, r io.Reader, t MessageTracker) error {
	if m.writeFn != nil {
		return m.writeFn(cfgPath, catPath, r, t)
	}
	return nil
}

func TestDestinationRunnerSpec(t *testing.T) {
	// Save and restore os.Args
	origArgs := os.Args
	defer func() { os.Args = origArgs }()

	os.Args = []string{"connector", "spec"}

	var buf bytes.Buffer
	dst := &mockDestination{
		specFn: func(lt LogTracker) (*ConnectorSpecification, error) {
			return &ConnectorSpecification{
				DocumentationURL:    "https://example.com",
				SupportsIncremental: true,
				ConnectionSpecification: ConnectionSpecification{
					Title: "Test Destination",
					Type:  "object",
				},
			}, nil
		},
	}

	runner := NewDestinationRunner(dst, &buf)
	err := runner.Start()
	require.NoError(t, err)

	var msg map[string]interface{}
	err = json.Unmarshal(buf.Bytes(), &msg)
	require.NoError(t, err)

	assert.Equal(t, "SPEC", msg["type"])
	spec := msg["spec"].(map[string]interface{})
	assert.Equal(t, "https://example.com", spec["documentationUrl"])
	assert.Equal(t, true, spec["supportsIncremental"])
}

func TestDestinationRunnerCheckSuccess(t *testing.T) {
	origArgs := os.Args
	defer func() { os.Args = origArgs }()

	// Create temp config file
	tmpFile, err := os.CreateTemp("", "dst-config-*.json")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	tmpFile.WriteString(`{"host":"localhost"}`)
	tmpFile.Close()

	os.Args = []string{"connector", "check", "--config", tmpFile.Name()}

	var checkedPath string
	var buf bytes.Buffer
	dst := &mockDestination{
		checkFn: func(cfgPath string, lt LogTracker) error {
			checkedPath = cfgPath
			return nil
		},
	}

	runner := NewDestinationRunner(dst, &buf)
	err = runner.Start()
	require.NoError(t, err)
	assert.Equal(t, tmpFile.Name(), checkedPath)

	var msg map[string]interface{}
	err = json.Unmarshal(buf.Bytes(), &msg)
	require.NoError(t, err)

	assert.Equal(t, "CONNECTION_STATUS", msg["type"])
	cs := msg["connectionStatus"].(map[string]interface{})
	assert.Equal(t, "SUCCEEDED", cs["status"])
}

func TestDestinationRunnerCheckFailure(t *testing.T) {
	origArgs := os.Args
	defer func() { os.Args = origArgs }()

	tmpFile, err := os.CreateTemp("", "dst-config-*.json")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	tmpFile.WriteString(`{"host":"localhost"}`)
	tmpFile.Close()

	os.Args = []string{"connector", "check", "--config", tmpFile.Name()}

	var buf bytes.Buffer
	dst := &mockDestination{
		checkFn: func(cfgPath string, lt LogTracker) error {
			return errors.New("connection refused")
		},
	}

	runner := NewDestinationRunner(dst, &buf)
	err = runner.Start()
	require.NoError(t, err) // Start itself returns nil; it writes FAILED status

	var msg map[string]interface{}
	err = json.Unmarshal(buf.Bytes(), &msg)
	require.NoError(t, err)

	assert.Equal(t, "CONNECTION_STATUS", msg["type"])
	cs := msg["connectionStatus"].(map[string]interface{})
	assert.Equal(t, "FAILED", cs["status"])
}

func TestDestinationRunnerWrite(t *testing.T) {
	origArgs := os.Args
	defer func() { os.Args = origArgs }()

	// Create temp config file
	cfgFile, err := os.CreateTemp("", "dst-config-*.json")
	require.NoError(t, err)
	defer os.Remove(cfgFile.Name())
	cfgFile.WriteString(`{"host":"localhost"}`)
	cfgFile.Close()

	// Create temp catalog file
	catFile, err := os.CreateTemp("", "dst-catalog-*.json")
	require.NoError(t, err)
	defer os.Remove(catFile.Name())
	catFile.WriteString(`{"streams":[]}`)
	catFile.Close()

	os.Args = []string{"connector", "write", "--config", cfgFile.Name(), "--catalog", catFile.Name()}

	var receivedCfgPath, receivedCatPath string
	var receivedReader io.Reader
	var buf bytes.Buffer
	dst := &mockDestination{
		writeFn: func(cfgPath string, catPath string, r io.Reader, tracker MessageTracker) error {
			receivedCfgPath = cfgPath
			receivedCatPath = catPath
			receivedReader = r
			return nil
		},
	}

	runner := NewDestinationRunner(dst, &buf)
	err = runner.Start()
	require.NoError(t, err)

	assert.Equal(t, cfgFile.Name(), receivedCfgPath)
	assert.Equal(t, catFile.Name(), receivedCatPath)
	assert.Equal(t, os.Stdin, receivedReader)
}
