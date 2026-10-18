package synclet

import (
	"io"
	"log"
	"os"
)

type sourceRunnerOptions struct {
	writer               io.Writer
	configReaderProvider func() (io.Reader, error)
}

func SourceWithWriter(w io.Writer) func(*sourceRunnerOptions) {
	return func(sro *sourceRunnerOptions) {
		sro.writer = w
	}
}

func SourceWithConfigReader(configReader func() (io.Reader, error)) func(*sourceRunnerOptions) {
	return func(sro *sourceRunnerOptions) {
		sro.configReaderProvider = configReader
	}
}

// SourceRunner acts as an "orchestrator" of sorts to run your source for you
type SourceRunner struct {
	writer     io.Writer
	source     Source
	msgTracker MessageTracker
}

// NewSourceRunner takes your defined Source and plugs it in with the rest of airbyte
func NewSourceRunner(src Source, w io.Writer) SourceRunner {
	w = newSafeWriter(w)
	msgTracker := MessageTracker{
		Record:  newRecordWriter(w),
		State:   newStateWriter(w),
		Log:     newLogWriter(w),
		Trace:   newTraceWriter(w),
		Control: newControlWriter(w),
	}

	return SourceRunner{
		writer:     w,
		source:     src,
		msgTracker: msgTracker,
	}
}

// Start starts your source
// Example usage would look like this in your main.go
//
//	 func() main {
//		source := newCoolSource()
//		runner := synclet.NewSourceRunner(source)
//		err := runner.Start()
//		if err != nil {
//			log.Fatal(err)
//		 }
//	 }
//
// Yes, it really is that easy!
func (sr SourceRunner) Start() error {
	switch cmd(os.Args[1]) {
	case cmdSpec:
		spec, err := sr.source.Spec(LogTracker{
			Log: sr.msgTracker.Log,
		})
		if err != nil {
			_ = sr.msgTracker.Log(LogLevelError, "failed"+err.Error())
			return err
		}
		return write(sr.writer, &message{
			Type:                   msgTypeSpec,
			ConnectorSpecification: spec,
		})

	case cmdCheck:
		inP, err := getSourceConfigPath()
		if err != nil {
			return err
		}
		err = sr.source.Check(inP, LogTracker{
			Log: sr.msgTracker.Log,
		})
		if err != nil {
			log.Println(err)
			return write(sr.writer, &message{
				Type: msgTypeConnectionStat,
				connectionStatus: &connectionStatus{
					Status: checkStatusFailed,
				},
			})
		}

		return write(sr.writer, &message{
			Type: msgTypeConnectionStat,
			connectionStatus: &connectionStatus{
				Status: checkStatusSuccess,
			},
		})

	case cmdDiscover:
		inP, err := getSourceConfigPath()
		if err != nil {
			return err
		}
		ct, err := sr.source.Discover(inP, LogTracker{
			Log: sr.msgTracker.Log},
		)
		if err != nil {
			return err
		}
		return write(sr.writer, &message{
			Type:    msgTypeCatalog,
			Catalog: ct,
		})

	case cmdRead:
		var incat ConfiguredCatalog
		p, err := getCatalogPath()
		if err != nil {
			return err
		}

		err = UnmarshalFromPath(p, &incat)
		if err != nil {
			return err
		}

		srp, err := getSourceConfigPath()
		if err != nil {
			return err
		}

		stp, err := getStatePath()
		if err != nil {
			return err
		}

		err = sr.source.Read(srp, stp, &incat, sr.msgTracker)
		if err != nil {
			log.Println("failed")
			return err
		}

	}

	return nil
}
