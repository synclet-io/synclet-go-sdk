package synclet

import (
	"io"
	"log"
	"os"
)

// DestinationRunner acts as an "orchestrator" of sorts to run your destination for you
type DestinationRunner struct {
	writer      io.Writer
	destination Destination
	msgTracker  MessageTracker
}

// NewDestinationRunner takes your defined Destination and plugs it in with the rest of airbyte
func NewDestinationRunner(dst Destination, w io.Writer) DestinationRunner {
	w = newSafeWriter(w)
	msgTracker := MessageTracker{
		Record:  newRecordWriter(w),
		State:   newStateWriter(w),
		Log:     newLogWriter(w),
		Trace:   newTraceWriter(w),
		Control: newControlWriter(w),
	}

	return DestinationRunner{
		writer:      w,
		destination: dst,
		msgTracker:  msgTracker,
	}
}

// Start starts your destination
func (dr DestinationRunner) Start() error {
	switch cmd(os.Args[1]) {
	case cmdSpec:
		spec, err := dr.destination.Spec(LogTracker{
			Log: dr.msgTracker.Log,
		})
		if err != nil {
			dr.msgTracker.Log(LogLevelError, "failed"+err.Error())
			return err
		}
		return write(dr.writer, &message{
			Type:                   msgTypeSpec,
			ConnectorSpecification: spec,
		})

	case cmdCheck:
		inP, err := getSourceConfigPath()
		if err != nil {
			return err
		}
		err = dr.destination.Check(inP, LogTracker{
			Log: dr.msgTracker.Log,
		})
		if err != nil {
			log.Println(err)
			return write(dr.writer, &message{
				Type: msgTypeConnectionStat,
				connectionStatus: &connectionStatus{
					Status: checkStatusFailed,
				},
			})
		}

		return write(dr.writer, &message{
			Type: msgTypeConnectionStat,
			connectionStatus: &connectionStatus{
				Status: checkStatusSuccess,
			},
		})

	case cmdWrite:
		cfgPath, err := getSourceConfigPath()
		if err != nil {
			return err
		}
		catPath, err := getCatalogPath()
		if err != nil {
			return err
		}
		return dr.destination.Write(cfgPath, catPath, os.Stdin, dr.msgTracker)
	}

	return nil
}
