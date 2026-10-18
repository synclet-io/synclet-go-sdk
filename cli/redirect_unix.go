//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package cli

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// redirectStdout moves the stream behind stdout to a new descriptor, which it
// returns, and points stdout's descriptor at stderr, so that anything writing
// to that descriptor, through os.Stdout or a copy of it taken earlier, reaches
// stderr. restore puts the stream back on stdout's descriptor.
func redirectStdout(stdout, stderr *os.File) (protocol *os.File, restore func() error, err error) {
	out := int(stdout.Fd())
	syscall.ForkLock.RLock()
	fd, err := syscall.Dup(out)
	if err == nil {
		syscall.CloseOnExec(fd)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return nil, nil, fmt.Errorf("duplicate stdout: %w", err)
	}
	if err := dup2(int(stderr.Fd()), out); err != nil {
		return nil, nil, errors.Join(fmt.Errorf("redirect stdout: %w", err), syscall.Close(fd))
	}
	protocol = os.NewFile(uintptr(fd), "protocol")
	return protocol, func() error { return errors.Join(dup2(fd, out), protocol.Close()) }, nil
}
