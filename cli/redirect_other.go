//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package cli

import "os"

// redirectStdout points os.Stdout at stderr and returns stdout for the
// protocol stream. A copy of os.Stdout taken earlier still writes to stdout.
func redirectStdout(stdout, stderr *os.File) (protocol *os.File, restore func() error, err error) {
	saved := os.Stdout
	os.Stdout = stderr
	return stdout, func() error {
		os.Stdout = saved
		return nil
	}, nil
}
