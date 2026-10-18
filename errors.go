package synclet

import "errors"

type configError struct{ err error }

func (e *configError) Error() string { return e.err.Error() }
func (e *configError) Unwrap() error { return e.err }

// ConfigError marks err as caused by the connector's configuration, so the
// runner reports it with failure_type config_error. It returns nil for nil.
func ConfigError(err error) error {
	if err == nil {
		return nil
	}
	return &configError{err: err}
}

func isConfigError(err error) bool {
	var ce *configError
	return errors.As(err, &ce)
}
