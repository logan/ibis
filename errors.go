package ibis

import "errors"
import "fmt"

var (
	ErrNotFound          = errors.New("not found")
	ErrAlreadyExists     = errors.New("already exists")
	ErrTableNotBound     = errors.New("table not connected to a cluster")
	ErrNothingToCommit   = errors.New("nothing to commit")
	ErrInvalidKey        = errors.New("invalid key")
	ErrInvalidRowType    = errors.New("row doesn't match schema")
	ErrInvalidSchemaType = errors.New("schema must be reflected from a pointer to a struct")
)

type WrappedError struct {
	err     error
	wrapped error
}

func WrapError(msg string, err error) error { return WrappedError{errors.New(msg), err} }
func (wrap WrappedError) Error() string     { return fmt.Sprintf("%s: %s", wrap.err, wrap.wrapped) }
func (wrap WrappedError) Unwrap() error     { return wrap.wrapped }
