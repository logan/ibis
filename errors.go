package ibis

import "runtime"
import "strings"

type ErrorKey string

const (
	ErrNotFound          = ErrorKey("not found")
	ErrAlreadyExists     = ErrorKey("already exists")
	ErrTableNotBound     = ErrorKey("table not connected to a cluster")
	ErrNothingToCommit   = ErrorKey("nothing to commit")
	ErrInvalidKey        = ErrorKey("invalid key")
	ErrInvalidRowType    = ErrorKey("row doesn't match schema")
	ErrInvalidSchemaType = ErrorKey("schema must be reflected from a pointer to a struct")
)

// New returns a new ibis error with this key.
func (key ErrorKey) New() *Error {
	trace := make([]byte, 4096)
	trace = trace[:runtime.Stack(trace, false)]
	return &Error{Key: key, StackTrace: string(trace)}
}

type Error struct {
	Key        ErrorKey
	Message    string
	StackTrace string
	Cause      error
}

// NewError returns a new ibis error with the given key and message. If multiple strings are
// given for the message, they will be joined together with a single space between them.
func NewError(key ErrorKey, msg ...string) *Error {
	trace := make([]byte, 4096)
	trace = trace[:runtime.Stack(trace, false)]
	return &Error{Key: key, Message: strings.Join(msg, " "), StackTrace: string(trace)}
}

// NewError returns a new ibis error wrapping the given error and using the given message. If
// multiple strings are given for the message, they will be joined together with a single space
// between them.
//
// If the error being wrapped is an ibis error, then the returned error will share the same
// ErrorKey.
func ChainError(cause error, msg ...string) *Error {
	trace := make([]byte, 4096)
	trace = trace[:runtime.Stack(trace, false)]
	var key ErrorKey
	if e, ok := cause.(*Error); ok {
		key = e.Key
	}
	return &Error{
		Key:        key,
		Cause:      cause,
		Message:    strings.Join(msg, " "),
		StackTrace: string(trace),
	}
}

func (err *Error) shortMessage() string {
	msg := string(err.Key)
	if err.Message != "" {
		if msg != "" {
			msg += ": "
		}
		msg += err.Message
	}
	return msg
}

func (err *Error) Error() string {
	msg := err.shortMessage()
	if err.Cause != nil {
		if msg == "" {
			msg = err.Cause.Error()
		} else {
			msg += " (caused by " + err.Cause.Error() + ")"
		}
	}
	if msg == "" {
		return "unknown ibis error"
	}
	return msg
}

func (err *Error) Details() string {
	msg := err.shortMessage()
	if err.StackTrace != "" {
		msg += "\n" + err.StackTrace
	}
	if err.Cause != nil {
		msg += "\nCaused by: "
		if ibiserr, ok := err.Cause.(*Error); ok {
			msg += ibiserr.Details()
		} else {
			msg += err.Cause.Error()
		}
	}
	return msg
}
