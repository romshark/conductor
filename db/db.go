// Package db defines the database interfaces the Conductor relies on.
package db

import (
	"context"
	"errors"
	"time"
)

// ErrVersionMismatch is returned by Writer.AppendEvent when the assumed version
// doesn't match the actual version of the event log.
var ErrVersionMismatch = errors.New("version mismatch")

type Event struct {
	Version int64

	// Payload contains arbitrary event data in JSON format.
	Payload  string
	TypeName string

	// Time is the time the event was appended.
	Time time.Time

	// RevisionVCS is the version control system revision of the event writer.
	RevisionVCS string
}

type Reader interface {
	ReadEventAtVersion(ctx context.Context, version int64) (Event, error)
	ReadEventAfterVersion(ctx context.Context, afterVersion int64) (Event, error)
	ReadSystemVersion(ctx context.Context) (version int64, err error)
	ReadProjectionVersion(ctx context.Context, id int32) (version int64, err error)
	ReadEvents(
		ctx context.Context, atVersion int64, reverse bool, buffer []Event,
	) (read int, err error)
}

type Writer interface {
	// InitProjectionVersion prepares the projection version associated with id for use.
	// If this entry doesn't exist yet then it sets version 0.
	InitProjectionVersion(ctx context.Context, id int32) (version int64, err error)

	// SetProjectionVersion sets the projection associated with id to version.
	SetProjectionVersion(ctx context.Context, id int32, version int64) error

	// AppendEvent appends event onto the immutable event log assuming that
	// its version equals assumedVersion, otherwise returns ErrVersionMismatch.
	AppendEvent(
		ctx context.Context, assumedVersion int64, event Event,
	) (version int64, err error)
}

// TxRW is a read-write transaction.
type TxRW interface {
	Reader
	Writer
}

// TxReadOnly is a read-only transaction.
type TxReadOnly interface {
	Reader
}

// Listener is listening for event insertion notification.
// This interface may be implemented optionally. If not implemented
// the Conductor will rely on polling.
type Listener interface {
	// ListenEventInserted calls onReady once it's listening and onEventInserted every
	// time a new event was appended onto the event log.
	ListenEventInserted(
		ctx context.Context,
		onReady func(),
		onEventInserted func(version int64) error,
	) error
}

type DB interface {
	// TxReadOnly starts a read-only transaction and commits it if fn returns no error.
	// If fn either panics or returns an error the transaction is rolled back.
	TxReadOnly(
		ctx context.Context,
		fn func(ctx context.Context, tx TxReadOnly) error,
	) error

	// TxReadOnly starts a read-write transaction and commits it if fn returns no error.
	// If fn either panics or returns an error the transaction is rolled back.
	TxRW(
		ctx context.Context,
		fn func(ctx context.Context, tx TxRW) error,
	) error
}
