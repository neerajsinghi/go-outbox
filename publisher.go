package outbox

import (
	"context"
	"errors"
	"fmt"
)

// Publisher delivers a message to a broker (Kafka, SQS, NATS, RabbitMQ, an
// HTTP webhook, ...). Implementations must be safe for concurrent use when
// more than one relay shares them.
//
// Publish should return only after the broker has durably accepted the
// message (e.g. Kafka acks=all, SQS SendMessage success). Returning nil marks
// the message as sent; returning an error schedules a retry with backoff.
// Wrap an error with [Permanent] to skip retries and dead-letter the message
// immediately (for example, a payload the broker will never accept).
type Publisher interface {
	Publish(ctx context.Context, msg Message) error
}

// PublisherFunc adapts an ordinary function to the [Publisher] interface.
type PublisherFunc func(ctx context.Context, msg Message) error

// Publish calls f(ctx, msg).
func (f PublisherFunc) Publish(ctx context.Context, msg Message) error { return f(ctx, msg) }

type permanentError struct{ err error }

func (e *permanentError) Error() string { return fmt.Sprintf("permanent: %v", e.err) }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent wraps err to signal that retrying is pointless. The relay moves
// the message straight to the dead state.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err (or any error it wraps) was created by
// [Permanent].
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}
