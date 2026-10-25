// Package memory provides in-process [outbox.Publisher] implementations for
// tests, local development and single-binary deployments.
package memory

import (
	"context"
	"sync"

	"github.com/neerajsinghi/go-outbox"
)

// Recorder is a Publisher that stores every published message in memory.
// Set Fail to inject errors. The zero value is ready to use.
type Recorder struct {
	// Fail, when non-nil, is called before recording; a non-nil return
	// value is returned from Publish and the message is not recorded.
	Fail func(msg outbox.Message) error

	mu       sync.Mutex
	messages []outbox.Message
	calls    int
}

// Publish implements outbox.Publisher.
func (r *Recorder) Publish(_ context.Context, msg outbox.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.Fail != nil {
		if err := r.Fail(msg); err != nil {
			return err
		}
	}
	r.messages = append(r.messages, msg)
	return nil
}

// Messages returns a copy of the successfully published messages, in order.
func (r *Recorder) Messages() []outbox.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]outbox.Message(nil), r.messages...)
}

// Calls returns the number of Publish calls, including failed ones.
func (r *Recorder) Calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// Reset clears recorded messages and the call counter.
func (r *Recorder) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages, r.calls = nil, 0
}

// Channel is a Publisher that sends every message on a Go channel. Publish
// blocks until the message is received or ctx is done, which gives natural
// back-pressure to the relay.
type Channel struct {
	C chan outbox.Message
}

// NewChannel returns a Channel publisher with the given buffer size.
func NewChannel(buffer int) *Channel {
	return &Channel{C: make(chan outbox.Message, buffer)}
}

// Publish implements outbox.Publisher.
func (c *Channel) Publish(ctx context.Context, msg outbox.Message) error {
	select {
	case c.C <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
