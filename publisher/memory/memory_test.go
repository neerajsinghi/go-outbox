package memory

import (
	"context"
	"errors"
	"testing"

	"github.com/neerajsinghi/go-outbox"
)

func TestRecorder(t *testing.T) {
	boom := errors.New("boom")
	r := &Recorder{Fail: func(m outbox.Message) error {
		if m.Topic == "bad" {
			return boom
		}
		return nil
	}}
	ctx := context.Background()
	tests := []struct {
		topic   string
		wantErr error
	}{
		{"good", nil},
		{"bad", boom},
		{"good", nil},
	}
	for _, tt := range tests {
		if err := r.Publish(ctx, outbox.Message{Topic: tt.topic}); !errors.Is(err, tt.wantErr) {
			t.Fatalf("Publish(%s) = %v, want %v", tt.topic, err, tt.wantErr)
		}
	}
	if r.Calls() != 3 || len(r.Messages()) != 2 {
		t.Fatalf("calls=%d messages=%d", r.Calls(), len(r.Messages()))
	}
	r.Reset()
	if r.Calls() != 0 || len(r.Messages()) != 0 {
		t.Fatal("Reset did not clear state")
	}
}

func TestChannelRespectsContext(t *testing.T) {
	c := NewChannel(1)
	ctx, cancel := context.WithCancel(context.Background())
	if err := c.Publish(ctx, outbox.Message{ID: "1"}); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := c.Publish(ctx, outbox.Message{ID: "2"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("full channel with cancelled ctx: err = %v", err)
	}
	if m := <-c.C; m.ID != "1" {
		t.Fatalf("got %q", m.ID)
	}
}
