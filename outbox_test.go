package outbox_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/neerajsinghi/go-outbox"
	"github.com/neerajsinghi/go-outbox/dialect"
	"github.com/neerajsinghi/go-outbox/publisher/memory"
)

// fakeClock is a manually advanced clock safe for concurrent use.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

var quietLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// openDB opens a file-backed SQLite database in a temp dir. WAL plus a busy
// timeout lets several connections (and relays) share it.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := "file:" + filepath.Join(t.TempDir(), "test.db") +
		"?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func setup(t *testing.T, opts ...outbox.Option) (*sql.DB, *outbox.Outbox) {
	t.Helper()
	db := openDB(t)
	ob, err := outbox.New(dialect.SQLite, opts...)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := ob.CreateSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE orders (id TEXT PRIMARY KEY, total INTEGER)`); err != nil {
		t.Fatal(err)
	}
	return db, ob
}

// placeOrder writes an order and its event in one transaction and commits
// or rolls back.
func placeOrder(t *testing.T, db *sql.DB, ob *outbox.Outbox, id string, commit bool) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO orders (id, total) VALUES (?, ?)`, id, 42); err != nil {
		t.Fatal(err)
	}
	if _, err := ob.Enqueue(ctx, tx, outbox.Message{
		Topic:   "orders.created",
		Key:     id,
		Payload: []byte(`{"id":"` + id + `"}`),
		Headers: map[string]string{"content-type": "application/json"},
	}); err != nil {
		t.Fatal(err)
	}
	if commit {
		err = tx.Commit()
	} else {
		err = tx.Rollback()
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestNewRejectsBadTableName(t *testing.T) {
	tests := []struct {
		name  string
		table string
		ok    bool
	}{
		{"default", outbox.DefaultTable, true},
		{"schema qualified", "events.outbox", true},
		{"injection", "outbox; DROP TABLE users", false},
		{"empty", "", false},
		{"quoted", `"outbox"`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := outbox.New(dialect.Postgres, outbox.WithTable(tt.table))
			if (err == nil) != tt.ok {
				t.Fatalf("New(%q) err = %v, want ok=%v", tt.table, err, tt.ok)
			}
		})
	}
}

func TestEnqueueRequiresTopic(t *testing.T) {
	db, ob := setup(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := ob.Enqueue(context.Background(), tx, outbox.Message{Payload: []byte("x")}); !errors.Is(err, outbox.ErrInvalidMessage) {
		t.Fatalf("err = %v, want ErrInvalidMessage", err)
	}
}

func TestRelayPublishesOnlyCommittedRows(t *testing.T) {
	tests := []struct {
		name      string
		committed []string
		rolled    []string
	}{
		{"rollback only", nil, []string{"o-1"}},
		{"commit only", []string{"o-1", "o-2"}, nil},
		{"mixed", []string{"o-1", "o-3"}, []string{"o-2", "o-4"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, ob := setup(t)
			for _, id := range tt.committed {
				placeOrder(t, db, ob, id, true)
			}
			for _, id := range tt.rolled {
				placeOrder(t, db, ob, id, false)
			}
			pub := &memory.Recorder{}
			relay := ob.NewRelay(db, pub, outbox.WithLogger(quietLogger))
			if _, err := relay.ProcessOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			got := pub.Messages()
			if len(got) != len(tt.committed) {
				t.Fatalf("published %d messages, want %d", len(got), len(tt.committed))
			}
			for i, m := range got {
				if m.Key != tt.committed[i] {
					t.Errorf("message %d key = %q, want %q (order must follow enqueue order)", i, m.Key, tt.committed[i])
				}
				if m.ID == "" || m.Topic != "orders.created" || m.Headers["content-type"] != "application/json" {
					t.Errorf("message %d not round-tripped: %+v", i, m)
				}
			}
			// Nothing left to publish on the next poll.
			if n, err := relay.ProcessOnce(context.Background()); err != nil || n != 0 {
				t.Fatalf("second ProcessOnce = %d, %v; want 0, nil", n, err)
			}
			counts, err := ob.Counts(context.Background(), db)
			if err != nil {
				t.Fatal(err)
			}
			if counts[outbox.StatusSent] != len(tt.committed) || counts[outbox.StatusPending] != 0 {
				t.Fatalf("counts = %v", counts)
			}
		})
	}
}

func TestRelayRetriesWithBackoffThenDeadLetters(t *testing.T) {
	clock := newFakeClock()
	db, ob := setup(t, outbox.WithClock(clock.Now))
	placeOrder(t, db, ob, "o-1", true)

	boom := errors.New("broker down")
	pub := &memory.Recorder{Fail: func(outbox.Message) error { return boom }}
	var dead []outbox.Message
	relay := ob.NewRelay(db, pub,
		outbox.WithMaxAttempts(3),
		outbox.WithBackoff(func(attempt int) time.Duration { return time.Duration(attempt) * time.Minute }),
		outbox.WithLogger(quietLogger),
		outbox.WithOnDead(func(m outbox.Message, err error) { dead = append(dead, m) }),
	)
	ctx := context.Background()

	steps := []struct {
		advance   time.Duration
		wantCalls int
		wantState string
	}{
		{0, 1, outbox.StatusPending},                // attempt 1 fails, retry in 1m
		{30 * time.Second, 1, outbox.StatusPending}, // not due yet
		{30 * time.Second, 2, outbox.StatusPending}, // attempt 2 fails, retry in 2m
		{time.Minute, 2, outbox.StatusPending},      // not due yet
		{time.Minute, 3, outbox.StatusDead},         // attempt 3 fails -> dead
		{time.Hour, 3, outbox.StatusDead},           // dead messages are never retried
	}
	for i, s := range steps {
		clock.Advance(s.advance)
		if _, err := relay.ProcessOnce(ctx); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if got := pub.Calls(); got != s.wantCalls {
			t.Fatalf("step %d: publish calls = %d, want %d", i, got, s.wantCalls)
		}
		var status, lastErr string
		var attempts int
		if err := db.QueryRow(`SELECT status, attempts, last_error FROM outbox_messages`).Scan(&status, &attempts, &lastErr); err != nil {
			t.Fatal(err)
		}
		if status != s.wantState || attempts != s.wantCalls || lastErr != boom.Error() {
			t.Fatalf("step %d: row = (%s, %d, %q), want (%s, %d, %q)", i, status, attempts, lastErr, s.wantState, s.wantCalls, boom)
		}
	}
	if len(dead) != 1 || dead[0].Attempts != 3 {
		t.Fatalf("onDead calls = %+v", dead)
	}

	// Operators can requeue dead messages once the broker is fixed.
	pub.Fail = nil
	if n, err := ob.RetryDead(ctx, db); err != nil || n != 1 {
		t.Fatalf("RetryDead = %d, %v", n, err)
	}
	if _, err := relay.ProcessOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if len(pub.Messages()) != 1 {
		t.Fatalf("message not delivered after RetryDead")
	}
}

func TestPermanentErrorDeadLettersImmediately(t *testing.T) {
	db, ob := setup(t)
	placeOrder(t, db, ob, "o-1", true)
	pub := &memory.Recorder{Fail: func(outbox.Message) error { return outbox.Permanent(errors.New("schema rejected")) }}
	relay := ob.NewRelay(db, pub, outbox.WithMaxAttempts(10), outbox.WithLogger(quietLogger))
	if _, err := relay.ProcessOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	counts, _ := ob.Counts(context.Background(), db)
	if counts[outbox.StatusDead] != 1 {
		t.Fatalf("counts = %v, want 1 dead", counts)
	}
}

func TestConcurrentRelaysDoNotDoublePublish(t *testing.T) {
	db, ob := setup(t)
	const total = 300
	for i := 0; i < total; i++ {
		placeOrder(t, db, ob, fmt.Sprintf("o-%03d", i), true)
	}

	var mu sync.Mutex
	seen := make(map[string]int)
	pub := outbox.PublisherFunc(func(ctx context.Context, m outbox.Message) error {
		mu.Lock()
		seen[m.ID]++
		mu.Unlock()
		return nil
	})

	const relays = 6
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, relays)
	for i := 0; i < relays; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := ob.NewRelay(db, pub, outbox.WithBatchSize(7), outbox.WithLease(time.Minute), outbox.WithLogger(quietLogger))
			for {
				n, err := r.ProcessOnce(ctx)
				if err != nil {
					errs <- err
					return
				}
				if n == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if len(seen) != total {
		t.Fatalf("published %d distinct messages, want %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("message %s published %d times", id, n)
		}
	}
}

func TestExpiredLeaseIsReclaimed(t *testing.T) {
	clock := newFakeClock()
	db, ob := setup(t, outbox.WithClock(clock.Now))
	placeOrder(t, db, ob, "o-1", true)

	// Simulate a relay that claimed the row and then died.
	if _, err := db.Exec(`UPDATE outbox_messages SET locked_by = 'crashed', locked_until = ?`,
		clock.Now().Add(time.Minute).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	pub := &memory.Recorder{}
	relay := ob.NewRelay(db, pub, outbox.WithLogger(quietLogger))
	if n, _ := relay.ProcessOnce(context.Background()); n != 0 {
		t.Fatalf("claimed %d messages under someone else's lease", n)
	}
	clock.Advance(time.Minute)
	if n, err := relay.ProcessOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("after lease expiry ProcessOnce = %d, %v; want 1", n, err)
	}
	if len(pub.Messages()) != 1 {
		t.Fatal("message not published after lease expiry")
	}
}

func TestRunStopsGracefullyAndReleasesBatch(t *testing.T) {
	db, ob := setup(t)
	for i := 0; i < 5; i++ {
		placeOrder(t, db, ob, fmt.Sprintf("o-%d", i), true)
	}
	ctx, cancel := context.WithCancel(context.Background())
	pub := memory.NewChannel(0)
	relay := ob.NewRelay(db, pub, outbox.WithBatchSize(10), outbox.WithLogger(quietLogger))
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()

	// Receive two messages, then shut down while the relay is blocked on the third.
	for i := 0; i < 2; i++ {
		select {
		case <-pub.C:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for message")
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}

	var sent, pendingUnlocked, attempted int
	row := db.QueryRow(`SELECT
		SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END),
		SUM(CASE WHEN status = 'pending' AND locked_by IS NULL THEN 1 ELSE 0 END),
		SUM(CASE WHEN status = 'pending' AND attempts > 0 THEN 1 ELSE 0 END)
		FROM outbox_messages`)
	if err := row.Scan(&sent, &pendingUnlocked, &attempted); err != nil {
		t.Fatal(err)
	}
	if sent != 2 || pendingUnlocked != 3 || attempted != 0 {
		t.Fatalf("sent=%d pendingUnlocked=%d attemptedPending=%d; want 2, 3, 0", sent, pendingUnlocked, attempted)
	}
}

func TestPurgeSent(t *testing.T) {
	clock := newFakeClock()
	db, ob := setup(t, outbox.WithClock(clock.Now))
	placeOrder(t, db, ob, "o-1", true)
	relay := ob.NewRelay(db, &memory.Recorder{}, outbox.WithLogger(quietLogger))
	if _, err := relay.ProcessOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n, _ := ob.PurgeSent(context.Background(), db, time.Hour); n != 0 {
		t.Fatalf("purged %d fresh rows", n)
	}
	clock.Advance(2 * time.Hour)
	if n, err := ob.PurgeSent(context.Background(), db, time.Hour); err != nil || n != 1 {
		t.Fatalf("PurgeSent = %d, %v; want 1", n, err)
	}
}

func TestNewIDIsUUIDv4(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := outbox.NewID()
		if len(id) != 36 || id[14] != '4' || id[8] != '-' {
			t.Fatalf("bad id %q", id)
		}
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	}
}
