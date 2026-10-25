// Command relay is a self-contained demo of the transactional outbox using
// SQLite: it places a few orders (one of which rolls back), then runs a relay
// that prints every published event and stops once the outbox is drained.
//
//	go run ./examples/relay
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"github.com/neerajsinghi/go-outbox"
	"github.com/neerajsinghi/go-outbox/dialect"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dir, err := os.MkdirTemp("", "go-outbox-example")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "shop.db")+
		"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_txlock=immediate")
	if err != nil {
		return err
	}
	defer db.Close()

	ctx := context.Background()
	ob, err := outbox.New(dialect.SQLite)
	if err != nil {
		return err
	}
	if err := ob.CreateSchema(ctx, db); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE orders (id TEXT PRIMARY KEY, total_cents INTEGER NOT NULL)`); err != nil {
		return err
	}

	for i, total := range []int{1299, 0, 4550} {
		id := fmt.Sprintf("ord-%d", i+1)
		if err := placeOrder(ctx, db, ob, id, total); err != nil {
			fmt.Printf("order %s rejected: %v (its event is rolled back too)\n", id, err)
		}
	}

	// Publish to stdout. In production this would be Kafka, SQS, NATS, ...
	pub := outbox.PublisherFunc(func(ctx context.Context, m outbox.Message) error {
		fmt.Printf("published %-15s key=%s id=%s payload=%s\n", m.Topic, m.Key, m.ID, m.Payload)
		return nil
	})

	runCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	relay := ob.NewRelay(db, pub, outbox.WithPollInterval(100*time.Millisecond))
	done := make(chan error, 1)
	go func() { done <- relay.Run(runCtx) }()

	// Wait until everything is sent, then shut the relay down gracefully.
	for {
		counts, err := ob.Counts(ctx, db)
		if err != nil {
			return err
		}
		if counts[outbox.StatusPending] == 0 {
			fmt.Printf("outbox drained: %v\n", counts)
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	return <-done
}

// placeOrder stores the order and its OrderPlaced event atomically.
func placeOrder(ctx context.Context, db *sql.DB, ob *outbox.Outbox, id string, totalCents int) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() // no-op after Commit

	if _, err := tx.ExecContext(ctx, `INSERT INTO orders (id, total_cents) VALUES (?, ?)`, id, totalCents); err != nil {
		return err
	}
	if _, err := ob.Enqueue(ctx, tx, outbox.Message{
		Topic:   "orders.placed",
		Key:     id,
		Payload: []byte(fmt.Sprintf(`{"order_id":%q,"total_cents":%d}`, id, totalCents)),
		Headers: map[string]string{"content-type": "application/json"},
	}); err != nil {
		return err
	}
	if totalCents == 0 {
		return errors.New("empty order") // deferred Rollback discards order and event
	}
	return tx.Commit()
}
