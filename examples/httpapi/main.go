// Command httpapi serves an idempotent "create payment" endpoint. Each
// payment is written together with its outbox event in one transaction, and
// the Idempotency-Key middleware makes client retries safe.
//
//	go run ./examples/httpapi
//
//	curl -i -X POST localhost:8080/payments -H 'Idempotency-Key: abc' -d '{"amount":100}'
//	curl -i -X POST localhost:8080/payments -H 'Idempotency-Key: abc' -d '{"amount":100}'  # replayed
//	curl -i -X POST localhost:8080/payments -H 'Idempotency-Key: abc' -d '{"amount":999}'  # 422
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "modernc.org/sqlite"

	"github.com/neerajsinghi/go-outbox"
	"github.com/neerajsinghi/go-outbox/dialect"
	"github.com/neerajsinghi/go-outbox/idempotency"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	db, err := sql.Open("sqlite", "file:payments.db?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_txlock=immediate")
	if err != nil {
		return err
	}
	defer db.Close()

	ob, err := outbox.New(dialect.SQLite)
	if err != nil {
		return err
	}
	keys, err := idempotency.NewSQLStore(db, dialect.SQLite)
	if err != nil {
		return err
	}
	if err := ob.CreateSchema(ctx, db); err != nil {
		return err
	}
	if err := keys.CreateSchema(ctx); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS payments (id TEXT PRIMARY KEY, amount INTEGER NOT NULL)`); err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.Handle("POST /payments", idempotency.Middleware(keys, idempotency.WithRequired())(createPayment(db, ob)))

	srv := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	logPub := outbox.PublisherFunc(func(_ context.Context, m outbox.Message) error {
		log.Printf("publish %s %s", m.Topic, m.Payload)
		return nil
	})
	relayDone := make(chan error, 1)
	go func() { relayDone <- ob.NewRelay(db, logPub).Run(ctx) }()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	log.Printf("listening on %s", srv.Addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return <-relayDone
}

func createPayment(db *sql.DB, ob *outbox.Outbox) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Amount int `json:"amount"`
		}
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &in); err != nil || in.Amount <= 0 {
			http.Error(w, "invalid payment", http.StatusBadRequest)
			return
		}
		id := outbox.NewID()
		tx, err := db.BeginTx(r.Context(), nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(r.Context(), `INSERT INTO payments (id, amount) VALUES (?, ?)`, id, in.Amount); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		event := fmt.Sprintf(`{"payment_id":%q,"amount":%d}`, id, in.Amount)
		if _, err := ob.Enqueue(r.Context(), tx, outbox.Message{Topic: "payments.created", Key: id, Payload: []byte(event)}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := tx.Commit(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, `{"id":%q,"amount":%d}`+"\n", id, in.Amount)
	})
}
