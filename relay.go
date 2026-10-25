package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/neerajsinghi/go-outbox/dialect"
)

// Relay defaults.
const (
	DefaultBatchSize    = 100
	DefaultPollInterval = time.Second
	DefaultLease        = 30 * time.Second
	DefaultMaxAttempts  = 10
)

// Relay polls the outbox table, claims due messages with a time-bounded
// lease, hands them to a [Publisher] and records the outcome.
//
// Any number of relays (goroutines, pods) can run against the same table:
// a message claimed by one relay is invisible to the others until its lease
// expires, so within a lease every message is published by exactly one
// relay. If a relay dies mid-batch its lease simply expires and another
// relay picks the messages up, which is why delivery is at-least-once.
type Relay struct {
	o   *Outbox
	db  *sql.DB
	pub Publisher

	batchSize      int
	pollInterval   time.Duration
	lease          time.Duration
	publishTimeout time.Duration
	maxAttempts    int
	backoff        Backoff
	logger         *slog.Logger
	onDead         func(Message, error)
}

// RelayOption configures a [Relay].
type RelayOption func(*Relay)

// WithBatchSize sets how many messages are claimed per poll (default 100).
func WithBatchSize(n int) RelayOption {
	return func(r *Relay) {
		if n > 0 {
			r.batchSize = n
		}
	}
}

// WithPollInterval sets how long the relay sleeps when the outbox is
// drained (default 1s). A full batch triggers the next poll immediately.
func WithPollInterval(d time.Duration) RelayOption {
	return func(r *Relay) {
		if d > 0 {
			r.pollInterval = d
		}
	}
}

// WithLease sets how long a claimed batch stays reserved for this relay
// (default 30s). It should comfortably exceed the time needed to publish a
// whole batch; messages whose lease runs out mid-batch are skipped and left
// for the next claim rather than risk a concurrent duplicate.
func WithLease(d time.Duration) RelayOption {
	return func(r *Relay) {
		if d > 0 {
			r.lease = d
		}
	}
}

// WithPublishTimeout bounds a single Publish call (default: the lease).
func WithPublishTimeout(d time.Duration) RelayOption {
	return func(r *Relay) {
		if d > 0 {
			r.publishTimeout = d
		}
	}
}

// WithMaxAttempts sets the total number of delivery attempts before a
// message is moved to the dead state (default 10).
func WithMaxAttempts(n int) RelayOption {
	return func(r *Relay) {
		if n > 0 {
			r.maxAttempts = n
		}
	}
}

// WithBackoff sets the retry schedule (default ExponentialBackoff(1s, 5m)).
func WithBackoff(b Backoff) RelayOption {
	return func(r *Relay) {
		if b != nil {
			r.backoff = b
		}
	}
}

// WithLogger sets the structured logger (default slog.Default()).
func WithLogger(l *slog.Logger) RelayOption {
	return func(r *Relay) {
		if l != nil {
			r.logger = l
		}
	}
}

// WithOnDead registers a callback invoked after a message is moved to the
// dead state, e.g. to emit a metric or page someone.
func WithOnDead(fn func(msg Message, err error)) RelayOption {
	return func(r *Relay) { r.onDead = fn }
}

// NewRelay returns a Relay that reads from db and publishes with pub.
func (o *Outbox) NewRelay(db *sql.DB, pub Publisher, opts ...RelayOption) *Relay {
	r := &Relay{
		o:            o,
		db:           db,
		pub:          pub,
		batchSize:    DefaultBatchSize,
		pollInterval: DefaultPollInterval,
		lease:        DefaultLease,
		maxAttempts:  DefaultMaxAttempts,
		backoff:      ExponentialBackoff(time.Second, 5*time.Minute),
		logger:       slog.Default(),
	}
	for _, opt := range opts {
		opt(r)
	}
	if r.publishTimeout == 0 {
		r.publishTimeout = r.lease
	}
	return r
}

// Run polls and publishes until ctx is cancelled. Cancellation is graceful:
// the message currently being published finishes (bounded by the publish
// timeout), its outcome is recorded, and the rest of the claimed batch is
// released for other relays. Run returns nil on cancellation.
func (r *Relay) Run(ctx context.Context) error {
	for {
		n, err := r.ProcessOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			r.logger.ErrorContext(ctx, "outbox relay: batch failed", "error", err)
		} else if n == r.batchSize {
			continue // more work is likely waiting
		}
		t := time.NewTimer(r.pollInterval)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C:
		}
	}
}

// ProcessOnce claims at most one batch of due messages and publishes it.
// It returns the number of messages claimed. It is useful for tests, cron
// style workers and serverless functions that cannot run a loop.
func (r *Relay) ProcessOnce(ctx context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	token := NewID()
	leaseUntil := r.o.now().Add(r.lease).UnixMilli()
	msgs, err := r.claim(ctx, token, leaseUntil)
	if err != nil {
		return 0, err
	}
	// Bookkeeping must survive cancellation, otherwise a shutdown would
	// leave published messages marked pending (and republish them later).
	dbCtx := context.WithoutCancel(ctx)

	var errs []error
	for i, m := range msgs {
		if ctx.Err() != nil || r.o.now().UnixMilli() >= leaseUntil {
			if err := r.release(dbCtx, token); err != nil {
				errs = append(errs, err)
			}
			r.logger.DebugContext(ctx, "outbox relay: released unprocessed messages", "count", len(msgs)-i)
			break
		}
		perr := r.publish(ctx, m)
		switch {
		case perr == nil:
			err = r.markSent(dbCtx, token, m)
		case ctx.Err() != nil && !IsPermanent(perr):
			// Interrupted by shutdown: not the message's fault, don't burn
			// an attempt. The loop releases it on the next iteration.
			continue
		default:
			err = r.markFailed(dbCtx, token, m, perr)
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	if ctx.Err() != nil {
		if err := r.release(dbCtx, token); err != nil {
			errs = append(errs, err)
		}
	}
	return len(msgs), errors.Join(errs...)
}

func (r *Relay) publish(ctx context.Context, m Message) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("publisher panic: %v", p)
		}
	}()
	pctx, cancel := context.WithTimeout(ctx, r.publishTimeout)
	defer cancel()
	return r.pub.Publish(pctx, m)
}

const dueCond = `status = ? AND next_attempt_at <= ? AND (locked_until IS NULL OR locked_until <= ?)`

// claim reserves up to batchSize due messages for token and returns them.
func (r *Relay) claim(ctx context.Context, token string, leaseUntil int64) ([]Message, error) {
	d, t := r.o.d, r.o.table
	now := r.o.now().UnixMilli()

	if d.SupportsSkipLocked() {
		if err := r.claimSkipLocked(ctx, token, now, leaseUntil); err != nil {
			return nil, err
		}
	} else {
		// Single writer databases (SQLite): one atomic statement.
		q := d.Rebind(fmt.Sprintf(`UPDATE %s SET locked_by = ?, locked_until = ?
			WHERE id IN (SELECT id FROM %s WHERE %s ORDER BY seq LIMIT ?)`, t, t, dueCond))
		if _, err := r.db.ExecContext(ctx, q, token, leaseUntil, StatusPending, now, now, r.batchSize); err != nil {
			return nil, fmt.Errorf("outbox: claim: %w", err)
		}
	}

	q := d.Rebind(fmt.Sprintf(`SELECT id, topic, msg_key, payload, headers, attempts, created_at
		FROM %s WHERE locked_by = ? ORDER BY seq`, t))
	rows, err := r.db.QueryContext(ctx, q, token)
	if err != nil {
		return nil, fmt.Errorf("outbox: load claimed: %w", err)
	}
	defer rows.Close()
	var msgs []Message
	for rows.Next() {
		var (
			m       Message
			headers string
			created int64
		)
		if err := rows.Scan(&m.ID, &m.Topic, &m.Key, &m.Payload, &headers, &m.Attempts, &created); err != nil {
			return nil, fmt.Errorf("outbox: scan: %w", err)
		}
		if headers != "" && headers != "{}" {
			if err := json.Unmarshal([]byte(headers), &m.Headers); err != nil {
				return nil, fmt.Errorf("outbox: decode headers of %s: %w", m.ID, err)
			}
		}
		m.CreatedAt = time.UnixMilli(created)
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

// claimSkipLocked locks candidate rows with FOR UPDATE SKIP LOCKED so that
// concurrent relays pick disjoint batches without blocking each other.
func (r *Relay) claimSkipLocked(ctx context.Context, token string, now, leaseUntil int64) (err error) {
	d, t := r.o.d, r.o.table
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("outbox: claim: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	q := d.Rebind(fmt.Sprintf(`SELECT id FROM %s WHERE %s ORDER BY seq LIMIT ? FOR UPDATE SKIP LOCKED`, t, dueCond))
	rows, err := tx.QueryContext(ctx, q, StatusPending, now, now, r.batchSize)
	if err != nil {
		return fmt.Errorf("outbox: claim: %w", err)
	}
	var ids []any
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("outbox: claim: %w", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return fmt.Errorf("outbox: claim: %w", err)
	}
	if len(ids) > 0 {
		q = d.Rebind(fmt.Sprintf(`UPDATE %s SET locked_by = ?, locked_until = ? WHERE id IN (%s)`,
			t, dialect.Placeholders(len(ids))))
		args := append([]any{token, leaseUntil}, ids...)
		if _, err = tx.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("outbox: claim: %w", err)
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("outbox: claim: %w", err)
	}
	return nil
}

func (r *Relay) markSent(ctx context.Context, token string, m Message) error {
	q := r.o.d.Rebind(fmt.Sprintf(`UPDATE %s SET status = ?, sent_at = ?, attempts = attempts + 1,
		locked_by = NULL, locked_until = NULL, last_error = NULL WHERE id = ? AND locked_by = ?`, r.o.table))
	if _, err := r.db.ExecContext(ctx, q, StatusSent, r.o.now().UnixMilli(), m.ID, token); err != nil {
		return fmt.Errorf("outbox: mark sent %s: %w", m.ID, err)
	}
	return nil
}

func (r *Relay) markFailed(ctx context.Context, token string, m Message, perr error) error {
	attempts := m.Attempts + 1
	now := r.o.now()
	status, next := StatusPending, now.Add(r.backoff(attempts)).UnixMilli()
	dead := IsPermanent(perr) || attempts >= r.maxAttempts
	if dead {
		status, next = StatusDead, now.UnixMilli()
	}
	msg := perr.Error()
	if len(msg) > 1000 {
		msg = msg[:1000]
	}
	q := r.o.d.Rebind(fmt.Sprintf(`UPDATE %s SET status = ?, attempts = ?, next_attempt_at = ?, last_error = ?,
		locked_by = NULL, locked_until = NULL WHERE id = ? AND locked_by = ?`, r.o.table))
	res, err := r.db.ExecContext(ctx, q, status, attempts, next, msg, m.ID, token)
	if err != nil {
		return fmt.Errorf("outbox: mark failed %s: %w", m.ID, err)
	}
	if dead {
		if n, _ := res.RowsAffected(); n > 0 {
			r.logger.WarnContext(ctx, "outbox relay: message dead-lettered",
				"id", m.ID, "topic", m.Topic, "attempts", attempts, "error", perr)
			if r.onDead != nil {
				m.Attempts = attempts
				r.onDead(m, perr)
			}
		}
	} else {
		r.logger.InfoContext(ctx, "outbox relay: publish failed, will retry",
			"id", m.ID, "topic", m.Topic, "attempts", attempts, "error", perr)
	}
	return nil
}

// release gives back every message still leased to token.
func (r *Relay) release(ctx context.Context, token string) error {
	q := r.o.d.Rebind(fmt.Sprintf(`UPDATE %s SET locked_by = NULL, locked_until = NULL WHERE locked_by = ?`, r.o.table))
	if _, err := r.db.ExecContext(ctx, q, token); err != nil {
		return fmt.Errorf("outbox: release: %w", err)
	}
	return nil
}
