package idempotency

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/neerajsinghi/go-outbox/dialect"
)

// DefaultTable is the idempotency table name used by [NewSQLStore].
const DefaultTable = "idempotency_keys"

// SQLStore is a [Store] backed by a SQL table, shared by every replica of a
// service. Expired rows are ignored and overwritten lazily; call
// [SQLStore.DeleteExpired] periodically to reclaim space.
type SQLStore struct {
	db    *sql.DB
	d     dialect.Dialect
	table string
	now   func() time.Time
}

// SQLOption configures a [SQLStore].
type SQLOption func(*SQLStore)

// WithSQLTable overrides the table name (default [DefaultTable]).
func WithSQLTable(name string) SQLOption { return func(s *SQLStore) { s.table = name } }

// WithSQLClock overrides the time source. Intended for tests.
func WithSQLClock(now func() time.Time) SQLOption { return func(s *SQLStore) { s.now = now } }

// NewSQLStore returns a SQL-backed store.
func NewSQLStore(db *sql.DB, d dialect.Dialect, opts ...SQLOption) (*SQLStore, error) {
	s := &SQLStore{db: db, d: d, table: DefaultTable, now: time.Now}
	for _, opt := range opts {
		opt(s)
	}
	if !dialect.ValidIdentifier(s.table) {
		return nil, fmt.Errorf("idempotency: invalid table name %q", s.table)
	}
	return s, nil
}

// Schema returns the DDL statements for the idempotency table.
func (s *SQLStore) Schema() []string { return s.d.IdempotencySchema(s.table) }

// CreateSchema creates the idempotency table if it does not exist.
func (s *SQLStore) CreateSchema(ctx context.Context) error {
	for _, stmt := range s.Schema() {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("idempotency: create schema: %w", err)
		}
	}
	return nil
}

// Reserve implements [Store].
func (s *SQLStore) Reserve(ctx context.Context, key, fingerprint string, lockTTL time.Duration) (*Record, error) {
	now := s.now()
	// Clear an expired record for this key so the insert below can win.
	del := s.d.Rebind(fmt.Sprintf(`DELETE FROM %s WHERE idem_key = ? AND expires_at <= ?`, s.table))
	if _, err := s.db.ExecContext(ctx, del, key, now.UnixMilli()); err != nil {
		return nil, fmt.Errorf("idempotency: reserve: %w", err)
	}
	ins := s.d.Rebind(s.d.InsertIgnore(s.table,
		[]string{"idem_key", "fingerprint", "completed", "status_code", "created_at", "expires_at"}))
	res, err := s.db.ExecContext(ctx, ins, key, fingerprint, 0, 0, now.UnixMilli(), now.Add(lockTTL).UnixMilli())
	if err != nil {
		return nil, fmt.Errorf("idempotency: reserve: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return nil, fmt.Errorf("idempotency: reserve: %w", err)
	} else if n == 1 {
		return nil, nil
	}
	rec, err := s.get(ctx, key)
	if errors.Is(err, sql.ErrNoRows) {
		// Released between our insert and select; report as in flight so
		// the client retries rather than racing again.
		return &Record{Fingerprint: fingerprint, ExpiresAt: now.Add(lockTTL)}, nil
	}
	return rec, err
}

func (s *SQLStore) get(ctx context.Context, key string) (*Record, error) {
	q := s.d.Rebind(fmt.Sprintf(`SELECT fingerprint, completed, status_code, headers, body, expires_at
		FROM %s WHERE idem_key = ?`, s.table))
	var (
		rec       Record
		completed int
		code      int
		headers   sql.NullString
		body      []byte
		expires   int64
	)
	err := s.db.QueryRowContext(ctx, q, key).Scan(&rec.Fingerprint, &completed, &code, &headers, &body, &expires)
	if err != nil {
		return nil, err
	}
	rec.ExpiresAt = time.UnixMilli(expires)
	if completed == 1 {
		rec.Completed = true
		rec.Response = &Response{StatusCode: code, Body: body, Header: http.Header{}}
		if headers.Valid && headers.String != "" {
			if err := json.Unmarshal([]byte(headers.String), &rec.Response.Header); err != nil {
				return nil, fmt.Errorf("idempotency: decode headers: %w", err)
			}
		}
	}
	return &rec, nil
}

// Complete implements [Store].
func (s *SQLStore) Complete(ctx context.Context, key string, resp *Response, ttl time.Duration) error {
	hdr, err := json.Marshal(resp.Header)
	if err != nil {
		return fmt.Errorf("idempotency: encode headers: %w", err)
	}
	body := resp.Body
	if body == nil {
		body = []byte{}
	}
	q := s.d.Rebind(fmt.Sprintf(`UPDATE %s SET completed = 1, status_code = ?, headers = ?, body = ?, expires_at = ?
		WHERE idem_key = ?`, s.table))
	if _, err := s.db.ExecContext(ctx, q, resp.StatusCode, string(hdr), body, s.now().Add(ttl).UnixMilli(), key); err != nil {
		return fmt.Errorf("idempotency: complete: %w", err)
	}
	return nil
}

// Release implements [Store].
func (s *SQLStore) Release(ctx context.Context, key string) error {
	q := s.d.Rebind(fmt.Sprintf(`DELETE FROM %s WHERE idem_key = ? AND completed = 0`, s.table))
	if _, err := s.db.ExecContext(ctx, q, key); err != nil {
		return fmt.Errorf("idempotency: release: %w", err)
	}
	return nil
}

// DeleteExpired removes expired records and returns how many were deleted.
func (s *SQLStore) DeleteExpired(ctx context.Context) (int64, error) {
	q := s.d.Rebind(fmt.Sprintf(`DELETE FROM %s WHERE expires_at <= ?`, s.table))
	res, err := s.db.ExecContext(ctx, q, s.now().UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("idempotency: delete expired: %w", err)
	}
	return res.RowsAffected()
}
