package idempotency

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// Response is a captured HTTP response that can be replayed.
type Response struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

// Record is the stored state of an idempotency key.
type Record struct {
	// Fingerprint identifies the request that first used the key.
	Fingerprint string
	// Completed is false while the original request is still in flight.
	Completed bool
	// Response is the stored response; nil while in flight.
	Response *Response
	// ExpiresAt is when the record stops being honoured.
	ExpiresAt time.Time
}

// Store persists idempotency records. Implementations must make Reserve
// atomic: of many concurrent Reserve calls for the same free key exactly one
// may succeed.
type Store interface {
	// Reserve claims key for a new request. If no unexpired record exists it
	// stores an in-flight record expiring after lockTTL and returns
	// (nil, nil). Otherwise it returns the existing record untouched.
	Reserve(ctx context.Context, key, fingerprint string, lockTTL time.Duration) (*Record, error)
	// Complete stores the final response for key, keeping it for ttl.
	Complete(ctx context.Context, key string, resp *Response, ttl time.Duration) error
	// Release deletes an in-flight record so the client may retry, e.g.
	// after a server error.
	Release(ctx context.Context, key string) error
}

// MemoryStore is an in-process [Store]. It is suitable for tests and for
// single-instance services; use [SQLStore] when running several replicas.
type MemoryStore struct {
	mu      sync.Mutex
	now     func() time.Time
	records map[string]*Record
}

// MemoryOption configures a [MemoryStore].
type MemoryOption func(*MemoryStore)

// WithMemoryClock overrides the store's time source. Intended for tests.
func WithMemoryClock(now func() time.Time) MemoryOption {
	return func(s *MemoryStore) { s.now = now }
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore(opts ...MemoryOption) *MemoryStore {
	s := &MemoryStore{now: time.Now, records: make(map[string]*Record)}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Reserve implements [Store].
func (s *MemoryStore) Reserve(_ context.Context, key, fingerprint string, lockTTL time.Duration) (*Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if rec, ok := s.records[key]; ok && now.Before(rec.ExpiresAt) {
		cp := *rec
		return &cp, nil
	}
	s.records[key] = &Record{Fingerprint: fingerprint, ExpiresAt: now.Add(lockTTL)}
	return nil, nil
}

// Complete implements [Store].
func (s *MemoryStore) Complete(_ context.Context, key string, resp *Response, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[key]
	if !ok {
		rec = &Record{}
		s.records[key] = rec
	}
	rec.Completed = true
	rec.Response = cloneResponse(resp)
	rec.ExpiresAt = s.now().Add(ttl)
	return nil
}

// Release implements [Store].
func (s *MemoryStore) Release(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec, ok := s.records[key]; ok && !rec.Completed {
		delete(s.records, key)
	}
	return nil
}

// Cleanup removes expired records. Call it periodically for long-running
// processes.
func (s *MemoryStore) Cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	for k, rec := range s.records {
		if !now.Before(rec.ExpiresAt) {
			delete(s.records, k)
		}
	}
}

func cloneResponse(r *Response) *Response {
	if r == nil {
		return nil
	}
	return &Response{
		StatusCode: r.StatusCode,
		Header:     r.Header.Clone(),
		Body:       append([]byte(nil), r.Body...),
	}
}
