// Package idempotency provides net/http middleware implementing the
// Idempotency-Key header (draft-ietf-httpapi-idempotency-key-header), so
// that clients can safely retry non-idempotent requests such as POST.
//
// For a request carrying an Idempotency-Key:
//
//   - first use: the handler runs and its response is stored;
//   - repeat with the same request: the stored response is replayed and the
//     handler does not run again (the replay carries "Idempotent-Replayed: true");
//   - repeat while the first request is still running: 409 Conflict;
//   - same key with a different method, path or body: 422 Unprocessable Entity.
//
// Responses with a 5xx status (and handler panics) are not stored; the key
// is released so the client can retry.
package idempotency

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"time"
)

// Defaults for [Middleware].
const (
	DefaultHeader       = "Idempotency-Key"
	DefaultTTL          = 24 * time.Hour
	DefaultLockTimeout  = time.Minute
	DefaultMaxBodyBytes = 1 << 20
	DefaultMaxKeyLength = 255
	// ReplayedHeader is set on responses served from the store.
	ReplayedHeader = "Idempotent-Replayed"
)

type config struct {
	header       string
	ttl          time.Duration
	lockTimeout  time.Duration
	maxBody      int64
	methods      map[string]bool
	required     bool
	scope        func(*http.Request) string
	onStoreError func(*http.Request, error)
}

// Option configures [Middleware].
type Option func(*config)

// WithTTL sets how long completed responses are kept (default 24h).
func WithTTL(d time.Duration) Option { return func(c *config) { c.ttl = d } }

// WithLockTimeout sets how long an in-flight key stays locked if the
// request never completes, e.g. because the process crashed (default 1m).
func WithLockTimeout(d time.Duration) Option { return func(c *config) { c.lockTimeout = d } }

// WithMaxBodyBytes limits the request body read for fingerprinting
// (default 1 MiB). Larger requests get 413.
func WithMaxBodyBytes(n int64) Option { return func(c *config) { c.maxBody = n } }

// WithMethods sets which HTTP methods are subject to idempotency handling
// (default POST and PATCH). Other methods pass straight through.
func WithMethods(methods ...string) Option {
	return func(c *config) {
		c.methods = make(map[string]bool, len(methods))
		for _, m := range methods {
			c.methods[m] = true
		}
	}
}

// WithRequired makes the header mandatory for the configured methods;
// requests without it get 400.
func WithRequired() Option { return func(c *config) { c.required = true } }

// WithHeaderName overrides the request header name.
func WithHeaderName(name string) Option { return func(c *config) { c.header = name } }

// WithScope namespaces keys, typically by authenticated principal, so two
// clients choosing the same key do not collide:
//
//	idempotency.WithScope(func(r *http.Request) string { return userID(r.Context()) })
func WithScope(fn func(*http.Request) string) Option { return func(c *config) { c.scope = fn } }

// WithStoreErrorHook is called when the store fails; the request then gets
// 503 so the client retries later.
func WithStoreErrorHook(fn func(*http.Request, error)) Option {
	return func(c *config) { c.onStoreError = fn }
}

// Middleware returns net/http middleware backed by store.
func Middleware(store Store, opts ...Option) func(http.Handler) http.Handler {
	c := &config{
		header:      DefaultHeader,
		ttl:         DefaultTTL,
		lockTimeout: DefaultLockTimeout,
		maxBody:     DefaultMaxBodyBytes,
		methods:     map[string]bool{http.MethodPost: true, http.MethodPatch: true},
	}
	for _, opt := range opts {
		opt(c)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c.serve(store, next, w, r)
		})
	}
}

func (c *config) serve(store Store, next http.Handler, w http.ResponseWriter, r *http.Request) {
	if !c.methods[r.Method] {
		next.ServeHTTP(w, r)
		return
	}
	key := r.Header.Get(c.header)
	if key == "" {
		if c.required {
			http.Error(w, c.header+" header is required", http.StatusBadRequest)
			return
		}
		next.ServeHTTP(w, r)
		return
	}
	if len(key) > DefaultMaxKeyLength {
		http.Error(w, c.header+" is too long", http.StatusBadRequest)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, c.maxBody+1))
	if err != nil {
		http.Error(w, "cannot read request body", http.StatusBadRequest)
		return
	}
	if int64(len(body)) > c.maxBody {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))

	storeKey := key
	if c.scope != nil {
		storeKey = c.scope(r) + "\x00" + key
	}
	fp := Fingerprint(r, body)

	ctx := r.Context()
	rec, err := store.Reserve(ctx, storeKey, fp, c.lockTimeout)
	if err != nil {
		c.storeFailed(w, r, err)
		return
	}
	if rec != nil {
		switch {
		case rec.Fingerprint != fp:
			http.Error(w, c.header+" was already used with a different request", http.StatusUnprocessableEntity)
		case !rec.Completed:
			w.Header().Set("Retry-After", "1")
			http.Error(w, "a request with this "+c.header+" is already in progress", http.StatusConflict)
		default:
			replay(w, rec.Response)
		}
		return
	}

	rw := &recorder{ResponseWriter: w, status: http.StatusOK}
	completed := false
	defer func() {
		if completed {
			return
		}
		// Panic or 5xx: let the client retry with the same key.
		_ = store.Release(context.WithoutCancel(ctx), storeKey)
	}()
	next.ServeHTTP(rw, r)

	if rw.status >= 500 {
		return
	}
	resp := &Response{StatusCode: rw.status, Header: rw.snapshot, Body: rw.buf.Bytes()}
	if resp.Header == nil {
		resp.Header = w.Header().Clone()
	}
	if err := store.Complete(context.WithoutCancel(ctx), storeKey, resp, c.ttl); err != nil {
		if c.onStoreError != nil {
			c.onStoreError(r, err)
		}
		return // response already sent; the deferred Release unlocks the key
	}
	completed = true
}

func (c *config) storeFailed(w http.ResponseWriter, r *http.Request, err error) {
	if c.onStoreError != nil {
		c.onStoreError(r, err)
	}
	if errors.Is(err, context.Canceled) {
		return
	}
	w.Header().Set("Retry-After", "1")
	http.Error(w, "idempotency store unavailable", http.StatusServiceUnavailable)
}

// Fingerprint returns a hex SHA-256 over the request method, path, query
// and body. Two requests with the same fingerprint are considered the same
// request for idempotency purposes.
func Fingerprint(r *http.Request, body []byte) string {
	h := sha256.New()
	io.WriteString(h, r.Method)
	h.Write([]byte{0})
	io.WriteString(h, r.URL.Path)
	h.Write([]byte{0})
	io.WriteString(h, r.URL.RawQuery)
	h.Write([]byte{0})
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

func replay(w http.ResponseWriter, resp *Response) {
	if resp == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	dst := w.Header()
	for k, vs := range resp.Header {
		dst[k] = append([]string(nil), vs...)
	}
	dst.Set(ReplayedHeader, "true")
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(resp.Body)
}

// recorder passes the response through to the client while keeping a copy.
type recorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
	snapshot    http.Header
	buf         bytes.Buffer
}

func (r *recorder) WriteHeader(code int) {
	if r.wroteHeader {
		return
	}
	r.wroteHeader = true
	r.status = code
	r.snapshot = r.ResponseWriter.Header().Clone()
	r.ResponseWriter.WriteHeader(code)
}

func (r *recorder) Write(p []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	r.buf.Write(p)
	return r.ResponseWriter.Write(p)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }
