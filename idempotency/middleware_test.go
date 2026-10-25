package idempotency_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/neerajsinghi/go-outbox/dialect"
	"github.com/neerajsinghi/go-outbox/idempotency"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type storeFactory func(t *testing.T, now func() time.Time) idempotency.Store

var stores = map[string]storeFactory{
	"memory": func(t *testing.T, now func() time.Time) idempotency.Store {
		return idempotency.NewMemoryStore(idempotency.WithMemoryClock(now))
	},
	"sqlite": func(t *testing.T, now func() time.Time) idempotency.Store {
		dsn := "file:" + filepath.Join(t.TempDir(), "idem.db") + "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)"
		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		s, err := idempotency.NewSQLStore(db, dialect.SQLite, idempotency.WithSQLClock(now))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.CreateSchema(context.Background()); err != nil {
			t.Fatal(err)
		}
		return s
	},
}

// forEachStore runs fn against every Store implementation.
func forEachStore(t *testing.T, fn func(t *testing.T, store idempotency.Store, clk *clock)) {
	for name, mk := range stores {
		t.Run(name, func(t *testing.T) {
			clk := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
			fn(t, mk(t, clk.Now), clk)
		})
	}
}

// chargeHandler creates a "charge" and counts how often it really ran.
func chargeHandler(calls *atomic.Int64) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Location", fmt.Sprintf("/charges/%d", n))
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, `{"charge":%d,"req":%s}`, n, body)
	})
}

type result struct {
	code     int
	body     string
	replayed bool
}

func do(t *testing.T, h http.Handler, method, key, body string) result {
	t.Helper()
	req := httptest.NewRequest(method, "/charges", strings.NewReader(body))
	if key != "" {
		req.Header.Set(idempotency.DefaultHeader, key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return result{rec.Code, rec.Body.String(), rec.Header().Get(idempotency.ReplayedHeader) == "true"}
}

func TestMiddlewareSequential(t *testing.T) {
	type step struct {
		method, key, body string
		advance           time.Duration
		want              result
		wantCalls         int64
	}
	tests := []struct {
		name  string
		opts  []idempotency.Option
		steps []step
	}{
		{
			name: "replay identical request",
			steps: []step{
				{method: "POST", key: "k1", body: `{"amt":10}`, want: result{201, `{"charge":1,"req":{"amt":10}}`, false}, wantCalls: 1},
				{method: "POST", key: "k1", body: `{"amt":10}`, want: result{201, `{"charge":1,"req":{"amt":10}}`, true}, wantCalls: 1},
			},
		},
		{
			name: "same key different body is 422",
			steps: []step{
				{method: "POST", key: "k1", body: `{"amt":10}`, want: result{code: 201}, wantCalls: 1},
				{method: "POST", key: "k1", body: `{"amt":99}`, want: result{code: 422}, wantCalls: 1},
			},
		},
		{
			name: "different keys run independently",
			steps: []step{
				{method: "POST", key: "k1", body: `{}`, want: result{code: 201}, wantCalls: 1},
				{method: "POST", key: "k2", body: `{}`, want: result{code: 201}, wantCalls: 2},
			},
		},
		{
			name: "no key passes through",
			steps: []step{
				{method: "POST", body: `{}`, want: result{code: 201}, wantCalls: 1},
				{method: "POST", body: `{}`, want: result{code: 201}, wantCalls: 2},
			},
		},
		{
			name: "required key",
			opts: []idempotency.Option{idempotency.WithRequired()},
			steps: []step{
				{method: "POST", body: `{}`, want: result{code: 400}, wantCalls: 0},
			},
		},
		{
			name: "GET is not handled by default",
			steps: []step{
				{method: "GET", key: "k1", want: result{code: 201}, wantCalls: 1},
				{method: "GET", key: "k1", want: result{code: 201}, wantCalls: 2},
			},
		},
		{
			name: "expired key runs again",
			opts: []idempotency.Option{idempotency.WithTTL(time.Hour)},
			steps: []step{
				{method: "POST", key: "k1", body: `{}`, want: result{code: 201}, wantCalls: 1},
				{method: "POST", key: "k1", body: `{}`, advance: 59 * time.Minute, want: result{code: 201, replayed: true}, wantCalls: 1},
				{method: "POST", key: "k1", body: `{}`, advance: 2 * time.Minute, want: result{code: 201}, wantCalls: 2},
			},
		},
		{
			name: "oversized body is 413",
			opts: []idempotency.Option{idempotency.WithMaxBodyBytes(4)},
			steps: []step{
				{method: "POST", key: "k1", body: `{"too":"big"}`, want: result{code: 413}, wantCalls: 0},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			forEachStore(t, func(t *testing.T, store idempotency.Store, clk *clock) {
				var calls atomic.Int64
				h := idempotency.Middleware(store, tt.opts...)(chargeHandler(&calls))
				for i, s := range tt.steps {
					clk.Advance(s.advance)
					got := do(t, h, s.method, s.key, s.body)
					if got.code != s.want.code || got.replayed != s.want.replayed {
						t.Fatalf("step %d: got %d replayed=%v (%q), want %d replayed=%v", i, got.code, got.replayed, got.body, s.want.code, s.want.replayed)
					}
					if s.want.body != "" && got.body != s.want.body {
						t.Fatalf("step %d: body %q, want %q", i, got.body, s.want.body)
					}
					if c := calls.Load(); c != s.wantCalls {
						t.Fatalf("step %d: handler ran %d times, want %d", i, c, s.wantCalls)
					}
				}
			})
		})
	}
}

func TestMiddlewareReplaysHeaders(t *testing.T) {
	forEachStore(t, func(t *testing.T, store idempotency.Store, _ *clock) {
		var calls atomic.Int64
		h := idempotency.Middleware(store)(chargeHandler(&calls))
		for i := 0; i < 2; i++ {
			req := httptest.NewRequest("POST", "/charges", strings.NewReader(`{}`))
			req.Header.Set("Idempotency-Key", "k")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if got := rec.Header().Get("Location"); got != "/charges/1" {
				t.Fatalf("request %d: Location = %q", i, got)
			}
			if got := rec.Header().Get("Content-Type"); got != "application/json" {
				t.Fatalf("request %d: Content-Type = %q", i, got)
			}
		}
	})
}

func TestMiddlewareInFlightDuplicateIs409(t *testing.T) {
	forEachStore(t, func(t *testing.T, store idempotency.Store, _ *clock) {
		entered := make(chan struct{})
		release := make(chan struct{})
		var calls atomic.Int64
		slow := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			close(entered)
			<-release
			w.WriteHeader(http.StatusAccepted)
			io.WriteString(w, "done")
		})
		h := idempotency.Middleware(store)(slow)

		first := make(chan result, 1)
		go func() { first <- do(t, h, "POST", "k1", `{}`) }()
		<-entered

		if got := do(t, h, "POST", "k1", `{}`); got.code != http.StatusConflict {
			t.Fatalf("in-flight duplicate: got %d, want 409", got.code)
		}
		// A different body is still rejected as a mismatch, even in flight.
		if got := do(t, h, "POST", "k1", `{"x":1}`); got.code != http.StatusUnprocessableEntity {
			t.Fatalf("in-flight mismatch: got %d, want 422", got.code)
		}

		close(release)
		if got := <-first; got.code != http.StatusAccepted || got.body != "done" {
			t.Fatalf("first request: %+v", got)
		}
		if got := do(t, h, "POST", "k1", `{}`); got.code != http.StatusAccepted || !got.replayed || got.body != "done" {
			t.Fatalf("after completion: %+v, want replayed 202", got)
		}
		if calls.Load() != 1 {
			t.Fatalf("handler ran %d times, want 1", calls.Load())
		}
	})
}

func TestMiddlewareConcurrentSameKeyRunsOnce(t *testing.T) {
	forEachStore(t, func(t *testing.T, store idempotency.Store, _ *clock) {
		var calls atomic.Int64
		h := idempotency.Middleware(store)(chargeHandler(&calls))
		var wg sync.WaitGroup
		codes := make(chan int, 20)
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				codes <- do(t, h, "POST", "same", `{}`).code
			}()
		}
		wg.Wait()
		close(codes)
		for c := range codes {
			if c != http.StatusCreated && c != http.StatusConflict {
				t.Errorf("unexpected status %d", c)
			}
		}
		if calls.Load() != 1 {
			t.Fatalf("handler ran %d times, want exactly 1", calls.Load())
		}
	})
}

func TestMiddlewareServerErrorReleasesKey(t *testing.T) {
	forEachStore(t, func(t *testing.T, store idempotency.Store, _ *clock) {
		var calls atomic.Int64
		flaky := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if calls.Add(1) == 1 {
				http.Error(w, "db down", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusCreated)
		})
		h := idempotency.Middleware(store)(flaky)
		if got := do(t, h, "POST", "k1", `{}`); got.code != 503 {
			t.Fatalf("first: %d", got.code)
		}
		if got := do(t, h, "POST", "k1", `{}`); got.code != 201 || got.replayed {
			t.Fatalf("retry after 5xx: %+v, want fresh 201", got)
		}
	})
}

func TestMiddlewarePanicReleasesKey(t *testing.T) {
	forEachStore(t, func(t *testing.T, store idempotency.Store, _ *clock) {
		var calls atomic.Int64
		h := idempotency.Middleware(store)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if calls.Add(1) == 1 {
				panic("boom")
			}
			w.WriteHeader(http.StatusCreated)
		}))
		func() {
			defer func() { _ = recover() }()
			do(t, h, "POST", "k1", `{}`)
		}()
		if got := do(t, h, "POST", "k1", `{}`); got.code != 201 {
			t.Fatalf("retry after panic: %+v", got)
		}
	})
}

func TestMiddlewareScope(t *testing.T) {
	forEachStore(t, func(t *testing.T, store idempotency.Store, _ *clock) {
		var calls atomic.Int64
		h := idempotency.Middleware(store, idempotency.WithScope(func(r *http.Request) string {
			return r.Header.Get("X-User")
		}))(chargeHandler(&calls))
		for _, user := range []string{"alice", "bob"} {
			req := httptest.NewRequest("POST", "/charges", strings.NewReader(`{}`))
			req.Header.Set("Idempotency-Key", "same")
			req.Header.Set("X-User", user)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != 201 || rec.Header().Get(idempotency.ReplayedHeader) != "" {
				t.Fatalf("%s: %d replayed=%q", user, rec.Code, rec.Header().Get(idempotency.ReplayedHeader))
			}
		}
		if calls.Load() != 2 {
			t.Fatalf("handler ran %d times, want 2", calls.Load())
		}
	})
}

func TestInFlightLockExpires(t *testing.T) {
	forEachStore(t, func(t *testing.T, store idempotency.Store, clk *clock) {
		ctx := context.Background()
		if rec, err := store.Reserve(ctx, "k", "fp", time.Minute); err != nil || rec != nil {
			t.Fatalf("first reserve = %v, %v", rec, err)
		}
		if rec, _ := store.Reserve(ctx, "k", "fp", time.Minute); rec == nil || rec.Completed {
			t.Fatalf("second reserve should see in-flight record, got %+v", rec)
		}
		clk.Advance(time.Minute)
		if rec, err := store.Reserve(ctx, "k", "fp", time.Minute); err != nil || rec != nil {
			t.Fatalf("reserve after crash timeout = %+v, %v; want fresh reservation", rec, err)
		}
	})
}
