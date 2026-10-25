package outbox

import (
	"math/rand/v2"
	"time"
)

// Backoff returns how long to wait before the next delivery attempt, given
// the number of attempts made so far (1 after the first failure).
type Backoff func(attempt int) time.Duration

// ExponentialBackoff returns a [Backoff] that doubles the delay on every
// attempt, starting at base and capped at max, with "equal jitter": the
// result is uniformly distributed in [d/2, d] where d is the capped
// exponential delay. Jitter spreads retries out so a recovering broker is
// not hit by a synchronized thundering herd.
func ExponentialBackoff(base, max time.Duration) Backoff {
	if base <= 0 {
		base = time.Second
	}
	if max < base {
		max = base
	}
	return func(attempt int) time.Duration {
		d := capped(base, max, attempt)
		half := d / 2
		return half + rand.N(d-half+1)
	}
}

// capped computes min(max, base*2^(attempt-1)) without overflowing.
func capped(base, max time.Duration, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := base
	for i := 1; i < attempt; i++ {
		if d >= max/2 {
			return max
		}
		d *= 2
	}
	if d > max {
		return max
	}
	return d
}
