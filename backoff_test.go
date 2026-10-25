package outbox

import (
	"testing"
	"time"
)

func TestCapped(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, time.Second},
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{6, 32 * time.Second},
		{7, time.Minute},
		{1000, time.Minute}, // no overflow
	}
	for _, tt := range tests {
		if got := capped(time.Second, time.Minute, tt.attempt); got != tt.want {
			t.Errorf("capped(attempt=%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestExponentialBackoffJitterBounds(t *testing.T) {
	b := ExponentialBackoff(100*time.Millisecond, 10*time.Second)
	tests := []struct {
		attempt  int
		min, max time.Duration
	}{
		{1, 50 * time.Millisecond, 100 * time.Millisecond},
		{2, 100 * time.Millisecond, 200 * time.Millisecond},
		{4, 400 * time.Millisecond, 800 * time.Millisecond},
		{20, 5 * time.Second, 10 * time.Second},
	}
	for _, tt := range tests {
		distinct := map[time.Duration]bool{}
		for i := 0; i < 200; i++ {
			d := b(tt.attempt)
			if d < tt.min || d > tt.max {
				t.Fatalf("attempt %d: %v outside [%v, %v]", tt.attempt, d, tt.min, tt.max)
			}
			distinct[d] = true
		}
		if len(distinct) < 2 {
			t.Errorf("attempt %d: no jitter observed", tt.attempt)
		}
	}
}
