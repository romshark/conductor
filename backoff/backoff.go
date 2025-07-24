// Package backoff provides a calculator for backoff with jitter.
package backoff

import (
	"fmt"
	"iter"
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

var timeNow = func() time.Time { return time.Now() }

// RandReader provides https://pkg.go.dev/math/rand/v2#Float64.
type RandReader interface{ Float64() float64 }

// Backoff is a stateless exponential backoff with jitter calculator.
type Backoff struct {
	Min        time.Duration // Minimum backoff duration (must be greater 0 and Max).
	Max        time.Duration // Maximum backoff duration.
	Factor     float64       // Exponential growth factor. Must be greater 1.0.
	Jitter     float64       // Jitter ratio in [0.0, 1.0]
	RandSource RandReader
}

// New checks the parameters and returns a new backoff if they're correct,
// otherwise returns an error. If randSource==nil a new default PCG randomness source
// is created automatically.
func New(
	min, max time.Duration, factor, jitter float64, randSource RandReader,
) (Backoff, error) {
	if min <= 0 {
		return Backoff{}, fmt.Errorf("min(%d) must be >0", min)
	}
	if min > max {
		return Backoff{}, fmt.Errorf("min(%s) > max(%s)", min, max)
	}
	if factor <= 1.0 {
		return Backoff{}, fmt.Errorf("factor(%g) must be >1.0", factor)
	}
	if jitter < 0 || jitter > 1 {
		return Backoff{}, fmt.Errorf("jitter(%g) must be >=0.0 && <=1.0", jitter)
	}
	if randSource == nil {
		randSource = rand.New(rand.NewPCG(uint64(timeNow().Unix()), rand.Uint64()))
	}
	return Backoff{
		Min:        min,
		Max:        max,
		Factor:     factor,
		Jitter:     jitter,
		RandSource: randSource,
	}, nil
}

// Duration returns the backoff delay for attempt.
// Returns 0 when attempt <1.
func (b Backoff) Duration(attempt int) time.Duration {
	if attempt < 1 {
		return 0 // Ignore first attempt.
	}
	exp := float64(b.Min) * math.Pow(b.Factor, float64(attempt-1))
	d := min(time.Duration(exp), b.Max)
	if b.Jitter == 0 {
		return d
	}
	randomJitterFactor := b.RandSource.Float64()*2 - 1 // In [-1.0, 1.0]
	delta := float64(d) * b.Jitter * randomJitterFactor
	return max(d+time.Duration(delta), b.Min)
}

// Atomic is a stateful backoff with internal atomic counter.
type Atomic struct {
	lock         sync.Mutex
	retryAttempt int32
	lastAttempt  time.Time
	config       Backoff
}

// Reset resets the attempt counter.
func (b *Atomic) Reset() {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.lastAttempt, b.retryAttempt = time.Time{}, 0
}

// Duration returns zero for the first attempt and if the time difference between
// now and the last call to Duration() is greater than the backoff duration,
// otherwise returns the backoff duration minus the time since last call.
func (b *Atomic) Duration() time.Duration {
	b.lock.Lock()
	defer b.lock.Unlock()

	now := timeNow()
	attempt := b.retryAttempt
	b.retryAttempt++
	d := b.config.Duration(int(attempt))
	if b.lastAttempt.IsZero() { // The is the first ever attempt.
		b.lastAttempt = now
		return d
	}
	alreadyWaited := now.Sub(b.lastAttempt)
	b.lastAttempt = now
	if alreadyWaited > d {
		return 0
	}
	return d - alreadyWaited
}

// Iter returns an iterator over an atomic backoff.
func (b *Atomic) Iter() iter.Seq2[int, time.Duration] {
	return func(yield func(int, time.Duration) bool) {
		for i := 0; ; i++ {
			if !yield(i, b.Duration()) {
				break
			}
		}
	}
}

func NewAtomic(config Backoff) *Atomic {
	return &Atomic{config: config}
}
