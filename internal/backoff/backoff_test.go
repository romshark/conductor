package backoff

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type FakeRndSrc struct {
	i      int
	Series []float64
}

func (f *FakeRndSrc) Reset() { f.i = 0 }

func (f *FakeRndSrc) Float64() float64 {
	v := f.Series[f.i]
	f.i++
	if f.i >= len(f.Series) {
		f.i = 0
	}
	if v > 1.0 || v < -1.0 {
		// https://pkg.go.dev/math/rand/v2#Float64
		panic("Float64 returns, as a float64, a pseudo-random number " +
			"in the half-open interval [0.0,1.0) from the default Source. ")
	}
	return v
}

func testRnd(series ...float64) *FakeRndSrc { return &FakeRndSrc{Series: series} }

func TestDuration(t *testing.T) {
	f := func(t *testing.T, expect []time.Duration,
		min, max time.Duration, factor, jitter float64, rand *FakeRndSrc,
	) {
		t.Helper()
		b, err := New(min, max, factor, jitter, rand)
		require.NoError(t, err)
		for i, exp := range expect {
			actual := b.Duration(i)
			if actual != exp {
				require.Equal(t, exp, actual, "attempt %d", i)
			}
		}

		rand.Reset()
		timeNow = func() time.Time { return time.Date(2025, 1, 1, 1, 1, 1, 0, time.UTC) }
		a := NewAtomic(b)
		for i, dur := range a.Iter() {
			if i >= len(expect) {
				break
			}
			require.Equal(t, expect[i], dur, "iter index %d", i)
		}

		rand.Reset()
		a.Reset()
		for i, dur := range a.Iter() {
			if i >= len(expect) {
				break
			}
			require.Equal(t, expect[i], dur, "iter index %d", i)
		}
	}

	f(t, []time.Duration{
		0,
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
		3200 * time.Millisecond,
		6400 * time.Millisecond, // Capped at max.
		10 * time.Second,
		10 * time.Second,
		10 * time.Second,
	}, 100*time.Millisecond, 10*time.Second, 2, 0, testRnd(1))

	f(t, []time.Duration{
		0,
		time.Second,
		3 * time.Second,
		9 * time.Second,
		27 * time.Second,
		time.Minute, // Capped at max.
	}, time.Second, 60*time.Second, 3, 0, testRnd(1))

	f(t, []time.Duration{
		0,
		time.Minute, // Capped at max.
		time.Minute,
		time.Minute,
	}, time.Minute, time.Minute, 60.0, 0, testRnd(1))

	f(t, []time.Duration{
		0,
		time.Second,
		1500 * time.Millisecond,
		2250 * time.Millisecond,
		3375 * time.Millisecond,
		4000 * time.Millisecond, // Capped at max.
	}, time.Second, 4*time.Second, 1.5, 0, testRnd(1))

	// Jitter
	f(t, []time.Duration{
		0,
		100 * time.Millisecond,
		185 * time.Millisecond,
		380 * time.Millisecond,
		656 * time.Millisecond,
		1424 * time.Millisecond,
		2960 * time.Millisecond,
		6080 * time.Millisecond,
		8200 * time.Millisecond,
		8900 * time.Millisecond,
		// This pattern will keep repeating according to the simulated rand. seq.
		9250 * time.Millisecond,
		9500 * time.Millisecond,
		8200 * time.Millisecond,
		8900 * time.Millisecond,
		// ...
		9250 * time.Millisecond,
		9500 * time.Millisecond,
		8200 * time.Millisecond,
		8900 * time.Millisecond,
	}, 100*time.Millisecond, 10*time.Second, 2, 0.1, testRnd(-0.05, 0.125, 0.25, -0.4))

	// Jitter with clamp to minimum.
	f(t, []time.Duration{
		0,
		// base=100ms, rand=0.0 -> jitter factor=-1.0
		// delta=100ms * 1.0 * -1.0 = -100ms -> result=0ms < min -> clamped to 100ms
		100 * time.Millisecond,
		// base=200ms, rand=0.1 -> jitter factor=-0.8
		// delta=200ms * 1.0 * -0.8 = -160ms -> result=40ms < min -> clamped to 100ms
		100 * time.Millisecond,
		// base=400ms, rand=0.12 -> jitter factor=-0.76
		// delta=400ms * 1.0 * -0.76 = -304ms -> result=96ms < min -> clamped to 100ms
		100 * time.Millisecond,
		// base=800ms, rand=0.12 -> jitter factor=-0.76
		// delta=800ms * 1.0 * -0.76 = -608ms -> result=192ms >= min -> used as-is
		192 * time.Millisecond,
		// base=1600ms, rand=0.25 -> jitter factor=-0.5
		// delta=1600ms * 1.0 * -0.5 = -800ms -> result=800ms >= min -> used as-is
		800 * time.Millisecond,
	}, 100*time.Millisecond, 10*time.Second, 2.0, 1.0,
		testRnd(0.0, 0.1, 0.12, 0.12, 0.25))
}

func TestNew(t *testing.T) {
	b, err := New(2*time.Second, 1*time.Second, 2.0, 0.0, testRnd())
	require.EqualError(t, err, "min(2s) > max(1s)")
	require.Zero(t, b)

	b, err = New(0, 0, 2.0, 0.0, testRnd())
	require.EqualError(t, err, "min(0) must be >0")
	require.Zero(t, b)
	b, err = New(-1, 0, 2.0, 0.0, testRnd())
	require.EqualError(t, err, "min(-1) must be >0")
	require.Zero(t, b)
	b, err = New(time.Second, 2*time.Second, 0.9, 0.1, testRnd())
	require.EqualError(t, err, "factor(0.9) must be >1.0")
	require.Zero(t, b)
	b, err = New(time.Second, 2*time.Second, 0, 0.1, testRnd())
	require.EqualError(t, err, "factor(0) must be >1.0")
	require.Zero(t, b)
	b, err = New(time.Second, 2*time.Second, 2.0, -2, testRnd())
	require.EqualError(t, err, "jitter(-2) must be >=0.0 && <=1.0")
	require.Zero(t, b)
	b, err = New(time.Second, 2*time.Second, 2.0, 1.1, testRnd())
	require.EqualError(t, err, "jitter(1.1) must be >=0.0 && <=1.0")
	require.Zero(t, b)
}

func TestAtomicLastAttempt(t *testing.T) {
	b, err := New(1*time.Second, 1*time.Second, 2.0, 0.0, testRnd())
	require.NoError(t, err)

	a := NewAtomic(b)

	firstAttemptTime := time.Date(2025, 1, 1, 1, 1, 1, 0, time.UTC)
	timeNow = func() time.Time { return firstAttemptTime }

	require.Zero(t, a.Duration())

	secondAttemptTime := firstAttemptTime.Add(1100 * time.Millisecond)
	timeNow = func() time.Time { return secondAttemptTime }

	require.Zero(t, a.Duration())

	require.Equal(t, time.Second, a.Duration())

	a.Reset()
	require.Zero(t, a.Duration())
}
