package conductor_test

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/romshark/conductor"
	"github.com/romshark/conductor/db"
	"github.com/romshark/conductor/db/dbpgx"
	"github.com/romshark/conductor/internal/testdb"
	"golang.org/x/sync/errgroup"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

//go:embed db/dbpgx/roles.sql
var rolesSQL string

//go:embed db/dbpgx/system.sql
var systemSQL string

//go:embed db/dbpgx/permissions.sql
var permissionsSQL string

func TestMain(m *testing.M) {
	// Roles must be created globally before systemSQL is executed.
	testdb.MustExecGlobal(rolesSQL)
	os.Exit(m.Run())
}

type EventTest struct {
	conductor.EventMetadata

	Foo string `json:"foo"`
	Bar int    `json:"bar"`
}

func NewTestCodec(t *testing.T) *conductor.EventCodec {
	t.Helper()
	ec := conductor.NewTypeCodec("test-revision")
	conductor.MustRegisterEventTypeIn[*EventTest](ec, "test-event")
	return ec
}

type MockPoller struct{ Ch <-chan time.Time }

func (m *MockPoller) C() <-chan time.Time { return m.Ch }
func (m *MockPoller) Stop()               {}

var _ conductor.Poller = new(MockPoller)

type MockStatelessProcessor struct{ mock.Mock }

func (m *MockStatelessProcessor) requireCallProcess(expect *EventTest) *mock.Call {
	return m.On("Process",
		mock.Anything, // Context
		mock.MatchedBy(func(e conductor.Event) bool {
			et, ok := e.(*EventTest)
			return ok && et.Foo == expect.Foo && et.Bar == expect.Bar
		}),
		mock.Anything, // Transaction
	)
}

func (m *MockStatelessProcessor) Backoff() (
	min, max time.Duration, factor, jitter float64,
) {
	min, max = 50*time.Millisecond, 500*time.Millisecond
	factor, jitter = 2.0, 0
	return
}

func (m *MockStatelessProcessor) Process(
	ctx context.Context, e conductor.Event, tx db.TxRW,
) error {
	args := m.Called(ctx, e, tx)
	return args.Error(0)
}

type MockStatefulProcessor struct{ mock.Mock }

func (m *MockStatefulProcessor) requireCallApply(
	assumedVersion int64, expect *EventTest,
) *mock.Call {
	return m.On("Apply",
		mock.Anything,
		assumedVersion,
		mock.MatchedBy(func(e conductor.Event) bool {
			et, ok := e.(*EventTest)
			return ok && et.Foo == expect.Foo && et.Bar == expect.Bar
		}),
		mock.Anything,
	)
}

func (m *MockStatefulProcessor) requireCallVersion() *mock.Call {
	return m.On("Version", mock.Anything)
}

func (m *MockStatefulProcessor) Backoff() (
	min, max time.Duration, factor, jitter float64,
) {
	min, max = 50*time.Millisecond, 500*time.Millisecond
	factor, jitter = 2.0, 0
	return
}

func (m *MockStatefulProcessor) Version(ctx context.Context) (version int64, err error) {
	args := m.Called(ctx)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockStatefulProcessor) Apply(
	ctx context.Context, assumedVersion int64, e conductor.Event,
) (commit func(context.Context) error, err error) {
	args := m.Called(ctx, assumedVersion, e)
	return args.Get(0).(func(context.Context) error), args.Error(1)
}

type MockReactor struct {
	mock.Mock
	projectionID int32
}

func (m *MockReactor) requireCallReact(version int64, expect *EventTest) *mock.Call {
	return m.On("React",
		mock.Anything, // Context
		version,
		mock.MatchedBy(func(e conductor.Event) bool {
			et, ok := e.(*EventTest)
			return ok && et.Foo == expect.Foo && et.Bar == expect.Bar
		}),
		mock.Anything, // Transaction
	)
}

func (m *MockReactor) ProjectionID() int32 { return m.projectionID }

func (m *MockReactor) React(
	ctx context.Context, version int64, e conductor.Event, tx db.TxReadOnly,
) error {
	args := m.Called(ctx, version, e, tx)
	return args.Error(0)
}

func (m *MockReactor) Backoff() (
	min, max time.Duration, factor, jitter float64,
) {
	min, max = 50*time.Millisecond, 500*time.Millisecond
	factor, jitter = 2.0, 0
	return
}

func migrateDB(t *testing.T, db *dbpgx.DB) {
	t.Helper()
	_, err := db.Exec(t.Context(), systemSQL)
	require.NoError(t, err)
	_, err = db.Exec(t.Context(), permissionsSQL)
	require.NoError(t, err)
}

func setup(t *testing.T) (
	log *slog.Logger, db *dbpgx.DB, ec *conductor.EventCodec,
) {
	log = slog.Default()
	db, _ = testdb.NewDBPGX(t, log)
	migrateDB(t, db)
	ec = NewTestCodec(t)
	conductor.MustRegisterEventTypeIn[*EventTest](ec, "event-test")
	return log, db, ec
}

// TestStatelessProcessorVeto tests stateless processor veto ability.
func TestStatelessProcessorVeto(t *testing.T) {
	log, db, ec := setup(t)

	p1 := &MockStatelessProcessor{}
	p2 := &MockStatelessProcessor{}
	defer p1.AssertExpectations(t)
	defer p2.AssertExpectations(t)

	errExpected := errors.New("expected processor error")
	{
		c1 := p1.requireCallProcess(&EventTest{Foo: "foo", Bar: 42}).
			Return(error(nil)).
			Times(1)
		p2.requireCallProcess(&EventTest{Foo: "foo", Bar: 42}).
			Return(errExpected).
			Once().
			NotBefore(c1)
	}

	ctx := t.Context()
	orch, err := conductor.Make(
		ctx, ec, log, db,
		[]conductor.StatelessProcessor{p1, p2},
		nil,
		nil,
	)
	require.NoError(t, err)

	requireVersion(t, 0, orch)

	// p2 will veto this append.
	v, err := orch.Append(ctx, log, &EventTest{Foo: "foo", Bar: 42})
	require.ErrorIs(t, err, errExpected)
	require.Equal(t, int64(0), v)
	requireVersion(t, 0, orch)
}

// TestStatefulProcessorVeto tests stateless processor veto ability.
func TestStatefulProcessorVeto(t *testing.T) {
	log, db, ec := setup(t)

	errExpected := errors.New("expected processor error")

	p1 := &MockStatefulProcessor{}
	p2 := &MockStatefulProcessor{}
	defer p1.AssertExpectations(t)
	defer p2.AssertExpectations(t)

	p1.requireCallApply(0, &EventTest{Foo: "foo", Bar: 42}).
		Once().
		Return(func(context.Context) error { return nil }, error(nil))

	p2.requireCallApply(0, &EventTest{Foo: "foo", Bar: 42}).
		Once().
		Return(func(context.Context) error { return nil }, errExpected)

	ctx := t.Context()
	orch, err := conductor.Make(
		ctx, ec, log, db,
		nil,
		[]conductor.StatefulProcessor{p1, p2},
		nil,
	)
	require.NoError(t, err)

	requireVersion(t, 0, orch)

	// p2 will veto this append.
	v, err := orch.Append(ctx, log, &EventTest{Foo: "foo", Bar: 42})
	require.ErrorIs(t, err, errExpected)
	require.Equal(t, int64(0), v)
	requireVersion(t, 0, orch)
}

// TestStatefulProcessorsOK tests both stateful and stateless processor.
func TestStatefulProcessorsOK(t *testing.T) {
	log, db, ec := setup(t)

	p1 := &MockStatelessProcessor{}
	p2 := &MockStatelessProcessor{}
	ps1 := &MockStatefulProcessor{}
	ps2 := &MockStatefulProcessor{}
	defer p1.AssertExpectations(t)
	defer p2.AssertExpectations(t)
	defer ps1.AssertExpectations(t)
	defer ps2.AssertExpectations(t)

	var commitPS1 atomic.Int32
	var commitPS2 atomic.Int32

	{
		p1c := p1.requireCallProcess(&EventTest{Foo: "foo", Bar: 42}).
			Once().
			Return(error(nil))
		p2c := p2.requireCallProcess(&EventTest{Foo: "foo", Bar: 42}).
			Once().
			NotBefore(p1c).
			Return(error(nil))

		ps1.requireCallApply(0, &EventTest{Foo: "foo", Bar: 42}).
			Once().
			NotBefore(p2c).
			Return(func(context.Context) error {
				commitPS1.Add(1)
				return nil
			}, error(nil))

		ps2.requireCallApply(0, &EventTest{Foo: "foo", Bar: 42}).
			Once().
			NotBefore(p2c).
			Return(func(context.Context) error {
				commitPS2.Add(1)
				return nil
			}, error(nil))
	}

	ctx := t.Context()
	orch, err := conductor.Make(
		ctx, ec, log, db,
		[]conductor.StatelessProcessor{p1, p2},
		[]conductor.StatefulProcessor{ps1, ps2},
		nil,
	)
	require.NoError(t, err)

	requireVersion(t, 0, orch)

	v, err := orch.Append(ctx, log, &EventTest{Foo: "foo", Bar: 42})
	require.NoError(t, err)
	require.Equal(t, int64(1), v)
	requireVersion(t, 1, orch)

	require.Equal(t, int32(1), commitPS1.Load())
	require.Equal(t, int32(1), commitPS2.Load())
}

// TestStatefulProcessorsFailCommit tests commit failure on
// one of the stateful processors.
func TestStatefulProcessorsFailCommit(t *testing.T) {
	log, db, ec := setup(t)

	ps1 := &MockStatefulProcessor{}
	ps2 := &MockStatefulProcessor{}
	defer ps1.AssertExpectations(t)
	defer ps2.AssertExpectations(t)

	var commitPS1 atomic.Int32
	var commitPS2 atomic.Int32

	errExpectCommitFailed := errors.New("commit failed for unknown reasons")

	{
		ps1.requireCallApply(0, &EventTest{Foo: "foo", Bar: 42}).
			Once().
			Return(func(context.Context) error {
				commitPS1.Add(1)
				return errExpectCommitFailed
			}, error(nil))

		ps2.requireCallApply(0, &EventTest{Foo: "foo", Bar: 42}).
			Once().
			Return(func(context.Context) error {
				commitPS2.Add(1)
				return nil
			}, error(nil))
	}

	ctx := t.Context()
	orch, err := conductor.Make(
		ctx, ec, log, db,
		nil,
		[]conductor.StatefulProcessor{ps1, ps2},
		nil,
	)
	require.NoError(t, err)

	requireVersion(t, 0, orch)

	v, err := orch.Append(ctx, log, &EventTest{Foo: "foo", Bar: 42})
	require.NoError(t, err, "expect no error even though commit failed")
	require.Equal(t, int64(1), v)
	requireVersion(t, 1, orch)

	require.Equal(t, int32(1), commitPS1.Load())
	require.Equal(t, int32(1), commitPS2.Load())
}

// TestStatefulProcessorsAutoResync tests automatic resync when stateful processor
// returns ErrOutOfSync.
func TestStatefulProcessorsAutoResync(t *testing.T) {
	log, db, ec := setup(t)

	p := &MockStatefulProcessor{}
	defer p.AssertExpectations(t)

	var commitP atomic.Int32

	{
		c1 := p.requireCallApply(2, &EventTest{Foo: "foo", Bar: 42}).
			Return(func(context.Context) error {
				commitP.Add(1)
				return nil
			}, conductor.ErrOutOfSync).
			Once()

		c2 := p.requireCallVersion().
			Return(int64(0), error(nil)).
			Once().
			NotBefore(c1)

		c3 := p.requireCallApply(0, &EventTest{Foo: "pre1", Bar: 1}).
			Return(func(context.Context) error {
				commitP.Add(1)
				return nil
			}, error(nil)).
			Once().
			NotBefore(c2)

		c4 := p.requireCallVersion().
			Return(int64(1), error(nil)).
			Once().
			NotBefore(c3)

		c5 := p.requireCallApply(1, &EventTest{Foo: "pre2", Bar: 2}).
			Return(func(context.Context) error {
				commitP.Add(1)
				return nil
			}, error(nil)).
			Once().
			NotBefore(c4)

		c6 := p.requireCallVersion().
			Return(int64(2), error(nil)).
			Once().
			NotBefore(c5)

		p.requireCallApply(2, &EventTest{Foo: "foo", Bar: 42}).
			Return(func(context.Context) error {
				commitP.Add(1)
				return nil
			}, error(nil)).
			Once().
			NotBefore(c6)
	}

	ctx := t.Context()
	orch, err := conductor.Make(
		ctx, ec, log, db,
		nil,
		[]conductor.StatefulProcessor{p},
		nil,
	)
	require.NoError(t, err)

	// Simulate 2 existing events.
	addEventsToDB(t, db, ec,
		&EventTest{Foo: "pre1", Bar: 1},
		&EventTest{Foo: "pre2", Bar: 2})

	requireVersion(t, 2, orch)

	require.Zero(t, commitP.Load())

	// SyncAppend should apply pre1 and pre2 first and then apply foo.
	v, err := orch.SyncAppend(ctx, log, &EventTest{Foo: "foo", Bar: 42})
	require.NoError(t, err, "expect no error even though commit failed")
	require.Equal(t, int64(3), v)
	requireVersion(t, 3, orch)

	require.Equal(t, int32(3), commitP.Load())
}

// TestSync tests the Sync method.
func TestSync(t *testing.T) {
	log, db, ec := setup(t)

	p := &MockStatelessProcessor{}
	ps := &MockStatefulProcessor{}
	r := &MockReactor{projectionID: 1}
	defer p.AssertExpectations(t)
	defer ps.AssertExpectations(t)
	defer r.AssertExpectations(t)

	var commitP atomic.Int32

	{
		c1 := ps.requireCallVersion().
			Return(int64(0), error(nil)).
			Once()

		c2 := ps.requireCallApply(0, &EventTest{Foo: "pre1", Bar: 1}).
			Return(func(context.Context) error {
				commitP.Add(1)
				return nil
			}, error(nil)).
			Once().
			NotBefore(c1)

		c3 := ps.requireCallVersion().
			Return(int64(1), error(nil)).
			Once().
			NotBefore(c2)

		c4 := ps.requireCallApply(1, &EventTest{Foo: "pre2", Bar: 2}).
			Return(func(context.Context) error {
				commitP.Add(1)
				return nil
			}, error(nil)).
			Once().
			NotBefore(c3)

		ps.requireCallVersion().
			Return(int64(2), error(nil)).
			Once().
			NotBefore(c4)
	}
	{
		rc1 := r.requireCallReact(1, &EventTest{Foo: "pre1", Bar: 1}).
			Return(error(nil)).
			Once()
		r.requireCallReact(2, &EventTest{Foo: "pre2", Bar: 2}).
			Return(error(nil)).
			Once().
			NotBefore(rc1)
	}

	ctx := t.Context()
	orch, err := conductor.Make(
		ctx, ec, log, db,
		[]conductor.StatelessProcessor{p},
		[]conductor.StatefulProcessor{ps},
		[]conductor.Reactor{r},
	)
	require.NoError(t, err)

	// Simulate 2 existing events.
	addEventsToDB(t, db, ec,
		&EventTest{Foo: "pre1", Bar: 1},
		&EventTest{Foo: "pre2", Bar: 2})

	requireVersion(t, 2, orch)
	require.Zero(t, commitP.Load())

	err = orch.Sync(ctx, log)
	require.NoError(t, err)

	require.Equal(t, int32(2), commitP.Load())
}

// TestReactorsNotification tests the pgxdb.Listener implementation.
func TestReactorsNotification(t *testing.T) {
	log, db, ec := setup(t)

	reactorFirst := &MockReactor{projectionID: 1}
	reactorSecond := &MockReactor{projectionID: 2}
	defer reactorFirst.AssertExpectations(t)
	defer reactorSecond.AssertExpectations(t)

	var wg sync.WaitGroup
	wg.Add(2)

	{
		first1 := reactorFirst.requireCallReact(
			1, &EventTest{Foo: "foo", Bar: 42}).
			Once().
			Return(error(nil))

		second1 := reactorSecond.requireCallReact(
			1, &EventTest{Foo: "foo", Bar: 42}).
			Once().
			Return(error(nil))

		reactorFirst.requireCallReact(
			2, &EventTest{Foo: "foo2", Bar: 242}).
			Return(error(nil)).
			Once().
			NotBefore(first1).
			Run(func(mock.Arguments) { wg.Done() })

		reactorSecond.requireCallReact(
			2, &EventTest{Foo: "foo2", Bar: 242}).
			Return(error(nil)).
			Once().
			NotBefore(second1).
			Run(func(mock.Arguments) { wg.Done() })
	}

	ctx := t.Context()
	orch, err := conductor.Make(
		ctx, ec, log, db,
		nil,
		nil,
		[]conductor.Reactor{reactorFirst, reactorSecond},
	)
	require.NoError(t, err)

	go func() {
		err := orch.Listen(ctx, log, nil /* Disable polling */, 1024)
		if !errors.Is(err, context.Canceled) {
			panic(fmt.Errorf("expected context canceled err, received: %w", err))
		}
	}()

	requireVersion(t, 0, orch)

	vPub, err := orch.Append(ctx, log, &EventTest{Foo: "foo", Bar: 42})
	require.NoError(t, err)
	require.Equal(t, int64(1), vPub)

	requireVersion(t, 1, orch)

	vPub2, err := orch.Append(ctx, log, &EventTest{Foo: "foo2", Bar: 242})
	require.NoError(t, err)
	require.Equal(t, int64(2), vPub2)

	requireVersion(t, 2, orch)

	wg.Wait()
}

// TestReactorsPolling tests database polling.
func TestReactorsPolling(t *testing.T) {
	log, db, ec := setup(t)

	reactorFirst := &MockReactor{projectionID: 1}
	reactorSecond := &MockReactor{projectionID: 2}
	defer reactorFirst.AssertExpectations(t)
	defer reactorSecond.AssertExpectations(t)

	var wg sync.WaitGroup
	wg.Add(2)

	{
		first1 := reactorFirst.requireCallReact(
			1, &EventTest{Foo: "foo", Bar: 42}).
			Once().
			Return(error(nil))

		second1 := reactorSecond.requireCallReact(
			1, &EventTest{Foo: "foo", Bar: 42}).
			Once().
			Return(error(nil))

		reactorFirst.requireCallReact(
			2, &EventTest{Foo: "foo2", Bar: 242}).
			Return(error(nil)).
			Once().
			NotBefore(first1).
			Run(func(mock.Arguments) { wg.Done() })

		reactorSecond.requireCallReact(
			2, &EventTest{Foo: "foo2", Bar: 242}).
			Return(error(nil)).
			Once().
			NotBefore(second1).
			Run(func(mock.Arguments) { wg.Done() })
	}

	addEventsToDB(t, db, ec,
		&EventTest{Foo: "foo", Bar: 42},
		&EventTest{Foo: "foo2", Bar: 242})

	ctx := t.Context()
	orch, err := conductor.Make(
		ctx, ec, log, db,
		nil,
		nil,
		[]conductor.Reactor{reactorFirst, reactorSecond},
	)
	require.NoError(t, err)
	requireVersion(t, 2, orch)

	poll := make(chan time.Time, 1)
	go func() {
		err := orch.Listen(ctx, log, &MockPoller{Ch: poll}, 1024)
		if !errors.Is(err, context.Canceled) {
			panic(fmt.Errorf("expected context canceled err, received: %w", err))
		}
	}()
	poll <- time.Now() // Trigger polling.

	wg.Wait()
}

func TestEventIterator(t *testing.T) {
	log, db, ec := setup(t)

	ctx := t.Context()
	orch, err := conductor.Make(ctx, ec, log, db, nil, nil, nil)
	require.NoError(t, err)
	requireVersion(t, 0, orch)

	for _, e := range []conductor.Event{
		&EventTest{Foo: "first", Bar: 1},
		&EventTest{Foo: "second", Bar: 2},
		&EventTest{Foo: "third", Bar: 3},
		&EventTest{Foo: "fourth", Bar: 4},
		&EventTest{Foo: "fifth", Bar: 5},
		&EventTest{Foo: "sixth", Bar: 6},
		&EventTest{Foo: "seventh", Bar: 7},
	} {
		_, err := orch.Append(ctx, log, e)
		require.NoError(t, err)
	}

	requireVersion(t, 7, orch)

	{ // zero buffer size yields one
		i := orch.NewEventsIterator(0)
		seq, err := i.Next(ctx, 0)
		require.NoError(t, err)
		requireEventsInSeq(t, seq)
	}

	{ // 1-7
		i := orch.NewEventsIterator(0)
		seq, err := i.Next(ctx, 2)
		require.NoError(t, err)
		requireEventsInSeq(t, seq,
			expEv{Version: 1, E: &EventTest{Foo: "first", Bar: 1}},
			expEv{Version: 2, E: &EventTest{Foo: "second", Bar: 2}})

		seq, err = i.Next(ctx, 4)
		require.NoError(t, err)
		requireEventsInSeq(t, seq,
			expEv{Version: 3, E: &EventTest{Foo: "third", Bar: 3}},
			expEv{Version: 4, E: &EventTest{Foo: "fourth", Bar: 4}},
			expEv{Version: 5, E: &EventTest{Foo: "fifth", Bar: 5}},
			expEv{Version: 6, E: &EventTest{Foo: "sixth", Bar: 6}})

		seq, err = i.Next(ctx, 3)
		require.NoError(t, err)
		requireEventsInSeq(t, seq,
			expEv{Version: 7, E: &EventTest{Foo: "seventh", Bar: 7}})

		seq, err = i.Next(ctx, 1)
		require.ErrorIs(t, err, conductor.ErrNoMoreEvents)
		require.Nil(t, seq)
	}

	{ // 1-1 (single event)
		i := orch.NewEventsIterator(1)
		seq, err := i.Next(ctx, 1)
		require.NoError(t, err)
		requireEventsInSeq(t, seq,
			expEv{Version: 1, E: &EventTest{Foo: "first", Bar: 1}})

		seq, err = i.Next(ctx, 1)
		require.NoError(t, err)
		requireEventsInSeq(t, seq,
			expEv{Version: 2, E: &EventTest{Foo: "second", Bar: 2}})

		// Reset
		i.Reset(0)
		seq, err = i.Next(ctx, 1)
		require.NoError(t, err)
		requireEventsInSeq(t, seq,
			expEv{Version: 1, E: &EventTest{Foo: "first", Bar: 1}})

	}

	{ // 7-7 (single last event)
		i := orch.NewEventsIterator(-1)
		seq, err := i.Next(ctx, 1)
		require.NoError(t, err)
		requireEventsInSeq(t, seq,
			expEv{Version: 7, E: &EventTest{Foo: "seventh", Bar: 7}})
	}

	{ // 3-4
		i := orch.NewEventsIterator(3)
		seq, err := i.Next(ctx, 2)
		require.NoError(t, err)
		requireEventsInSeq(t, seq,
			expEv{Version: 3, E: &EventTest{Foo: "third", Bar: 3}},
			expEv{Version: 4, E: &EventTest{Foo: "fourth", Bar: 4}})
	}

	{ // Start at inexistent version.
		requireVersion(t, 7, orch)
		i := orch.NewEventsIterator(7 + 123)
		seq, err := i.Next(ctx, 1)
		require.ErrorIs(t, err, conductor.ErrNoMoreEvents)
		require.Nil(t, seq)
	}

	{ // 7-1 (iterate in reverse order)
		requireVersion(t, 7, orch)
		i := orch.NewEventsIterator(7)
		seq, err := i.Next(ctx, -10)
		require.NoError(t, err)
		requireEventsInSeq(t, seq,
			expEv{Version: 7, E: &EventTest{Foo: "seventh", Bar: 7}},
			expEv{Version: 6, E: &EventTest{Foo: "sixth", Bar: 6}},
			expEv{Version: 5, E: &EventTest{Foo: "fifth", Bar: 5}},
			expEv{Version: 4, E: &EventTest{Foo: "fourth", Bar: 4}},
			expEv{Version: 3, E: &EventTest{Foo: "third", Bar: 3}},
			expEv{Version: 2, E: &EventTest{Foo: "second", Bar: 2}},
			expEv{Version: 1, E: &EventTest{Foo: "first", Bar: 1}})

		seq, err = i.Next(ctx, -10)
		require.ErrorIs(t, err, conductor.ErrNoMoreEvents)
		require.Nil(t, seq)
	}

	{ // back and forth.
		i := orch.NewEventsIterator(-1)
		seq, err := i.Next(ctx, -3)
		require.NoError(t, err)
		requireEventsInSeq(t, seq,
			expEv{Version: 7, E: &EventTest{Foo: "seventh", Bar: 7}},
			expEv{Version: 6, E: &EventTest{Foo: "sixth", Bar: 6}},
			expEv{Version: 5, E: &EventTest{Foo: "fifth", Bar: 5}})

		seq, err = i.Next(ctx, 3)
		require.NoError(t, err)
		requireEventsInSeq(t, seq,
			expEv{Version: 4, E: &EventTest{Foo: "fourth", Bar: 4}},
			expEv{Version: 5, E: &EventTest{Foo: "fifth", Bar: 5}},
			expEv{Version: 6, E: &EventTest{Foo: "sixth", Bar: 6}})
	}

	{ // 4-3 (iterate in reverse order)
		i := orch.NewEventsIterator(4)
		seq, err := i.Next(ctx, -2)
		require.NoError(t, err)
		requireEventsInSeq(t, seq,
			expEv{Version: 4, E: &EventTest{Foo: "fourth", Bar: 4}},
			expEv{Version: 3, E: &EventTest{Foo: "third", Bar: 3}})
	}
}

type expEv struct {
	Version int64
	E       *EventTest
}

func requireEventsInSeq(
	t *testing.T, seq iter.Seq2[int64, conductor.Event], expect ...expEv,
) {
	t.Helper()
	var actual []expEv
	for version, e := range seq {
		actual = append(actual, expEv{Version: version, E: e.(*EventTest)})
	}

	require.Len(t, actual, len(expect), "event count mismatch")
	for i, exp := range expect {
		act := actual[i]
		require.Equal(t, exp.Version, act.Version,
			"event version mismatch at index %d", i)
		require.Equal(t, exp.E.Foo, act.E.Foo,
			"event value mismatch at index %d", i)
		require.Equal(t, exp.E.Bar, act.E.Bar,
			"event value mismatch at index %d", i)
		require.NotZero(t, act.E.Time())
		require.Equal(t, "event-test", act.E.Name())
		require.Equal(t, "test-revision", act.E.RevisionVCS())
	}
}

// TestSyncAppend simulates OCC version mismatch err and ensures SyncAppend auto-retries.
func TestSyncAppend(t *testing.T) {
	log, d, ec := setup(t)
	ctx := t.Context()

	ps := &MockStatelessProcessor{}
	defer ps.AssertExpectations(t)

	appendStartedC := make(chan struct{}, 1)
	appendStartedC2 := make(chan struct{}, 1)
	blockC := make(chan struct{}, 1)
	blockC2 := make(chan struct{}, 1)

	{
		// Attempt that will fail.
		ps1 := ps.requireCallProcess(&EventTest{Foo: "will-be-dropped", Bar: 1}).
			Run(func(mock.Arguments) { appendStartedC <- struct{}{}; <-blockC }).
			Once().
			Return(error(nil))

		// First attempt
		ps2 := ps.requireCallProcess(&EventTest{Foo: "third", Bar: 3}).
			Return(error(nil)).
			Run(func(mock.Arguments) {
				appendStartedC2 <- struct{}{}
				<-blockC2
			}).
			Once().
			NotBefore(ps1)

		// Final attempt that will suceed.
		ps.requireCallProcess(&EventTest{Foo: "third", Bar: 3}).
			Return(error(nil)).
			Once().
			NotBefore(ps2)
	}

	orch, err := conductor.Make(ctx, ec, log, d,
		[]conductor.StatelessProcessor{ps}, nil, nil)
	require.NoError(t, err)

	requireVersion(t, 0, orch)

	var g errgroup.Group
	g.Go(func() error {
		// Append will block until blockC is written to.
		// This event won't be appended.
		_, err := orch.Append(ctx, log, &EventTest{Foo: "will-be-dropped", Bar: 1})
		return err
	})

	g.Go(func() error {
		<-appendStartedC // Wait until append actually started.

		// Manually append a new event while the first Append is blocked.
		err := d.TxRW(ctx, func(ctx context.Context, tx db.TxRW) error {
			_, err := tx.AppendEvent(ctx, 0, db.Event{
				Version:     1,
				Payload:     `{"foo":"first","bar":1}`,
				TypeName:    "event-test",
				Time:        time.Now(),
				RevisionVCS: "test-revision",
			})
			return err
		})
		if err != nil {
			return err
		}

		// Unblock processor to continue Append and hit OCC version mismatch.
		blockC <- struct{}{}

		return nil
	})

	require.ErrorIs(t, g.Wait(), conductor.ErrOutOfSync)
	requireVersion(t, 1, orch)

	g = errgroup.Group{}

	g.Go(func() error {
		// This time, use SyncAppend instead.
		_, err := orch.SyncAppend(ctx, log, &EventTest{Foo: "third", Bar: 3})
		return err
	})

	g.Go(func() error {
		<-appendStartedC2 // Wait until append actually started.

		// Manually append a new event while the first Append is blocked.
		err := d.TxRW(ctx, func(ctx context.Context, tx db.TxRW) error {
			_, err := tx.AppendEvent(ctx, 1, db.Event{
				Version:     2,
				Payload:     `{"foo":"second","bar":2}`,
				TypeName:    "event-test",
				Time:        time.Now(),
				RevisionVCS: "test-revision",
			})
			return err
		})
		if err != nil {
			return err
		}

		// Unblock processor to continue Append and hit OCC version mismatch.
		blockC2 <- struct{}{}

		return nil
	})

	// This time there is no error because SyncAppend automatically resynced and retried.
	require.NoError(t, g.Wait())

	requireVersion(t, 5, orch)
}

type FakeStatefulProcessor struct {
	CommitCounter sync.WaitGroup
	Events        []*EventTest
}

var _ conductor.StatefulProcessor = new(FakeStatefulProcessor)

func (p *FakeStatefulProcessor) Backoff() (
	min, max time.Duration, factor, jitter float64,
) {
	min, max = 50*time.Millisecond, 500*time.Millisecond
	factor, jitter = 2.0, 0
	return
}

func (p *FakeStatefulProcessor) Version(ctx context.Context) (int64, error) {
	return int64(len(p.Events)), nil
}

func (p *FakeStatefulProcessor) Apply(
	ctx context.Context, assumedVersion int64, e conductor.Event,
) (commit func(ctx context.Context) error, err error) {
	ev, ok := e.(*EventTest)
	if !ok {
		return nil, fmt.Errorf("unexpected event type: %#v", e)
	}
	if assumedVersion != int64(len(p.Events)) {
		return nil, conductor.ErrOutOfSync
	}
	p.Events = append(p.Events, ev)
	return func(ctx context.Context) error { p.CommitCounter.Done(); return nil }, nil
}

func TestAppendCluster(t *testing.T) {
	log, db, ec := setup(t)
	ctx := t.Context()

	psA := &FakeStatefulProcessor{}
	psB := &FakeStatefulProcessor{}

	oA, err := conductor.Make(
		ctx, ec, log, db,
		nil,
		[]conductor.StatefulProcessor{psA},
		nil,
	)
	require.NoError(t, err)

	oB, err := conductor.Make(
		ctx, ec, log, db,
		nil,
		[]conductor.StatefulProcessor{psB},
		nil,
	)
	require.NoError(t, err)

	requireVersion(t, 0, oA)
	requireVersion(t, 0, oB)

	// oA will append the first and second events and update psA.
	psA.CommitCounter.Add(2)
	newVersion, err := oA.Append(ctx, log, &EventTest{Foo: "first", Bar: 1})
	require.NoError(t, err)
	require.Equal(t, int64(1), newVersion)
	requireVersion(t, 1, oA)

	newVersion, err = oA.Append(ctx, log, &EventTest{Foo: "second", Bar: 2})
	require.NoError(t, err)
	require.Equal(t, int64(2), newVersion)
	requireVersion(t, 2, oA)

	psA.CommitCounter.Wait()
	require.Len(t, psA.Events, 2)
	require.Equal(t, "first", psA.Events[0].Foo)
	require.Equal(t, "second", psA.Events[1].Foo)

	// Try append third event on oB, but oB is out of sync and needs to resync first.
	psB.CommitCounter.Add(3)

	// Append on B now should automatically trigger a resync on the processor.
	newVersion, err = oB.Append(ctx, log, &EventTest{Foo: "third", Bar: 3})
	require.NoError(t, err)
	require.Equal(t, int64(3), newVersion)
	requireVersion(t, 3, oB)

	psB.CommitCounter.Wait()
	require.Len(t, psB.Events, 3)
	require.Equal(t, "first", psB.Events[0].Foo)
	require.Equal(t, "second", psB.Events[1].Foo)
	require.Equal(t, "third", psB.Events[2].Foo)

	// A isn't aware of the first two events yet but will catch up through its listener.
	require.Len(t, psA.Events, 2)
	require.Equal(t, "first", psA.Events[0].Foo)
	require.Equal(t, "second", psA.Events[1].Foo)

	poll := make(chan time.Time, 1)
	go func() {
		err := oA.Listen(ctx, log, &MockPoller{Ch: poll}, 1024)
		if !errors.Is(err, context.Canceled) {
			panic(fmt.Errorf("expected context canceled err, received: %w", err))
		}
	}()

	psA.CommitCounter.Add(1)
	poll <- time.Now()
	psA.CommitCounter.Wait()

	require.Len(t, psA.Events, 3)
	require.Equal(t, "first", psA.Events[0].Foo)
	require.Equal(t, "second", psA.Events[1].Foo)
	require.Equal(t, "third", psA.Events[2].Foo)
}

func requireVersion(t *testing.T, expect int64, o *conductor.Conductor) {
	t.Helper()
	v, err := o.Version(context.Background())
	require.NoError(t, err)
	require.Equal(t, expect, v)
}

func addEventsToDB(
	t *testing.T, db *dbpgx.DB, ec *conductor.EventCodec, e ...*EventTest,
) {
	t.Helper()
	tm := time.Date(2025, 1, 2, 3, 4, 5, 6, time.UTC)
	for _, e := range e {
		jsonPayload, err := ec.EncodeJSON(e)
		require.NoError(t, err)
		_, err = db.Exec(t.Context(), `
			INSERT INTO system.events (type, payload, vcs_revision, time)
			VALUES ($1, $2, $3, $4)
		`, "test-event", jsonPayload, "test-previous-revision", tm)
		require.NoError(t, err)
	}
}
