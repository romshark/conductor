package conductor

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/romshark/conductor/backoff"
	"github.com/romshark/conductor/db"
	"golang.org/x/sync/errgroup"
)

var (
	ErrProjectionIDCollision = errors.New("projection identifier must be globally unique")
	ErrSyncInProgress        = errors.New("sync already in progress")
	ErrAlreadyListening      = errors.New("already listening")
	ErrNoNextVersion         = errors.New("no next version")
	ErrNoMoreEvents          = errors.New("no more events")
	ErrOutOfSync             = errors.New("projection is out of sync")
	ErrNothingToListenTo     = errors.New(
		"database doesn't satisfy Listener interface and polling is disabled",
	)
)

type BackoffConfigReader interface {
	// Backoff provides the backoff configuration.
	// min must be greater 0 and max.
	// factor must be greater 1.0.
	// jitter is a ratio in [0.0, 1.0].
	//
	// Backoff must be idempotent, meaning it always returns the same value.
	Backoff() (min, max time.Duration, factor, jitter float64)
}

// StatelessProcessor is an event processor that's strongly consistent with the event log
// and is able to enforce invariants, which means it can prevent an event from being
// appended. The state of this processor (if any) lives in the event log database and
// the processor is therefore considered stateless since it's always in sync with the log.
// StatelessProcessor must not manage any state living outside the event log database,
// including in-memory state. Implement ProcessorStateful if any external state is
// required.
type StatelessProcessor interface {
	BackoffConfigReader

	// Process applies event onto the projection.
	// If Process returns an error, the transaction is rolled back and
	// event appending is rejected.
	Process(ctx context.Context, event Event, tx db.TxRW) error
}

// StatefulProcessor is similar to StatelessProcessor but manages state outside the
// event log database. Stateful processors may manage in-memory projections as well as
// projections living in other databases. In a cluster setup reads on the state of
// a stateful processor is eventually consistent.
type StatefulProcessor interface {
	BackoffConfigReader

	// Apply applies event onto the projection.
	// If Apply returns an error, the transaction is rolled back and
	// event appending is rejected.
	// Apply must return ErrOutOfSync if assumedVersion isn't the current
	// projection version of the processor.
	// The returned commit function must reliably commit the applied changes
	// (two-phase commit).
	// If commit is never called the changes must never be committed.
	// No changes to the state of the processor should be visible until commit is called.
	Apply(
		ctx context.Context, assumedVersion int64, event Event,
	) (commit func(context.Context) error, err error)

	// Version returns the current version of the processor's state.
	Version(ctx context.Context) (version int64, err error)
}

// Reactor is an eventually consistent projector invoked after an event was appended.
// Unlike Processor a Reactor can't enforce invariants,
// which means it can't prevent an event from being appended, like a Processor does.
// The reactor is not strongly consistent with the event log and relies on
// an at-least-once delivery guarantee.
// In case of a multi instance cluster setup the Reactor may require its own
// explicit synchronization mechanism to avoid duplicate side effects.
type Reactor interface {
	BackoffConfigReader

	// ProjectionID returns a globally unique identifier of the projection.
	// This identifier is associated with a version in the database.
	//
	// ProjectionID must be idempotent, meaning it always returns the same value.
	ProjectionID() int32

	// React is expected to react the event, produce side-effects (if any) and return nil.
	// If React returns an error its version is not updated and React is called
	// again, until it returns nil. React is always called by a new goroutine.
	// The conductor can't guarantee exactly-once delivery, instead it guarantees
	// at-least-once, meaning that if React managed to execute successfully and
	// returned nil but the system crashed before it could update the reactor version
	// then React will be called at least twice or more times for this version.
	React(ctx context.Context, version int64, e Event, tx db.TxReadOnly) error
}

type reactor struct {
	lock    sync.Mutex
	backoff *backoff.Atomic
	Reactor
}

type statelessProcessors struct {
	backoff *backoff.Atomic
	StatelessProcessor
}

type statefulProcessor struct {
	backoff *backoff.Atomic
	StatefulProcessor
}

func newAtomicBackoff(
	r BackoffConfigReader, randSeed1, randSeed2 uint64,
) (*backoff.Atomic, error) {
	rnd := rand.New(rand.NewPCG(randSeed1, randSeed2))

	min, max, factor, jitter := r.Backoff()
	conf, err := backoff.New(min, max, factor, jitter, rnd)
	if err != nil {
		return nil, err
	}
	return backoff.NewAtomic(conf), nil
}

// Conductor is the core backbone of the event sourced system responsible for
// synchronizing projections, invoking reactors and appending events onto the
// immutable event log.
// Create an instance of the conductor using Make and run the dispatcher using
// Listen in a new goroutine.
type Conductor struct {
	db                  db.DB
	eventCodec          *EventCodec
	listenLock          sync.Mutex
	syncLock            sync.Mutex
	versionLock         sync.RWMutex
	version             int64
	processorsStateless []*statelessProcessors
	processorsStateful  []*statefulProcessor
	reactorsByID        map[int32]*reactor
}

// Make creates and initializes a new conductor instance.
func Make(
	ctx context.Context,
	eventCodec *EventCodec,
	log *slog.Logger,
	database db.DB,
	processorsStateless []StatelessProcessor,
	processorsStateful []StatefulProcessor,
	reactors []Reactor,
) (*Conductor, error) {
	if eventCodec == nil {
		eventCodec = DefaultEventCodec
	}
	eventCodec.inUse = true

	o := &Conductor{
		db:         database,
		eventCodec: eventCodec,
		processorsStateless: make(
			[]*statelessProcessors, len(processorsStateless),
		),
		processorsStateful: make(
			[]*statefulProcessor, len(processorsStateful),
		),
		reactorsByID: make(map[int32]*reactor, len(reactors)),
	}

	randSeed1, randSeed2 := uint64(time.Now().Unix()), rand.Uint64()
	for i, s := range processorsStateless {
		bk, err := newAtomicBackoff(s, randSeed1, randSeed2)
		if err != nil {
			return nil, fmt.Errorf("setting backoff for stateless processor %d: %w",
				i, err)
		}
		o.processorsStateless[i] = &statelessProcessors{
			backoff:            bk,
			StatelessProcessor: s,
		}
	}
	for i, s := range processorsStateful {
		bk, err := newAtomicBackoff(s, randSeed1, randSeed2)
		if err != nil {
			return nil, fmt.Errorf("setting backoff for stateful processor %d: %w",
				i, err)
		}
		o.processorsStateful[i] = &statefulProcessor{
			backoff:           bk,
			StatefulProcessor: s,
		}
	}

	for i, h := range reactors {
		id := h.ProjectionID()
		if _, ok := o.reactorsByID[id]; ok {
			// ID isn't unique.
			return nil, fmt.Errorf("%w (reactor index: %d): %d",
				ErrProjectionIDCollision, i, id)
		}

		bk, err := newAtomicBackoff(h, randSeed1, randSeed2)
		if err != nil {
			return nil, fmt.Errorf("setting backoff for reactor %d: %w", id, err)
		}
		o.reactorsByID[id] = &reactor{
			backoff: bk,
			Reactor: h,
		}
	}

	err := database.TxRW(ctx, func(ctx context.Context, tx db.TxRW) error {
		for id := range o.reactorsByID {
			v, err := initProjectionVersion(ctx, tx, id)
			if err != nil {
				return fmt.Errorf("initializing reactor projection (%d) version: %w",
					id, err)
			}
			log.Info("initialized reactor projection version",
				slog.Int("reactor.id", int(id)),
				slog.Int64("version", v))
		}
		return nil
	})
	log.Info("all projection versions initialized")
	if err != nil {
		return nil, err
	}
	return o, nil
}

func (o *Conductor) syncStatefulProcessors(
	ctx, ctxGraceful context.Context, log *slog.Logger, concurrencyLimit int,
) error {
	var g errgroup.Group
	g.SetLimit(concurrencyLimit)
	for i := range o.processorsStateful {
		g.Go(func() error {
			return o.syncStatefulProcessor(ctx, ctxGraceful, log, i)
		})
	}
	return g.Wait()
}

func (o *Conductor) getCachedVersion() int64 {
	o.versionLock.RLock()
	defer o.versionLock.RUnlock()
	return o.version
}

func (o *Conductor) updateCachedVersion(
	ctx context.Context, tx db.TxReadOnly,
) (systemVersion int64, err error) {
	o.versionLock.Lock()
	defer o.versionLock.Unlock()
	if tx == nil {
		err = o.db.TxReadOnly(ctx, func(ctx context.Context, tx db.TxReadOnly) error {
			systemVersion, err = tx.ReadSystemVersion(ctx)
			return err
		})
	} else {
		systemVersion, err = tx.ReadSystemVersion(ctx)
	}
	if err != nil {
		return 0, err
	}
	o.version = systemVersion
	return systemVersion, nil
}

func (o *Conductor) syncStatefulProcessor(
	ctx, ctxGraceful context.Context, log *slog.Logger, index int,
) error {
	p := o.processorsStateful[index]
	for {
		if err := ctxGraceful.Err(); err != nil {
			return err
		}

		if d := p.backoff.Duration(); d > 0 {
			log.Info("backing off for stateful processor retry",
				slog.Int("processor", index),
				slog.String("backoff", d.String()))
			if err := sleepContext(ctx, d); err != nil {
				return err
			}
		}

		v, err := p.Version(ctx)
		if err != nil {
			return fmt.Errorf("reading stateful processor projection version: %w",
				err)
		}

		if v >= o.getCachedVersion() {
			p.backoff.Reset()
			return nil // Projection is up to date.
		}

		event, err := o.queryNextEvent(ctx, v)
		if err != nil {
			return fmt.Errorf("querying event %d: %w", v, err)
		}
		commit, err := p.Apply(ctx, v, event) // Assume the version is the same still.
		if err != nil {
			if errors.Is(err, ErrOutOfSync) {
				continue
			}
			return err
		}
		if err := commit(ctx); err != nil {
			return err
		}
		// Reset backoff after a successful sync.
		p.backoff.Reset()
	}
}

func (o *Conductor) syncReactor(
	ctx, ctxGraceful context.Context, log *slog.Logger, r *reactor,
) error {
	for {
		if err := ctxGraceful.Err(); err != nil {
			return err
		}
		_, _, err := o.syncReactorToNextVersion(ctx, log, r)
		if err != nil {
			if errors.Is(err, ErrNoNextVersion) {
				break
			}
			return err
		}
	}
	return nil
}

// Poller periodically triggers a event store polling.
type Poller interface {
	Stop()
	C() <-chan time.Time
}

// A TimedPoller is a Poller with reset capabilities.
type TimedPoller interface {
	Poller
	Reset()
}

// TickingPoller uses a time.Ticker to trigger polling regularly.
type TickingPoller struct {
	ticker   *time.Ticker
	interval time.Duration
}

var (
	_ Poller      = new(TickingPoller)
	_ TimedPoller = new(TickingPoller)
)

func NewTickingPoller(interval time.Duration) *TickingPoller {
	if interval == 0 {
		panic("don't use ticking poller with zero interval")
	}
	return &TickingPoller{ticker: time.NewTicker(interval), interval: interval}
}

func (t *TickingPoller) Stop()               { t.ticker.Stop() }
func (t *TickingPoller) Reset()              { t.ticker.Reset(t.interval) }
func (t *TickingPoller) C() <-chan time.Time { return t.ticker.C }

// Listen runs the conductor dispatcher that listens for new events and triggers
// synchronization.
//
// Canceling ctx will stop both the listener and any potentially ongoing
// synchronization, potentially causing I/O errors to be logged because the underlying
// database transactions are canceled before Sync finishes. Canceling ctxListen
// will gracefuly wait for any ongoing synchronizations to finish and then exits the
// listener loop.
//
// Listen will periodically poll the database for new events if pollingInterval > 0,
// otherwise polling is disabled. Parameter queueBufferLen specifies the listener queue
// buffer size. If the listener goroutine can't keep up with the notifications then those
// will be queued in that buffer. If the buffer is full the notification will be dropped
// and the state of the orchesrator will have to synchronized otherwise
// (polling update, on append or manually)
// If the conductor's database doesn't satisfy the Listener interface and
// poller == nil then ErrNothingToListenTo is returned.
// If poller satisfies TimedPoller then poller.Reset is called to prevent the timed
// poller from triggering prematurely.
//
// onListening is called once the listener is listening.
func (o *Conductor) Listen(
	ctx, ctxGraceful context.Context, log *slog.Logger,
	poller Poller, queueBufferLen int, onListening func(),
) error {
	if !o.listenLock.TryLock() {
		return ErrAlreadyListening
	}
	defer o.listenLock.Unlock()

	var eventInserted chan int64
	eventInsertedErr := make(chan error, 1)
	if listener, ok := o.db.(db.Listener); ok {
		eventInserted = make(chan int64, queueBufferLen)
		go func() {
			err := listener.ListenEventInserted(ctxGraceful,
				onListening,
				func(version int64) error {
					select {
					case eventInserted <- version:
					default:
						log.Warn("listener buffer overflow, "+
							"event insertion notifications may be dropped",
							slog.Int("len", len(eventInserted)))
					}
					return nil
				})
			if err != nil && !errors.Is(err, context.Canceled) {
				close(eventInserted)
				eventInsertedErr <- err
			}
		}()
	} else if poller == nil {
		return ErrNothingToListenTo
	}

	defer func() {
		if poller != nil {
			poller.Stop()
		}
	}()

	var pollerC <-chan time.Time
	if poller != nil {
		pollerC = poller.C()
	}

	for {
		select {
		case <-ctx.Done(): // Hard stop.
			return ctx.Err()

		case <-ctxGraceful.Done(): // Gracefuly stop dispatcher.
			return ctxGraceful.Err()

		case <-pollerC: // Poll database for new events.
			if poller != nil {
				poller.Stop()
			}
			log.Debug("dispatcher polling database")
			if err := o.Sync(ctx, ctxGraceful, log); err != nil {
				if errors.Is(err, ErrSyncInProgress) {
					continue
				}
				return err
			}
			if tp, ok := poller.(TimedPoller); ok {
				tp.Reset()
			}

		case version, ok := <-eventInserted: // Database notified about event insertion.
			if !ok {
				select {
				case err := <-eventInsertedErr:
					return err
				default:
					return nil
				}
			}
			if poller != nil {
				poller.Stop()
			}
			log.Debug("resync after notification", slog.Int64("version", version))
			if err := o.Sync(ctx, ctxGraceful, log); err != nil {
				if errors.Is(err, ErrSyncInProgress) {
					continue
				}
				return err
			}
			if tp, ok := poller.(TimedPoller); ok {
				tp.Reset()
			}
		}
	}
}

// Sync synchronizes the conductor projection against the database and invokes
// reactors if necessary.
// Returns ErrSyncInProgress if another sync is currently in flight.
func (o *Conductor) Sync(
	ctx, ctxGraceful context.Context, log *slog.Logger,
) error {
	if !o.syncLock.TryLock() {
		return ErrSyncInProgress
	}
	defer o.syncLock.Unlock()

	if _, err := o.updateCachedVersion(ctx, nil); err != nil {
		return err
	}

	err := o.syncStatefulProcessors(ctx, ctxGraceful, log, len(o.processorsStateful))
	if err != nil {
		return fmt.Errorf("synchronizing stateful processors: %w", err)
	}
	for id, r := range o.reactorsByID {
		if err := o.syncReactor(ctx, ctxGraceful, log, r); err != nil {
			return fmt.Errorf("synchronizing reactor %d: %w", id, err)
		}
	}
	return nil
}

func (o *Conductor) syncReactorToNextVersion(
	ctx context.Context, log *slog.Logger, r *reactor,
) (oldVersion, newVersion int64, err error) {
	id := r.ProjectionID()
	if !r.lock.TryLock() {
		return 0, 0, ErrSyncInProgress
	}
	defer r.lock.Unlock()

	if d := r.backoff.Duration(); d > 0 {
		log.Info("backing off for retry", slog.String("backoff", d.String()))
		if err := sleepContext(ctx, d); err != nil {
			return oldVersion, newVersion, err
		}
	}

	err = o.db.TxRW(ctx, func(ctx context.Context, tx db.TxRW) error {
		oldVersion, err = queryProjectionVersion(ctx, tx, id)
		if err != nil {
			return err
		}

		if oldVersion >= o.getCachedVersion() {
			r.backoff.Reset()
			return ErrNoNextVersion
		}

		// Reactor requires update.
		ev, err := o.queryNextEvent(ctx, oldVersion)
		if err != nil {
			return fmt.Errorf("querying event: %w", err)
		}
		newVersion = ev.Version()

		if err = r.React(ctx, newVersion, ev, tx); err != nil {
			return fmt.Errorf("invoking reactor: %w", err)
		}

		// Reset backoff counter after successful reactor execution.
		r.backoff.Reset()

		if err := tx.SetProjectionVersion(ctx, id, newVersion); err != nil {
			return fmt.Errorf("updating projection (%d) version: %w", id, err)
		}

		return nil
	})
	return oldVersion, newVersion, err
}

func (o *Conductor) queryNextEvent(
	ctx context.Context, afterVersion int64,
) (event Event, err error) {
	var d db.Event
	err = o.db.TxReadOnly(ctx, func(ctx context.Context, tx db.TxReadOnly) error {
		var err error
		d, err = tx.ReadEventAfterVersion(ctx, afterVersion)
		return err
	})
	if err != nil {
		return nil, err
	}
	ev, err := o.eventCodec.DecodeJSON(d.TypeName, []byte(d.Payload))
	if err != nil {
		return nil, fmt.Errorf("unmarshaling payload json: %w", err)
	}
	m := ev.metadata()
	m.t, m.revisionVCS, m.version = d.Time, d.RevisionVCS, d.Version
	return ev, nil
}

func (o *Conductor) appendEvent(
	ctx context.Context, tx db.Writer, assumedVersion int64, e Event,
) (newVersion int64, err error) {
	jsonPayload, err := o.eventCodec.EncodeJSON(e)
	if err != nil {
		return 0, fmt.Errorf("marshaling event json: %w", err)
	}
	// Events aren't expected to have zero time or zero name at this point.
	// initializeEvent should have set it.
	m := e.metadata()
	switch {
	case m.t.IsZero():
		panic(fmt.Errorf("event has zero time: %#v", e))
	case m.name == "":
		panic(fmt.Sprintf("event has no type name: %#v", e))
	}
	newVersion, err = tx.AppendEvent(ctx, assumedVersion, db.Event{
		TypeName:    m.name,
		Payload:     jsonPayload,
		Time:        m.t,
		RevisionVCS: o.eventCodec.revisionVCS,
	})
	if err != nil {
		if errors.Is(err, db.ErrVersionMismatch) {
			// Avoid the wrapping in case of a regular expected error to
			// reduce memory allocations.
			return 0, err
		}
		return 0, fmt.Errorf("appending event: %w", err)
	}
	o.versionLock.Lock()
	defer o.versionLock.Unlock()
	o.version = newVersion
	return newVersion, nil
}

// VersionCached returns the cached version of the system
// (version of latest event from the cache of this conductor instance).
func (o *Conductor) VersionCached() (version int64) { return o.getCachedVersion() }

// Version returns the current version of the system (version of latest event).
func (o *Conductor) Version(ctx context.Context) (version int64, err error) {
	return o.updateCachedVersion(ctx, nil)
}

// SyncAppend is similar to Append but will automatically resync processors and retry
// if Append returns ErrOutOfSync.
// If ctx is canceled an appropriate error will be returned and newVersion will be the
// last synchronized version.
// SyncAppend doesn't resync reactors, this is the dispatcher's job.
func (o *Conductor) SyncAppend(
	ctx context.Context, log *slog.Logger, e Event,
) (newVersion int64, err error) {
	for {
		newVersion, err = o.Append(ctx, log, e)
		if err == nil {
			break
		}
		if !errors.Is(err, ErrOutOfSync) {
			return newVersion, err
		}
		// Retry.
	}
	return newVersion, nil
}

// Append starts a new read-write database transaction, calls all stateless processors'
// Process method,  calls all processors' Apply method and irreversibly appends e onto
// the system event log if all synchronizations are successful and the current projection
// is up to date. If any Apply call returns an error, the transaction is rolled back,
// the event isn't appended and the error is returned.
// If a stateful processor is out of sync it's automatically synchronized to the latest
// version of the system.
func (o *Conductor) Append(
	ctx context.Context, log *slog.Logger, e Event,
) (newVersion int64, err error) {
	o.syncLock.Lock()
	defer o.syncLock.Unlock()

	if err := o.eventCodec.initializeEvent(e, time.Now); err != nil {
		return 0, err
	}
	commitFns := make([]func(context.Context) error, len(o.processorsStateful))
	err = o.db.TxRW(ctx, func(ctx context.Context, tx db.TxRW) error {
		for {
			v, err := o.updateCachedVersion(ctx, tx)
			if err != nil {
				return err
			}

			for _, p := range o.processorsStateless {
				if err := p.Process(ctx, e, tx); err != nil {
					return err
				}
			}

			var g errgroup.Group
			g.SetLimit(len(o.processorsStateful))
			for i, p := range o.processorsStateful {
				g.Go(func() error {
					for {
						commit, err := p.Apply(ctx, v, e)
						if errors.Is(err, ErrOutOfSync) {
							// Resync projection.
							err = o.syncStatefulProcessor(
								ctx, context.Background(), log, i,
							)
							if err != nil {
								return err
							}
							continue
						}
						if err != nil {
							return err
						}
						commitFns[i] = commit
						break
					}

					// At this point, if the system fails then some of the stateful
					// processors may never receive their commit() call for this
					// newVersion. In that case their state will lag behind the event log,
					// but they will eventually synchronize through update notifications
					// and OCC (optimistic concurrency control). Since this is a
					// two-phase commit, no partial writes ever leak through.
					return nil
				})
			}
			if err := g.Wait(); err != nil {
				return err
			}

			// Projections are updated. Append e to the immutable event log.
			newVersion, err = o.appendEvent(ctx, tx, v, e)
			if err != nil {
				// The eventlog advanced in the meantime. Retry.
				if errors.Is(err, db.ErrVersionMismatch) {
					return ErrOutOfSync
				}
				return fmt.Errorf("appending event: %w", err)
			}
			return nil
		}
	})
	if err != nil {
		return 0, err
	}

	for i, fn := range commitFns {
		if err := fn(ctx); err != nil {
			// Don't return the error, consider the append successful.
			// The projection can later be synced even though two-phase commit failed.
			log.Error("committing stateful processor",
				slog.Int("processor", i),
				slog.Any("err", err))
		}
	}

	// TODO: optimize update notification delivery by immediately notifying the listener
	// about a new event appended without waiting for the db notification or poll.

	return newVersion, nil
}

func queryProjectionVersion(
	ctx context.Context, tx db.Reader, id int32,
) (int64, error) {
	version, err := tx.ReadProjectionVersion(ctx, id)
	if err != nil {
		return 0, fmt.Errorf("querying projection version: %w", err)
	}
	return version, err
}

func initProjectionVersion(
	ctx context.Context, tx db.Writer, id int32,
) (version int64, err error) {
	if _, err := tx.InitProjectionVersion(ctx, id); err != nil {
		return 0, fmt.Errorf("initializing projection (%d) version: %w", id, err)
	}
	return version, nil
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case <-timer.C:
	}
	return nil
}

// NewEventsIterator creates a new iterator over a slice of the event log.
// startVersion=-1 will start iterating from the last version,
// whereas startVersion=0 will start iterating from the first version.
// Iterator slice includes the events at startVersion and endVersion.
func (o *Conductor) NewEventsIterator(
	startVersion int64,
) *EventsIterator {
	return &EventsIterator{o: o, v: startVersion}
}

// EventsIterator iterates over the event log.
type EventsIterator struct {
	o   *Conductor
	v   int64 // Current version.
	buf []db.Event
}

// Reset resets the iterator at the given start version.
func (i *EventsIterator) Reset(startVersion int64) {
	i.v = startVersion
}

// Next returns an iterator over the next n events.
// Iterates in reverse order if n is negative.
// Returns ErrNoMoreEvents if no more events are available.
func (i *EventsIterator) Next(ctx context.Context, n int) (
	seq iter.Seq2[int64, Event], err error,
) {
	if n == 0 {
		return func(yield func(int64, Event) bool) {}, nil
	}
	if i.v < 0 {
		i.v, err = i.o.updateCachedVersion(ctx, nil)
		if err != nil {
			return nil, fmt.Errorf("updating cached version: %w", err)
		}
	}

	err = i.o.db.TxReadOnly(ctx, func(ctx context.Context, tx db.TxReadOnly) error {
		limit := n
		reverse := n < 0
		if reverse {
			limit = -limit
		}

		if len(i.buf) < limit {
			i.buf = make([]db.Event, limit)
		} else {
			// Reuse buffer.
			i.buf = i.buf[:limit]
		}
		read, err := tx.ReadEvents(ctx, i.v, reverse, i.buf)
		if err != nil {
			return fmt.Errorf("querying batch: %w", err)
		}
		if read == 0 {
			return ErrNoMoreEvents
		}
		i.buf = i.buf[:read]

		seq = func(yield func(int64, Event) bool) {
			for _, e := range i.buf {
				ev, err := i.o.eventCodec.DecodeJSON(e.TypeName, []byte(e.Payload))
				if err != nil {
					panic(fmt.Errorf("decoding json: %w", err))
				}
				m := ev.metadata()
				m.t = e.Time
				m.revisionVCS = e.RevisionVCS
				if !yield(e.Version, ev) {
					break
				}
			}
		}

		if reverse {
			i.v = i.buf[len(i.buf)-1].Version - 1
		} else {
			i.v = i.buf[len(i.buf)-1].Version + 1
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	return seq, nil
}
