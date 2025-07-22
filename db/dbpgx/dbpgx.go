// Package dbpgx implements the Conductor's database interface with a PostgreSQL
// over a jackc/pgx/v5 SQL driver based implementation.
package dbpgx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/romshark/conductor/db"
	"github.com/romshark/conductor/internal/backoff"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var defaultBackoff backoff.Backoff

func DefaultBackoff() backoff.Backoff { return defaultBackoff }

func init() {
	var err error
	defaultBackoff, err = backoff.New(100*time.Millisecond, 2*time.Second, 2, .1, nil)
	if err != nil {
		panic(fmt.Errorf("init default backoff: %w", err))
	}
}

// DB is a pgx connection pool that implements the Conductor's DB interface.
type DB struct {
	log  *slog.Logger
	pool *pgxpool.Pool
}

var (
	_ db.TxRW       = new(Tx)
	_ db.TxReadOnly = new(Tx)
	_ db.DB         = new(DB)
)

// Open connects to the database using pgx. It will ping and retry until either
// a successful connection is established or ctx is canceled.
func Open(
	ctx context.Context, log *slog.Logger, dsn string, maxConns int32,
	backoffConf backoff.Backoff,
) (*DB, error) {
	if maxConns < 1 {
		maxConns = int32(runtime.NumCPU())
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid DSN: %w", err)
	}
	cfg.MaxConns = int32(maxConns)

	var pool *pgxpool.Pool
	for i, dur := range backoff.NewAtomic(backoffConf).Iter() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		time.Sleep(dur) // First is always 0.

		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("connecting database timed out: %w", err)
		}

		p, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("creating pgx pool with config: %w", err)
		}

		ctxPing, cancel := context.WithTimeout(ctx, 1*time.Second)
		err = p.Ping(ctxPing)
		cancel()
		if err != nil {
			slog.Error("pinging database",
				slog.Any("err", err),
				slog.Int("attempt", i))
			p.Close()
			continue
		}

		pool = p
		break
	}

	return &DB{log: log, pool: pool}, nil
}

type Tx struct {
	lock sync.Mutex
	tx   pgx.Tx
}

func (t *Tx) ReadEventAtVersion(
	ctx context.Context, version int64,
) (e db.Event, err error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	var revisionVCS *string
	err = t.tx.QueryRow(ctx, `
		SELECT payload, type, time, vcs_revision FROM system.events WHERE version=$1
    `, version).Scan(&e.Payload, &e.TypeName, &e.Time, &revisionVCS)
	if err != nil {
		return db.Event{}, fmt.Errorf("querying event at version: %w", err)
	}
	if revisionVCS != nil {
		e.RevisionVCS = *revisionVCS
	}
	e.Version = version
	return e, nil
}

func (t *Tx) ReadEventAfterVersion(
	ctx context.Context, afterVersion int64,
) (e db.Event, err error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	var revisionVCS *string
	var nextVersion int64
	err = t.tx.QueryRow(ctx, `
		SELECT version, payload, type, time, vcs_revision
		FROM system.events
		WHERE version>$1
		LIMIT 1
    `, afterVersion).Scan(&nextVersion, &e.Payload, &e.TypeName, &e.Time, &revisionVCS)
	if err != nil {
		return db.Event{}, fmt.Errorf("querying event after version: %w", err)
	}
	if revisionVCS != nil {
		e.RevisionVCS = *revisionVCS
	}
	e.Version = nextVersion
	return e, nil
}

func (t *Tx) ReadEvents(
	ctx context.Context, atVersion int64, reverse bool, buffer []db.Event,
) (read int, err error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	if len(buffer) < 1 {
		return 0, nil
	}
	query := `
		SELECT version, type, payload, time, vcs_revision
		FROM system.events
		WHERE version >= $1
		ORDER BY version ASC
		LIMIT $2
	`
	if reverse {
		query = `
			SELECT version, type, payload, time, vcs_revision
			FROM system.events
			WHERE version <= $1
			ORDER BY version DESC
			LIMIT $2
		`
	}

	rows, err := t.tx.Query(ctx, query, atVersion, len(buffer))
	if err != nil {
		return 0, fmt.Errorf("querying batch: %w", err)
	}
	defer rows.Close()
	bufferIndex := 0
	for rows.Next() {
		var (
			v           int64
			typeName    string
			payload     []byte
			tm          time.Time
			revisionVCS *string
		)
		if err := rows.Scan(&v, &typeName, &payload, &tm, &revisionVCS); err != nil {
			return 0, fmt.Errorf("scanning row: %w", err)
		}
		buffer[bufferIndex] = db.Event{
			Payload:  string(payload),
			TypeName: typeName,
			Version:  v,
			Time:     tm,
		}
		if revisionVCS != nil {
			buffer[bufferIndex].RevisionVCS = *revisionVCS
		}
		bufferIndex++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("rows error: %w", err)
	}
	return bufferIndex, nil
}

func (t *Tx) ReadSystemVersion(ctx context.Context) (version int64, err error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	err = t.tx.QueryRow(ctx, `
		SELECT COALESCE(MAX(version), 0) FROM system.events
	`).Scan(&version)
	if err != nil {
		return 0, err
	}
	return version, err
}

func (t *Tx) ReadProjectionVersion(
	ctx context.Context, id int32,
) (version int64, err error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	err = t.tx.QueryRow(ctx, `
		SELECT version from system.projection_versions WHERE id=$1
	`, id).Scan(&version)
	return version, err
}

func (t *Tx) InitProjectionVersion(
	ctx context.Context, id int32,
) (version int64, err error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	_, err = t.tx.Exec(ctx, `
		INSERT INTO system.projection_versions (id, version) VALUES ($1, 0)
		ON CONFLICT (id) DO NOTHING
	`, id)
	if err != nil {
		return 0, fmt.Errorf("creating projection_versions row for %d: %w",
			id, err)
	}
	err = t.tx.QueryRow(ctx,
		`SELECT version FROM system.projection_versions WHERE id = $1`,
		id,
	).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("retrieving version for projection %d: %w", id, err)
	}
	return version, nil
}

func (t *Tx) SetProjectionVersion(ctx context.Context, id int32, version int64) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	_, err := t.tx.Exec(ctx, `
		UPDATE system.projection_versions SET version=$1 WHERE id=$2
	`, version, id)
	return err
}

func (t *Tx) AppendEvent(
	ctx context.Context,
	assumeVersion int64,
	event db.Event,
) (version int64, err error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// This CTE first reads the current max(version) and then only
	// does the INSERT if it matches assumeVersion.
	const sql = `
		WITH
			current AS (
				SELECT COALESCE(MAX(version), 0) AS v
				FROM system.events
			),
			insert AS (
				INSERT INTO system.events (type, payload, vcs_revision, time)
				SELECT $2, $3, $4, $5
				FROM current
				WHERE current.v = $1
				RETURNING version
			)
		SELECT version FROM insert;
	`
	err = t.tx.QueryRow(ctx, sql,
		assumeVersion, event.TypeName, event.Payload, event.RevisionVCS, event.Time,
	).Scan(&version)
	if err != nil {
		// Treat both CTE mismatch and serialization failures as version conflicts.
		if errors.Is(err, pgx.ErrNoRows) || isSerializationFailure(err) {
			return 0, db.ErrVersionMismatch
		}
		return 0, err
	}
	return version, nil
}

func isSerializationFailure(err error) bool {
	pgErr, ok := err.(*pgconn.PgError)
	if !ok {
		return false
	}
	return pgErr.Code == "40001"
}

func (t *Tx) Exec(
	ctx context.Context, sql string, args ...any,
) (pgconn.CommandTag, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.tx.Exec(ctx, sql, args...)
}

func (t *Tx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.tx.Query(ctx, sql, args...)
}

func (t *Tx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.tx.QueryRow(ctx, sql, args...)
}

func (d DB) ListenEventInserted(
	ctx context.Context, onReady func(), onEventInserted func(version int64) error,
) error {
	// Dedicated connection from the pool.
	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection from pool: %w", err)
	}
	defer conn.Release() // give back to pool

	// LISTEN event_inserted
	if _, err = conn.Exec(ctx, `LISTEN "event_inserted"`); err != nil {
		return fmt.Errorf("executing listen: %w", err)
	}

	onReady()

	for {
		n, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		version, err := strconv.ParseInt(n.Payload, 10, 64)
		if err != nil {
			return fmt.Errorf("bad payload in notification on event_inserted: %q",
				n.Payload)
		}
		if err := onEventInserted(version); err != nil {
			return err
		}
	}
}

// TxRW starts a new read-write transaction and executes fn inside of it.
// If fn returns an error or panic occurs, the transaction is rolled back,
// otherwise it is committed.
func (d DB) TxRW(
	ctx context.Context, fn func(context.Context, db.TxRW) error,
) error {
	return d.withTx(ctx, pgx.ReadWrite, func(ctx context.Context, tx *Tx) error {
		return fn(ctx, tx)
	})
}

// TxReadOnly starts a new read-only transaction and executes fn inside.
func (d DB) TxReadOnly(
	ctx context.Context, fn func(context.Context, db.TxReadOnly) error,
) error {
	return d.withTx(ctx, pgx.ReadWrite, func(ctx context.Context, tx *Tx) error {
		return fn(ctx, tx)
	})
}

func (d DB) withTx(
	ctx context.Context, mode pgx.TxAccessMode, fn func(context.Context, *Tx) error,
) (err error) {
	tx, err := d.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.Serializable,
		AccessMode: mode,
	})
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			if rb := tx.Rollback(ctx); rb != nil {
				d.log.Error("rollback after panic failure",
					slog.Any("panic", p),
					slog.Any("err", rb))
			}
			panic(p)
		}
	}()

	if err := fn(ctx, &Tx{tx: tx}); err != nil {
		if rb := tx.Rollback(ctx); rb != nil {
			return fmt.Errorf("rolling back transaction: %v (original: %w)", rb, err)
		}
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

func (d DB) Exec(
	ctx context.Context, sql string, args ...any,
) (pgconn.CommandTag, error) {
	return d.pool.Exec(ctx, sql, args...)
}

func (d DB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return d.pool.QueryRow(ctx, sql, args...)
}

func (d DB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return d.pool.Query(ctx, sql, args...)
}

func (d DB) Close() {
	d.pool.Close()
}
