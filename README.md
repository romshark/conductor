<a href="https://pkg.go.dev/github.com/romshark/conductor">
    <img src="https://godoc.org/github.com/romshark/conductor?status.svg" alt="GoDoc">
</a>
<a href="https://goreportcard.com/report/github.com/romshark/conductor">
    <img src="https://goreportcard.com/badge/github.com/romshark/conductor" alt="GoReportCard">
</a>
<a href='https://coveralls.io/github/romshark/conductor?branch=main'>
    <img src='https://coveralls.io/repos/github/romshark/conductor/badge.svg?branch=main&service=github' alt='Coverage Status' />
</a>

# Conductor

Conductor is an
[event sourced architecture](https://learn.microsoft.com/en-us/azure/architecture/patterns/event-sourcing)
orchestrator that uses PostgreSQL (or any other database that satisfies the interfaces)
as its event store.
It abstracts away the complex plumbing typically required to implement event sourcing
in Go for single-instance, clustered, or distributed deployments.

## Why

I was designing a typical small to medium size CRUD application around a PostgreSQL
database with rather low write-throughput but strong consistentcy requirements,
email notifications and an audit log. Event sourcing felt like a natural fit.
Yet, I didn't want to introduce a complex and heavy [Kafka](https://kafka.apache.org/)
setup, or similar. It would only need to scale to maybe a couple instances at most.
Simplicity and reliability were my highest priority.

I created this package to abstract away most of the complex moving parts
when implementing an event sourced architecture in Go and PostgreSQL, but in a way
that doesn't prevent you from using other databases.

## Architecture

There are 3 types of middleware modules that can plug into a Conductor instance:

- **StatelessProcessor**:
  - Can enforce invariants and veto appends.
  - Can manage strongly consistent projections within the database of the event log.
  - ⚠️ Must not produce external side-effects or modify state outside the event log
    database.
- **StatefulProcessor**:
  - Can enforce invariants and veto appends.
  - Manages projections stored outside of the event log database
    (including in-memory projections) that are eventually consistent in a cluster setup.
  - Is responsible for keeping track of its projection version on its own.
  - Uses [two-phase commits](https://en.wikipedia.org/wiki/Two-phase_commit_protocol)
    to prevent partial or dangling writes.
- **Reactor**: reacts to appended events with an *at-least-once* delivery guarantee.
  - ⚠️ May require external coordination in a cluster setup.
  - Projection version is automatically tracked by Conductor.

| module             | setup     | consistency |
| :----------------- | :-------- | :---------- |
| StatefulProcessor  | 👤 single  | strong      |
| StatefulProcessor  | 👥 cluster | eventual    |
| StatelessProcessor | 👤 single  | strong      |
| StatelessProcessor | 👥 cluster | strong      |
| Reactor            | 👤 single  | eventual    |
| Reactor            | 👥 cluster | eventual    |

In a cluster setup the instances are synchronized by:

- Active subscription (in `dbpgx` it's the `LISTEN` on `event_inserted` notification)
  - ⚠️ This mechanism relies on an internal buffer. If the Conductor is too slow to
  process the buffered queue updates get lost and Conductor will rely on polling instead.
- Periodic Polling
- Sync on write (OCC)

[OCC (Optimistic Concurrency Control)](https://en.wikipedia.org/wiki/Optimistic_concurrency_control)
is utilized to keep cluster instances of the Conductor synchronized.
When Conductor instances try to append a new event they also send the
assumed current version. If the assumed version is behind the actual system version then
the projections are synchronized and the entire append procedure is retried until the
version matches on append.

## Database

The database is abstracted through interfaces and a PostgreSQL implementation is provided
in the `db/dbpgx` package. These interfaces can be satisfied with implementations based
on most transactional databases.

### `dbpgx`

`dbpgx` satisfies the database interfaces using the SQL driver
[github.com/jackc/pgx/v5](https://pkg.go.dev/github.com/jackc/pgx/v5) and is tested with
PostgreSQL 17.

To migrate a PostgreSQL database for dbpgx to use, do in the given order:

1. Run `db/dbpgx/roles.sql` to create global database roles.
2. Run `db/dbpgx/system.sql` to create the tables and notification trigger.
3. Run `db/dbpgx/permissions.sql` sets the role permissions.

You may customize roles and permissions, the defaults here are just a recommendation.
