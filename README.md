# nabla

Incrementally maintained views for PostgreSQL that never block writers, with
real-time delta subscriptions. A PostgreSQL 17 extension written in Rust
(pgrx). Version 0.1 is a walking skeleton: small on purpose, correct on what it
accepts, explicit about what it rejects.

## Three decisions

1. **Deferred transactional consistency.** A view always equals its defining
   query evaluated at some committed snapshot of the base table, identified by
   the view's `frontier_lsn`. Deltas of one source transaction are applied
   atomically and in commit order; a reader never sees an intermediate state.
2. **Logical decoding, never triggers.** A background worker consumes a
   replication slot (`pgoutput`) through the SQL peek/advance functions.
   Nothing runs in the writer's commit path, so writers never wait for view
   maintenance. The only write pause is the brief `SHARE` lock taken while
   `create_view` or `refresh` builds the starting snapshot.
3. **Delta-row subscriptions on a bounded durable log.** Each applied
   transaction appends its view-level deltas (`I`/`D` rows as JSON) to
   `nabla.deltas` in the same transaction that updates the view, so every delta
   is exactly consistent with the frontier. `pg_notify` is only the wake-up
   signal (`nabla:<view name>`, payload `<seq>:<lsn>`).

Two safety rules keep everything bounded:

- Delta retention is capped per view (`nabla.retain_deltas`). A subscriber that
  falls behind gets `nabla: subscriber lagged behind retention` and must resync
  from the view table and `nabla.current_seq()`.
- Slot WAL retention is capped (`nabla.max_slot_lag_bytes`). If exceeded the
  worker marks every view `stale` and drops the slot rather than fill the disk;
  `nabla.refresh()` rebuilds a view and recreates the slot.

### Failure isolation

Each view is applied in its own subtransaction of the worker's transaction.
If applying a source transaction to one view raises (for example `100 / k`
meeting `k = 0`), only that view's subtransaction is rolled back; the other
views absorb the transaction and advance. The failing view keeps its previous
`frontier_lsn`; `apply_failures` is incremented and `last_error` /
`last_error_at` record the PostgreSQL error. The slot is not advanced past a
transaction that a live view has not absorbed, so the view is retried on the
next poll while healthy views skip the transaction through their frontier.
After `nabla.max_apply_failures` consecutive failures (default 3) the view is
marked `stale` with `stale_reason = 'apply failed N times: <error>'`, one
WARNING is logged, the slot advances and everything else continues. Damage is
bounded to `max_apply_failures x poll_interval`. A success on retry resets the
counter.

Observe it with
`SELECT name, status, apply_failures, last_error, last_error_at, stale_reason FROM nabla.views`.
`nabla.changes()` and `nabla.wait_for()` on a stale view raise
`nabla: view "<name>" is stale: <reason>` with a hint to run
`nabla.refresh('<name>')` after fixing the cause; `nabla.frontier()` keeps
returning the last absorbed LSN. `refresh` rebuilds the table from the current
data and clears the bookkeeping; if the definition still fails on the current
rows, refresh surfaces PostgreSQL's error and the view stays stale, which is
the correct outcome until the data or the definition is fixed.

## v0.1 scope: two accepted shapes

One base table per view. Definitions are parsed and analyzed by PostgreSQL
itself (`raw_parser` + `parse_analyze`), so quoted and mixed-case identifiers,
aliases, schema-qualified names, comments and string literals all behave
exactly as in `psql`; the shape decision is made on the analyzed query tree
(`src/definition.rs`).

1. **Projection**: `SELECT expr [AS alias][, ...] FROM table [JOIN ...] [WHERE predicate]`.
   Output expressions may be plain columns or IMMUTABLE expressions over the
   base tables' columns. For a single table its primary key columns must be
   selected as plain columns (they may be aliased); join views get hidden key
   columns instead (see "Joins and shadow tables").
2. **Aggregate**: `SELECT key [AS alias][, ...], <aggregate> [AS a][, ...]
   FROM table [WHERE predicate] GROUP BY key[, ...]`. Accepted aggregates are
   `count(*)`, `count(<expr>)` and `sum(<expr>)`, with PostgreSQL's NULL
   semantics: `count(<expr>)` counts non-NULL values and `sum(<expr>)` is NULL
   while no non-NULL value contributes. Keys and aggregate arguments may be
   IMMUTABLE expressions; every `GROUP BY` expression must be selected. The
   base table needs `REPLICA IDENTITY FULL`. nabla appends hidden columns to
   the view table: `_nabla_nn_<n> bigint` for every `sum` (its non-NULL
   counter; n is the sum's 0-based position among the aggregates) and, when
   the definition has no `count(*)`, `_nabla_n bigint` with the group count.
   They appear in the view table and in every delta row; clients should
   ignore all `_nabla_*` columns, and definitions may not use such names.

Everything else is rejected with `ERROR: nabla: unsupported view definition:
<reason>` and a hint listing the two shapes: joins, subqueries, CTEs, set
operations, window functions, set-returning functions, `DISTINCT`, `ORDER BY`,
`LIMIT`, `HAVING`, grouping sets, `FOR UPDATE`, other aggregates, `DISTINCT`/
`FILTER`/`ORDER BY` inside aggregates, expressions over aggregates, any
STABLE or VOLATILE function (`now()`, `random()`, `to_char(timestamptz, ...)`),
and base relations that are not ordinary tables (partitioned tables, views,
materialized views, foreign tables, tables with inheritance children, nabla
views and catalog tables). Syntax errors and unknown columns surface
PostgreSQL's own error text. `TRUNCATE` of a base table marks its views stale.

## Prerequisites

```
wal_level = logical
shared_preload_libraries = 'nabla'
nabla.database = 'your_database'    # the worker maintains views in one database
max_replication_slots >= 1
```

Aggregate views additionally need `ALTER TABLE <base> REPLICA IDENTITY FULL;`.

## Quickstart

```sql
CREATE EXTENSION nabla;

CREATE TABLE orders (id bigserial PRIMARY KEY, k int, amount numeric, status text);
ALTER TABLE orders REPLICA IDENTITY FULL;

SELECT nabla.create_view('public.paid_orders',
  'SELECT id, k, amount FROM orders WHERE status = ''paid''');
SELECT nabla.create_view('public.orders_by_k',
  'SELECT k, count(*) AS n, sum(amount) AS total FROM orders WHERE status = ''paid'' GROUP BY k');

INSERT INTO orders (k, amount, status) VALUES (1, 10, 'paid');

-- Block until the view reflects everything committed so far (default 5 s).
SELECT nabla.wait_for('public.orders_by_k', pg_current_wal_lsn());
SELECT * FROM orders_by_k;

-- Subscribe: read the table, take the cursor, then follow deltas.
SELECT nabla.current_seq('public.orders_by_k');           -- e.g. 42
LISTEN "nabla:public.orders_by_k";
SELECT * FROM nabla.changes('public.orders_by_k', 42);    -- rows with seq > 42
```

SQL API (schema `nabla`):

- `create_view(name, definition) -> text` returns the canonical (stored) view
  name; `drop_view(name)`; `refresh(name)`.
- `status(name) -> TABLE(name text, status text, epoch int, frontier_lsn pg_lsn,
  frontier text, current_seq bigint, stale_reason text)`: everything a
  subscriber needs, from the caller's snapshot. `LISTEN` on
  `'nabla:' || status.name`.
- `changes(name, after_seq bigint, epoch int, max_rows int default 1000,
  include_hidden bool default false) -> TABLE(seq bigint, lsn text, xid bigint,
  op text, row jsonb)`: deltas with `seq > after_seq`, `op` is `'I'` or `'D'`,
  `lsn` is the `X/Y` text form. Whole source transactions only: a result
  with fewer rows than `max_rows` is drained; a trailing transaction that
  straddles `max_rows` is returned in full, so a result may exceed
  `max_rows`. `epoch` must be the epoch from the subscriber's bootstrap.
  `_nabla_*` columns are removed from `row` unless `include_hidden`.
- `visible_columns(name) -> text[]`: the definition's output columns, for
  clients that read the view table and want to drop the `_nabla_*` columns.
- `frontier(name) -> pg_lsn`; `wait_for(name, lsn pg_lsn | text, timeout_ms
  default 5000) -> bool` (the text overload takes the form `changes()` and
  `status()` return); `current_seq(name) -> bigint`.

Conditions a client must branch on carry nabla-specific SQLSTATEs:

| SQLSTATE | condition | message / DETAIL / HINT |
|---|---|---|
| `NB001` | cursor older than retention | `nabla: subscriber lagged behind retention for view "<name>"` / `oldest retained seq is N` / resync from the view and continue from `nabla.status(...).current_seq` |
| `NB002` | view is stale | `nabla: view "<name>" is stale` / the stale reason / `run nabla.refresh('<name>') after fixing the cause` |
| `NB003` | epoch differs (the view was refreshed) | `nabla: view "<name>" epoch changed` / `epoch N -> M` / resync from the view |
| `NB004` | unsupported view definition | `nabla: unsupported view definition: <reason>` / - / the accepted shapes |
| `NB005` | direct write to a nabla-managed table | `nabla: cannot modify a nabla view directly` |

`changes()` checks stale, then epoch, then retention, so a refresh is always
reported as `NB003`, never as `NB001`.

Changelog: the v0.1 `changes()` signature changed (added `epoch` and
`include_hidden`, `lsn`/`op` are text, the `epoch` column was dropped);
`create_view` returns the canonical name; `status()` gained `name` and
`frontier`; `wait_for` gained a text overload; `nabla.views.resync_seq` was
removed.

View names must be schema-qualified. View tables reject direct DML
(`nabla: cannot modify a nabla view directly`).

GUCs: `nabla.database`, `nabla.poll_interval_ms` (100),
`nabla.retain_deltas` (100000), `nabla.max_slot_lag_bytes` (1 GiB),
`nabla.max_apply_failures` (3).

## Joins and shadow tables

A view may join 2..N ordinary tables with inner joins. Under deferred
consistency the worker applies a source transaction T after the live tables
have possibly moved on, and for V = A JOIN B the delta of a change on A is
dA JOIN B-as-of-T, which the live B is not. PostgreSQL cannot serve a snapshot
of a past LSN, so nabla keeps a **shadow copy** of every base table used by a
join view (`nabla_shadow.t<oid>`, catalog `nabla.shadows`), maintained from
the same change stream, in the same order and the same worker transaction as
the views. Invariant: after absorbing T, shadow(X) == X as of T. Join deltas
are evaluated against shadows, never against live tables; the changed table
itself enters the join as a one-row VALUES built from the decoded row.

- Cost: one full copy of each such base table (all columns in v0.1), shared
  by every join view that uses it (`refcount`).
- Every joined table needs a primary key; because old rows come from the
  shadow, join views do NOT need `REPLICA IDENTITY FULL`.
- Projection join views carry hidden `_nabla_pk<rti>_<column>` columns (one
  per primary-key column of each table, rti = the table's 1-based position
  in the FROM list) that identify a joined row; the unique index is on them.
- `create_view` on a table that already has a shadow reuses it without
  re-snapshotting: each object has its own frontier and the worker skips
  transactions at or below it, so an older shadow catches up on its own.
- `refresh` of a join view re-snapshots the view, every shadow it uses and,
  because a shared shadow at a new LSN would be wrong for a view still at an
  older one, every other join view sharing any of those shadows
  (transitively), all under SHARE locks in one transaction: refresh cascades
  to views sharing a shadow.
- A shadow that cannot be maintained (schema drift) is flagged in
  `nabla.shadows.stale_reason`, its dependent views go stale, and
  `refresh` rebuilds it. Shadows are not dumped by pg_dump; refresh rebuilds
  them after a restore.

Not yet: outer joins, self-joins, column pruning of views, shadow column
pruning.

## Subscribing from a client

The reference implementation is `clients/rust/nabla-client` (library plus
the `follow` example); clients in other languages follow the same steps.

1. Connect, read `name` from `nabla.status('<view>')` and
   `LISTEN "nabla:<name>"` (the canonical stored name, quoted as one
   identifier). Notifications are only wake-ups; their payload
   (`<seq>:<lsn>`) is advisory.
2. Bootstrap atomically: in one `REPEATABLE READ` transaction run
   `SELECT * FROM nabla.status('<view>')`, `nabla.visible_columns('<view>')`
   and `SELECT <visible columns> FROM <view>`. Keep `epoch` and
   `current_seq` (the cursor) from the same snapshot as the rows. If
   `status` is `stale`, wait with backoff and bootstrap again;
   `stale_reason` says why.
3. Follow: on every notification, and on a fallback timer (about one second)
   so a lost notification cannot stall you, call
   `nabla.changes('<view>', cursor, epoch, batch)` until it returns fewer
   rows than `batch`. Rows are contiguous in `seq`; consecutive rows with the
   same `(xid, lsn)` are one source transaction (never split by the server)
   and should be applied atomically (`D` before `I` for an update). Advance
   the cursor to the last `seq` only after the transaction was handed to the
   application.
4. Resync on SQLSTATE: `NB001` (lagged), `NB003` (the view was refreshed) and
   `NB002` (stale; wait with backoff) mean the local copy is no longer
   continuable: discard it and bootstrap again. Never branch on message
   text. On a lost connection, reconnect, `LISTEN` again and bootstrap again.
5. Read-your-writes: after committing, take `pg_current_wal_lsn()` (or its
   text form) and call `nabla.wait_for('<view>', lsn, timeout_ms)`; when it
   returns true the view and the delta log include your transaction.

Events a client should surface: `Snapshot { epoch, frontier, cursor, rows }`,
`Transaction { xid, lsn, epoch, deltas[] }` and `Resync { reason }` where the
reason is one of lagged, epoch changed, stale (with the reason) or
disconnected. Keep buffers bounded: one batch plus the trailing transaction.

## Build and test

Everything compiles and runs inside the `nabla-dev:17` Docker image
(`docker/Dockerfile.dev`: Rust 1.90, PostgreSQL 17, cargo-pgrx 0.17.0).

```
docker build -t nabla-dev:17 -f docker/Dockerfile.dev .
scripts/dev.sh build     # cargo build inside the container
scripts/dev.sh test      # install the extension, start a throwaway cluster, run tests/integration.sh
scripts/dev.sh client    # build the reference client (clients/rust/nabla-client)
scripts/dev.sh shell     # interactive shell in the container
```

`target/` and the cargo registry live in named Docker volumes.

## How it works

`create_view` parses the definition (`src/definition.rs`), creates the `nabla`
replication slot if needed (before any write, as PostgreSQL requires), adds the
base table to the `nabla` publication, takes a `SHARE` lock on the base table,
runs `CREATE TABLE <name> AS <definition>`, adds a unique index (primary key or
group columns), installs the write guard, and records `frontier_lsn =
pg_current_wal_lsn()`.

The worker loop (`src/worker.rs`), every `nabla.poll_interval_ms`:

1. Skip unless the extension is installed and the slot exists. If the slot lags
   more than `nabla.max_slot_lag_bytes`, mark views stale and drop it.
2. Peek `pg_logical_slot_peek_binary_changes` up to the current flush LSN and
   decode the `pgoutput` stream (`src/pgoutput.rs`) into complete transactions.
3. For each source transaction, in one worker transaction: set
   `nabla.internal_write`, apply each row change to every live view of that
   table (`src/apply.rs`: SQL over SPI with the decoded row bound as typed
   parameters), append deltas, advance `frontier_lsn` to the transaction's end
   LSN, notify, and garbage-collect deltas beyond retention. Commit, then
   advance the slot in a separate transaction. Transactions at or below a
   view's frontier are skipped, so a crash between the two steps replays
   safely.
4. When the peek is drained, advance every live view's frontier to the flush
   LSN read before the peek: the view reflects all commits up to that point.

## Not yet

- Outer joins, self-joins, subqueries, other aggregates (`avg`, `min`,
  `max`, ...), `HAVING`, `DISTINCT`.
- Single-group aggregates without `GROUP BY` (`SELECT count(*) FROM t`) and
  `min`/`max`/`avg`.
- A streaming transport for subscribers; `changes()` is pull-based
  (see `clients/rust/nabla-client` for the pull protocol).
- Snapshot export at `create_view` so subscribers can start without a
  `SHARE`-locked rebuild.
- Incremental `TRUNCATE`; unchanged TOAST values for columns a view needs
  (a view goes stale instead of being silently wrong).
- Only one worker and one database per cluster (`nabla.database`).
