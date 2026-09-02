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

1. **Projection**: `SELECT expr [AS alias][, ...] FROM table [WHERE predicate]`.
   Output expressions may be plain columns or IMMUTABLE expressions over the
   base table's columns. The base table's primary key columns must be selected
   as plain columns (they may be aliased).
2. **Aggregate**: `SELECT key [AS alias][, ...], count(*) [AS a][, sum(expr) [AS b]]...
   FROM table [WHERE predicate] GROUP BY key[, ...]`. Keys and sum arguments may
   be IMMUTABLE expressions; every `GROUP BY` expression must be selected. The
   base table needs `REPLICA IDENTITY FULL`. If the definition has no
   `count(*)`, nabla adds a column `_nabla_n bigint` carrying the group count;
   it is part of the view table and of every delta row.

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

SQL API (schema `nabla`): `create_view(name, definition)`, `drop_view(name)`,
`refresh(name)`, `frontier(name) -> pg_lsn`,
`wait_for(name, lsn, timeout_ms default 5000) -> bool`,
`current_seq(name) -> bigint`,
`changes(name, after_seq, max_rows default 1000) -> TABLE(seq, lsn, xid, op, row, epoch)`.

View names must be schema-qualified. View tables reject direct DML
(`nabla: cannot modify a nabla view directly`).

GUCs: `nabla.database`, `nabla.poll_interval_ms` (100),
`nabla.retain_deltas` (100000), `nabla.max_slot_lag_bytes` (1 GiB),
`nabla.max_apply_failures` (3).

## Build and test

Everything compiles and runs inside the `nabla-dev:17` Docker image
(`docker/Dockerfile.dev`: Rust 1.90, PostgreSQL 17, cargo-pgrx 0.17.0).

```
docker build -t nabla-dev:17 -f docker/Dockerfile.dev .
scripts/dev.sh build     # cargo build inside the container
scripts/dev.sh test      # install the extension, start a throwaway cluster, run tests/integration.sh
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

- Joins, multiple base tables, subqueries, expressions, other aggregates
  (`avg`, `min`, `max`, ...), `HAVING`, `DISTINCT`.
- Single-group aggregates without `GROUP BY` (`SELECT count(*) FROM t`),
  `count(expr)`, and `min`/`max`/`avg`.
- `sum()` over a group whose only remaining rows have NULL values yields 0
  rather than NULL (no per-column non-null counter yet).
- A streaming transport for subscribers; `changes()` is pull-based.
- Snapshot export at `create_view` so subscribers can start without a
  `SHARE`-locked rebuild.
- Incremental `TRUNCATE`; unchanged TOAST values for columns a view needs
  (a view goes stale instead of being silently wrong).
- Only one worker and one database per cluster (`nabla.database`).
