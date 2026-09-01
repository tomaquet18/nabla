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

## v0.1 scope: two accepted shapes

One base table per view, plain column names, no quoted identifiers.

1. **Projection**: `SELECT col[, col ...] FROM table [WHERE predicate]`. Every
   column is a bare column name (optionally `AS alias`). The base table's
   primary key columns must be in the select list.
2. **Aggregate**: `SELECT groupcol[, ...], count(*) [AS a][, sum(col) [AS b]]...
   FROM table [WHERE predicate] GROUP BY groupcol[, ...]`. Group columns first,
   then aggregates; the `GROUP BY` list must equal the group columns. The base
   table needs `REPLICA IDENTITY FULL`.

Anything else (joins, subqueries, other aggregates, `DISTINCT`, `ORDER BY`,
`LIMIT`, `HAVING`, expressions, functions) fails with
`ERROR: nabla: unsupported view definition: <reason>` and a hint listing the
two shapes. `TRUNCATE` of a base table marks its views stale.

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
`nabla.retain_deltas` (100000), `nabla.max_slot_lag_bytes` (1 GiB).

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
- Parse-tree analysis: definitions are matched with regular expressions and
  validated by preparing the statement. Quoted identifiers and SQL comments are
  not supported.
- `sum()` over a group whose only remaining rows have NULL values yields 0
  rather than NULL (no per-column non-null counter yet).
- A streaming transport for subscribers; `changes()` is pull-based.
- Snapshot export at `create_view` so subscribers can start without a
  `SHARE`-locked rebuild.
- Incremental `TRUNCATE`; unchanged TOAST values for columns a view needs
  (a view goes stale instead of being silently wrong).
- A failing apply (for example a type cast error) is retried every poll and
  logged as a warning; it does not yet mark the affected view stale.
- Only one worker and one database per cluster (`nabla.database`).
