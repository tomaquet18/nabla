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
   maintenance, not even while a view is being created or rebuilt: the
   starting snapshot comes from a logical-decoding consistent point, not
   from a lock.
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

Every source transaction is applied to each view in its own subtransaction
of the worker's round transaction. If applying a source transaction to one
view raises (for example `100 / k` meeting `k = 0`), only that view's
subtransaction is rolled back; the other views absorb the transaction and
advance. The failing view keeps the frontier of the last transaction it
absorbed (healthy transactions earlier in the same round are kept);
`apply_failures` is incremented once per round and `last_error` /
`last_error_at` record the PostgreSQL error. The round stops after that
transaction and the slot is advanced to just before it, so the view is
retried on the next poll while healthy views skip the transaction through
their frontier.
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

SELECT nabla.create_view('paid_orders',
  'SELECT id, k, amount FROM orders WHERE status = ''paid''');
SELECT nabla.create_view('orders_by_k',
  'SELECT k, count(*) AS n, sum(amount) AS total FROM orders WHERE status = ''paid'' GROUP BY k');

INSERT INTO orders (k, amount, status) VALUES (1, 10, 'paid');

-- Block until the view reflects everything committed so far (default 5 s).
SELECT nabla.await_ready('orders_by_k');
SELECT nabla.wait_for('orders_by_k', pg_current_wal_lsn());
SELECT * FROM orders_by_k;

-- Subscribe: read the table, take the cursor, then follow deltas.
SELECT nabla.current_seq('orders_by_k');                  -- e.g. 42
LISTEN "nabla:public.orders_by_k";
SELECT * FROM nabla.changes('orders_by_k', 42, 1);        -- rows with seq > 42, epoch 1
```

SQL API (schema `nabla`):

- `create_view(name, definition) -> text` validates the definition, records
  the view with status `initializing` and returns its canonical name at
  once; the worker builds the table asynchronously. `refresh(name)` marks
  the view (and, for join views, every view sharing a shadow with it)
  `refreshing` and returns; until the rebuilt content is committed the view
  is frozen at its old epoch and frontier, readers keep seeing the complete
  old content (never an empty or half-built table), old-epoch cursors keep
  working, and afterwards the epoch is one higher (`NB003` for old cursors).
  `drop_view(name)` works in any status.
- `await_ready(name, timeout_ms default 60000) -> bool`: waits until the
  view is `live` (true), raises `NB006` if the build failed (DETAIL carries
  PostgreSQL's error; the catalog row stays with status `failed` and
  `last_error`, no table is left behind), or returns false on timeout.
- Status vocabulary (`status()`): `initializing`, `refreshing`, `live`,
  `stale`, `failed`.
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
- `visible_columns(name) -> text[]`: the definition's output columns (the
  columns of the VIEW); `storage_table(name) -> regclass`: the storage table
  behind it, with the hidden `_nabla_*` columns.
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
| `NB006` | the build or rebuild of a view failed | `nabla: view "<name>" failed to build` / PostgreSQL's error / fix the cause, then `nabla.refresh('<name>')` or `nabla.drop_view('<name>')` |

`changes()` checks stale, then epoch, then retention, so a refresh is always
reported as `NB003`, never as `NB001`.

Changelog: the v0.1 `changes()` signature changed (added `epoch` and
`include_hidden`, `lsn`/`op` are text, the `epoch` column was dropped);
`create_view` returns the canonical name; `status()` gained `name` and
`frontier`; `wait_for` gained a text overload; `nabla.views.resync_seq` was
removed.

View names are resolved like relation names: unqualified names are created
in the search_path's creation namespace and looked up through the
search_path; quoted identifiers keep their case, unquoted ones fold to
lowercase; the canonical name (`status().name`, the LISTEN channel) is the
schema-qualified form quoted only where needed. The user object is a plain
VIEW exposing the definition's columns over a storage table
`nabla_store.v<id>` (`nabla.storage_table(name)`), which also carries the
hidden `_nabla_*` maintenance columns, the unique index and the write
guard; DML through the VIEW is rewritten onto the storage table and
rejected (`nabla: cannot modify a nabla view directly`, NB005). Renaming or
moving the VIEW with ALTER VIEW is not supported (maintenance continues, the
channel name does not follow).

GUCs: `nabla.database`, `nabla.poll_interval_ms` (100),
`nabla.batch_changes` (5000), `nabla.retain_deltas` (100000),
`nabla.max_slot_lag_bytes` (1 GiB), `nabla.max_apply_failures` (3).

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
  (transitively), all from one consistent snapshot in one transaction:
  refresh cascades to views sharing a shadow.
- A shadow that cannot be maintained (schema drift) is flagged in
  `nabla.shadows.stale_reason`, its dependent views go stale, and
  `refresh` rebuilds it. Shadows are not dumped by pg_dump; refresh rebuilds
  them after a restore.

Not yet: outer joins, self-joins, column pruning of views, shadow column
pruning.

## Schema changes on base tables

Every column is addressed by name from the logical decoding stream, so DDL
that does not touch a column a view uses is tolerated silently: adding,
dropping or renaming unused columns, and updates that change only unused
columns (no delta is produced). Shadows hold only the primary key plus the
columns the views sharing them use (`nabla.shadows.columns`); a new view
that needs more columns extends the shadow in place, backfilled from the
base table under the population snapshot (columns are never removed when a
view is dropped).

Dropping, renaming or changing the type of a column a view uses marks
exactly the views that use it `stale` with the reason
`column "x" of <table> was dropped, renamed or changed type`; a shadow
drops that column from its active set and keeps serving the other views.
`refresh` re-validates the stored definition against the current schema:
if it still resolves (a type change), the view and its shadows are rebuilt
with the new types and recover; if not (a dropped or renamed column),
`await_ready` raises `NB006` with PostgreSQL's error and the view stays
`failed` until it is dropped or the schema is restored.

View tables and shadow tables depend on their base tables in `pg_depend`,
like SQL views: `DROP TABLE base` fails listing the dependent nabla tables
unless `CASCADE`, which drops them. An event trigger on `sql_drop` keeps the
catalog consistent whenever a view table, a shadow table or a base table is
dropped (directly, by CASCADE, or with `DROP SCHEMA ... CASCADE`): catalog
rows go, shadow references are released, orphaned shadows are dropped and
tables nobody needs leave the publication. Renaming a base table or moving
it to another schema keeps maintenance running (oids do not change);
`refresh` fails with `NB006` if the stored definition no longer resolves.

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
   `status` is `initializing` or `refreshing`, retry shortly (or call
   `nabla.await_ready`); if it is `stale`, wait with backoff and bootstrap
   again (`stale_reason` says why); if it is `failed`, stop and report the
   recorded error.
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

What a delta is: the rows of one source transaction are its net effect per
identity key (the group keys of an aggregate view, the primary key of a
projection). Per key the batch holds at most a `D` carrying the row as it
was before the transaction, then an `I` carrying the row after it; a key
whose visible columns did not change, or that was created and removed
within the transaction, is silent. Intermediate states the maintenance
passed through (a group counted 1 then 2 then 3) never appear. Changes that
only touch hidden `_nabla_*` maintenance columns produce no delta, so the
hidden values seen with `include_hidden = true` are best-effort. A
transaction with an empty net effect appends nothing and sends no
notification for that view, but still advances the view's frontier.

Events a client should surface: `Snapshot { epoch, frontier, cursor, rows }`,
`Transaction { xid, lsn, epoch, deltas[] }` and `Resync { reason }` where the
reason is one of lagged, epoch changed, stale (with the reason) or
disconnected. Keep buffers bounded: one batch plus the trailing transaction.

## Performance

Writers are never blocked: the worker reads the WAL through a logical slot
and applies changes to the storage tables on its own. What matters is how
fast it applies them, because everything a subscriber sees is behind the
worker's frontier.

### What the worker does per round

One round, every `nabla.poll_interval_ms` (or immediately while there is a
backlog):

1. peek up to `nabla.batch_changes` decoded changes from the slot; PostgreSQL
   never splits a transaction across that limit;
2. in one worker transaction: for every complete source transaction, in
   commit order, plan and execute the row changes against every live view
   (each view in its own subtransaction, so a failure rolls back that view
   alone), maintain the shadow tables of join views, and net the deltas per
   source transaction; then, once per view, update the frontier, append the
   round's deltas with one multi-row `INSERT`, garbage-collect and send one
   notification carrying the last `seq:lsn`;
3. commit and advance the slot once, to the end of the last applied
   transaction.

Hot statements (view upserts, shadow lookups and writes) run through kept
SPI plans, so they are parsed and planned once per view and change kind for
the life of the worker.

### Why one transaction per round is correct

nabla offers deferred transactional consistency: a reader sees the view as
it was after some committed source transaction, never a state that did not
exist. Committing once per round only means readers skip the intermediate
committed states of that round; the frontier still names the exact source
LSN the view reflects, delta batches still map one-to-one to source
transactions (netted per transaction, in commit order), and a subscriber
following `changes()` sees the same sequence it would have seen with one
commit per transaction. The trade is latency inside a round against
throughput: one WAL flush, one slot advance and one catalog update per
round instead of per transaction.

### Measured

`scripts/dev.sh bench` (`scripts/bench.sh`) builds a throwaway cluster with
200k orders, 1000 customers and 500 products, creates two three-table join
views (`revenue_by_region`, an aggregate; `paid_orders`, a projection), runs
pgbench single-row `INSERT` transactions against a control table and against
`orders`, then measures the worker's drain rate and the time to drain a 20k
single-row backlog. On a laptop under Docker Desktop for Windows:

| | before (one transaction and one slot advance per source transaction) | after (one per round, kept plans) |
|---|---|---|
| writer throughput retained, 1 / 4 / 16 pgbench clients | 108% / 87% / 110% | 95% / 76% / 86% (other runs: 101-113%; the host was shared and the spread between runs is about 20 points either way) |
| worker drain rate, source transactions per second | 22 | 1333 |
| 20k single-row backlog | not drained in 120 s (2729 applied) | drained in 15.0 s |
| per round of 1667 single-row transactions | - | peek and decode 10-70 ms, apply 1.1-1.3 s, slot advance 5-50 ms |

The step-by-step gains on the same script: one worker transaction and one
slot advance per round took the drain rate from 22 to 1000 tx/s; kept SPI
plans took it to 1300-1500 tx/s. Writing a running-transactions record after
each round (to move the slot's restart point sooner) changed nothing
measurable and was dropped. The remaining cost is the apply phase itself:
about 0.7 ms per source transaction for two three-table join views, spent in
the delta query against the shadow tables, the storage upsert and the
subtransaction around each view.

### The honest limit

The worker is one process applying one round at a time. A sustained write
rate above its apply rate accumulates lag: the frontier falls behind, the
slot retains WAL, and `nabla.max_slot_lag_bytes` eventually marks every view
stale and drops the slot. That cap is a safety valve against filling the
disk, not a target: size the workload (or the views) so the worker keeps up,
and watch `nabla.status()` for a frontier that keeps drifting away from
`pg_current_wal_lsn()`.

A long-running transaction slows the worker down in a second way: while it
holds its snapshot, the old versions of the storage rows the worker keeps
updating (an aggregate view's group rows above all) cannot be pruned, and
every upsert walks a longer chain. `nabla.wait_for()` with a long timeout is
such a transaction. Wait with LISTEN or with short, repeated calls rather
than one call that spans a whole backlog; the benchmark script does exactly
that after having measured the cost of its own observer.

Knobs: `nabla.batch_changes` (changes per peek, default 5000; larger rounds
amortise better but hold more work in one transaction and lengthen the delay
before the first notification of a backlog), `nabla.poll_interval_ms`
(latency floor when idle; a backlog is drained without sleeping),
`nabla.retain_deltas` (how far a slow subscriber may fall behind).

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
replication slot if needed (before any write, as PostgreSQL requires), adds
the base tables to the `nabla` publication, records the view with status
`initializing` and returns. The worker then builds the table under a
consistent snapshot (`src/populate.rs`): it creates a temporary logical slot,
lets PostgreSQL find a consistent point (waiting for transactions that were
already running to complete, never blocking them), installs the slot's
initial snapshot as its REPEATABLE READ transaction snapshot, runs
`CREATE TABLE <name> AS <definition>`, adds the unique index and the write
guard, and sets `frontier_lsn` to the consistent point; the main slot is at
or below that point, so everything committed later is decoded normally.

The worker loop (`src/worker.rs`), every `nabla.poll_interval_ms`:

1. Skip unless the extension is installed and the slot exists. If the slot lags
   more than `nabla.max_slot_lag_bytes`, mark views stale and drop it.
2. Peek `pg_logical_slot_peek_binary_changes` up to the current flush LSN and
   decode the `pgoutput` stream (`src/pgoutput.rs`) into complete transactions.
3. In one worker transaction per round: set `nabla.internal_write`, then for
   each complete source transaction of the peek, in commit order, apply each
   row change to every live view of that table (`src/apply.rs`: SQL over SPI
   with the decoded row bound as typed parameters, through kept plans) and
   net the view's deltas. Once per view per round: advance `frontier_lsn` to
   the end LSN of the last absorbed transaction, append the deltas, notify,
   and garbage-collect deltas beyond retention. Commit, then advance the
   slot once, in a separate transaction. Transactions at or below a view's
   frontier are skipped, so a crash between the two steps replays safely.
4. When the peek is drained, advance every live view's frontier to the flush
   LSN read before the peek: the view reflects all commits up to that point.

## Not yet

- Outer joins, self-joins, subqueries, other aggregates (`avg`, `min`,
  `max`, ...), `HAVING`, `DISTINCT`.
- Single-group aggregates without `GROUP BY` (`SELECT count(*) FROM t`) and
  `min`/`max`/`avg`.
- A streaming transport for subscribers; `changes()` is pull-based
  (see `clients/rust/nabla-client` for the pull protocol).
- Incremental `TRUNCATE`; unchanged TOAST values for columns a view needs
  (a view goes stale instead of being silently wrong).
- Only one worker and one database per cluster (`nabla.database`).

## License

nabla is licensed per component:

| Component | Path | License |
|---|---|---|
| PostgreSQL extension | `src/`, `sql/`, `nabla.control` | [AGPL-3.0-or-later](LICENSE) |
| Reference client and subscription protocol | `clients/` | [MIT](clients/rust/nabla-client/LICENSE-MIT) OR [Apache-2.0](clients/rust/nabla-client/LICENSE-APACHE) |
| Delta engine crate (once split out of the extension) | — | Apache-2.0 |

Applications talk to nabla over SQL and the subscription protocol; the client
libraries are permissive so that embedding them never affects an application's
own license. Every source file carries an `SPDX-License-Identifier` header.

Contributions require a signed [Contributor License Agreement](CLA.md); see
[CONTRIBUTING.md](CONTRIBUTING.md).
