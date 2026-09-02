# Head-to-head: no view vs pg_ivm vs nabla

This directory holds a reproducible benchmark that puts the two incremental
view maintenance engines for PostgreSQL side by side on one host, with a
baseline that has no derived view at all. The generated numbers are in
[RESULTS.md](RESULTS.md).

The point of the benchmark is the trade, not a winner. pg_ivm updates the view
inside the writing transaction; nabla updates it afterwards from the WAL. Each
choice buys something and costs something, and the tables are meant to let a
reader see both halves.

## Running it

```sh
scripts/dev.sh h2h
```

That builds `nabla-h2h:17` (the nabla development image plus pg_ivm) and runs
`run.sh` inside it. Without the helper:

```sh
docker build -t nabla-dev:17 -f docker/Dockerfile.dev docker
docker build -t nabla-h2h:17 bench/head-to-head
docker run --rm -v "$PWD:/work" -w /work nabla-h2h:17 bash bench/head-to-head/run.sh
```

A full run took about three and a half hours on the machine that produced
RESULTS.md. Only about half an hour of that is pgbench: the rest is the nabla
arm waiting for its view to catch up after each run, which is itself one of
the measurements. Knobs, all optional environment variables: `DURATION`
(seconds per run, default 10), `REPS` (repetitions per cell, 3), `CLIENTS`
("1 4 16"), `ORDERS` (200000), `CUSTOMERS` (1000), `PRODUCTS` (500), `ARMS`,
`WORKLOADS`, `CATCHUP_CEILING_S` (1800; a catch-up that hits the ceiling is
reported and the rest of that workload is skipped rather than the benchmark
failing).

## What is compared

Three arms, run one after another, never concurrently:

1. **none** - the workload against the tables with no derived view. The
   baseline every other number is a percentage of.
2. **pg_ivm** - the same view as an IMMV created with `pgivm.create_immv`.
3. **nabla** - the same view created with `nabla.create_view`.

The view is the same text in both engines:

```sql
SELECT c.region, count(*) AS orders, sum(o.qty * p.price) AS revenue
  FROM orders o
  JOIN customers c ON c.id = o.customer_id
  JOIN products  p ON p.id = o.product_id
 WHERE o.status = 'paid'
 GROUP BY c.region
```

pg_ivm 1.15 accepts this definition unchanged: a three-table inner join with
`GROUP BY`, `count(*)` and `sum()`. Nothing in the shape was chosen to suit
either engine, and nothing had to be weakened to make both accept it.

Three workloads, each a single-statement pgbench script:

| workload | statement | why |
|---|---|---|
| insert | `INSERT INTO orders (customer_id, product_id, qty, status) VALUES (<random>, <random>, 1, 'paid')` | the common case: one new fact row touches one group row |
| update-fact | `UPDATE orders SET qty = qty + 1 WHERE id = <random>` | an update of a measure, which both engines must translate into a change of one group row |
| update-dimension | `UPDATE customers SET region = <random of 5> WHERE id = <random>` | the fan-out case: one row change moves every order of that customer between two group rows |

## Why this is a fair comparison

**Same binary, same host.** Both engines are built against the same
`/usr/lib/postgresql/17/bin/pg_config` in the same image and run on the same
PostgreSQL 17 server process on the same machine, one arm at a time.

**Same cluster configuration.** Every arm gets a freshly `initdb`'d cluster
with an identical `postgresql.conf`: same `shared_buffers`, `max_wal_size`,
`min_wal_size`, `checkpoint_timeout`, `wal_level = logical`,
`max_replication_slots`, `max_wal_senders`, and `fsync` left at its default
for all three. The exact file is reproduced in RESULTS.md.

Two deliberate asymmetries, both of them things nabla needs and pg_ivm does
not, both of them stated rather than hidden:

* `wal_level = logical` is set in **all three** arms. nabla requires it; pg_ivm
  does not, and a pg_ivm-only cluster could run at `wal_level = replica` and
  write somewhat less WAL. Setting it everywhere keeps the WAL volume of the
  workload identical across arms, so the comparison measures the engines and
  not their WAL settings. It means the pg_ivm and none arms carry a cost they
  would not have to carry alone; that cost applies to their baseline too.
* `shared_preload_libraries = 'nabla'` and `nabla.database` are set in the
  nabla arm only, because nabla is a background worker in a shared library and
  cannot run without them. This is the one configuration difference between
  arms. It also means the nabla arm has a background process competing for CPU
  with the writers during the measurement, which is exactly how nabla runs in
  production and is not corrected for.

**Same dataset, loaded identically.** 1000 customers, 500 products, 200000
orders generated from a fixed `setseed(0.42)`, with indexes on
`orders(customer_id)` and `orders(product_id)`, then `ANALYZE`, then the view.
The cluster is rebuilt from scratch for every (arm, workload) pair, not just
for every arm, so each workload starts from the identical database in all
three arms and one workload cannot contaminate the next.

**Each engine got what it needs and nothing more.** pg_ivm creates its own
unique index on the `GROUP BY` column when the IMMV is created (it says so in
a NOTICE); that index is left in place. nabla needs primary keys on the joined
tables, which the schema has anyway. `REPLICA IDENTITY FULL` is **not** set on
any table: neither engine required it for these statements, and setting it
would have changed the WAL volume for everyone.

**Every run starts from a settled state.** After each measured run the nabla
arm waits for the view to catch up (that wait is the freshness measurement),
and the other two arms pause a second. No run inherits a backlog from the run
before it.

**Warm-up and repetition.** Each (arm, workload) pair runs one discarded
warm-up run before measuring. Every cell is then run three times; RESULTS.md
reports the median and the min-max spread, so a reader can judge how much of a
difference is signal.

**Correctness is asserted, not assumed.** After the view is created and again
after all the runs of a workload, the derived view is compared with its query
in both directions (`EXCEPT` twice). A single differing row stops the
benchmark. A throughput number from an engine that got the answer wrong would
be meaningless.

## Freshness

Throughput alone would flatter nabla, because nabla's writers do less work per
transaction by deferring the view's work. The other half of the trade is when
the view actually reflects the write:

* **pg_ivm: zero by construction.** The view is updated in the same
  transaction as the base table, so it is current the moment the write
  commits. There is nothing to time, and RESULTS.md does not pretend to
  measure it.
* **nabla: measured.** At the end of every run the script records the frontier
  lag in bytes (`pg_current_wal_lsn() - nabla.frontier(view)`, how far behind
  the view was at the moment the writers stopped) and then the catch-up time:
  how long until the view reflects the WAL position at the end of the run. The
  catch-up is polled with short repeated statements, never one long
  `nabla.wait_for` call, because a long-running statement holds its snapshot
  and slows down the very apply path being timed.

Lag and catch-up are properties of the run that produced them: they say what a
subscriber would have had to wait for after that burst of writes, not what
steady-state staleness is at a lower write rate.

## Threats to validity

* The numbers in RESULTS.md were produced on a laptop under Docker Desktop for
  Windows, sharing the host with a desktop environment. Run-to-run spread on
  that host is visible in the tables; treat differences smaller than the
  spread as noise. Reproducing on a quiet Linux host is expected to give
  cleaner, and possibly different, numbers.
* `fsync` is at its default (on) for all arms, but the container's filesystem
  is not a production storage stack.
* pgbench runs with `--max-tries=10`, so a transaction that hits a
  serialization or deadlock error is retried rather than counted as a failure.
  Retries and failures, if any occurred, are listed in RESULTS.md; they are a
  result in their own right, since an engine that updates the view inside the
  writing transaction has more opportunity to conflict.
* One view, one shape, one dataset size. A different view (more groups, a
  wider join, no aggregate) would move both engines, and not necessarily in
  the same direction.
* nabla's background worker is single-threaded and applies changes for all
  views of the database. The catch-up figures are for one view on an otherwise
  idle worker.
