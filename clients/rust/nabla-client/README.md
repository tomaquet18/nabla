# nabla-client

Reference Rust client for nabla view subscriptions, built on `tokio-postgres`.
It is the executable specification of the subscription protocol described in
the top-level README ("Subscribing from a client"); clients in other languages
should behave the same way.

## Library

```rust
use nabla_client::{Event, Subscription};

let mut sub = Subscription::open("host=localhost dbname=app", "public.order_lines").await?;
loop {
    match sub.next().await? {
        Event::Snapshot { epoch, frontier, cursor, rows } => { /* replace local state */ }
        Event::Transaction { xid, lsn, epoch, deltas } => { /* apply deltas atomically */ }
        Event::Resync { reason } => { /* a Snapshot follows; discard local state */ }
    }
}
```

- `Subscription::open(config, view)` connects, `LISTEN`s on
  `nabla:<canonical name>` (from `nabla.status`) and
  prepares the first bootstrap; `open_with` takes `Options` (batch size,
  fallback poll interval, whether to keep `_nabla_*` columns, stale backoff).
- `next()` returns the next event: a `Snapshot` (rows from one
  `REPEATABLE READ` transaction, with the cursor to continue from), a
  `Transaction` (all deltas of one source transaction: same `xid` and `lsn`,
  contiguous `seq`), or a `Resync` (`lagged`, `epoch changed`, `stale`,
  `disconnected`), always followed by a fresh `Snapshot`. Server conditions are
  recognized by SQLSTATE (`NB001`, `NB002`, `NB003`) only.
- `wait_for(lsn, timeout)` blocks until the view has absorbed everything up to
  `lsn` (read-your-writes; pass `pg_current_wal_lsn()` taken after your commit).

Buffers are bounded: one batch plus the trailing transaction, and coalesced
notifications. A lost connection is re-established with backoff and reported
as `Resync { Disconnected }`.

## Example

```
cargo build --release --example follow
target/release/examples/follow [--rows] "host=/tmp port=5499 dbname=nabla_test user=dev" public.order_lines
```

Output:

```
snapshot: rows=7 epoch=1 frontier=0/1A2B3C cursor=42
tx lsn=0/1A2C00 xid=751 deltas=2
  43 -{"customer":"Alice","order_id":9,"product":"pen","total":3}
  44 +{"customer":"Alicia","order_id":9,"product":"pen","total":3}
resync: epoch changed (1 -> 2)
snapshot: rows=7 epoch=2 frontier=0/1A3000 cursor=45
```

`--rows` also prints every snapshot row as `= {json}`. Ctrl-C exits with
status 0. From the repository root, `scripts/dev.sh client` builds the crate
inside the development container.
