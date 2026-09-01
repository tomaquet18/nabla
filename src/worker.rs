//! The background worker: consumes the `nabla` replication slot through the
//! SQL peek/advance functions and applies each source transaction to the
//! views in commit order.
//!
//! Ordering per source transaction: apply deltas + advance frontiers and
//! COMMIT, then advance the slot in a separate transaction. A crash between
//! the two replays the transaction, and the per-view frontier check makes the
//! replay a no-op (at-least-once delivery, effectively exactly-once).

use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, BgWorkerStartTime, SignalWakeFlags};
use pgrx::datum::{DatumWithOid, JsonB};
use pgrx::pg_sys::panic::CaughtError;
use pgrx::prelude::*;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use crate::apply::{self, Outcome, ViewTarget};
use crate::definition::ViewSpec;
use crate::guc;
use crate::lsn;
use crate::pgoutput::{Change, Decoder, Relation, SourceTransaction};

const SLOT: &str = "nabla";
const PUBLICATION: &str = "nabla";
/// Starting `upto_nchanges` for a peek. PostgreSQL never splits a transaction
/// across the limit (it is checked between WAL records), so a large transaction
/// simply returns more rows than the limit.
const PEEK_LIMIT: i64 = 1000;

pub fn register() {
    if !unsafe { pg_sys::process_shared_preload_libraries_in_progress } {
        pgrx::warning!(
            "nabla: not loaded via shared_preload_libraries; the background worker will not start"
        );
        return;
    }
    BackgroundWorkerBuilder::new("nabla worker")
        .set_type("nabla worker")
        .set_library("nabla")
        .set_function("nabla_worker_main")
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(5)))
        .load();
}

/// Run `body` in its own transaction. Errors abort the transaction and are
/// returned instead of terminating the worker.
fn try_transaction<R>(body: impl FnOnce() -> R) -> Result<R, String> {
    unsafe {
        pg_sys::SetCurrentStatementStartTimestamp();
        pg_sys::StartTransactionCommand();
        pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
    }
    let result = pgrx::pg_sys::pg_try::PgTryBuilder::new(AssertUnwindSafe(|| Ok(body())))
        .catch_others(|e| {
            let message = match &e {
                CaughtError::PostgresError(r) | CaughtError::ErrorReport(r) => r.message().to_string(),
                CaughtError::RustPanic { ereport, .. } => ereport.message().to_string(),
            };
            Err(message)
        })
        .execute();
    unsafe {
        match &result {
            Ok(_) => {
                pg_sys::PopActiveSnapshot();
                pg_sys::CommitTransactionCommand();
            }
            Err(_) => pg_sys::AbortCurrentTransaction(),
        }
    }
    result
}

fn spi_err(e: pgrx::spi::Error) -> String {
    e.to_string()
}

/// Read-only single-row query. The mutable SPI path (`Spi::run`, `get_one`)
/// assigns a transaction id, which would write a commit record on every poll;
/// idle rounds must stay free of WAL so the worker does not chase itself.
fn select_one<T: FromDatum + IntoDatum>(sql: &str, args: &[DatumWithOid]) -> Result<Option<T>, String> {
    Spi::connect(|client| {
        let table = client.select(sql, Some(1), args)?;
        if table.is_empty() {
            Ok(None)
        } else {
            table.first().get_one::<T>()
        }
    })
    .map_err(spi_err)
}

fn select_two<A: FromDatum + IntoDatum, B: FromDatum + IntoDatum>(
    sql: &str,
    args: &[DatumWithOid],
) -> Result<Option<(Option<A>, Option<B>)>, String> {
    Spi::connect(|client| {
        let table = client.select(sql, Some(1), args)?;
        if table.is_empty() {
            Ok(None)
        } else {
            let table = table.first();
            Ok(Some((table.get::<A>(1)?, table.get::<B>(2)?)))
        }
    })
    .map_err(spi_err)
}

struct LiveView {
    id: i32,
    name: String,
    base_oid: u32,
    spec: ViewSpec,
    frontier: u64,
    last_seq: i64,
    start_seq: i64,
    definition: String,
    touched: bool,
    stale: Option<String>,
}

impl LiveView {
    fn mentions(&self, column: &str) -> bool {
        // Conservative text check; false positives only make a view stale earlier.
        self.definition.to_lowercase().contains(&column.to_lowercase())
    }
}

fn load_live_views() -> Result<Vec<LiveView>, String> {
    Spi::connect(|client| {
        let mut views = Vec::new();
        for r in client.select(
            "SELECT id, name, base_table::oid::int8, spec, frontier_lsn::text, last_seq, definition \
             FROM nabla.views WHERE status = 'live' ORDER BY id",
            None,
            &[],
        )? {
            let spec: JsonB = r.get::<JsonB>(4)?.expect("spec");
            let spec: ViewSpec = match serde_json::from_value(spec.0) {
                Ok(s) => s,
                Err(e) => {
                    pgrx::warning!("nabla worker: skipping view with corrupt spec: {e}");
                    continue;
                }
            };
            views.push(LiveView {
                id: r.get::<i32>(1)?.expect("id"),
                name: r.get::<String>(2)?.expect("name"),
                base_oid: r.get::<i64>(3)?.expect("base") as u32,
                spec,
                frontier: lsn::parse(&r.get::<String>(5)?.expect("frontier")).unwrap_or(0),
                last_seq: r.get::<i64>(6)?.expect("last_seq"),
                start_seq: r.get::<i64>(6)?.expect("last_seq"),
                definition: r.get::<String>(7)?.expect("definition"),
                touched: false,
                stale: None,
            });
        }
        Ok(views)
    })
    .map_err(spi_err)
}

fn resolve_types(rel: &mut Relation) -> Result<(), String> {
    for col in rel.columns.iter_mut() {
        if col.type_name.is_some() {
            continue;
        }
        let args: [DatumWithOid; 2] = [(col.typid as i64).into(), col.typmod.into()];
        let name = select_one::<String>("SELECT pg_catalog.format_type($1::oid, $2)", &args)?
            .ok_or_else(|| format!("unknown type oid {}", col.typid))?;
        col.type_name = Some(name);
    }
    Ok(())
}

fn record_deltas(view: &mut LiveView, tx: &SourceTransaction, deltas: Vec<apply::Delta>) -> Result<(), String> {
    for d in deltas {
        view.last_seq += 1;
        let args: [DatumWithOid; 6] = [
            view.id.into(),
            view.last_seq.into(),
            lsn::format(tx.end_lsn).into(),
            (tx.xid as i64).into(),
            d.op.to_string().into(),
            d.row_json.into(),
        ];
        Spi::run_with_args(
            "INSERT INTO nabla.deltas (view_id, seq, lsn, xid, op, row) \
             VALUES ($1, $2, $3::pg_lsn, $4, $5::\"char\", $6::jsonb)",
            &args,
        )
        .map_err(spi_err)?;
        view.touched = true;
    }
    Ok(())
}

/// Apply one change of a source transaction to one view.
fn apply_change(view: &mut LiveView, rel: &Relation, change: &Change, tx: &SourceTransaction) -> Result<(), String> {
    let target = ViewTarget { name: &view.name, spec: &view.spec };
    let mut outcomes: Vec<Outcome> = Vec::new();
    match change {
        Change::Insert { new, .. } => {
            let new = match apply::resolve_unchanged(rel, new, None, |c| view.mentions(c)) {
                Ok(t) => t,
                Err(s) => {
                    view.stale = Some(s.0);
                    return Ok(());
                }
            };
            outcomes.push(apply::apply_insert(&target, rel, &new)?);
        }
        Change::Delete { key_kind, old, .. } => {
            outcomes.push(apply::apply_delete(&target, rel, *key_kind, old)?);
        }
        Change::Update { old, new, .. } => {
            // Without an old tuple the key is unchanged: take it from the new row.
            let (key_kind, old_tuple) = match old {
                Some((k, t)) => (*k, t.clone()),
                None => (b'K', new.clone()),
            };
            outcomes.push(apply::apply_delete(&target, rel, key_kind, &old_tuple)?);
            let new = match apply::resolve_unchanged(rel, new, old.as_ref(), |c| view.mentions(c)) {
                Ok(t) => t,
                Err(s) => {
                    view.stale = Some(s.0);
                    return Ok(());
                }
            };
            outcomes.push(apply::apply_insert(&target, rel, &new)?);
        }
        Change::Truncate { .. } => {
            view.stale = Some("base table was truncated; TRUNCATE is not maintained incrementally".to_string());
            return Ok(());
        }
    }
    for outcome in outcomes {
        match outcome {
            Outcome::Applied(deltas) => record_deltas(view, tx, deltas)?,
            Outcome::Stale(s) => {
                view.stale = Some(s.0);
                return Ok(());
            }
        }
    }
    Ok(())
}

struct ApplyStats {
    deltas: usize,
}

/// Apply one complete source transaction in one worker transaction.
fn apply_transaction(decoder: &mut Decoder, tx: &SourceTransaction) -> Result<ApplyStats, String> {
    Spi::run("SET LOCAL nabla.internal_write = on").map_err(spi_err)?;
    let mut views = load_live_views()?;
    let mut stats = ApplyStats { deltas: 0 };

    for change in &tx.changes {
        let relids: Vec<u32> = match change {
            Change::Insert { relid, .. } | Change::Update { relid, .. } | Change::Delete { relid, .. } => vec![*relid],
            Change::Truncate { relids } => relids.clone(),
        };
        for relid in relids {
            let Some(rel) = decoder.relations.get_mut(&relid) else {
                return Err(format!("change for unknown relation {relid}"));
            };
            resolve_types(rel)?;
            let rel = rel.clone();
            for view in views.iter_mut() {
                if view.base_oid != relid || view.stale.is_some() || tx.end_lsn <= view.frontier {
                    continue;
                }
                apply_change(view, &rel, change, tx)?;
            }
        }
    }

    let retain = guc::RETAIN_DELTAS.get() as i64;
    for view in views.iter_mut() {
        if let Some(reason) = &view.stale {
            pgrx::warning!("nabla worker: view {} marked stale: {reason}", view.name);
            Spi::run_with_args("UPDATE nabla.views SET status = 'stale' WHERE id = $1", &[view.id.into()])
                .map_err(spi_err)?;
            continue;
        }
        if tx.end_lsn <= view.frontier {
            continue;
        }
        let args: [DatumWithOid; 3] = [lsn::format(tx.end_lsn).into(), view.last_seq.into(), view.id.into()];
        Spi::run_with_args(
            "UPDATE nabla.views SET frontier_lsn = $1::pg_lsn, last_seq = $2 WHERE id = $3",
            &args,
        )
        .map_err(spi_err)?;
        if view.touched {
            let gc: [DatumWithOid; 2] = [view.id.into(), (view.last_seq - retain).into()];
            Spi::run_with_args("DELETE FROM nabla.deltas WHERE view_id = $1 AND seq <= $2", &gc).map_err(spi_err)?;
            let channel = format!("nabla:{}", view.name);
            let payload = format!("{}:{}", view.last_seq, lsn::format(tx.end_lsn));
            let notify: [DatumWithOid; 2] = [channel.into(), payload.into()];
            Spi::run_with_args("SELECT pg_catalog.pg_notify($1, $2)", &notify).map_err(spi_err)?;
        }
    }
    stats.deltas = views.iter().map(|v| (v.last_seq - v.start_seq).max(0)).sum::<i64>() as usize;
    Ok(stats)
}

fn advance_slot(to: u64) -> Result<(), String> {
    let args: [DatumWithOid; 2] = [SLOT.into(), lsn::format(to).into()];
    Spi::run_with_args(
        "SELECT pg_catalog.pg_replication_slot_advance($1, GREATEST($2::pg_lsn, confirmed_flush_lsn)) \
         FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
        &args,
    )
    .map_err(spi_err)
}

struct WorkerState {
    decoder: Decoder,
    /// Flush position right after our own last idle-advance write, so the
    /// worker does not chase its own WAL every poll.
    self_flush: u64,
    reported_missing_extension: bool,
}

enum Round {
    Idle,
    MoreWork,
}

fn run_round(state: &mut WorkerState) -> Result<Round, String> {
    // The extension may not be installed (yet) in this database.
    let installed = try_transaction(|| {
        select_one::<bool>("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'nabla')", &[])
    })??
    .unwrap_or(false);
    if !installed {
        if !state.reported_missing_extension {
            pgrx::log!("nabla worker: extension not installed in this database; waiting");
            state.reported_missing_extension = true;
        }
        return Ok(Round::Idle);
    }
    state.reported_missing_extension = false;

    // Slot presence and lag.
    let slot = try_transaction(|| {
        select_two::<String, i64>(
            "SELECT confirmed_flush_lsn::text, (pg_catalog.pg_current_wal_lsn() - confirmed_flush_lsn)::int8 \
             FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[SLOT.into()],
        )
    })??;
    let Some((Some(confirmed_text), Some(lag))) = slot else {
        return Ok(Round::Idle);
    };
    let confirmed = lsn::parse(&confirmed_text).unwrap_or(0);
    if lag > guc::MAX_SLOT_LAG_BYTES.get() as i64 {
        try_transaction(|| {
            Spi::run("UPDATE nabla.views SET status = 'stale' WHERE status = 'live'").map_err(spi_err)?;
            Spi::run_with_args("SELECT pg_catalog.pg_drop_replication_slot($1)", &[SLOT.into()]).map_err(spi_err)
        })??;
        pgrx::warning!(
            "nabla worker: slot lag {lag} bytes exceeds nabla.max_slot_lag_bytes; all views marked stale and slot dropped"
        );
        return Ok(Round::Idle);
    }

    // Peek everything committed up to the current flush point.
    let (target, rows) = try_transaction(|| {
        let target = select_one::<String>("SELECT pg_catalog.pg_current_wal_flush_lsn()::text", &[])?
            .and_then(|t| lsn::parse(&t))
            .ok_or("could not read flush lsn")?;
        if target <= confirmed {
            return Ok::<_, String>((target, Vec::new()));
        }
        let args: [DatumWithOid; 2] = [SLOT.into(), lsn::format(target).into()];
        let rows = Spi::connect(|client| {
            let mut rows: Vec<Vec<u8>> = Vec::new();
            for r in client.select(
                &format!(
                    "SELECT data FROM pg_catalog.pg_logical_slot_peek_binary_changes($1, $2::pg_lsn, {PEEK_LIMIT}, \
                     'proto_version', '1', 'publication_names', '{PUBLICATION}')"
                ),
                None,
                &args,
            )? {
                rows.push(r.get::<Vec<u8>>(1)?.unwrap_or_default());
            }
            Ok::<_, pgrx::spi::Error>(rows)
        })
        .map_err(spi_err)?;
        Ok((target, rows))
    })??;

    let drained = (rows.len() as i64) < PEEK_LIMIT;
    if rows.is_empty() && target == state.self_flush {
        return Ok(Round::Idle);
    }
    for data in &rows {
        state.decoder.feed(data)?;
    }
    if state.decoder.has_incomplete() {
        pgrx::warning!("nabla worker: discarding an incomplete trailing transaction; it will be re-read");
        state.decoder.discard_incomplete();
    }

    let transactions = state.decoder.take_complete();
    let mut applied = 0usize;
    let mut total_deltas = 0usize;
    let mut last_end = confirmed;
    for tx in &transactions {
        let decoder = &mut state.decoder;
        let stats = try_transaction(AssertUnwindSafe(|| apply_transaction(decoder, tx)))??;
        total_deltas += stats.deltas;
        try_transaction(|| advance_slot(tx.end_lsn))??;
        last_end = last_end.max(tx.end_lsn);
        applied += 1;
    }

    if drained {
        // Everything up to `target` was decoded: views reflect the base at `target`.
        let frontier = target.max(last_end);
        let after_flush = try_transaction(|| {
            let args: [DatumWithOid; 1] = [lsn::format(frontier).into()];
            Spi::run_with_args(
                "UPDATE nabla.views SET frontier_lsn = $1::pg_lsn WHERE status = 'live' AND frontier_lsn < $1::pg_lsn",
                &args,
            )
            .map_err(spi_err)?;
            advance_slot(frontier)
        })??;
        let _ = after_flush;
        state.self_flush = try_transaction(|| {
            select_one::<String>("SELECT pg_catalog.pg_current_wal_flush_lsn()::text", &[])
                .map(|t| t.and_then(|t| lsn::parse(&t)).unwrap_or(0))
        })??;
    }
    if applied > 0 {
        pgrx::log!(
            "nabla worker: applied {applied} transaction(s), {total_deltas} delta(s), frontier {}",
            lsn::format(target.max(last_end))
        );
    }
    Ok(if drained { Round::Idle } else { Round::MoreWork })
}

/// Entry point registered in `_PG_init`. Exported by hand because
/// `#[pg_guard]` only adds `no_mangle` for `_PG_init`/`_PG_fini`.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn nabla_worker_main(_arg: pg_sys::Datum) {
    unsafe { pgrx::pg_sys::submodules::panic::pgrx_extern_c_guard(worker_main) }
}

fn worker_main() {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let Some(database) = guc::database() else {
        pgrx::log!("nabla worker: nabla.database is not set; idling");
        while BackgroundWorker::wait_latch(Some(Duration::from_secs(60))) {}
        return;
    };
    BackgroundWorker::connect_worker_to_spi(Some(&database), None);
    pgrx::log!("nabla worker: connected to database {database}");

    let mut state = WorkerState { decoder: Decoder::default(), self_flush: 0, reported_missing_extension: false };
    let mut busy = false;
    loop {
        if !busy {
            let poll = Duration::from_millis(guc::POLL_INTERVAL_MS.get().max(1) as u64);
            if !BackgroundWorker::wait_latch(Some(poll)) {
                break;
            }
        } else if BackgroundWorker::sigterm_received() {
            break;
        }
        busy = match run_round(&mut state) {
            Ok(Round::MoreWork) => true,
            Ok(Round::Idle) => false,
            Err(message) => {
                pgrx::warning!("nabla worker: round failed, will retry: {message}");
                state.decoder = Decoder::default();
                false
            }
        };
    }
    pgrx::log!("nabla worker: shutting down");
}
