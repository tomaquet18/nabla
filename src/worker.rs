//! The background worker: consumes the `nabla` replication slot through the
//! SQL peek/advance functions and applies each source transaction to the
//! views (and the shadow tables join views depend on) in commit order.
//!
//! Ordering per source transaction: apply deltas + advance frontiers and
//! COMMIT, then advance the slot in a separate transaction. A crash between
//! the two replays the transaction, and the per-object frontier check makes
//! the replay a no-op (at-least-once delivery, effectively exactly-once).
//!
//! Skip rule: an object (view or shadow) with `frontier >= T.end_lsn` has
//! already absorbed T and is skipped. This is also what lets a new view share
//! an existing shadow without re-snapshotting it: the view is populated from
//! the live tables at F_new and skips everything at or below F_new, while an
//! older shadow at F_s < F_new keeps absorbing (F_s, F_new] on its own; from
//! F_new on both absorb the same transactions.
//!
//! Failure isolation: every view's planning step and its final write step
//! run in subtransactions. A failing view is rolled back alone, its failure
//! is recorded in the catalog, and the slot is held back until the view
//! either absorbs the transaction on a later poll or exceeds
//! `nabla.max_apply_failures` and goes stale.

use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, BgWorkerStartTime, SignalWakeFlags};
use pgrx::datum::{DatumWithOid, JsonB};
use pgrx::pg_sys::panic::CaughtError;
use pgrx::pg_sys::pg_try::PgTryBuilder;
use pgrx::prelude::*;
use pgrx::spi::quote_identifier;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use crate::apply::{self, Op, Planned, ViewTarget};
use crate::definition::ViewSpec;
use crate::guc;
use crate::lsn;
use crate::pgoutput::{Change, ColumnValue, Decoder, Relation, SourceTransaction, Tuple};

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

fn describe(e: &CaughtError) -> String {
    match e {
        CaughtError::PostgresError(r) | CaughtError::ErrorReport(r) => r.message().to_string(),
        CaughtError::RustPanic { ereport, .. } => ereport.message().to_string(),
    }
}

/// Run `body` in its own transaction. Errors abort the transaction and are
/// returned instead of terminating the worker.
fn try_transaction<R>(body: impl FnOnce() -> R) -> Result<R, String> {
    unsafe {
        pg_sys::SetCurrentStatementStartTimestamp();
        pg_sys::StartTransactionCommand();
        pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
    }
    let result = PgTryBuilder::new(AssertUnwindSafe(|| Ok(body())))
        .catch_others(|e| Err(describe(&e)))
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

/// Run `body` in a subtransaction of the current transaction. A PostgreSQL
/// error or an `Err` from the body rolls the subtransaction back and returns
/// the message; the enclosing transaction stays usable.
fn in_subtransaction<R>(body: impl FnOnce() -> Result<R, String>) -> Result<R, String> {
    unsafe {
        let memory_context = pg_sys::CurrentMemoryContext;
        let resource_owner = pg_sys::CurrentResourceOwner;
        pg_sys::BeginInternalSubTransaction(std::ptr::null());
        let result = PgTryBuilder::new(AssertUnwindSafe(body)).catch_others(|e| Err(describe(&e))).execute();
        match &result {
            Ok(_) => pg_sys::ReleaseCurrentSubTransaction(),
            Err(_) => pg_sys::RollbackAndReleaseCurrentSubTransaction(),
        }
        pg_sys::MemoryContextSwitchTo(memory_context);
        pg_sys::CurrentResourceOwner = resource_owner;
        result
    }
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

// --- per-transaction state -----------------------------------------------------

struct LiveView {
    id: i32,
    name: String,
    base_oid: u32,
    spec: ViewSpec,
    frontier: u64,
    last_seq: i64,
    start_seq: i64,
    apply_failures: i32,
    definition: String,
    touched: bool,
    /// Planned operations for the current source transaction.
    ops: Vec<Op>,
    /// Set when a planning step failed: the view must retry the transaction.
    failed: Option<String>,
    stale: Option<String>,
}

impl LiveView {
    /// Position of `relid` in the view's relation list, if the view uses it.
    fn relation_index(&self, relid: u32) -> Option<usize> {
        if self.spec.is_join() {
            self.spec.relations.iter().position(|r| r.oid == relid)
        } else if self.base_oid == relid {
            Some(0)
        } else {
            None
        }
    }

    fn active_for(&self, relid: u32, end_lsn: u64) -> bool {
        self.failed.is_none() && self.stale.is_none() && end_lsn > self.frontier && self.relation_index(relid).is_some()
    }
}

struct Shadow {
    relid: u32,
    table: String,
    frontier: u64,
    /// Maintenance failed (schema drift): dependents are stale, refresh rebuilds it.
    failed: bool,
}

fn load_live_views() -> Result<Vec<LiveView>, String> {
    Spi::connect(|client| {
        let mut views = Vec::new();
        for r in client.select(
            "SELECT id, name, base_table::oid::int8, spec, frontier_lsn::text, last_seq, definition, apply_failures \
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
            let last_seq = r.get::<i64>(6)?.expect("last_seq");
            views.push(LiveView {
                id: r.get::<i32>(1)?.expect("id"),
                name: r.get::<String>(2)?.expect("name"),
                base_oid: r.get::<i64>(3)?.expect("base") as u32,
                spec,
                frontier: lsn::parse(&r.get::<String>(5)?.expect("frontier")).unwrap_or(0),
                last_seq,
                start_seq: last_seq,
                definition: r.get::<String>(7)?.expect("definition"),
                apply_failures: r.get::<i32>(8)?.unwrap_or(0),
                touched: false,
                ops: Vec::new(),
                failed: None,
                stale: None,
            });
        }
        Ok(views)
    })
    .map_err(spi_err)
}

fn load_shadows() -> Result<HashMap<u32, Shadow>, String> {
    Spi::connect(|client| {
        let mut shadows = HashMap::new();
        for r in client.select(
            "SELECT relid::int8, table_name, frontier_lsn::text, stale_reason IS NOT NULL FROM nabla.shadows",
            None,
            &[],
        )? {
            let relid = r.get::<i64>(1)?.expect("relid") as u32;
            shadows.insert(
                relid,
                Shadow {
                    relid,
                    table: r.get::<String>(2)?.expect("table"),
                    frontier: lsn::parse(&r.get::<String>(3)?.expect("frontier")).unwrap_or(0),
                    failed: r.get::<bool>(4)?.unwrap_or(false),
                },
            );
        }
        Ok(shadows)
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

fn change_relids(change: &Change) -> Vec<u32> {
    match change {
        Change::Insert { relid, .. } | Change::Update { relid, .. } | Change::Delete { relid, .. } => vec![*relid],
        Change::Truncate { relids } => relids.clone(),
    }
}

// --- shadows -------------------------------------------------------------------

/// Primary key values of a change, taken from the old key tuple when pgoutput
/// sent one, otherwise from the new tuple (the key did not change).
fn key_values(pk_columns: &[String], rel: &Relation, old: Option<&Tuple>, new: Option<&Tuple>) -> Result<Vec<(usize, String)>, String> {
    let mut out = Vec::new();
    for pk in pk_columns {
        let idx = rel.columns.iter().position(|c| &c.name == pk).ok_or_else(|| format!("primary key column {pk} is missing from the decoded row"))?;
        let value = old
            .and_then(|t| t.get(idx))
            .and_then(|v| if let ColumnValue::Text(s) = v { Some(s.clone()) } else { None })
            .or_else(|| new.and_then(|t| t.get(idx)).and_then(|v| if let ColumnValue::Text(s) = v { Some(s.clone()) } else { None }))
            .ok_or_else(|| format!("primary key column {pk} has no value in the decoded row"))?;
        out.push((idx, value));
    }
    Ok(out)
}

fn key_predicate(rel: &Relation, keys: &[(usize, String)], first_param: usize) -> Result<(String, Vec<Option<String>>), String> {
    let mut conds = Vec::new();
    let mut values = Vec::new();
    for (n, (idx, value)) in keys.iter().enumerate() {
        let col = &rel.columns[*idx];
        let ty = col.type_name.as_deref().ok_or("type not resolved")?;
        conds.push(format!("{} = ${}::{}", quote_identifier(&col.name), first_param + n, ty));
        values.push(Some(value.clone()));
    }
    Ok((conds.join(" AND "), values))
}

/// The full old row of a change, read from the shadow by primary key, in the
/// decoded relation's column order.
fn shadow_old_row(shadow: &Shadow, pk_columns: &[String], rel: &Relation, old: Option<&Tuple>, new: Option<&Tuple>) -> Result<Tuple, String> {
    let keys = key_values(pk_columns, rel, old, new)?;
    let (conds, values) = key_predicate(rel, &keys, 1)?;
    let cols: Vec<String> = rel.columns.iter().map(|c| format!("{}::text", quote_identifier(&c.name))).collect();
    let sql = format!("SELECT {} FROM {} WHERE {conds}", cols.join(", "), shadow.table);
    let args: Vec<DatumWithOid> = values.into_iter().map(|v| v.into()).collect();
    let row = Spi::connect(|client| {
        let table = client.select(&sql, Some(1), &args)?;
        if table.is_empty() {
            return Ok(None);
        }
        let table = table.first();
        let mut tuple = Vec::with_capacity(rel.columns.len());
        for i in 1..=rel.columns.len() {
            tuple.push(match table.get::<String>(i)? {
                Some(s) => ColumnValue::Text(s),
                None => ColumnValue::Null,
            });
        }
        Ok(Some(tuple))
    })
    .map_err(spi_err)?;
    row.ok_or_else(|| format!("row not found in shadow {} (shadow out of sync with the base table)", shadow.table))
}

/// Apply a change to the shadow: insert, delete by key, or delete + insert.
fn shadow_apply(shadow: &Shadow, pk_columns: &[String], rel: &Relation, change: &Change, old_full: Option<&Tuple>, new_full: Option<&Tuple>) -> Result<(), String> {
    let delete = |old: Option<&Tuple>, new: Option<&Tuple>| -> Result<(), String> {
        let keys = key_values(pk_columns, rel, old, new)?;
        let (conds, values) = key_predicate(rel, &keys, 1)?;
        let args: Vec<DatumWithOid> = values.into_iter().map(|v| v.into()).collect();
        Spi::run_with_args(&format!("DELETE FROM {} WHERE {conds}", shadow.table), &args).map_err(spi_err)
    };
    let insert = |row: &Tuple| -> Result<(), String> {
        let mut cols = Vec::new();
        let mut casts = Vec::new();
        for (i, c) in rel.columns.iter().enumerate() {
            let ty = c.type_name.as_deref().ok_or("type not resolved")?;
            cols.push(quote_identifier(&c.name));
            casts.push(format!("${}::{}", i + 1, ty));
        }
        Spi::run_with_args(
            &format!("INSERT INTO {} ({}) VALUES ({})", shadow.table, cols.join(", "), casts.join(", ")),
            &apply::row_args(row),
        )
        .map_err(spi_err)
    };
    match change {
        Change::Insert { .. } => insert(new_full.ok_or("missing new row")?),
        Change::Delete { old, .. } => delete(Some(old), None),
        Change::Update { old, new, .. } => {
            delete(old_full.or(old.as_ref().map(|(_, t)| t)), Some(new))?;
            insert(new_full.ok_or("missing new row")?)
        }
        Change::Truncate { .. } => Ok(()),
    }
}

// --- catalog bookkeeping -------------------------------------------------------

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

fn mark_stale(view: &LiveView, reason: &str) -> Result<(), String> {
    pgrx::warning!("nabla worker: view {} marked stale: {reason}", view.name);
    let args: [DatumWithOid; 2] = [view.id.into(), reason.into()];
    Spi::run_with_args("UPDATE nabla.views SET status = 'stale', stale_reason = $2 WHERE id = $1", &args)
        .map_err(spi_err)
}

/// Record a failed apply. Returns true when the view is still live (and the
/// transaction must be retried), false when it just went stale.
fn record_failure(view: &mut LiveView, tx: &SourceTransaction, message: &str) -> Result<bool, String> {
    let max = guc::MAX_APPLY_FAILURES.get().max(1);
    let failures = view.apply_failures + 1;
    view.apply_failures = failures;
    if failures >= max {
        let reason = format!("apply failed {failures} times: {message}");
        pgrx::warning!("nabla worker: view {} marked stale: {reason}", view.name);
        let args: [DatumWithOid; 4] = [view.id.into(), reason.as_str().into(), failures.into(), message.into()];
        Spi::run_with_args(
            "UPDATE nabla.views SET status = 'stale', stale_reason = $2, apply_failures = $3, \
             last_error = $4, last_error_at = now() WHERE id = $1",
            &args,
        )
        .map_err(spi_err)?;
        view.stale = Some(reason);
        return Ok(false);
    }
    pgrx::log!(
        "nabla worker: view {} failed to apply the transaction ending at {} (attempt {failures} of {max}): {message}",
        view.name,
        lsn::format(tx.end_lsn)
    );
    let args: [DatumWithOid; 3] = [view.id.into(), failures.into(), message.into()];
    Spi::run_with_args(
        "UPDATE nabla.views SET apply_failures = $2, last_error = $3, last_error_at = now() WHERE id = $1",
        &args,
    )
    .map_err(spi_err)?;
    Ok(true)
}

/// Advance a view that absorbed the transaction: frontier, sequence counter,
/// failure bookkeeping reset, delta garbage collection and notification.
fn record_success(view: &LiveView, tx: &SourceTransaction) -> Result<(), String> {
    let args: [DatumWithOid; 3] = [lsn::format(tx.end_lsn).into(), view.last_seq.into(), view.id.into()];
    Spi::run_with_args(
        "UPDATE nabla.views SET frontier_lsn = $1::pg_lsn, last_seq = $2, \
         apply_failures = 0, last_error = NULL, last_error_at = NULL WHERE id = $3",
        &args,
    )
    .map_err(spi_err)?;
    if view.touched {
        let retain = guc::RETAIN_DELTAS.get() as i64;
        let gc: [DatumWithOid; 2] = [view.id.into(), (view.last_seq - retain).into()];
        Spi::run_with_args("DELETE FROM nabla.deltas WHERE view_id = $1 AND seq <= $2", &gc).map_err(spi_err)?;
        let channel = format!("nabla:{}", view.name);
        let payload = format!("{}:{}", view.last_seq, lsn::format(tx.end_lsn));
        let notify: [DatumWithOid; 2] = [channel.into(), payload.into()];
        Spi::run_with_args("SELECT pg_catalog.pg_notify($1, $2)", &notify).map_err(spi_err)?;
    }
    Ok(())
}

fn record_shadow_failure(shadow: &mut Shadow, message: &str) -> Result<(), String> {
    shadow.failed = true;
    let reason = format!("shadow {} could not be maintained: {message}", shadow.table);
    pgrx::warning!("nabla worker: {reason}");
    let args: [DatumWithOid; 2] = [(shadow.relid as i64).into(), reason.as_str().into()];
    Spi::run_with_args("UPDATE nabla.shadows SET stale_reason = $2 WHERE relid = $1::oid", &args).map_err(spi_err)
}

// --- planning one change for one view ----------------------------------------------

/// Plan the effect of one change on one view. `old_full` is the complete old
/// row when it came from a shadow.
fn plan_change(view: &mut LiveView, rel: &Relation, change: &Change, old_full: Option<&Tuple>) -> Result<Planned, String> {
    let target = ViewTarget { name: &view.name, spec: &view.spec };
    let mentions = |c: &str| view.definition.to_lowercase().contains(&c.to_lowercase());
    let mut ops = Vec::new();
    if view.spec.is_join() {
        let index = view.relation_index(rel.id).ok_or("view does not use this relation")?;
        match change {
            Change::Insert { new, .. } => {
                let new = match apply::resolve_unchanged(rel, new, None, mentions) {
                    Ok(t) => t,
                    Err(s) => return Ok(Planned::Stale(s)),
                };
                ops.extend(apply::plan_join(&target, index, rel, &new, 1)?);
            }
            Change::Delete { .. } => {
                let Some(old) = old_full else {
                    return Ok(Planned::Stale(apply::Stale(format!(
                        "old row of {} unavailable (its shadow is ahead of the view)",
                        rel.name
                    ))));
                };
                ops.extend(apply::plan_join(&target, index, rel, old, -1)?);
            }
            Change::Update { new, .. } => {
                let Some(old) = old_full else {
                    return Ok(Planned::Stale(apply::Stale(format!(
                        "old row of {} unavailable (its shadow is ahead of the view)",
                        rel.name
                    ))));
                };
                ops.extend(apply::plan_join(&target, index, rel, old, -1)?);
                let new = match apply::resolve_unchanged(rel, new, Some(old), mentions) {
                    Ok(t) => t,
                    Err(s) => return Ok(Planned::Stale(s)),
                };
                ops.extend(apply::plan_join(&target, index, rel, &new, 1)?);
            }
            Change::Truncate { .. } => {
                return Ok(Planned::Stale(apply::Stale(
                    "base table was truncated; TRUNCATE is not maintained incrementally".to_string(),
                )))
            }
        }
    } else {
        match change {
            Change::Insert { new, .. } => {
                let new = match apply::resolve_unchanged(rel, new, None, mentions) {
                    Ok(t) => t,
                    Err(s) => return Ok(Planned::Stale(s)),
                };
                ops.extend(apply::plan_single_insert(&target, rel, &new)?);
            }
            Change::Delete { key_kind, old, .. } => match apply::plan_single_delete(&target, rel, *key_kind, old)? {
                Planned::Ops(o) => ops.extend(o),
                Planned::Stale(s) => return Ok(Planned::Stale(s)),
            },
            Change::Update { old, new, .. } => {
                // Without an old tuple the key is unchanged: take it from the new row.
                let (key_kind, old_tuple) = match old {
                    Some((k, t)) => (*k, t.clone()),
                    None => (b'K', new.clone()),
                };
                match apply::plan_single_delete(&target, rel, key_kind, &old_tuple)? {
                    Planned::Ops(o) => ops.extend(o),
                    Planned::Stale(s) => return Ok(Planned::Stale(s)),
                }
                let new = match apply::resolve_unchanged(rel, new, old.as_ref().map(|(_, t)| t), mentions) {
                    Ok(t) => t,
                    Err(s) => return Ok(Planned::Stale(s)),
                };
                ops.extend(apply::plan_single_insert(&target, rel, &new)?);
            }
            Change::Truncate { .. } => {
                return Ok(Planned::Stale(apply::Stale(
                    "base table was truncated; TRUNCATE is not maintained incrementally".to_string(),
                )))
            }
        }
    }
    Ok(Planned::Ops(ops))
}

struct ApplyStats {
    deltas: usize,
    /// A live view failed and must retry: the slot must not advance past `tx`.
    blocked: bool,
}

/// Apply one complete source transaction in one worker transaction.
///
/// Phase 1 walks the changes in stream order: for each change, every
/// affected view plans its delta rows (join views against the shadows as
/// they stand before this change), then the changed table's shadow absorbs
/// the change. Views before shadow would only matter for self-joins, which
/// are rejected. Phase 2 writes each view's planned rows in its own
/// subtransaction and advances the frontiers.
fn apply_transaction(decoder: &mut Decoder, tx: &SourceTransaction) -> Result<ApplyStats, String> {
    Spi::run("SET LOCAL nabla.internal_write = on").map_err(spi_err)?;
    let mut views = load_live_views()?;
    let mut shadows = load_shadows()?;

    let mut relations: HashMap<u32, Relation> = HashMap::new();
    for change in &tx.changes {
        for relid in change_relids(change) {
            if relations.contains_key(&relid) {
                continue;
            }
            let Some(rel) = decoder.relations.get_mut(&relid) else {
                return Err(format!("change for unknown relation {relid}"));
            };
            resolve_types(rel)?;
            relations.insert(relid, rel.clone());
        }
    }
    // Primary keys of shadowed tables, from any join view's spec.
    let mut pk_of: HashMap<u32, Vec<String>> = HashMap::new();
    for v in &views {
        for r in &v.spec.relations {
            pk_of.entry(r.oid).or_insert_with(|| r.pk_columns.clone());
        }
    }

    // Phase 1: plan per change, maintain shadows in stream order.
    for change in &tx.changes {
        for relid in change_relids(change) {
            let rel = &relations[&relid];
            let shadow_active = shadows.get(&relid).map_or(false, |s| !s.failed && tx.end_lsn > s.frontier);

            // Old row from the shadow (before this change), full row for
            // join deltas and for the shadow's own delete-by-key.
            let mut old_full: Option<Tuple> = None;
            let mut shadow_error: Option<String> = None;
            if shadow_active && matches!(change, Change::Update { .. } | Change::Delete { .. }) {
                let (old, new): (Option<&Tuple>, Option<&Tuple>) = match change {
                    Change::Update { old, new, .. } => (old.as_ref().map(|(_, t)| t), Some(new)),
                    Change::Delete { old, .. } => (Some(old), None),
                    _ => (None, None),
                };
                let shadow = &shadows[&relid];
                let pk = pk_of.get(&relid).cloned().unwrap_or_default();
                match in_subtransaction(AssertUnwindSafe(|| shadow_old_row(shadow, &pk, rel, old, new))) {
                    Ok(row) => old_full = Some(row),
                    Err(e) => shadow_error = Some(e),
                }
            }

            for view in views.iter_mut() {
                if !view.active_for(relid, tx.end_lsn) {
                    continue;
                }
                if let Some(e) = &shadow_error {
                    if view.spec.is_join() {
                        view.stale = Some(format!("shadow of {} could not provide the old row: {e}", rel.name));
                        continue;
                    }
                }
                let old_ref = old_full.as_ref();
                match in_subtransaction(AssertUnwindSafe(|| plan_change(view, rel, change, old_ref))) {
                    Ok(Planned::Ops(ops)) => view.ops.extend(ops),
                    Ok(Planned::Stale(s)) => view.stale = Some(s.0),
                    Err(message) => view.failed = Some(message),
                }
            }

            if shadow_active {
                let new_full: Option<Tuple> = match change {
                    Change::Insert { new, .. } => Some(new.clone()),
                    Change::Update { new, .. } => {
                        apply::resolve_unchanged(rel, new, old_full.as_ref(), |_| true).ok()
                    }
                    _ => None,
                };
                let outcome = if let Some(e) = shadow_error.take() {
                    Err(e)
                } else if matches!(change, Change::Update { .. }) && new_full.is_none() {
                    Err("unchanged TOAST value could not be recovered from the shadow".to_string())
                } else {
                    let shadow = &shadows[&relid];
                    let pk = pk_of.get(&relid).cloned().unwrap_or_default();
                    let (old_ref, new_ref) = (old_full.as_ref(), new_full.as_ref());
                    in_subtransaction(AssertUnwindSafe(|| shadow_apply(shadow, &pk, rel, change, old_ref, new_ref)))
                };
                if let Err(message) = outcome {
                    let shadow = shadows.get_mut(&relid).expect("shadow present");
                    record_shadow_failure(shadow, &message)?;
                    for view in views.iter_mut() {
                        if view.spec.is_join() && view.relation_index(relid).is_some() && view.stale.is_none() {
                            view.stale = Some(format!("shadow of {} could not be maintained: {message}", rel.name));
                        }
                    }
                }
            }
        }
    }

    // Phase 2: write each view in its own subtransaction, then bookkeeping.
    let mut stats = ApplyStats { deltas: 0, blocked: false };
    for view in views.iter_mut() {
        if tx.end_lsn <= view.frontier {
            continue; // already absorbed (replay after a crash or a retry round)
        }
        if let Some(reason) = view.stale.take() {
            mark_stale(view, &reason)?;
            continue;
        }
        if let Some(message) = view.failed.take() {
            if record_failure(view, tx, &message)? {
                stats.blocked = true;
            }
            continue;
        }
        let ops = std::mem::take(&mut view.ops);
        let (seq_before, touched_before) = (view.last_seq, view.touched);
        let outcome = in_subtransaction(AssertUnwindSafe(|| {
            let target = ViewTarget { name: &view.name, spec: &view.spec };
            let deltas = apply::execute(&target, &ops)?;
            record_deltas(view, tx, deltas)
        }));
        match outcome {
            Ok(()) => record_success(view, tx)?,
            Err(message) => {
                view.last_seq = seq_before;
                view.touched = touched_before;
                if record_failure(view, tx, &message)? {
                    stats.blocked = true;
                }
            }
        }
    }
    for shadow in shadows.values() {
        if !shadow.failed && tx.end_lsn > shadow.frontier {
            let args: [DatumWithOid; 2] = [lsn::format(tx.end_lsn).into(), (shadow.relid as i64).into()];
            Spi::run_with_args("UPDATE nabla.shadows SET frontier_lsn = $1::pg_lsn WHERE relid = $2::oid", &args)
                .map_err(spi_err)?;
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
            Spi::run(
                "UPDATE nabla.views SET status = 'stale', \
                 stale_reason = 'replication slot lag exceeded nabla.max_slot_lag_bytes; slot dropped' \
                 WHERE status = 'live'",
            )
            .map_err(spi_err)?;
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
        if stats.blocked {
            // A live view must retry this transaction on the next poll: keep
            // the slot here and do not touch the frontiers of anyone else.
            return Ok(Round::Idle);
        }
        try_transaction(|| advance_slot(tx.end_lsn))??;
        last_end = last_end.max(tx.end_lsn);
        applied += 1;
    }

    if drained {
        // Everything up to `target` was decoded: views and shadows reflect
        // the base tables at `target`.
        let frontier = target.max(last_end);
        try_transaction(|| {
            let args: [DatumWithOid; 1] = [lsn::format(frontier).into()];
            Spi::run_with_args(
                "UPDATE nabla.views SET frontier_lsn = $1::pg_lsn WHERE status = 'live' AND frontier_lsn < $1::pg_lsn",
                &args,
            )
            .map_err(spi_err)?;
            Spi::run_with_args(
                "UPDATE nabla.shadows SET frontier_lsn = $1::pg_lsn WHERE stale_reason IS NULL AND frontier_lsn < $1::pg_lsn",
                &args,
            )
            .map_err(spi_err)?;
            advance_slot(frontier)
        })??;
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
        if BackgroundWorker::sighup_received() {
            // Pick up changed nabla.* settings (poll interval, retention, limits).
            unsafe { pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP) };
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
