// SPDX-License-Identifier: AGPL-3.0-or-later
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
//! an existing shadow without re-snapshotting it: the view is populated at a
//! consistent point F_new and skips everything at or below F_new, while an
//! older shadow at F_s < F_new keeps absorbing (F_s, F_new] on its own; from
//! F_new on both absorb the same transactions.
//!
//! Schema changes: every column is addressed by name from the pgoutput
//! Relation message. Columns a view or shadow does not use are ignored, so
//! adding, dropping or renaming unrelated columns changes nothing. A needed
//! column that disappears or changes type marks exactly the views that use
//! it stale, with a reason naming the column; a shadow drops that column from
//! its active set and keeps serving the other views.
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
use std::time::{Duration, Instant};

use crate::api::storage_name;
use crate::apply::{self, Op, Planned, ViewTarget};
use crate::definition::ViewSpec;
use crate::guc;
use crate::idle;
use crate::lsn;
use crate::pgoutput::{Change, ColumnValue, Decoder, Relation, SourceTransaction, Tuple};
use crate::populate;

const SLOT: &str = "nabla";
const PUBLICATION: &str = "nabla";
/// pgoutput tag of a complete old tuple (REPLICA IDENTITY FULL).
const FULL_OLD_TUPLE: u8 = 79; // b'O'

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
    let report = match e {
        CaughtError::PostgresError(r) | CaughtError::ErrorReport(r) => r,
        CaughtError::RustPanic { ereport, .. } => ereport,
    };
    match report.detail() {
        Some(detail) => format!("{} ({})", report.message(), detail.replace('\n', " ")),
        None => report.message().to_string(),
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
    /// The storage table behind the VIEW: where the worker writes.
    storage: String,
    spec: ViewSpec,
    frontier: u64,
    last_seq: i64,
    apply_failures: i32,
    definition: String,
    touched: bool,
    /// Planned operations for the current source transaction.
    ops: Vec<Op>,
    /// Set when a planning step failed: the view must retry the transaction.
    failed: Option<String>,
    stale: Option<String>,
    /// Stopped absorbing for the rest of the round (failed or went stale).
    dead: bool,
    /// A failure was recorded for this view in the current round; the
    /// round-end catalog update must not clear that bookkeeping.
    failed_this_round: bool,
}

impl LiveView {
    /// Position of `relid` in the view's relation list, if the view uses it.
    fn relation_index(&self, relid: u32) -> Option<usize> {
        self.spec.relations.iter().position(|r| r.oid == relid)
    }

    /// True when `old` and `new` differ in at least one column of `relid`
    /// this view uses. An unchanged-TOAST marker counts as equal. Without a
    /// full old row the answer is conservatively true.
    fn touches_used_columns(&self, relid: u32, rel: &Relation, old: Option<&Tuple>, new: &Tuple) -> bool {
        let Some(index) = self.relation_index(relid) else {
            return true;
        };
        let Some(old) = old else {
            return true;
        };
        for name in &self.spec.relations[index].used_columns {
            let Some(i) = rel.columns.iter().position(|c| &c.name == name) else {
                return true;
            };
            match (old.get(i), new.get(i)) {
                (_, Some(ColumnValue::Unchanged)) => {}
                (Some(o), Some(n)) if o == n => {}
                _ => return true,
            }
        }
        false
    }

    fn active_for(&self, relid: u32, end_lsn: u64) -> bool {
        self.failed.is_none() && self.stale.is_none() && end_lsn > self.frontier && self.relation_index(relid).is_some()
    }

    /// The first column of `relid` the view uses that the decoded relation
    /// no longer provides with the expected type.
    fn drift(&self, relid: u32, rel: &Relation) -> Option<String> {
        let index = self.relation_index(relid)?;
        let base = &self.spec.relations[index];
        for (name, ty) in base.used_columns.iter().zip(base.used_column_types.iter()) {
            let ok = rel.columns.iter().any(|c| &c.name == name && c.type_name.as_deref() == Some(ty.as_str()));
            if !ok {
                return Some(format!("column \"{name}\" of {} was dropped, renamed or changed type", base.qualified));
            }
        }
        None
    }
}

struct Shadow {
    relid: u32,
    table: String,
    frontier: u64,
    /// Maintenance failed; dependents are stale, refresh rebuilds it.
    failed: bool,
    /// Active column set (primary key plus columns some view uses) and types.
    columns: Vec<String>,
    column_types: Vec<String>,
    pk_columns: Vec<String>,
    /// Frontier moved during this round (written once at the end).
    advanced: bool,
}

fn load_live_views() -> Result<Vec<LiveView>, String> {
    Spi::connect(|client| {
        let mut views = Vec::new();
        for r in client.select(
            "SELECT id, name, spec, frontier_lsn::text, last_seq, definition, apply_failures \
             FROM nabla.views WHERE status = 'live' ORDER BY id",
            None,
            &[],
        )? {
            let spec: JsonB = r.get::<JsonB>(3)?.expect("spec");
            let spec: ViewSpec = match serde_json::from_value(spec.0) {
                Ok(s) => s,
                Err(e) => {
                    pgrx::warning!("nabla worker: skipping view with corrupt spec: {e}");
                    continue;
                }
            };
            let last_seq = r.get::<i64>(5)?.expect("last_seq");
            let id = r.get::<i32>(1)?.expect("id");
            views.push(LiveView {
                id,
                name: r.get::<String>(2)?.expect("name"),
                storage: storage_name(id),
                spec,
                frontier: lsn::parse(&r.get::<String>(4)?.expect("frontier")).unwrap_or(0),
                last_seq,
                definition: r.get::<String>(6)?.expect("definition"),
                apply_failures: r.get::<i32>(7)?.unwrap_or(0),
                touched: false,
                ops: Vec::new(),
                failed: None,
                stale: None,
                dead: false,
                failed_this_round: false,
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
            "SELECT relid::int8, table_name, frontier_lsn::text, failed, columns, column_types, pk_columns \
             FROM nabla.shadows",
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
                    columns: r.get::<Vec<String>>(5)?.unwrap_or_default(),
                    column_types: r.get::<Vec<String>>(6)?.unwrap_or_default(),
                    pk_columns: r.get::<Vec<String>>(7)?.unwrap_or_default(),
                    advanced: false,
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
    if pk_columns.is_empty() {
        return Err("no primary key columns recorded for the shadow".to_string());
    }
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
    if conds.is_empty() {
        return Err("empty key predicate".to_string());
    }
    Ok((conds.join(" AND "), values))
}

/// Indexes (in the decoded relation) of the shadow's active columns.
fn shadow_column_indexes(shadow: &Shadow, rel: &Relation) -> Vec<usize> {
    shadow
        .columns
        .iter()
        .filter_map(|name| rel.columns.iter().position(|c| &c.name == name))
        .collect()
}

/// The full old row of a change, read from the shadow by primary key, in the
/// decoded relation's column order (NULL for columns the shadow does not hold).
fn shadow_old_row(shadow: &Shadow, rel: &Relation, old: Option<&Tuple>, new: Option<&Tuple>) -> Result<Tuple, String> {
    let keys = key_values(&shadow.pk_columns, rel, old, new)?;
    let (conds, values) = key_predicate(rel, &keys, 1)?;
    let indexes = shadow_column_indexes(shadow, rel);
    let cols: Vec<String> = indexes.iter().map(|i| format!("{}::text", quote_identifier(&rel.columns[*i].name))).collect();
    let sql = format!("SELECT {} FROM {} WHERE {conds}", cols.join(", "), shadow.table);
    let args: Vec<DatumWithOid> = values.into_iter().map(|v| v.into()).collect();
    // Mutable SPI on purpose: the read-only path keeps the command id of the
    // snapshot pushed at transaction start and would not see a row this very
    // transaction inserted into the shadow (insert then delete of the same
    // row inside one source transaction).
    let row = Spi::connect_mut(|client| {
        let table = client.update(&sql, Some(1), &args)?;
        if table.is_empty() {
            return Ok(None);
        }
        let table = table.first();
        let mut tuple = vec![ColumnValue::Null; rel.columns.len()];
        for (n, idx) in indexes.iter().enumerate() {
            tuple[*idx] = match table.get::<String>(n + 1)? {
                Some(s) => ColumnValue::Text(s),
                None => ColumnValue::Null,
            };
        }
        Ok(Some(tuple))
    })
    .map_err(spi_err)?;
    row.ok_or_else(|| format!("row not found in shadow {} (shadow out of sync with the base table)", shadow.table))
}

/// Apply a change to the shadow: insert, delete by key, or delete + insert,
/// touching only the shadow's active columns (by name).
fn shadow_apply(shadow: &Shadow, rel: &Relation, change: &Change, old_full: Option<&Tuple>, new_full: Option<&Tuple>) -> Result<(), String> {
    let delete = |old: Option<&Tuple>, new: Option<&Tuple>| -> Result<(), String> {
        let keys = key_values(&shadow.pk_columns, rel, old, new)?;
        let (conds, values) = key_predicate(rel, &keys, 1)?;
        let args: Vec<DatumWithOid> = values.into_iter().map(|v| v.into()).collect();
        apply::execute_cached(&format!("DELETE FROM {} WHERE {conds}", shadow.table), &args)
    };
    let insert = |row: &Tuple| -> Result<(), String> {
        let indexes = shadow_column_indexes(shadow, rel);
        if indexes.is_empty() {
            return Err("the shadow has no active column present in the decoded row".to_string());
        }
        let mut cols = Vec::new();
        let mut casts = Vec::new();
        let mut args: Vec<DatumWithOid> = Vec::new();
        for (n, idx) in indexes.iter().enumerate() {
            let c = &rel.columns[*idx];
            let ty = c.type_name.as_deref().ok_or("type not resolved")?;
            cols.push(quote_identifier(&c.name));
            casts.push(format!("${}::{}", n + 1, ty));
            args.push(match &row[*idx] {
                ColumnValue::Text(s) => Some(s.clone()).into(),
                _ => Option::<String>::None.into(),
            });
        }
        apply::execute_cached(
            &format!("INSERT INTO {} ({}) VALUES ({})", shadow.table, cols.join(", "), casts.join(", ")),
            &args,
        )
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

/// Compare the decoded relation with the shadow's active columns. Returns the
/// columns that disappeared or changed type. Those are removed from the
/// active set unless a primary key column is among them, which makes the
/// shadow unmaintainable.
fn shadow_drift(shadow: &mut Shadow, rel: &Relation, qualified: &str) -> Result<Vec<String>, String> {
    let mut drifted = Vec::new();
    for (name, ty) in shadow.columns.iter().zip(shadow.column_types.iter()) {
        let ok = rel.columns.iter().any(|c| &c.name == name && c.type_name.as_deref() == Some(ty.as_str()));
        if !ok {
            drifted.push(name.clone());
        }
    }
    if drifted.is_empty() {
        return Ok(drifted);
    }
    let reason = drifted
        .iter()
        .map(|c| format!("column \"{c}\" of {qualified} was dropped, renamed or changed type"))
        .collect::<Vec<_>>()
        .join("; ");
    if drifted.iter().any(|c| shadow.pk_columns.contains(c)) {
        record_shadow_failure(shadow, &reason)?;
        return Ok(drifted);
    }
    let keep: Vec<(String, String)> = shadow
        .columns
        .iter()
        .zip(shadow.column_types.iter())
        .filter(|(c, _)| !drifted.contains(c))
        .map(|(c, t)| (c.clone(), t.clone()))
        .collect();
    let (names, types): (Vec<String>, Vec<String>) = keep.into_iter().unzip();
    shadow.columns = names.clone();
    shadow.column_types = types.clone();
    let args: [DatumWithOid; 4] = [(shadow.relid as i64).into(), names.into(), types.into(), reason.as_str().into()];
    Spi::run_with_args(
        "UPDATE nabla.shadows SET columns = $2, column_types = $3, stale_reason = $4 WHERE relid = $1::oid",
        &args,
    )
    .map_err(spi_err)?;
    pgrx::warning!("nabla worker: shadow {} no longer carries: {reason}", shadow.table);
    Ok(drifted)
}

// --- catalog bookkeeping -------------------------------------------------------

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

fn record_shadow_failure(shadow: &mut Shadow, message: &str) -> Result<(), String> {
    shadow.failed = true;
    let reason = format!("shadow {} could not be maintained: {message}", shadow.table);
    pgrx::warning!("nabla worker: {reason}");
    let args: [DatumWithOid; 2] = [(shadow.relid as i64).into(), reason.as_str().into()];
    Spi::run_with_args("UPDATE nabla.shadows SET stale_reason = $2, failed = true WHERE relid = $1::oid", &args)
        .map_err(spi_err)
}

// --- planning one change for one view ----------------------------------------------

/// Plan the effect of one change on one view. `old_full` is the complete old
/// row when it came from a shadow.
fn plan_change(view: &mut LiveView, rel: &Relation, change: &Change, old_full: Option<&Tuple>) -> Result<Planned, String> {
    let target = ViewTarget { name: &view.storage, spec: &view.spec };
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
                if !view.touches_used_columns(rel.id, rel, Some(old), new) {
                    return Ok(Planned::Ops(ops)); // only unused columns changed
                }
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
                if let Some((kind, full)) = old {
                    if *kind == FULL_OLD_TUPLE && !view.touches_used_columns(rel.id, rel, Some(full), new) {
                        return Ok(Planned::Ops(ops)); // only unused columns changed
                    }
                }
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

/// One delta row waiting for the round's batched insert.
struct PendingDelta {
    seq: i64,
    lsn: u64,
    xid: u32,
    op: char,
    row_json: String,
}

struct RoundStats {
    deltas: usize,
    /// Source transactions processed in this round (a blocked round stops
    /// after the transaction that failed for some live view).
    processed: usize,
    /// Index of the first transaction some live view could not absorb.
    blocked_at: Option<usize>,
}

fn record_shadow_failure_for_views(views: &mut [LiveView], relid: u32, rel: &Relation, message: &str) {
    for view in views.iter_mut() {
        if view.spec.is_join() && view.relation_index(relid).is_some() && view.stale.is_none() && !view.dead {
            view.stale = Some(format!("shadow of {} could not be maintained: {message}", rel.name));
        }
    }
}

/// Append a view's netted deltas of the round in one multi-row INSERT
/// (chunked to stay far below the parameter limit).
fn insert_deltas(view_id: i32, deltas: &[PendingDelta]) -> Result<(), String> {
    for chunk in deltas.chunks(200) {
        let mut values = Vec::with_capacity(chunk.len());
        let mut args: Vec<DatumWithOid> = Vec::with_capacity(chunk.len() * 6);
        for (i, d) in chunk.iter().enumerate() {
            let b = i * 6;
            values.push(format!(
                "(${}, ${}, ${}::pg_lsn, ${}, ${}::\"char\", ${}::jsonb)",
                b + 1, b + 2, b + 3, b + 4, b + 5, b + 6
            ));
            args.push(view_id.into());
            args.push(d.seq.into());
            args.push(lsn::format(d.lsn).into());
            args.push((d.xid as i64).into());
            args.push(d.op.to_string().into());
            args.push(d.row_json.clone().into());
        }
        Spi::run_with_args(
            &format!("INSERT INTO nabla.deltas (view_id, seq, lsn, xid, op, row) VALUES {}", values.join(", ")),
            &args,
        )
        .map_err(spi_err)?;
    }
    Ok(())
}

/// Apply every complete source transaction of a peek in ONE worker
/// transaction, in commit order.
///
/// Why one transaction per round is correct under deferred transactional
/// consistency: a reader only ever sees the view at the state it had after
/// the last applied source transaction, which is a committed snapshot of the
/// base tables. Committing once per round means readers skip the
/// intermediate committed states of the round; they never see a state that
/// was not committed. The frontier still means "reflects the base tables at
/// this LSN". Per-source-transaction delta batches and netting are unchanged:
/// each source transaction is planned, executed and netted on its own; only
/// the catalog bookkeeping (frontier update, delta insert, notification,
/// garbage collection) happens once per view per round, and the slot is
/// advanced once per round.
///
/// Failure isolation: each view's execution of each source transaction runs
/// in its own subtransaction, so a failing view rolls back alone; it counts
/// one failure per round and stops absorbing at the failing transaction,
/// while the other views absorb the whole peek. The slot is then held right
/// before the failing transaction, which is retried on the next poll.
fn apply_round(decoder: &mut Decoder, txs: &[SourceTransaction]) -> Result<RoundStats, String> {
    Spi::run("SET LOCAL nabla.internal_write = on").map_err(spi_err)?;
    let mut views = load_live_views()?;
    // Lock order storage table -> shadows -> catalog, the same order a DROP of
    // a view takes (ACCESS EXCLUSIVE on the storage table, then its sql_drop
    // trigger touches shadows and catalog rows). Taking the storage locks
    // first keeps the worker and concurrent DDL from deadlocking.
    for view in &views {
        Spi::run(&format!("LOCK TABLE {} IN ROW EXCLUSIVE MODE", view.storage)).map_err(spi_err)?;
    }
    let mut shadows = load_shadows()?;

    let mut relations: HashMap<u32, Relation> = HashMap::new();
    for tx in txs {
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
    }
    let round_end = txs.last().map_or(0, |t| t.end_lsn);

    // Schema drift, once per relation: views lose exactly the columns they
    // use; shadows drop the columns from their active set (or fail on a key).
    for (relid, rel) in &relations {
        let qualified = views
            .iter()
            .find_map(|v| v.spec.relations.iter().find(|r| r.oid == *relid).map(|r| r.qualified.clone()))
            .unwrap_or_else(|| format!("{}.{}", rel.namespace, rel.name));
        if let Some(shadow) = shadows.get_mut(relid) {
            if !shadow.failed && round_end > shadow.frontier {
                shadow_drift(shadow, rel, &qualified)?;
            }
        }
        for view in views.iter_mut() {
            if view.stale.is_none() && round_end > view.frontier {
                if let Some(reason) = view.drift(*relid, rel) {
                    view.stale = Some(reason);
                }
            }
        }
    }

    let mut pending: Vec<Vec<PendingDelta>> = views.iter().map(|_| Vec::new()).collect();
    let mut absorbed: Vec<Option<u64>> = vec![None; views.len()];
    let mut stats = RoundStats { deltas: 0, processed: 0, blocked_at: None };

    for (ti, tx) in txs.iter().enumerate() {
        // Phase 1: plan per change, maintain shadows in stream order.
        for change in &tx.changes {
            for relid in change_relids(change) {
                let rel = &relations[&relid];
                let shadow_active = shadows.get(&relid).map_or(false, |s| !s.failed && tx.end_lsn > s.frontier);

                let mut old_full: Option<Tuple> = None;
                let mut shadow_error: Option<String> = None;
                if shadow_active && matches!(change, Change::Update { .. } | Change::Delete { .. }) {
                    let (old, new): (Option<&Tuple>, Option<&Tuple>) = match change {
                        Change::Update { old, new, .. } => (old.as_ref().map(|(_, t)| t), Some(new)),
                        Change::Delete { old, .. } => (Some(old), None),
                        _ => (None, None),
                    };
                    let shadow = &shadows[&relid];
                    match in_subtransaction(AssertUnwindSafe(|| shadow_old_row(shadow, rel, old, new))) {
                        Ok(row) => old_full = Some(row),
                        Err(e) => shadow_error = Some(e),
                    }
                }

                for view in views.iter_mut() {
                    if view.dead || !view.active_for(relid, tx.end_lsn) {
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
                        let (old_ref, new_ref) = (old_full.as_ref(), new_full.as_ref());
                        in_subtransaction(AssertUnwindSafe(|| shadow_apply(shadow, rel, change, old_ref, new_ref)))
                    };
                    if let Err(message) = outcome {
                        let shadow = shadows.get_mut(&relid).expect("shadow present");
                        record_shadow_failure(shadow, &message)?;
                        record_shadow_failure_for_views(&mut views, relid, rel, &message);
                    }
                }
            }
        }

        // Phase 2: execute each view's planned rows in its own subtransaction.
        for (vi, view) in views.iter_mut().enumerate() {
            if view.dead || tx.end_lsn <= view.frontier {
                continue; // dead for this round, or already absorbed
            }
            if let Some(reason) = view.stale.take() {
                mark_stale(view, &reason)?;
                view.dead = true;
                continue;
            }
            if let Some(message) = view.failed.take() {
                if record_failure(view, tx, &message)? {
                    stats.blocked_at.get_or_insert(ti);
                }
                view.failed_this_round = true;
                view.dead = true;
                continue;
            }
            let ops = std::mem::take(&mut view.ops);
            let seq_before = view.last_seq;
            let outcome = in_subtransaction(AssertUnwindSafe(|| {
                let target = ViewTarget { name: &view.storage, spec: &view.spec };
                // Storage writes stay per change; the log gets the transaction's
                // net effect per identity key (see apply::net).
                Ok(apply::net(&view.spec, apply::execute(&target, &ops)?))
            }));
            match outcome {
                Ok(deltas) => {
                    for d in deltas {
                        view.last_seq += 1;
                        pending[vi].push(PendingDelta { seq: view.last_seq, lsn: tx.end_lsn, xid: tx.xid, op: d.op, row_json: d.row_json });
                    }
                    view.touched = view.touched || view.last_seq > seq_before;
                    view.frontier = tx.end_lsn;
                    absorbed[vi] = Some(tx.end_lsn);
                }
                Err(message) => {
                    view.last_seq = seq_before;
                    if record_failure(view, tx, &message)? {
                        stats.blocked_at.get_or_insert(ti);
                    }
                    view.failed_this_round = true;
                    view.dead = true;
                }
            }
        }
        for shadow in shadows.values_mut() {
            if !shadow.failed && tx.end_lsn > shadow.frontier {
                shadow.frontier = tx.end_lsn;
                shadow.advanced = true;
            }
        }
        stats.processed = ti + 1;
        // A blocked view stops absorbing (it is dead for the round) but the
        // healthy views go on to the end of the peek; the slot is held right
        // before the failing transaction and they skip the replay through
        // their frontier.
    }

    // Once per view per round: frontier, bookkeeping reset, deltas, GC, notify.
    for (vi, view) in views.iter().enumerate() {
        let Some(end) = absorbed[vi] else {
            continue;
        };
        let args: [DatumWithOid; 3] = [lsn::format(end).into(), view.last_seq.into(), view.id.into()];
        // A view that absorbed part of the round and then failed keeps the
        // failure bookkeeping record_failure just wrote; a clean round resets it.
        let sql = if view.failed_this_round {
            "UPDATE nabla.views SET frontier_lsn = $1::pg_lsn, last_seq = $2 WHERE id = $3"
        } else {
            "UPDATE nabla.views SET frontier_lsn = $1::pg_lsn, last_seq = $2, \
             apply_failures = 0, last_error = NULL, last_error_at = NULL WHERE id = $3"
        };
        Spi::run_with_args(sql, &args).map_err(spi_err)?;
        if pending[vi].is_empty() {
            continue;
        }
        insert_deltas(view.id, &pending[vi])?;
        stats.deltas += pending[vi].len();
        let retain = guc::RETAIN_DELTAS.get() as i64;
        let gc: [DatumWithOid; 2] = [view.id.into(), (view.last_seq - retain).into()];
        Spi::run_with_args("DELETE FROM nabla.deltas WHERE view_id = $1 AND seq <= $2", &gc).map_err(spi_err)?;
        let last = pending[vi].last().expect("non-empty");
        let channel = format!("nabla:{}", view.name);
        let payload = format!("{}:{}", last.seq, lsn::format(last.lsn));
        let notify: [DatumWithOid; 2] = [channel.into(), payload.into()];
        Spi::run_with_args("SELECT pg_catalog.pg_notify($1, $2)", &notify).map_err(spi_err)?;
    }
    for shadow in shadows.values() {
        if shadow.advanced {
            let args: [DatumWithOid; 2] = [lsn::format(shadow.frontier).into(), (shadow.relid as i64).into()];
            Spi::run_with_args("UPDATE nabla.shadows SET frontier_lsn = $1::pg_lsn WHERE relid = $2::oid", &args)
                .map_err(spi_err)?;
        }
    }
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
    /// Frontier every live object was advanced to by the last idle advance.
    last_advance: u64,
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

    // Build or rebuild views that create_view/refresh queued, each group
    // under its own consistent snapshot (populate.rs), before decoding.
    populate::run_pending(&|body| try_transaction(|| body()).and_then(|r| r))?;

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
    let round_started = Instant::now();
    let (target, rows) = try_transaction(|| {
        let target = select_one::<String>("SELECT pg_catalog.pg_current_wal_flush_lsn()::text", &[])?
            .and_then(|t| lsn::parse(&t))
            .ok_or("could not read flush lsn")?;
        if target <= confirmed {
            return Ok::<_, String>((target, Vec::new()));
        }
        // PostgreSQL never splits a transaction across upto_nchanges (it is
        // checked between WAL records), so a large transaction simply returns
        // more rows than the limit.
        let limit = guc::BATCH_CHANGES.get().max(1) as i64;
        let args: [DatumWithOid; 2] = [SLOT.into(), lsn::format(target).into()];
        let rows = Spi::connect(|client| {
            let mut rows: Vec<Vec<u8>> = Vec::new();
            for r in client.select(
                &format!(
                    "SELECT data FROM pg_catalog.pg_logical_slot_peek_binary_changes($1, $2::pg_lsn, {limit}, \
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

    let drained = (rows.len() as i64) < guc::BATCH_CHANGES.get().max(1) as i64;
    if rows.is_empty() && target == state.self_flush {
        // Nothing decodable and no WAL beyond our own last commit: the
        // range (last_advance, target] holds no published change, so a
        // view at last_advance also reflects the base tables at target.
        if state.last_advance > 0 {
            idle::publish(state.last_advance, target);
        }
        return Ok(Round::Idle);
    }
    for data in &rows {
        state.decoder.feed(data)?;
    }
    if state.decoder.has_incomplete() {
        pgrx::warning!("nabla worker: discarding an incomplete trailing transaction; it will be re-read");
        state.decoder.discard_incomplete();
    }

    let peek_ms = round_started.elapsed().as_millis();
    let transactions = state.decoder.take_complete();
    let mut applied = 0usize;
    let mut total_deltas = 0usize;
    let mut last_end = confirmed;
    let mut apply_ms = 0u128;
    if !transactions.is_empty() {
        let decoder = &mut state.decoder;
        let apply_started = Instant::now();
        let stats = try_transaction(AssertUnwindSafe(|| apply_round(decoder, &transactions)))??;
        apply_ms = apply_started.elapsed().as_millis();
        total_deltas += stats.deltas;
        applied = stats.processed;
        if let Some(k) = stats.blocked_at {
            // A live view must retry transaction k on the next poll: advance
            // the slot to just before it (the healthy views absorbed the whole
            // peek and skip the replay through their frontier). No idle
            // advance: the blocked view does not reflect `target`.
            if k > 0 {
                let end = transactions[k - 1].end_lsn;
                try_transaction(|| advance_slot(end))??;
            }
            return Ok(Round::Idle);
        }
        // One slot advance per round, to the end of the last applied transaction.
        last_end = last_end.max(transactions[applied - 1].end_lsn);
        let end = last_end;
        try_transaction(|| advance_slot(end))??;
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
                "UPDATE nabla.shadows SET frontier_lsn = $1::pg_lsn WHERE NOT failed AND frontier_lsn < $1::pg_lsn",
                &args,
            )
            .map_err(spi_err)?;
            advance_slot(frontier)
        })??;
        state.self_flush = try_transaction(|| {
            select_one::<String>("SELECT pg_catalog.pg_current_wal_flush_lsn()::text", &[])
                .map(|t| t.and_then(|t| lsn::parse(&t)).unwrap_or(0))
        })??;
        state.last_advance = frontier;
        idle::publish(frontier, frontier);
    }
    if applied > 0 {
        pgrx::log!(
            "nabla worker: applied {applied} transaction(s), {total_deltas} delta(s), frontier {} \
             (peek+decode {peek_ms} ms, apply {apply_ms} ms, advance {} ms)",
            lsn::format(target.max(last_end)),
            round_started.elapsed().as_millis() - peek_ms - apply_ms
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

    let mut state = WorkerState { decoder: Decoder::default(), self_flush: 0, last_advance: 0, reported_missing_extension: false };
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
