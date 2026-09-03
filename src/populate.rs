// SPDX-License-Identifier: AGPL-3.0-or-later
//! Asynchronous population of views: `create_view` and `refresh` only record
//! intent; the worker builds the table here, under a snapshot that is exactly
//! consistent with a logical-decoding consistent point, the way PostgreSQL's
//! own table synchronization does it.
//!
//! Mechanism, mirroring walsender's `CREATE_REPLICATION_SLOT ... USE_SNAPSHOT`:
//! start a REPEATABLE READ transaction that has not taken a snapshot yet,
//! create a temporary logical slot, let `DecodingContextFindStartpoint` find
//! a consistent point (it waits for transactions that were already running to
//! finish; it never blocks them), build the slot's initial snapshot and
//! install it as the transaction snapshot. Every transaction whose commit is
//! at or below the consistent point is visible to the population queries;
//! every later one is decoded by the main slot, whose `confirmed_flush_lsn`
//! is at or below the consistent point (the main slot only advances from this
//! very process, and only after this step). The view's frontier is set to the
//! consistent point, so the existing skip rule (`end_lsn <= frontier`) makes
//! the two halves meet exactly.
//!
//! A rebuild (refresh) re-validates the stored definition text first, so a
//! definition that no longer resolves (dropped column, renamed table) fails
//! with PostgreSQL's own error, and the regenerated spec picks up type
//! changes. View tables and shadow tables are recorded in `pg_depend` as
//! depending on their base tables, like SQL views.

use pgrx::datum::{DatumWithOid, JsonB};
use pgrx::pg_sys::panic::CaughtError;
use pgrx::pg_sys::pg_try::PgTryBuilder;
use pgrx::prelude::*;
use pgrx::spi::quote_identifier;
use std::collections::BTreeSet;
use std::ffi::{c_int, c_void, CString};
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use crate::api::storage_name;
use crate::definition::{self, Shape, ViewSpec};
use crate::guc;
use crate::lsn;
use crate::shadow;

extern "C-unwind" {
    /// replication/snapbuild.h is not covered by pgrx's bindings; the symbol
    /// is exported by the server binary.
    fn SnapBuildInitialSnapshot(builder: *mut pg_sys::SnapBuild) -> pg_sys::Snapshot;
    /// pgrx wraps these in Rust-ABI shims; the XLogReaderRoutine needs the raw
    /// C entry points (access/xlogutils.h).
    fn read_local_xlog_page(
        state: *mut pg_sys::XLogReaderState,
        target_page_ptr: pg_sys::XLogRecPtr,
        req_len: c_int,
        target_rec_ptr: pg_sys::XLogRecPtr,
        cur_page: *mut std::ffi::c_char,
    ) -> c_int;
    fn wal_segment_open(
        state: *mut pg_sys::XLogReaderState,
        next_seg_no: pg_sys::XLogSegNo,
        tli_p: *mut pg_sys::TimeLineID,
    );
    fn wal_segment_close(state: *mut pg_sys::XLogReaderState);
}

fn describe(e: &CaughtError) -> String {
    match e {
        CaughtError::PostgresError(r) | CaughtError::ErrorReport(r) => r.message().to_string(),
        CaughtError::RustPanic { ereport, .. } => ereport.message().to_string(),
    }
}

fn spi_err(e: pgrx::spi::Error) -> String {
    e.to_string()
}

/// Run `body` in a subtransaction; a PostgreSQL error rolls it back and is
/// returned as a message.
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

/// A view waiting to be built or rebuilt.
pub struct Pending {
    pub id: i32,
    pub name: String,
    pub status: String,
    pub definition: String,
    pub spec: ViewSpec,
    /// A build has committed before (shadow references exist, epoch bumps).
    pub populated: bool,
}

/// DDL that turns a freshly populated table into a nabla-managed view table.
pub fn view_ddl(name: &str, spec: &ViewSpec) -> Vec<String> {
    let cols: Vec<String> = match spec.shape {
        Shape::Projection => spec.pk_view_columns.iter().map(quote_identifier).collect(),
        Shape::Aggregate => spec.columns.iter().map(|c| quote_identifier(&c.alias)).collect(),
    };
    // GROUP BY treats NULLs as equal, so the group key index must as well.
    let nulls = if spec.shape == Shape::Aggregate { " NULLS NOT DISTINCT" } else { "" };
    vec![
        format!("CREATE UNIQUE INDEX ON {name} ({}){nulls}", cols.join(", ")),
        format!(
            "CREATE TRIGGER nabla_guard BEFORE INSERT OR UPDATE OR DELETE ON {name} \
             FOR EACH ROW EXECUTE FUNCTION nabla.guard_view()"
        ),
        format!(
            "CREATE TRIGGER nabla_guard_truncate BEFORE TRUNCATE ON {name} \
             FOR EACH STATEMENT EXECUTE FUNCTION nabla.guard_view()"
        ),
    ]
}

pub fn load_pending() -> Result<Vec<Pending>, String> {
    Spi::connect(|client| {
        let mut out = Vec::new();
        for r in client.select(
            "SELECT id, name, status, spec, populated_at IS NOT NULL, definition FROM nabla.views \
             WHERE status IN ('initializing', 'refreshing') ORDER BY id",
            None,
            &[],
        )? {
            let spec: JsonB = r.get::<JsonB>(4)?.expect("spec");
            let spec: ViewSpec = match serde_json::from_value(spec.0) {
                Ok(s) => s,
                Err(_) => continue,
            };
            out.push(Pending {
                id: r.get::<i32>(1)?.expect("id"),
                name: r.get::<String>(2)?.expect("name"),
                status: r.get::<String>(3)?.expect("status"),
                spec,
                populated: r.get::<bool>(5)?.unwrap_or(false),
                definition: r.get::<String>(6)?.unwrap_or_default(),
            });
        }
        Ok(out)
    })
    .map_err(spi_err)
}

/// Views rebuilt together: refreshing join views that share a shadow must be
/// re-snapshotted from the same consistent point (see `refresh_closure` in
/// api.rs); everything else is built on its own.
pub fn group(pending: Vec<Pending>) -> Vec<Vec<Pending>> {
    let mut groups: Vec<Vec<Pending>> = Vec::new();
    let mut group_rels: Vec<BTreeSet<u32>> = Vec::new();
    for view in pending {
        let shared = view.status == "refreshing" && view.spec.is_join();
        let rels: BTreeSet<u32> = view.spec.relations.iter().map(|r| r.oid).collect();
        let target = if shared { group_rels.iter().position(|g| !g.is_disjoint(&rels)) } else { None };
        match target {
            Some(i) => {
                group_rels[i].extend(rels);
                groups[i].push(view);
            }
            None => {
                group_rels.push(if shared { rels } else { BTreeSet::new() });
                groups.push(vec![view]);
            }
        }
    }
    groups
}

fn run(sql: &str) -> Result<(), String> {
    Spi::run(sql).map_err(spi_err)
}

fn run_args(sql: &str, args: &[DatumWithOid]) -> Result<(), String> {
    Spi::run_with_args(sql, args).map_err(spi_err)
}

fn read_text(sql: &str, args: &[DatumWithOid]) -> Result<Option<String>, String> {
    Spi::connect(|client| {
        let table = client.select(sql, Some(1), args)?;
        if table.is_empty() {
            Ok(None)
        } else {
            table.first().get_one::<String>()
        }
    })
    .map_err(spi_err)
}

/// What a shadow of `relid` must hold: the union of `used_columns` and of
/// `join_keys` over every view that references the table (any status),
/// plus this view's own.
fn needed_columns(relid: u32, own: &ViewSpec) -> Result<(BTreeSet<String>, BTreeSet<String>), String> {
    let mut needed: BTreeSet<String> = BTreeSet::new();
    let mut keys: BTreeSet<String> = BTreeSet::new();
    let specs: Vec<JsonB> = Spi::connect(|client| {
        let mut out = Vec::new();
        for r in client.select(
            "SELECT v.spec FROM nabla.views v JOIN nabla.view_relations vr ON vr.view_id = v.id \
             WHERE vr.relid = $1::oid",
            None,
            &[(relid as i64).into()],
        )? {
            if let Some(s) = r.get::<JsonB>(1)? {
                out.push(s);
            }
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .map_err(spi_err)?;
    for s in specs {
        if let Ok(spec) = serde_json::from_value::<ViewSpec>(s.0) {
            for rel in spec.relations.iter().filter(|r| r.oid == relid) {
                needed.extend(rel.used_columns.iter().cloned());
                keys.extend(rel.join_keys.iter().cloned());
            }
        }
    }
    for rel in own.relations.iter().filter(|r| r.oid == relid) {
        needed.extend(rel.used_columns.iter().cloned());
        keys.extend(rel.join_keys.iter().cloned());
    }
    Ok((needed, keys))
}

/// Build (or rebuild) one view of a group inside the population transaction.
fn build(view: &Pending, consistent_point: u64, shadows_done: &mut BTreeSet<u32>) -> Result<(), String> {
    run("SET LOCAL nabla.internal_write = on")?;
    let rebuild = view.status == "refreshing" && view.populated;

    // A rebuild re-validates the definition against the current schema and
    // stores the regenerated spec (types, used columns). A definition that
    // no longer resolves fails this view alone (NB006 via await_ready); the
    // other members of the group are still rebuilt.
    let spec = if rebuild {
        let definition = view.definition.clone();
        match in_subtransaction(move || Ok(definition::validate(&definition).0)) {
            Ok(spec) => {
                let json = serde_json::to_string(&spec).map_err(|e| e.to_string())?;
                run_args(
                    "UPDATE nabla.views SET spec = $2::jsonb WHERE id = $1",
                    &[view.id.into(), json.as_str().into()],
                )?;
                spec
            }
            Err(message) => {
                pgrx::warning!("nabla worker: rebuild of {} failed: {message}", view.name);
                let args: [DatumWithOid; 2] = [view.id.into(), message.as_str().into()];
                run_args(
                    "UPDATE nabla.views SET status = 'failed', last_error = $2, last_error_at = now() WHERE id = $1",
                    &args,
                )?;
                return Ok(());
            }
        }
    } else {
        view.spec.clone()
    };

    if spec.is_join() {
        for rel in &spec.relations {
            if !shadows_done.insert(rel.oid) {
                continue;
            }
            let (needed, keys) = needed_columns(rel.oid, &spec)?;
            if rebuild {
                // Every shadow of the group is rebuilt from this point.
                shadow::rebuild(rel, consistent_point, &needed, &keys);
            } else {
                // First build: existing healthy shadows are extended, never re-snapshotted.
                shadow::ensure(rel, consistent_point, &needed, &keys);
            }
        }
    }
    // Storage table nabla_store.v<id> (user columns + hidden maintenance
    // columns) behind a plain VIEW <schema>.<name> exposing the visible
    // columns. The VIEW is created once and survives every rebuild.
    let storage = storage_name(view.id);
    let exists = read_text("SELECT pg_catalog.to_regclass($1)::text", &[storage.as_str().into()])?.is_some();
    if exists {
        // DELETE, not TRUNCATE: readers keep seeing the old rows under MVCC
        // until this transaction commits, never an empty table.
        run(&format!("DELETE FROM {storage}"))?;
        run(&format!("INSERT INTO {storage} {}", spec.populate_sql))?;
    } else {
        run(&format!("CREATE TABLE {storage} AS {}", spec.populate_sql))?;
        for ddl in view_ddl(&storage, &spec) {
            run(&ddl)?;
        }
        // The storage table depends on its base tables, like a SQL view
        // does; PostgreSQL records the VIEW's dependency on the storage.
        if let Some(storage_oid) = shadow::relation_oid(&storage) {
            for rel in &spec.relations {
                shadow::record_dependency(storage_oid, rel.oid);
            }
        }
        let columns: Vec<String> = spec.visible_columns.iter().map(quote_identifier).collect();
        run(&format!("CREATE VIEW {} AS SELECT {} FROM {storage}", view.name, columns.join(", ")))?;
        run_args(
            "UPDATE nabla.views SET relid = pg_catalog.to_regclass($2)::oid WHERE id = $1",
            &[view.id.into(), view.name.as_str().into()],
        )?;
    }
    let bump: i32 = if rebuild { 1 } else { 0 };
    let args: [DatumWithOid; 3] = [lsn::format(consistent_point).into(), bump.into(), view.id.into()];
    run_args(
        "UPDATE nabla.views SET frontier_lsn = $1::pg_lsn, epoch = epoch + $2, status = 'live', \
         apply_failures = 0, last_error = NULL, last_error_at = NULL, stale_reason = NULL, \
         populated_at = coalesce(populated_at, now()) WHERE id = $3",
        &args,
    )?;
    run_args("DELETE FROM nabla.deltas WHERE view_id = $1", &[view.id.into()])
}

/// Populate one group in one transaction under a consistent snapshot.
/// Returns the consistent point.
pub fn populate_group(group: &[Pending]) -> Result<u64, String> {
    let slot_name = CString::new(format!("nabla_init_{}", group[0].id)).expect("slot name");
    unsafe {
        pg_sys::SetCurrentStatementStartTimestamp();
        pg_sys::StartTransactionCommand();
        // Must precede the first snapshot: only REPEATABLE READ keeps the
        // restored snapshot for every statement of the transaction.
        pg_sys::XactIsoLevel = pg_sys::XACT_REPEATABLE_READ as c_int;
    }
    let slot_ptr = slot_name.as_ptr();
    let result: Result<u64, String> = PgTryBuilder::new(AssertUnwindSafe(|| {
        let consistent_point = unsafe {
            if pg_sys::FirstSnapshotSet {
                return Err("a snapshot was already taken in the population transaction".to_string());
            }
            pg_sys::ReplicationSlotCreate(
                slot_ptr,
                true,
                pg_sys::ReplicationSlotPersistency::RS_TEMPORARY,
                false,
                false,
                false,
            );
            let mut routine = pg_sys::XLogReaderRoutine {
                page_read: Some(read_local_xlog_page),
                segment_open: Some(wal_segment_open),
                segment_close: Some(wal_segment_close),
            };
            let ctx = pg_sys::CreateInitDecodingContext(
                c"pgoutput".as_ptr(),
                std::ptr::null_mut(),
                true,
                pg_sys::InvalidXLogRecPtr as pg_sys::XLogRecPtr,
                &mut routine,
                None,
                None,
                None,
            );
            // Waits for transactions that were already running to end; never
            // blocks them.
            pg_sys::DecodingContextFindStartpoint(ctx);
            let snapshot = SnapBuildInitialSnapshot((*ctx).snapshot_builder);
            pg_sys::RestoreTransactionSnapshot(snapshot, pg_sys::MyProc as *mut c_void);
            let consistent_point = (*pg_sys::MyReplicationSlot).data.confirmed_flush;
            pg_sys::FreeDecodingContext(ctx);
            pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
            consistent_point
        };

        // Invariant: the main slot has not advanced past the consistent point,
        // so everything after it will still be decoded for these views.
        let main = read_text(
            "SELECT confirmed_flush_lsn::text FROM pg_catalog.pg_replication_slots WHERE slot_name = 'nabla'",
            &[],
        )?
        .and_then(|t| lsn::parse(&t));
        if let Some(main) = main {
            if main > consistent_point {
                return Err(format!(
                    "main slot at {} is past the consistent point {}",
                    lsn::format(main),
                    lsn::format(consistent_point)
                ));
            }
        }

        let delay = guc::DEBUG_POPULATE_DELAY_MS.get();
        if delay > 0 {
            // Test hook: hold the snapshot (and the temporary slot) for a while.
            let mut left = Duration::from_millis(delay as u64);
            while !left.is_zero() {
                let step = left.min(Duration::from_millis(100));
                std::thread::sleep(step);
                left -= step;
                pgrx::pg_sys::check_for_interrupts!();
            }
        }

        let mut shadows_done = BTreeSet::new();
        for view in group {
            build(view, consistent_point, &mut shadows_done)?;
        }
        unsafe { pg_sys::PopActiveSnapshot() };
        Ok(consistent_point)
    }))
    .catch_others(|e| Err(describe(&e)))
    .execute();

    unsafe {
        match &result {
            Ok(_) => pg_sys::CommitTransactionCommand(),
            Err(_) => pg_sys::AbortCurrentTransaction(), // also drops temporary slots
        }
        // Drop the temporary slot explicitly rather than at process exit.
        if !pg_sys::MyReplicationSlot.is_null() {
            pg_sys::ReplicationSlotRelease();
        }
        PgTryBuilder::new(AssertUnwindSafe(|| {
            pg_sys::ReplicationSlotDrop(slot_ptr, true);
        }))
        .catch_others(|_| ())
        .execute();
    }
    result
}

pub fn record_failure(group: &[Pending], message: &str) -> Result<(), String> {
    for view in group {
        let args: [DatumWithOid; 2] = [view.id.into(), message.into()];
        run_args(
            "UPDATE nabla.views SET status = 'failed', last_error = $2, last_error_at = now() WHERE id = $1",
            &args,
        )?;
    }
    Ok(())
}

/// Runs a closure in its own ordinary worker transaction.
pub type InTransaction<'a> = &'a dyn Fn(&mut dyn FnMut() -> Result<(), String>) -> Result<(), String>;

/// Process every pending view. `in_transaction` runs a closure in its own
/// ordinary worker transaction. Returns true when something was attempted.
pub fn run_pending(in_transaction: InTransaction<'_>) -> Result<bool, String> {
    let mut pending: Vec<Pending> = Vec::new();
    in_transaction(&mut || {
        pending = load_pending()?;
        Ok(())
    })?;
    if pending.is_empty() {
        return Ok(false);
    }
    for g in group(pending) {
        let names = g.iter().map(|v| v.name.clone()).collect::<Vec<_>>().join(", ");
        match populate_group(&g) {
            Ok(point) => pgrx::log!("nabla worker: populated {names} at consistent point {}", lsn::format(point)),
            Err(message) => {
                pgrx::warning!("nabla worker: population of {names} failed: {message}");
                in_transaction(&mut || record_failure(&g, &message))?;
            }
        }
    }
    Ok(true)
}
