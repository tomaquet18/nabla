// SPDX-License-Identifier: AGPL-3.0-only
//! SQL-callable API, all in schema `nabla`.
//!
//! `create_view` and `refresh` only record intent and return immediately; the
//! worker builds the tables under a consistent snapshot (see populate.rs).
//! Status vocabulary: `initializing`, `refreshing`, `live`, `stale`, `failed`.
//!
//! Names are resolved the way PostgreSQL resolves relation names: an
//! unqualified name is created in the search_path's creation namespace and
//! looked up through the search_path; identifiers keep PostgreSQL's
//! case-folding (unquoted lowercased, quoted verbatim). The canonical stored
//! name is the schema-qualified form quoted only where needed, and the user
//! object is a plain VIEW over the storage table `nabla_store.v<id>`.

use pgrx::datum::{DatumWithOid, JsonB};
use pgrx::prelude::*;
use pgrx::spi::{quote_identifier, quote_qualified_identifier};
use std::collections::BTreeSet;
use std::ffi::{c_char, CStr, CString};
use std::time::{Duration, Instant};

use crate::definition::{self, Shape, ViewSpec};
use crate::errors;
use crate::idle;
use crate::lsn;

const SLOT: &str = "nabla";
const PUBLICATION: &str = "nabla";
/// RVR_MISSING_OK from catalog/namespace.h (not re-exported at the pg_sys root).
const RVR_MISSING_OK: u32 = 1;

pub fn storage_name(view_id: i32) -> String {
    format!("nabla_store.v{view_id}")
}

fn spi_fail(e: pgrx::spi::Error) -> ! {
    errors::raise(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, format!("nabla: {e}"), None)
}

fn run(sql: &str) {
    Spi::run(sql).unwrap_or_else(|e| spi_fail(e));
}

fn run_args(sql: &str, args: &[DatumWithOid]) {
    Spi::run_with_args(sql, args).unwrap_or_else(|e| spi_fail(e));
}

/// Read-only single-value query. Unlike `Spi::run`/`get_*_with_args`, the
/// read-only SPI path never assigns a transaction id, which matters because
/// PostgreSQL refuses to create a logical slot once the transaction has one.
fn read_one<T: FromDatum + IntoDatum>(sql: &str, args: &[DatumWithOid]) -> Option<T> {
    Spi::connect(|client| {
        let table = client.select(sql, Some(1), args)?;
        if table.is_empty() {
            Ok(None)
        } else {
            table.first().get_one::<T>()
        }
    })
    .unwrap_or_else(|e| spi_fail(e))
}

/// Read-only query returning the first column of every row as i64.
fn read_ids(sql: &str, args: &[DatumWithOid]) -> Vec<i64> {
    Spi::connect(|client| {
        let mut out = Vec::new();
        for row in client.select(sql, None, args)? {
            if let Some(v) = row.get::<i64>(1)? {
                out.push(v);
            }
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .unwrap_or_else(|e| spi_fail(e))
}

unsafe fn text(ptr: *const c_char) -> String {
    if ptr.is_null() {
        String::new()
    } else {
        CStr::from_ptr(ptr).to_string_lossy().into_owned()
    }
}

// --- name resolution -------------------------------------------------------------

/// A parsed view name: PostgreSQL's own parser (case folding, quoting) plus
/// the RangeVar it produced, for namespace resolution.
struct ParsedName {
    schema: Option<String>,
    rel: String,
    range_var: *mut pg_sys::RangeVar,
}

fn parse_name(name: &str) -> ParsedName {
    let c = CString::new(name).unwrap_or_else(|_| errors::invalid("nabla: view name contains a NUL byte", None));
    // SAFETY: parser entry points on a NUL-terminated string; errors raise.
    unsafe {
        let list = pg_sys::stringToQualifiedNameList(c.as_ptr(), std::ptr::null_mut());
        let rv = pg_sys::makeRangeVarFromNameList(list);
        if !(*rv).catalogname.is_null() {
            errors::invalid(
                format!("nabla: view name \"{name}\" must have at most two parts (schema.name)"),
                None,
            );
        }
        let schema = if (*rv).schemaname.is_null() { None } else { Some(text((*rv).schemaname)) };
        ParsedName { schema, rel: text((*rv).relname), range_var: rv }
    }
}

/// The canonical name `create_view` gives a view under the current
/// search_path: the explicit schema, or the creation namespace (like CREATE
/// TABLE), quoted only where needed.
fn creation_name(parsed: &ParsedName) -> String {
    let schema = match &parsed.schema {
        Some(s) => s.clone(),
        None => unsafe {
            let nsp = pg_sys::RangeVarGetCreationNamespace(parsed.range_var);
            text(pg_sys::get_namespace_name(nsp))
        },
    };
    quote_qualified_identifier(&schema, &parsed.rel)
}

/// The oid the name resolves to through the search_path, if any relation is
/// visible under it.
fn visible_relid(parsed: &ParsedName) -> Option<u32> {
    let oid = unsafe {
        pg_sys::RangeVarGetRelidExtended(
            parsed.range_var,
            pg_sys::NoLock as pg_sys::LOCKMODE,
            RVR_MISSING_OK,
            None,
            std::ptr::null_mut(),
        )
    };
    if oid == pg_sys::InvalidOid {
        None
    } else {
        Some(oid.to_u32())
    }
}

fn not_found(name: &str) -> ! {
    errors::raise(
        PgSqlErrorCode::ERRCODE_UNDEFINED_TABLE,
        format!("nabla: view \"{name}\" does not exist"),
        None,
    )
}

/// Find an existing view: through the search_path by the VIEW's oid, or,
/// when no relation is visible yet (still initializing), by the canonical
/// name the same search_path would create it under.
fn find_view(name: &str) -> CatalogRow {
    let parsed = parse_name(name);
    if let Some(relid) = visible_relid(&parsed) {
        if let Some(view) = load_view_where("relid = $1::oid", &[(relid as i64).into()]).into_iter().next() {
            return view;
        }
    }
    let canonical = creation_name(&parsed);
    load_view_where("name = $1", &[canonical.as_str().into()]).into_iter().next().unwrap_or_else(|| not_found(name))
}

fn slot_exists() -> bool {
    read_one::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1)",
        &[SLOT.into()],
    )
    .unwrap_or(false)
}

/// Must run before the calling transaction performs any write: PostgreSQL
/// refuses to create a logical slot in a transaction that already has an xid.
fn ensure_slot() {
    if !slot_exists() {
        // Read-only SPI on purpose: see read_one().
        Spi::connect(|client| {
            client
                .select("SELECT pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')", None, &[SLOT.into()])
                .map(|_| ())
        })
        .unwrap_or_else(|e| spi_fail(e));
    }
}

fn ensure_publication_includes(base_oid: u32) {
    let exists = read_one::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = $1)",
        &[PUBLICATION.into()],
    )
    .unwrap_or(false);
    if !exists {
        run(&format!("CREATE PUBLICATION {}", quote_identifier(PUBLICATION)));
    }
    let included = read_one::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_publication_rel pr \
         JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid \
         WHERE p.pubname = $1 AND pr.prrelid = $2::oid)",
        &[PUBLICATION.into(), (base_oid as i64).into()],
    )
    .unwrap_or(false);
    if !included {
        let table = regclass_text(base_oid)
            .unwrap_or_else(|| errors::failed("?", Some(&format!("base table with oid {base_oid} no longer exists"))));
        run(&format!("ALTER PUBLICATION {} ADD TABLE {table}", quote_identifier(PUBLICATION)));
    }
}

/// Quoted name of a relation, or None when it no longer exists (a dropped
/// regclass renders as a bare number, which must never reach SQL).
fn regclass_text(oid: u32) -> Option<String> {
    read_one::<String>(
        "SELECT pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname) \
         FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = $1::oid",
        &[(oid as i64).into()],
    )
}

fn relation_exists(name: &str) -> bool {
    read_one::<bool>("SELECT pg_catalog.to_regclass($1) IS NOT NULL", &[name.into()]).unwrap_or(false)
}

fn require_logical_wal() {
    let level = read_one::<String>("SELECT current_setting('wal_level')", &[]).unwrap_or_default();
    if level != "logical" {
        errors::prerequisite(
            format!("nabla: wal_level must be 'logical' (currently '{level}')"),
            Some("Set wal_level = logical in postgresql.conf and restart PostgreSQL."),
        );
    }
}

struct CatalogRow {
    id: i32,
    name: String,
    spec: ViewSpec,
    frontier: u64,
    epoch: i32,
    status: String,
    last_seq: i64,
    stale_reason: Option<String>,
    last_error: Option<String>,
}

fn load_view_where(clause: &str, args: &[DatumWithOid]) -> Vec<CatalogRow> {
    Spi::connect(|client| {
        let mut out = Vec::new();
        for r in client.select(
            &format!(
                "SELECT id, name, spec, epoch, status, last_seq, stale_reason, \
                        frontier_lsn::text, last_error \
                 FROM nabla.views WHERE {clause} ORDER BY id"
            ),
            None,
            args,
        )? {
            let spec: JsonB = r.get::<JsonB>(3)?.expect("spec");
            let spec: ViewSpec = serde_json::from_value(spec.0)
                .unwrap_or_else(|e| errors::invalid(format!("nabla: corrupt spec: {e}"), None));
            out.push(CatalogRow {
                id: r.get::<i32>(1)?.expect("id"),
                name: r.get::<String>(2)?.expect("name"),
                spec,
                epoch: r.get::<i32>(4)?.expect("epoch"),
                status: r.get::<String>(5)?.expect("status"),
                last_seq: r.get::<i64>(6)?.expect("last_seq"),
                stale_reason: r.get::<String>(7)?,
                frontier: lsn::parse(&r.get::<String>(8)?.unwrap_or_default()).unwrap_or(0),
                last_error: r.get::<String>(9)?,
            });
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .unwrap_or_else(|e| spi_fail(e))
}

fn oid_array(oids: &BTreeSet<u32>) -> String {
    let items: Vec<String> = oids.iter().map(|o| o.to_string()).collect();
    format!("ARRAY[{}]::oid[]", items.join(", "))
}

/// The refresh set of a join view: it and every other join view sharing any
/// of its shadows — transitively, because a shadow re-snapshotted at a new
/// consistent point would be wrong for a view still at an older one.
fn refresh_closure(view: &CatalogRow) -> Vec<CatalogRow> {
    let mut view_ids: BTreeSet<i64> = BTreeSet::from([view.id as i64]);
    let mut relids: BTreeSet<u32> = view.spec.relations.iter().map(|r| r.oid).collect();
    loop {
        let more = read_ids(
            &format!(
                "SELECT DISTINCT vr.view_id::int8 FROM nabla.view_relations vr \
                 JOIN nabla.views v ON v.id = vr.view_id \
                 WHERE vr.relid = ANY({}) AND jsonb_array_length(v.spec->'relations') > 1",
                oid_array(&relids)
            ),
            &[],
        );
        let before = (view_ids.len(), relids.len());
        view_ids.extend(more);
        let ids: Vec<String> = view_ids.iter().map(|i| i.to_string()).collect();
        relids.extend(
            read_ids(
                &format!("SELECT relid::int8 FROM nabla.view_relations WHERE view_id IN ({})", ids.join(", ")),
                &[],
            )
            .into_iter()
            .map(|r| r as u32),
        );
        if before == (view_ids.len(), relids.len()) {
            break;
        }
    }
    let ids: Vec<String> = view_ids.iter().map(|i| i.to_string()).collect();
    load_view_where(&format!("id IN ({})", ids.join(", ")), &[])
}

/// (frontier, status, stale_reason, last_error) of the canonical view name,
/// read with a fresh snapshot.
fn current_state(canonical: &str) -> (u64, String, Option<String>, Option<String>) {
    // Read-only SPI runs under the active snapshot, so push the latest one
    // to observe the worker's commits while polling.
    unsafe { pg_sys::PushActiveSnapshot(pg_sys::GetLatestSnapshot()) };
    let found = Spi::connect(|client| {
        let table = client.select(
            "SELECT frontier_lsn::text, status, stale_reason, last_error FROM nabla.views WHERE name = $1",
            Some(1),
            &[canonical.into()],
        )?;
        if table.is_empty() {
            Ok(None)
        } else {
            let t = table.first();
            Ok(Some((t.get::<String>(1)?, t.get::<String>(2)?, t.get::<String>(3)?, t.get::<String>(4)?)))
        }
    });
    unsafe { pg_sys::PopActiveSnapshot() };
    let (lsn_text, status, stale_reason, last_error) =
        found.unwrap_or_else(|e| spi_fail(e)).unwrap_or((None, None, None, None));
    let lsn_text = lsn_text.unwrap_or_else(|| not_found(canonical));
    (lsn::parse(&lsn_text).unwrap_or(0), status.unwrap_or_default(), stale_reason, last_error)
}

/// Poll the frontier every 10 ms until it reaches `target` or the timeout.
fn wait_for_frontier(canonical: &str, target: u64, timeout_ms: i32) -> bool {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms.max(0) as u64);
    loop {
        let (frontier, status, stale_reason, last_error) = current_state(canonical);
        match status.as_str() {
            "stale" => errors::stale(canonical, stale_reason.as_deref()),
            "failed" => errors::failed(canonical, last_error.as_deref()),
            _ => {}
        }
        if idle::effective_frontier(frontier) >= target {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        pgrx::pg_sys::check_for_interrupts!();
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// The SQL-visible functions. `#[pg_schema]` makes pgrx emit the schema
/// declaration so the functions can live in `nabla`.
#[pg_schema]
mod nabla {
    use super::*;

    /// Records the view and returns its canonical name immediately; the
    /// worker builds the storage table and the VIEW under a consistent
    /// snapshot (populate.rs). An unqualified name goes to the search_path's
    /// creation namespace, like CREATE TABLE. LISTEN on `'nabla:' || name`;
    /// nabla.await_ready() waits for the build.
    #[pg_extern]
    fn create_view(name: &str, definition: &str) -> String {
        let parsed = parse_name(name);
        let canonical = creation_name(&parsed);
        let (spec, base) = definition::validate(definition);
        require_logical_wal();

        // Single-table aggregates take old rows from pgoutput and need the
        // full row; join views take them from the shadows and need only a key.
        if !spec.is_join() && spec.shape == Shape::Aggregate && base.replident != "f" {
            errors::prerequisite(
                format!(
                    "nabla: the aggregate shape needs the full old row of every change, but {} has REPLICA IDENTITY '{}'",
                    base.qualified, base.replident
                ),
                Some(&format!("Run: ALTER TABLE {} REPLICA IDENTITY FULL;", base.qualified)),
            );
        }
        if relation_exists(&canonical) {
            errors::invalid(format!("nabla: relation {canonical} already exists"), None);
        }
        if read_one::<bool>("SELECT EXISTS (SELECT 1 FROM nabla.views WHERE name = $1)", &[canonical.as_str().into()])
            .unwrap_or(false)
        {
            errors::invalid(format!("nabla: view {canonical} already exists"), None);
        }

        // Order matters: the slot first (no writes yet), then catalog writes.
        // No lock and no table here: the worker populates asynchronously.
        ensure_slot();
        for rel in &spec.relations {
            ensure_publication_includes(rel.oid);
        }
        let spec_json = serde_json::to_string(&spec).expect("spec serializes");
        let view_id = Spi::get_one_with_args::<i32>(
            "INSERT INTO nabla.views (name, definition, shape, spec, frontier_lsn, status) \
             VALUES ($1, $2, $3, $4::jsonb, '0/0', 'initializing') RETURNING id",
            &[
                canonical.as_str().into(),
                definition.into(),
                spec.shape.as_str().into(),
                spec_json.as_str().into(),
            ],
        )
        .unwrap_or_else(|e| spi_fail(e))
        .expect("view id");
        for rel in &spec.relations {
            run_args(
                "INSERT INTO nabla.view_relations (view_id, relid, rti) VALUES ($1, $2::oid, $3)",
                &[view_id.into(), (rel.oid as i64).into(), (rel.rti as i32).into()],
            );
        }
        canonical
    }

    /// Drops the VIEW (its sql_drop event trigger drops the storage table,
    /// cleans the catalog, releases shadows and prunes the publication) and
    /// forgets the view even when no objects exist yet (failed or
    /// initializing) or a base table is already gone.
    #[pg_extern]
    fn drop_view(name: &str) {
        let view = find_view(name);
        if relation_exists(&view.name) {
            run(&format!("DROP VIEW {}", view.name));
        }
        let storage = storage_name(view.id);
        if relation_exists(&storage) {
            run(&format!("DROP TABLE {storage}"));
        }
        run_args("SELECT nabla.forget_view($1)", &[view.id.into()]);
        run("SELECT nabla.prune_publication()");
    }

    /// Marks the view — and, for join views, every view sharing a shadow
    /// with it — for rebuild and returns immediately. Until the worker
    /// commits the rebuilt content the view is frozen: readable at its old
    /// epoch and frontier, no new deltas. Afterwards the epoch is one higher.
    /// nabla.await_ready() waits for it.
    #[pg_extern]
    fn refresh(name: &str) {
        let view = find_view(name);
        for rel in &view.spec.relations {
            if regclass_text(rel.oid).is_none() {
                errors::failed(&view.name, Some(&format!("base table {} (oid {}) no longer exists", rel.qualified, rel.oid)));
            }
        }
        // The slot may have been dropped for lag; recreate it before any write.
        ensure_slot();
        let ids: Vec<String> = if view.spec.is_join() {
            refresh_closure(&view).iter().map(|v| v.id.to_string()).collect()
        } else {
            vec![view.id.to_string()]
        };
        for rel in &view.spec.relations {
            ensure_publication_includes(rel.oid);
        }
        run(&format!(
            "UPDATE nabla.views SET status = 'refreshing' \
             WHERE id IN ({}) AND status NOT IN ('initializing', 'refreshing')",
            ids.join(", ")
        ));
    }

    /// Blocks until the view is live (true), failed (NB006), stale (NB002),
    /// or the timeout elapses (false).
    #[pg_extern]
    fn await_ready(name: &str, timeout_ms: default!(i32, 60000)) -> bool {
        let canonical = find_view(name).name;
        let deadline = Instant::now() + Duration::from_millis(timeout_ms.max(0) as u64);
        loop {
            let (_, status, stale_reason, last_error) = current_state(&canonical);
            match status.as_str() {
                "live" => return true,
                "failed" => errors::failed(&canonical, last_error.as_deref()),
                "stale" => errors::stale(&canonical, stale_reason.as_deref()),
                _ => {}
            }
            if Instant::now() >= deadline {
                return false;
            }
            pgrx::pg_sys::check_for_interrupts!();
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[pg_extern(sql = r#"
    CREATE FUNCTION nabla.frontier(name text) RETURNS pg_lsn
    STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
    "#)]
    fn frontier(name: &str) -> i64 {
        let canonical = find_view(name).name;
        current_state(&canonical).0 as i64
    }

    #[pg_extern(sql = r#"
    CREATE FUNCTION nabla.wait_for(name text, lsn pg_lsn, timeout_ms int DEFAULT 5000) RETURNS bool
    STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
    "#)]
    fn wait_for(name: &str, lsn: i64, timeout_ms: i32) -> bool {
        let canonical = find_view(name).name;
        wait_for_frontier(&canonical, lsn as u64, timeout_ms)
    }

    /// Text overload: accepts the `X/Y` form returned by changes() and status().
    #[pg_extern(sql = r#"
    CREATE FUNCTION nabla.wait_for(name text, lsn text, timeout_ms int DEFAULT 5000) RETURNS bool
    STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
    "#)]
    fn wait_for_text(name: &str, lsn: &str, timeout_ms: i32) -> bool {
        let canonical = find_view(name).name;
        let target = lsn::parse(lsn)
            .unwrap_or_else(|| errors::invalid(format!("nabla: \"{lsn}\" is not a valid LSN (expected X/Y)"), None));
        wait_for_frontier(&canonical, target, timeout_ms)
    }

    /// Everything a subscriber needs to bootstrap, in one row read from the
    /// caller's snapshot (so it composes with a REPEATABLE READ transaction
    /// that also reads the view). `name` is the canonical view name:
    /// LISTEN on `'nabla:' || name`. `status` is one of initializing,
    /// refreshing, live, stale, failed.
    #[pg_extern(sql = r#"
    CREATE FUNCTION nabla.status(name text)
    RETURNS TABLE (name text, status text, epoch int, frontier_lsn pg_lsn, frontier text, current_seq bigint, stale_reason text)
    STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
    "#)]
    fn status(
        name: &str,
    ) -> TableIterator<
        'static,
        (
            name!(name, String),
            name!(status, String),
            name!(epoch, i32),
            name!(frontier_lsn, i64),
            name!(frontier, String),
            name!(current_seq, i64),
            name!(stale_reason, Option<String>),
        ),
    > {
        let view = find_view(name);
        let reason = match view.status.as_str() {
            "failed" => view.last_error.clone(),
            _ => view.stale_reason.clone(),
        };
        TableIterator::once((
            view.name,
            view.status,
            view.epoch,
            view.frontier as i64,
            lsn::format(view.frontier),
            view.last_seq,
            reason,
        ))
    }

    /// The definition's output columns, in order: the columns of the VIEW.
    #[pg_extern]
    fn visible_columns(name: &str) -> Vec<String> {
        find_view(name).spec.visible_columns
    }

    /// The storage table behind the VIEW (`nabla_store.v<id>`), which also
    /// carries the hidden `_nabla_*` maintenance columns; NULL until built.
    #[pg_extern(sql = r#"
    CREATE FUNCTION nabla.storage_table(name text) RETURNS regclass
    STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
    "#)]
    fn storage_table(name: &str) -> Option<pg_sys::Oid> {
        let view = find_view(name);
        read_one::<i64>("SELECT pg_catalog.to_regclass($1)::oid::int8", &[storage_name(view.id).as_str().into()])
            .map(|o| pg_sys::Oid::from(o as u32))
    }

    #[pg_extern]
    fn current_seq(name: &str) -> i64 {
        find_view(name).last_seq
    }

    /// Deltas after `after_seq`, whole source transactions only: a result with
    /// fewer rows than `max_rows` is drained; a trailing transaction that
    /// straddles `max_rows` is returned in full (so a result may exceed
    /// `max_rows`). Raises NB002 (stale), NB006 (failed), NB003 (epoch
    /// differs) or NB001 (cursor older than retention), in that order.
    #[pg_extern(sql = r#"
    CREATE FUNCTION nabla.changes(name text, after_seq bigint, epoch int, max_rows int DEFAULT 1000, include_hidden bool DEFAULT false)
    RETURNS TABLE (seq bigint, lsn text, xid bigint, op text, "row" jsonb)
    STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
    "#)]
    fn changes(
        name: &str,
        after_seq: i64,
        epoch: i32,
        max_rows: i32,
        include_hidden: bool,
    ) -> TableIterator<'static, (name!(seq, i64), name!(lsn, String), name!(xid, Option<i64>), name!(op, String), name!(row, JsonB))> {
        let view = find_view(name);
        let canonical = view.name.clone();
        match view.status.as_str() {
            "stale" => errors::stale(&canonical, view.stale_reason.as_deref()),
            "failed" => errors::failed(&canonical, view.last_error.as_deref()),
            _ => {}
        }
        if epoch != view.epoch {
            errors::epoch_changed(&canonical, epoch, view.epoch);
        }
        match read_one::<i64>("SELECT min(seq) FROM nabla.deltas WHERE view_id = $1", &[view.id.into()]) {
            Some(oldest) if after_seq < oldest - 1 => errors::lagged(&canonical, oldest),
            None if after_seq < view.last_seq => errors::lagged(&canonical, view.last_seq + 1),
            _ => {}
        }

        let hidden: Vec<String> = if include_hidden { Vec::new() } else { view.spec.hidden_columns.clone() };
        let limit = max_rows.max(1) as i64;
        let args: [DatumWithOid; 4] = [view.id.into(), after_seq.into(), limit.into(), hidden.into()];
        let rows = Spi::connect(|client| {
            let mut out = Vec::new();
            for r in client.select(
                "WITH page AS (SELECT seq, lsn, xid, op, row FROM nabla.deltas \
                               WHERE view_id = $1 AND seq > $2 ORDER BY seq LIMIT $3), \
                      last AS (SELECT seq, lsn, xid FROM page ORDER BY seq DESC LIMIT 1), \
                      rest AS (SELECT d.seq, d.lsn, d.xid, d.op, d.row FROM nabla.deltas d, last \
                               WHERE (SELECT count(*) FROM page) >= $3 AND d.view_id = $1 \
                                 AND d.seq > last.seq AND d.lsn = last.lsn AND d.xid IS NOT DISTINCT FROM last.xid) \
                 SELECT seq, lsn::text, xid, op::text, row - $4::text[] \
                 FROM (SELECT * FROM page UNION ALL SELECT * FROM rest) u ORDER BY seq",
                None,
                &args,
            )? {
                out.push((
                    r.get::<i64>(1)?.expect("seq"),
                    r.get::<String>(2)?.expect("lsn"),
                    r.get::<i64>(3)?,
                    r.get::<String>(4)?.expect("op"),
                    r.get::<JsonB>(5)?.expect("row"),
                ));
            }
            Ok::<_, pgrx::spi::Error>(out)
        })
        .unwrap_or_else(|e| spi_fail(e));
        TableIterator::new(rows)
    }
}
