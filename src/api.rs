//! SQL-callable API, all in schema `nabla`.

use pgrx::datum::{DatumWithOid, JsonB};
use pgrx::prelude::*;
use pgrx::spi::quote_identifier;
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use crate::definition::{self, Shape, ViewSpec};
use crate::errors;
use crate::idle;
use crate::lsn;
use crate::shadow;

const SLOT: &str = "nabla";
const PUBLICATION: &str = "nabla";

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

fn require_qualified_name(name: &str) -> String {
    let name = name.trim().to_lowercase();
    let ok = name.split('.').count() == 2
        && name.split('.').all(|part| {
            !part.is_empty()
                && part.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                && !part.starts_with(|c: char| c.is_ascii_digit())
        });
    if !ok {
        errors::invalid(
            format!("nabla: view name \"{name}\" must be schema-qualified (schema.name)"),
            Some("Example: nabla.create_view('public.paid_orders', ...)"),
        );
    }
    name
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
        run(&format!(
            "ALTER PUBLICATION {} ADD TABLE {}",
            quote_identifier(PUBLICATION),
            regclass_text(base_oid)
        ));
    }
}

/// Remove a base table from the publication when no view uses it any more.
fn release_publication(relid: u32) {
    let still_used = read_one::<bool>(
        "SELECT EXISTS (SELECT 1 FROM nabla.view_relations WHERE relid = $1::oid) \
         OR EXISTS (SELECT 1 FROM nabla.shadows WHERE relid = $1::oid)",
        &[(relid as i64).into()],
    )
    .unwrap_or(false);
    if !still_used {
        run(&format!("ALTER PUBLICATION {} DROP TABLE {}", quote_identifier(PUBLICATION), regclass_text(relid)));
    }
}

fn regclass_text(oid: u32) -> String {
    read_one::<String>("SELECT $1::oid::regclass::text", &[(oid as i64).into()])
        .unwrap_or_else(|| errors::invalid(format!("nabla: relation {oid} does not exist"), None))
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

fn view_index_sql(name: &str, spec: &ViewSpec) -> String {
    let cols: Vec<String> = match spec.shape {
        Shape::Projection => spec.pk_view_columns.iter().map(quote_identifier).collect(),
        Shape::Aggregate => spec.columns.iter().map(|c| quote_identifier(&c.alias)).collect(),
    };
    // GROUP BY treats NULLs as equal, so the group key index must as well.
    let nulls = if spec.shape == Shape::Aggregate { " NULLS NOT DISTINCT" } else { "" };
    format!("CREATE UNIQUE INDEX ON {name} ({}){nulls}", cols.join(", "))
}

/// Reset a view's bookkeeping after a rebuild: a new epoch invalidates every
/// subscriber cursor (they get NB003 from changes()).
fn record_refresh(view_id: i32) {
    run_args(
        "UPDATE nabla.views SET frontier_lsn = pg_catalog.pg_current_wal_lsn(), epoch = epoch + 1, \
         status = 'live', apply_failures = 0, last_error = NULL, last_error_at = NULL, stale_reason = NULL \
         WHERE id = $1",
        &[view_id.into()],
    );
    run_args("DELETE FROM nabla.deltas WHERE view_id = $1", &[view_id.into()]);
}

struct CatalogRow {
    id: i32,
    name: String,
    base_oid: u32,
    spec: ViewSpec,
    frontier: u64,
    epoch: i32,
    status: String,
    last_seq: i64,
    stale_reason: Option<String>,
}

fn load_view_where(clause: &str, args: &[DatumWithOid]) -> Vec<CatalogRow> {
    Spi::connect(|client| {
        let mut out = Vec::new();
        for r in client.select(
            &format!(
                "SELECT id, name, base_table::oid::int8, spec, epoch, status, last_seq, stale_reason, frontier_lsn::text \
                 FROM nabla.views WHERE {clause} ORDER BY id"
            ),
            None,
            args,
        )? {
            let spec: JsonB = r.get::<JsonB>(4)?.expect("spec");
            let spec: ViewSpec = serde_json::from_value(spec.0)
                .unwrap_or_else(|e| errors::invalid(format!("nabla: corrupt spec: {e}"), None));
            out.push(CatalogRow {
                id: r.get::<i32>(1)?.expect("id"),
                name: r.get::<String>(2)?.expect("name"),
                base_oid: r.get::<i64>(3)?.expect("base") as u32,
                spec,
                epoch: r.get::<i32>(5)?.expect("epoch"),
                status: r.get::<String>(6)?.expect("status"),
                last_seq: r.get::<i64>(7)?.expect("last_seq"),
                stale_reason: r.get::<String>(8)?,
                frontier: lsn::parse(&r.get::<String>(9)?.unwrap_or_default()).unwrap_or(0),
            });
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .unwrap_or_else(|e| spi_fail(e))
}

fn load_view(name: &str) -> CatalogRow {
    load_view_where("name = $1", &[name.into()]).into_iter().next().unwrap_or_else(|| {
        errors::raise(
            PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
            format!("nabla: view \"{name}\" does not exist"),
            None,
        )
    })
}

fn oid_array(oids: &BTreeSet<u32>) -> String {
    let items: Vec<String> = oids.iter().map(|o| o.to_string()).collect();
    format!("ARRAY[{}]::oid[]", items.join(", "))
}

/// The refresh set of a join view: it, every shadow it uses, and every other
/// join view sharing any of those shadows — transitively, because a shadow
/// re-snapshotted at a new LSN would be wrong for a view still at an older one.
fn refresh_closure(view: &CatalogRow) -> (Vec<CatalogRow>, BTreeSet<u32>) {
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
    let views = load_view_where(&format!("id IN ({})", ids.join(", ")), &[]);
    (views, relids)
}

/// (frontier, status, stale_reason) read with a fresh snapshot.
fn current_frontier(name: &str) -> (u64, String, Option<String>) {
    // Read-only SPI runs under the active snapshot, so push the latest one
    // to observe the worker's commits while polling.
    unsafe { pg_sys::PushActiveSnapshot(pg_sys::GetLatestSnapshot()) };
    let found = Spi::connect(|client| {
        let table = client.select(
            "SELECT frontier_lsn::text, status, stale_reason FROM nabla.views WHERE name = $1",
            Some(1),
            &[name.into()],
        )?;
        if table.is_empty() {
            Ok(None)
        } else {
            let table = table.first();
            Ok(Some((table.get::<String>(1)?, table.get::<String>(2)?, table.get::<String>(3)?)))
        }
    });
    unsafe { pg_sys::PopActiveSnapshot() };
    let (lsn_text, status, stale_reason) = found.unwrap_or_else(|e| spi_fail(e)).unwrap_or((None, None, None));
    let lsn_text = lsn_text.unwrap_or_else(|| {
        errors::raise(
            PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
            format!("nabla: view \"{name}\" does not exist"),
            None,
        )
    });
    (lsn::parse(&lsn_text).unwrap_or(0), status.unwrap_or_default(), stale_reason)
}

/// Poll the frontier every 10 ms until it reaches `target` or the timeout.
fn wait_for_frontier(name: &str, target: u64, timeout_ms: i32) -> bool {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms.max(0) as u64);
    loop {
        let (frontier, status, stale_reason) = current_frontier(name);
        if status == "stale" {
            errors::stale(name, stale_reason.as_deref());
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

    /// Returns the canonical (stored) view name; LISTEN on `'nabla:' || name`.
    #[pg_extern]
    fn create_view(name: &str, definition: &str) -> String {
        let name = require_qualified_name(name);
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
        if read_one::<bool>("SELECT pg_catalog.to_regclass($1) IS NOT NULL", &[name.as_str().into()]).unwrap_or(false) {
            errors::invalid(format!("nabla: relation \"{name}\" already exists"), None);
        }
        if read_one::<bool>("SELECT EXISTS (SELECT 1 FROM nabla.views WHERE name = $1)", &[name.as_str().into()])
            .unwrap_or(false)
        {
            errors::invalid(format!("nabla: view \"{name}\" already exists"), None);
        }

        // Order matters: the slot first (no writes yet), then locks, then writes.
        ensure_slot();

        // Brief, documented write pause on the base tables: SHARE waits for
        // in-flight writers to finish so the starting snapshot is exact and
        // shared by the view table and every shadow.
        for rel in &spec.relations {
            run(&format!("LOCK TABLE {} IN SHARE MODE", rel.qualified));
        }
        if spec.is_join() {
            for rel in &spec.relations {
                shadow::ensure(rel);
            }
        }
        for rel in &spec.relations {
            ensure_publication_includes(rel.oid);
        }

        run(&format!("CREATE TABLE {name} AS {}", spec.populate_sql));
        run(&view_index_sql(&name, &spec));
        run(&format!(
            "CREATE TRIGGER nabla_guard BEFORE INSERT OR UPDATE OR DELETE ON {name} \
             FOR EACH ROW EXECUTE FUNCTION nabla.guard_view()"
        ));
        run(&format!(
            "CREATE TRIGGER nabla_guard_truncate BEFORE TRUNCATE ON {name} \
             FOR EACH STATEMENT EXECUTE FUNCTION nabla.guard_view()"
        ));

        let spec_json = serde_json::to_string(&spec).expect("spec serializes");
        let view_id = Spi::get_one_with_args::<i32>(
            "INSERT INTO nabla.views (name, base_table, definition, shape, spec, frontier_lsn) \
             VALUES ($1, $2::oid::regclass, $3, $4, $5::jsonb, pg_catalog.pg_current_wal_lsn()) RETURNING id",
            &[
                name.as_str().into(),
                (base.oid as i64).into(),
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
        name
    }

    #[pg_extern]
    fn drop_view(name: &str) {
        let name = require_qualified_name(name);
        let view = load_view(&name);
        let relids: Vec<u32> = if view.spec.relations.is_empty() {
            vec![view.base_oid]
        } else {
            view.spec.relations.iter().map(|r| r.oid).collect()
        };
        run(&format!("DROP TABLE IF EXISTS {name}"));
        run_args("DELETE FROM nabla.views WHERE id = $1", &[view.id.into()]);
        if view.spec.is_join() {
            for relid in &relids {
                shadow::release(*relid);
            }
        }
        for relid in &relids {
            let used_by_base = read_one::<bool>(
                "SELECT EXISTS (SELECT 1 FROM nabla.views WHERE base_table = $1::oid::regclass)",
                &[(*relid as i64).into()],
            )
            .unwrap_or(false);
            if !used_by_base {
                release_publication(*relid);
            }
        }
    }

    #[pg_extern]
    fn refresh(name: &str) {
        let name = require_qualified_name(name);
        let view = load_view(&name);
        // The slot may have been dropped for lag; recreate it before any write.
        ensure_slot();
        if !view.spec.is_join() {
            ensure_publication_includes(view.base_oid);
            run(&format!("LOCK TABLE {} IN SHARE MODE", view.spec.base_table));
            run("SET LOCAL nabla.internal_write = on");
            run(&format!("DELETE FROM {name}"));
            run(&format!("INSERT INTO {name} {}", view.spec.populate_sql));
            record_refresh(view.id);
            return;
        }

        // Refresh cascades to views sharing a shadow: everything in the
        // closure is re-snapshotted under SHARE locks, in one transaction.
        let (views, relids) = refresh_closure(&view);
        let mut names: Vec<(u32, String)> = Vec::new();
        for v in &views {
            for r in &v.spec.relations {
                if relids.contains(&r.oid) && !names.iter().any(|(o, _)| *o == r.oid) {
                    names.push((r.oid, r.qualified.clone()));
                }
            }
        }
        names.sort_by_key(|(o, _)| *o); // stable lock order across concurrent refreshes
        for (oid, _) in &names {
            ensure_publication_includes(*oid);
        }
        for (_, qualified) in &names {
            run(&format!("LOCK TABLE {qualified} IN SHARE MODE"));
        }
        run("SET LOCAL nabla.internal_write = on");
        let mut done: BTreeSet<u32> = BTreeSet::new();
        for v in &views {
            for r in &v.spec.relations {
                if done.insert(r.oid) {
                    shadow::snapshot(r);
                }
            }
        }
        for v in &views {
            run(&format!("DELETE FROM {}", v.name));
            run(&format!("INSERT INTO {} {}", v.name, v.spec.populate_sql));
            record_refresh(v.id);
        }
    }

    #[pg_extern(sql = r#"
    CREATE FUNCTION nabla.frontier(name text) RETURNS pg_lsn
    STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
    "#)]
    fn frontier(name: &str) -> i64 {
        let name = require_qualified_name(name);
        load_view(&name);
        current_frontier(&name).0 as i64
    }

    #[pg_extern(sql = r#"
    CREATE FUNCTION nabla.wait_for(name text, lsn pg_lsn, timeout_ms int DEFAULT 5000) RETURNS bool
    STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
    "#)]
    fn wait_for(name: &str, lsn: i64, timeout_ms: i32) -> bool {
        let name = require_qualified_name(name);
        wait_for_frontier(&name, lsn as u64, timeout_ms)
    }

    /// Text overload: accepts the `X/Y` form returned by changes() and status().
    #[pg_extern(sql = r#"
    CREATE FUNCTION nabla.wait_for(name text, lsn text, timeout_ms int DEFAULT 5000) RETURNS bool
    STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
    "#)]
    fn wait_for_text(name: &str, lsn: &str, timeout_ms: i32) -> bool {
        let name = require_qualified_name(name);
        let target = lsn::parse(lsn)
            .unwrap_or_else(|| errors::invalid(format!("nabla: \"{lsn}\" is not a valid LSN (expected X/Y)"), None));
        wait_for_frontier(&name, target, timeout_ms)
    }

    /// Everything a subscriber needs to bootstrap, in one row read from the
    /// caller's snapshot (so it composes with a REPEATABLE READ transaction
    /// that also reads the view table). `name` is the canonical view name:
    /// LISTEN on `'nabla:' || name`.
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
        let name = require_qualified_name(name);
        let view = load_view(&name);
        TableIterator::once((
            view.name,
            view.status,
            view.epoch,
            view.frontier as i64,
            lsn::format(view.frontier),
            view.last_seq,
            view.stale_reason,
        ))
    }

    /// The definition's output columns, in order, without the hidden
    /// `_nabla_*` maintenance columns of the view table.
    #[pg_extern]
    fn visible_columns(name: &str) -> Vec<String> {
        let name = require_qualified_name(name);
        load_view(&name).spec.visible_columns
    }

    #[pg_extern]
    fn current_seq(name: &str) -> i64 {
        let name = require_qualified_name(name);
        load_view(&name).last_seq
    }

    /// Deltas after `after_seq`, whole source transactions only: a result with
    /// fewer rows than `max_rows` is drained; a trailing transaction that
    /// straddles `max_rows` is returned in full (so a result may exceed
    /// `max_rows`). Raises NB002 (stale), NB003 (epoch differs) or NB001
    /// (cursor older than retention), in that order.
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
        let name = require_qualified_name(name);
        let view = load_view(&name);
        if view.status == "stale" {
            errors::stale(&name, view.stale_reason.as_deref());
        }
        if epoch != view.epoch {
            errors::epoch_changed(&name, epoch, view.epoch);
        }
        match read_one::<i64>("SELECT min(seq) FROM nabla.deltas WHERE view_id = $1", &[view.id.into()]) {
            Some(oldest) if after_seq < oldest - 1 => errors::lagged(&name, oldest),
            None if after_seq < view.last_seq => errors::lagged(&name, view.last_seq + 1),
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
