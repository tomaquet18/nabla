//! SQL-callable API, all in schema `nabla`.

use pgrx::datum::{DatumWithOid, JsonB};
use pgrx::prelude::*;
use pgrx::spi::quote_identifier;
use std::time::{Duration, Instant};

use crate::definition::{self, Shape, ViewSpec};
use crate::errors;
use crate::lsn;

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

struct CatalogRow {
    id: i32,
    base_oid: u32,
    spec: ViewSpec,
    epoch: i32,
    status: String,
    last_seq: i64,
    resync_seq: i64,
}

fn load_view(name: &str) -> CatalogRow {
    let args = [name.into()];
    let row = Spi::connect(|client| {
        let mut out = None;
        for r in client.select(
            "SELECT id, base_table::oid::int8, spec, epoch, status, last_seq, resync_seq \
             FROM nabla.views WHERE name = $1",
            Some(1),
            &args,
        )? {
            let spec: JsonB = r.get::<JsonB>(3)?.expect("spec");
            let spec: ViewSpec = serde_json::from_value(spec.0)
                .unwrap_or_else(|e| errors::invalid(format!("nabla: corrupt spec: {e}"), None));
            out = Some(CatalogRow {
                id: r.get::<i32>(1)?.expect("id"),
                base_oid: r.get::<i64>(2)?.expect("base") as u32,
                spec,
                epoch: r.get::<i32>(4)?.expect("epoch"),
                status: r.get::<String>(5)?.expect("status"),
                last_seq: r.get::<i64>(6)?.expect("last_seq"),
                resync_seq: r.get::<i64>(7)?.expect("resync_seq"),
            });
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .unwrap_or_else(|e| spi_fail(e));
    row.unwrap_or_else(|| {
        errors::raise(
            PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
            format!("nabla: view \"{name}\" does not exist"),
            None,
        )
    })
}

/// The SQL-visible functions. `#[pg_schema]` makes pgrx emit the schema
/// declaration so the functions can live in `nabla`.
#[pg_schema]
mod nabla {
    use super::*;

    #[pg_extern]
    fn create_view(name: &str, definition: &str) {
        let name = require_qualified_name(name);
        let (spec, base) = definition::validate(definition);
        require_logical_wal();

        if spec.shape == Shape::Aggregate && base.replident != "f" {
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

        // Order matters: the slot first (no writes yet), then catalog writes.
        ensure_slot();
        ensure_publication_includes(base.oid);

        // Brief, documented write pause on the base table: SHARE waits for
        // in-flight writers to finish so the starting snapshot is exact.
        run(&format!("LOCK TABLE {} IN SHARE MODE", base.qualified));
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
        run_args(
            "INSERT INTO nabla.views (name, base_table, definition, shape, spec, frontier_lsn) \
             VALUES ($1, $2::oid::regclass, $3, $4, $5::jsonb, pg_catalog.pg_current_wal_lsn())",
            &[
                name.as_str().into(),
                (base.oid as i64).into(),
                definition.into(),
                spec.shape.as_str().into(),
                spec_json.as_str().into(),
            ],
        );
    }

    #[pg_extern]
    fn drop_view(name: &str) {
        let name = require_qualified_name(name);
        let view = load_view(&name);
        run(&format!("DROP TABLE IF EXISTS {name}"));
        run_args("DELETE FROM nabla.views WHERE id = $1", &[view.id.into()]);
        let still_used = read_one::<bool>(
            "SELECT EXISTS (SELECT 1 FROM nabla.views WHERE base_table = $1::oid::regclass)",
            &[(view.base_oid as i64).into()],
        )
        .unwrap_or(false);
        if !still_used {
            run(&format!(
                "ALTER PUBLICATION {} DROP TABLE {}",
                quote_identifier(PUBLICATION),
                regclass_text(view.base_oid)
            ));
        }
    }

    #[pg_extern]
    fn refresh(name: &str) {
        let name = require_qualified_name(name);
        let view = load_view(&name);
        // The slot may have been dropped for lag; recreate it before any write.
        ensure_slot();
        ensure_publication_includes(view.base_oid);
        run(&format!("LOCK TABLE {} IN SHARE MODE", view.spec.base_table));
        run("SET LOCAL nabla.internal_write = on");
        run(&format!("DELETE FROM {name}"));
        run(&format!("INSERT INTO {name} {}", view.spec.populate_sql));
        run_args(
            "UPDATE nabla.views SET frontier_lsn = pg_catalog.pg_current_wal_lsn(), epoch = epoch + 1, \
             status = 'live', last_seq = last_seq + 1, resync_seq = last_seq + 1 WHERE id = $1",
            &[view.id.into()],
        );
        run_args("DELETE FROM nabla.deltas WHERE view_id = $1", &[view.id.into()]);
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

    /// (frontier, status) read with a fresh snapshot.
    fn current_frontier(name: &str) -> (u64, String) {
        // Read-only SPI runs under the active snapshot, so push the latest one
        // to observe the worker's commits while polling.
        unsafe { pg_sys::PushActiveSnapshot(pg_sys::GetLatestSnapshot()) };
        let found = Spi::connect(|client| {
            let table = client.select(
                "SELECT frontier_lsn::text, status FROM nabla.views WHERE name = $1",
                Some(1),
                &[name.into()],
            )?;
            if table.is_empty() {
                Ok(None)
            } else {
                let table = table.first();
                Ok(Some((table.get::<String>(1)?, table.get::<String>(2)?)))
            }
        });
        unsafe { pg_sys::PopActiveSnapshot() };
        let (lsn_text, status) = found.unwrap_or_else(|e| spi_fail(e)).unwrap_or((None, None));
        let lsn_text = lsn_text.unwrap_or_else(|| {
            errors::raise(
                PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
                format!("nabla: view \"{name}\" does not exist"),
                None,
            )
        });
        (lsn::parse(&lsn_text).unwrap_or(0), status.unwrap_or_default())
    }

    #[pg_extern(sql = r#"
    CREATE FUNCTION nabla.wait_for(name text, lsn pg_lsn, timeout_ms int DEFAULT 5000) RETURNS bool
    STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
    "#)]
    fn wait_for(name: &str, lsn: i64, timeout_ms: i32) -> bool {
        let name = require_qualified_name(name);
        let target = lsn as u64;
        let deadline = Instant::now() + Duration::from_millis(timeout_ms.max(0) as u64);
        loop {
            let (frontier, status) = current_frontier(&name);
            if status == "stale" {
                errors::prerequisite(
                    format!("nabla: view \"{name}\" is stale and is no longer maintained"),
                    Some("Run nabla.refresh(name) to rebuild it."),
                );
            }
            if frontier >= target {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            pgrx::pg_sys::check_for_interrupts!();
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[pg_extern]
    fn current_seq(name: &str) -> i64 {
        let name = require_qualified_name(name);
        load_view(&name).last_seq
    }

    #[pg_extern(sql = r#"
    CREATE FUNCTION nabla.changes(name text, after_seq bigint, max_rows int DEFAULT 1000)
    RETURNS TABLE (seq bigint, lsn pg_lsn, xid bigint, op "char", "row" jsonb, epoch int)
    STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
    "#)]
    fn changes(
        name: &str,
        after_seq: i64,
        max_rows: i32,
    ) -> TableIterator<'static, (name!(seq, i64), name!(lsn, i64), name!(xid, Option<i64>), name!(op, i8), name!(row, JsonB), name!(epoch, i32))> {
        let name = require_qualified_name(name);
        let view = load_view(&name);
        if view.status == "stale" {
            errors::prerequisite(
                format!("nabla: view \"{name}\" is stale and is no longer maintained"),
                Some("Run nabla.refresh(name) to rebuild it."),
            );
        }
        let oldest = read_one::<i64>("SELECT min(seq) FROM nabla.deltas WHERE view_id = $1", &[view.id.into()]);
        let oldest_available = oldest.unwrap_or(view.last_seq + 1) - 1;
        if after_seq < view.resync_seq || after_seq < oldest_available {
            errors::raise(
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                format!("nabla: subscriber lagged behind retention for view \"{name}\""),
                Some("Resync from the view table and continue from nabla.current_seq(name)."),
            );
        }

        let epoch = view.epoch;
        let args: [DatumWithOid; 3] = [view.id.into(), after_seq.into(), (max_rows.max(0) as i64).into()];
        let rows = Spi::connect(|client| {
            let mut out = Vec::new();
            for r in client.select(
                "SELECT seq, lsn::text, xid, op, row FROM nabla.deltas \
                 WHERE view_id = $1 AND seq > $2 ORDER BY seq LIMIT $3",
                None,
                &args,
            )? {
                let seq = r.get::<i64>(1)?.expect("seq");
                let lsn_text = r.get::<String>(2)?.expect("lsn");
                let xid = r.get::<i64>(3)?;
                let op = r.get::<i8>(4)?.expect("op");
                let row = r.get::<JsonB>(5)?.expect("row");
                out.push((seq, lsn::parse(&lsn_text).unwrap_or(0) as i64, xid, op, row, epoch));
            }
            Ok::<_, pgrx::spi::Error>(out)
        })
        .unwrap_or_else(|e| spi_fail(e));
        TableIterator::new(rows)
    }
}
