//! Shadow tables: a copy of every base table used by a join view, kept at the
//! same position in the change stream as the views. Invariant after the
//! worker absorbs transaction T: shadow(X) == X as of T's commit. Join deltas
//! are evaluated against shadows, never against live tables, which may have
//! moved on since T.
//!
//! Shadow tables live in schema `nabla_shadow` as `t<oid>` and are written
//! only by the worker (same guard trigger as views). They are not dumped by
//! pg_dump; after a restore, `nabla.refresh` rebuilds them.

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::spi::quote_identifier;

use crate::definition::BaseRelation;
use crate::errors;

pub const SCHEMA: &str = "nabla_shadow";

pub fn table_name(oid: u32) -> String {
    format!("{SCHEMA}.t{oid}")
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

/// (refcount, stale_reason) of an existing shadow.
fn state(oid: u32) -> Option<(i32, Option<String>)> {
    Spi::get_two_with_args::<i32, String>(
        "SELECT refcount, stale_reason FROM nabla.shadows WHERE relid = $1::oid",
        &[(oid as i64).into()],
    )
    .ok()
    .and_then(|(r, s)| r.map(|r| (r, s)))
}

/// Populate the shadow from the live table. The caller holds SHARE locks on
/// every table involved, so the snapshot is exact.
pub fn snapshot(rel: &BaseRelation) {
    let table = table_name(rel.oid);
    run("SET LOCAL nabla.internal_write = on");
    run(&format!("DELETE FROM {table}"));
    run(&format!("INSERT INTO {table} SELECT * FROM {}", rel.qualified));
    run_args(
        "UPDATE nabla.shadows SET frontier_lsn = pg_catalog.pg_current_wal_lsn(), stale_reason = NULL \
         WHERE relid = $1::oid",
        &[(rel.oid as i64).into()],
    );
}

/// Create the shadow of `rel` for one more view, or add a reference to the
/// existing one. An existing healthy shadow is deliberately NOT re-snapshotted:
/// it has its own frontier and the worker skips transactions at or below it,
/// so it will absorb what the new view (populated later, at a higher LSN)
/// already contains — see the skip rule in worker.rs. A shadow whose
/// maintenance failed has no live dependents any more and is rebuilt.
pub fn ensure(rel: &BaseRelation) {
    let table = table_name(rel.oid);
    match state(rel.oid) {
        Some((_, None)) => {
            run_args("UPDATE nabla.shadows SET refcount = refcount + 1 WHERE relid = $1::oid", &[(rel.oid as i64).into()]);
        }
        Some((_, Some(_))) => {
            run_args("UPDATE nabla.shadows SET refcount = refcount + 1 WHERE relid = $1::oid", &[(rel.oid as i64).into()]);
            snapshot(rel);
        }
        None => {
            run(&format!("CREATE TABLE {table} AS SELECT * FROM {}", rel.qualified));
            let pk: Vec<String> = rel.pk_columns.iter().map(quote_identifier).collect();
            run(&format!("CREATE UNIQUE INDEX ON {table} ({})", pk.join(", ")));
            run(&format!(
                "CREATE TRIGGER nabla_guard BEFORE INSERT OR UPDATE OR DELETE ON {table} \
                 FOR EACH ROW EXECUTE FUNCTION nabla.guard_view()"
            ));
            run(&format!(
                "CREATE TRIGGER nabla_guard_truncate BEFORE TRUNCATE ON {table} \
                 FOR EACH STATEMENT EXECUTE FUNCTION nabla.guard_view()"
            ));
            run_args(
                "INSERT INTO nabla.shadows (relid, table_name, frontier_lsn, refcount) \
                 VALUES ($1::oid, $2, pg_catalog.pg_current_wal_lsn(), 1)",
                &[(rel.oid as i64).into(), table.as_str().into()],
            );
        }
    }
}

/// Drop one reference; the shadow is dropped when nobody uses it. Returns
/// true when the shadow was dropped.
pub fn release(oid: u32) -> bool {
    let Some((refcount, _)) = state(oid) else {
        return false;
    };
    if refcount > 1 {
        run_args("UPDATE nabla.shadows SET refcount = refcount - 1 WHERE relid = $1::oid", &[(oid as i64).into()]);
        return false;
    }
    run(&format!("DROP TABLE IF EXISTS {}", table_name(oid)));
    run_args("DELETE FROM nabla.shadows WHERE relid = $1::oid", &[(oid as i64).into()]);
    true
}
