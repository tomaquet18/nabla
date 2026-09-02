// SPDX-License-Identifier: AGPL-3.0-only
//! Shadow tables: a copy of the columns of every base table that join views
//! need, kept at the same position in the change stream as the views.
//! Invariant after the worker absorbs transaction T: shadow(X) == X as of
//! T's commit, for the columns it holds. Join deltas are evaluated against
//! shadows, never against live tables, which may have moved on since T.
//!
//! A shadow holds the primary key plus the union of the columns used by the
//! views that share it (`nabla.shadows.columns`, kept in base attribute
//! order). Columns are added when a new view needs them; they are never
//! removed when a view is dropped (a later compaction may do that). Shadow
//! tables live in schema `nabla_shadow` as `t<oid>`, are written only by the
//! worker (same guard trigger as views), depend on their base table in
//! `pg_depend` (so `DROP TABLE base` needs CASCADE and takes them along), are
//! created and re-snapshotted inside the worker's population transaction
//! under the consistent snapshot of populate.rs, and are not dumped by
//! pg_dump (`nabla.refresh` rebuilds them after a restore).

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::spi::quote_identifier;
use std::collections::BTreeSet;

use crate::definition::BaseRelation;
use crate::errors;
use crate::lsn;

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

/// Existing shadow: (refcount, columns, column types).
struct Existing {
    columns: Vec<String>,
    types: Vec<String>,
}

fn existing(oid: u32) -> Option<Existing> {
    Spi::connect(|client| {
        let table = client.select(
            "SELECT columns, column_types FROM nabla.shadows WHERE relid = $1::oid",
            Some(1),
            &[(oid as i64).into()],
        )?;
        if table.is_empty() {
            return Ok(None);
        }
        let t = table.first();
        Ok(Some(Existing {
            columns: t.get::<Vec<String>>(1)?.unwrap_or_default(),
            types: t.get::<Vec<String>>(2)?.unwrap_or_default(),
        }))
    })
    .unwrap_or_else(|e| spi_fail(e))
}

/// The base table's current columns (name, type) in attribute order, read
/// from the catalog under the caller's snapshot.
pub fn base_columns(oid: u32) -> Vec<(String, String)> {
    Spi::connect(|client| {
        let mut out = Vec::new();
        for r in client.select(
            "SELECT attname::text, pg_catalog.format_type(atttypid, atttypmod) FROM pg_catalog.pg_attribute \
             WHERE attrelid = $1::oid AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
            None,
            &[(oid as i64).into()],
        )? {
            out.push((r.get::<String>(1)?.unwrap_or_default(), r.get::<String>(2)?.unwrap_or_default()));
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .unwrap_or_else(|e| spi_fail(e))
}

/// The shadow table's physical columns (name, type): the active set in
/// `nabla.shadows.columns` can be smaller after schema drift pruned it, but
/// the column is still there.
fn physical_columns(table: &str) -> Vec<(String, String)> {
    match relation_oid(table) {
        Some(oid) => base_columns(oid),
        None => Vec::new(),
    }
}

/// Record `dependent` (a table) as depending on `referenced` (a table) with a
/// NORMAL dependency, the way a SQL view depends on its tables: dropping the
/// referenced table then requires CASCADE and takes the dependent along.
pub fn record_dependency(dependent: u32, referenced: u32) {
    let dependent = pg_sys::ObjectAddress {
        classId: pg_sys::RelationRelationId,
        objectId: pg_sys::Oid::from(dependent),
        objectSubId: 0,
    };
    let referenced = pg_sys::ObjectAddress {
        classId: pg_sys::RelationRelationId,
        objectId: pg_sys::Oid::from(referenced),
        objectSubId: 0,
    };
    unsafe {
        pg_sys::recordDependencyOn(&dependent, &referenced, pg_sys::DependencyType::DEPENDENCY_NORMAL);
        pg_sys::CommandCounterIncrement();
    }
}

pub fn relation_oid(name: &str) -> Option<u32> {
    Spi::connect(|client| {
        let table = client.select("SELECT pg_catalog.to_regclass($1)::oid::int8", Some(1), &[name.into()])?;
        if table.is_empty() {
            Ok(None)
        } else {
            table.first().get_one::<i64>().map(|o| o.map(|o| o as u32))
        }
    })
    .unwrap_or_else(|e| spi_fail(e))
}

fn quoted_list(cols: &[String]) -> String {
    cols.iter().map(|c| quote_identifier(c)).collect::<Vec<_>>().join(", ")
}

fn pk_match(pk: &[String]) -> String {
    pk.iter().map(|c| format!("s.{c} = b.{c}", c = quote_identifier(c))).collect::<Vec<_>>().join(" AND ")
}

/// The active column set of a shadow: primary key plus `needed`, restricted
/// to columns the base table currently has, in attribute order.
fn active_set(rel: &BaseRelation, needed: &BTreeSet<String>, base: &[(String, String)]) -> (Vec<String>, Vec<String>) {
    let mut names = Vec::new();
    let mut types = Vec::new();
    for (name, ty) in base {
        if rel.pk_columns.contains(name) || needed.contains(name) {
            names.push(name.clone());
            types.push(ty.clone());
        }
    }
    (names, types)
}

fn store_columns(oid: u32, names: &[String], types: &[String]) {
    run_args(
        "UPDATE nabla.shadows SET columns = $2, column_types = $3 WHERE relid = $1::oid",
        &[(oid as i64).into(), names.to_vec().into(), types.to_vec().into()],
    );
}

/// Add columns a new view needs to an existing shadow and backfill them from
/// the base table under the caller's snapshot.
///
/// Why this is correct although the shadow's frontier may be behind the
/// snapshot: the views that already use the shadow do not read the new
/// columns; the new view skips every transaction at or below its own
/// frontier (the snapshot's consistent point); and rows changed between the
/// shadow's frontier and the snapshot are rewritten in full when the worker
/// absorbs those transactions (INSERT/UPDATE tuples carry every column, a
/// DELETE removes the row, and unchanged-TOAST markers keep the backfilled
/// value).
fn extend(rel: &BaseRelation, current: &Existing, needed: &BTreeSet<String>) {
    let table = table_name(rel.oid);
    let base = base_columns(rel.oid);
    let physical = physical_columns(&table);
    let mut names = current.columns.clone();
    let mut types = current.types.clone();
    run("SET LOCAL nabla.internal_write = on");
    for (name, ty) in &base {
        if names.contains(name) || !(needed.contains(name) || rel.pk_columns.contains(name)) {
            continue;
        }
        let col = quote_identifier(name);
        match physical.iter().find(|(n, _)| n == name) {
            None => run(&format!("ALTER TABLE {table} ADD COLUMN {col} {ty}")),
            // Present but pruned after drift: bring it back, retyped if needed.
            Some((_, have)) if have != ty => {
                run(&format!("ALTER TABLE {table} ALTER COLUMN {col} TYPE {ty} USING NULL::{ty}"));
            }
            Some(_) => {}
        }
        run(&format!(
            "UPDATE {table} AS s SET {col} = b.{col} FROM {} AS b WHERE {}",
            rel.qualified,
            pk_match(&rel.pk_columns)
        ));
        names.push(name.clone());
        types.push(ty.clone());
    }
    // Keep attribute order for readability of nabla.shadows.columns.
    let order: Vec<&String> = base.iter().map(|(n, _)| n).collect();
    let mut pairs: Vec<(String, String)> = names.into_iter().zip(types).collect();
    pairs.sort_by_key(|(n, _)| order.iter().position(|o| *o == n).unwrap_or(usize::MAX));
    let (names, types): (Vec<String>, Vec<String>) = pairs.into_iter().unzip();
    store_columns(rel.oid, &names, &types);
}

fn create(rel: &BaseRelation, frontier: u64, needed: &BTreeSet<String>) {
    let table = table_name(rel.oid);
    let base = base_columns(rel.oid);
    let (names, types) = active_set(rel, needed, &base);
    run(&format!("CREATE TABLE {table} AS SELECT {} FROM {}", quoted_list(&names), rel.qualified));
    run(&format!("CREATE UNIQUE INDEX ON {table} ({})", quoted_list(&rel.pk_columns)));
    run(&format!(
        "CREATE TRIGGER nabla_guard BEFORE INSERT OR UPDATE OR DELETE ON {table} \
         FOR EACH ROW EXECUTE FUNCTION nabla.guard_view()"
    ));
    run(&format!(
        "CREATE TRIGGER nabla_guard_truncate BEFORE TRUNCATE ON {table} \
         FOR EACH STATEMENT EXECUTE FUNCTION nabla.guard_view()"
    ));
    run_args(
        "INSERT INTO nabla.shadows (relid, table_name, frontier_lsn, refcount, columns, pk_columns, column_types) \
         VALUES ($1::oid, $2, $3::pg_lsn, 1, $4, $5, $6)",
        &[
            (rel.oid as i64).into(),
            table.as_str().into(),
            lsn::format(frontier).into(),
            names.into(),
            rel.pk_columns.clone().into(),
            types.into(),
        ],
    );
    if let Some(shadow_oid) = relation_oid(&table) {
        record_dependency(shadow_oid, rel.oid);
    }
}

/// Reference the shadow of `rel` for one more view: create it with the
/// needed columns, or extend the existing one. An existing healthy shadow is
/// deliberately NOT re-snapshotted: it has its own frontier and the worker
/// skips transactions at or below it (see the skip rule in worker.rs). A
/// shadow whose maintenance failed has no live dependents and is rebuilt.
pub fn ensure(rel: &BaseRelation, frontier: u64, needed: &BTreeSet<String>) {
    match existing(rel.oid) {
        Some(current) => {
            run_args("UPDATE nabla.shadows SET refcount = refcount + 1 WHERE relid = $1::oid", &[(rel.oid as i64).into()]);
            let failed = Spi::get_one_with_args::<bool>(
                "SELECT failed FROM nabla.shadows WHERE relid = $1::oid",
                &[(rel.oid as i64).into()],
            )
            .ok()
            .flatten()
            .unwrap_or(false);
            if failed {
                rebuild(rel, frontier, needed);
            } else {
                extend(rel, &current, needed);
            }
        }
        None => create(rel, frontier, needed),
    }
}

/// Rebuild an existing shadow from scratch under the caller's snapshot with
/// the current column set and types (refresh path).
pub fn rebuild(rel: &BaseRelation, frontier: u64, needed: &BTreeSet<String>) {
    if existing(rel.oid).is_none() {
        return create(rel, frontier, needed);
    }
    let table = table_name(rel.oid);
    let base = base_columns(rel.oid);
    let physical = physical_columns(&table);
    let (names, types) = active_set(rel, needed, &base);
    run("SET LOCAL nabla.internal_write = on");
    run(&format!("DELETE FROM {table}"));
    for (name, ty) in names.iter().zip(types.iter()) {
        let col = quote_identifier(name);
        match physical.iter().find(|(n, _)| n == name) {
            None => run(&format!("ALTER TABLE {table} ADD COLUMN {col} {ty}")),
            Some((_, have)) if have != ty => {
                // The table is empty at this point, so the change is trivial.
                run(&format!("ALTER TABLE {table} ALTER COLUMN {col} TYPE {ty} USING NULL::{ty}"));
            }
            Some(_) => {}
        }
    }
    let list = quoted_list(&names);
    run(&format!("INSERT INTO {table} ({list}) SELECT {list} FROM {}", rel.qualified));
    run_args(
        "UPDATE nabla.shadows SET frontier_lsn = $2::pg_lsn, stale_reason = NULL, failed = false, \
         columns = $3, pk_columns = $4, column_types = $5 WHERE relid = $1::oid",
        &[
            (rel.oid as i64).into(),
            lsn::format(frontier).into(),
            names.into(),
            rel.pk_columns.clone().into(),
            types.into(),
        ],
    );
}
