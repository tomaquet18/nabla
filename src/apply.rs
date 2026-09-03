// SPDX-License-Identifier: AGPL-3.0-or-later
//! Delta computation for the two view shapes, in two phases:
//!
//! * planning (`plan_*`): evaluate the view's own select list over a
//!   one-row VALUES built from the decoded row (plus, for join views, the
//!   shadow tables of the other relations) and collect signed delta rows —
//!   full view rows shaped exactly like the view table, or a keyed delete;
//! * execution (`execute`): apply the collected rows to the view table and
//!   return the subscriber deltas.
//!
//! The split lets the worker interleave planning with shadow maintenance in
//! stream order while keeping every view's writes in one subtransaction.
//! Decoded values are bound as text parameters and cast to the base column
//! types, never interpolated.

use std::cell::RefCell;
use std::collections::HashMap;

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::spi::{quote_identifier, OwnedPreparedStatement};

use crate::definition::{Shape, ViewSpec};
use crate::pgoutput::{ColumnValue, Relation, Tuple};
use crate::shadow;

/// A delta row produced for the subscriber log.
#[derive(Clone, Debug)]
pub struct Delta {
    pub op: char,
    pub row_json: String,
}

/// Why a view can no longer be maintained incrementally.
pub struct Stale(pub String);

/// One planned change to a view table.
pub enum Op {
    /// A row shaped like the view table (JSON object keyed by column name).
    /// Projection: the row to insert (+1) or delete (-1). Aggregate: one
    /// group's contribution, added (+1) or removed (-1).
    Row { sign: i32, row_json: String },
    /// Delete by typed key values (single-table projection deletes, where
    /// the decoded old row carries only the key).
    DeleteByKey { conds: String, values: Vec<Option<String>> },
}

pub enum Planned {
    Ops(Vec<Op>),
    Stale(Stale),
}

/// A view with what planning needs.
pub struct ViewTarget<'a> {
    pub name: &'a str,
    pub spec: &'a ViewSpec,
}

fn rel_column_index(rel: &Relation, name: &str) -> Option<usize> {
    rel.columns.iter().position(|c| c.name == name)
}

/// `(VALUES ($1::t1, $2::t2, ...)) AS alias(c1, c2, ...)`, using the
/// resolved type of each column so text values are cast correctly.
fn values_source(rel: &Relation, alias: &str, first_param: usize) -> Result<String, String> {
    let mut casts = Vec::with_capacity(rel.columns.len());
    let mut names = Vec::with_capacity(rel.columns.len());
    for (i, col) in rel.columns.iter().enumerate() {
        let ty = col.type_name.as_deref().ok_or_else(|| format!("type of column {} not resolved", col.name))?;
        casts.push(format!("${}::{}", first_param + i, ty));
        names.push(quote_identifier(&col.name));
    }
    Ok(format!("(VALUES ({})) AS {}({})", casts.join(", "), quote_identifier(alias), names.join(", ")))
}

pub fn row_args<'a>(tuple: &[ColumnValue]) -> Vec<DatumWithOid<'a>> {
    tuple
        .iter()
        .map(|v| match v {
            ColumnValue::Text(s) => Some(s.clone()).into(),
            _ => Option::<String>::None.into(),
        })
        .collect()
}

/// Run a statement and return the first column of each returned row as text.
pub fn returning_text(sql: &str, args: &[DatumWithOid]) -> Result<Vec<String>, String> {
    Spi::connect_mut(|client| {
        with_plan(client, sql, args, |client, plan| {
            let mut out = Vec::new();
            for row in client.update(plan, None, args)? {
                if let Some(s) = row.get::<String>(1)? {
                    out.push(s);
                }
            }
            Ok(out)
        })
    })
    .map_err(|e| format!("{sql}: {e}"))
}

/// Run a statement for its side effects through the plan cache.
pub fn execute_cached(sql: &str, args: &[DatumWithOid]) -> Result<(), String> {
    Spi::connect_mut(|client| with_plan(client, sql, args, |client, plan| client.update(plan, None, args).map(|_| ())))
        .map_err(|e| format!("{sql}: {e}"))
}

thread_local! {
    /// Kept SPI plans (`SPI_keepplan`) keyed by statement text and argument
    /// types. The worker is a single backend that repeats a small set of
    /// statements per view, shadow and change kind for its whole life; a kept
    /// plan lives in CacheMemoryContext, survives the worker's transactions,
    /// and PostgreSQL's plan cache re-plans it when a relation it references
    /// changes (rebuilt storage table, extended shadow), so no explicit
    /// invalidation is needed. Statement texts carry the storage or shadow
    /// table name, hence a rebuilt view never resolves to an older plan.
    static PLANS: RefCell<HashMap<(String, Vec<pg_sys::Oid>), OwnedPreparedStatement>> = RefCell::new(HashMap::new());
}

/// Cache bound; DDL churn (views come and go) produces distinct texts, so the
/// cache is dropped wholesale when it grows past this.
const PLAN_CACHE_LIMIT: usize = 4096;

fn with_plan<'c, R>(
    client: &mut pgrx::spi::SpiClient<'c>,
    sql: &str,
    args: &[DatumWithOid],
    run: impl FnOnce(&mut pgrx::spi::SpiClient<'c>, &OwnedPreparedStatement) -> Result<R, pgrx::spi::Error>,
) -> Result<R, pgrx::spi::Error> {
    let key = (sql.to_owned(), args.iter().map(|a| a.oid()).collect::<Vec<_>>());
    if !PLANS.with(|p| p.borrow().contains_key(&key)) {
        let types: Vec<PgOid> = key.1.iter().map(|o| PgOid::from(*o)).collect();
        let plan = client.prepare(sql, &types)?.keep();
        PLANS.with(|p| {
            let mut plans = p.borrow_mut();
            if plans.len() >= PLAN_CACHE_LIMIT {
                plans.clear();
            }
            plans.insert(key.clone(), plan);
        });
    }
    // The plan is removed from the map while it runs so an error unwinding
    // through here (caught by the caller's subtransaction) can never observe
    // a live borrow; it is put back afterwards.
    let plan = PLANS.with(|p| p.borrow_mut().remove(&key)).expect("plan present");
    let result = run(client, &plan);
    PLANS.with(|p| p.borrow_mut().insert(key, plan));
    result
}

/// Substitute unchanged-TOAST markers from the old tuple when available.
/// Columns the definition never mentions are set to NULL; anything else that
/// stays unknown makes the view stale.
pub fn resolve_unchanged(
    rel: &Relation,
    new: &Tuple,
    old: Option<&Tuple>,
    definition_mentions: impl Fn(&str) -> bool,
) -> Result<Tuple, Stale> {
    let mut out = new.clone();
    for (i, v) in out.iter_mut().enumerate() {
        if *v != ColumnValue::Unchanged {
            continue;
        }
        let col = &rel.columns[i].name;
        if let Some(old_tuple) = old {
            if let Some(ov) = old_tuple.get(i) {
                if *ov != ColumnValue::Unchanged {
                    *v = ov.clone();
                    continue;
                }
            }
        }
        if definition_mentions(col) {
            return Err(Stale(format!(
                "unchanged TOAST value for column {col} could not be recovered (REPLICA IDENTITY FULL is required)"
            )));
        }
        *v = ColumnValue::Null;
    }
    Ok(out)
}

/// The view's own query over an arbitrary FROM list, returning each result
/// row as a JSON object shaped like the view table.
fn delta_rows(spec: &ViewSpec, from_list: &str, args: &[DatumWithOid], sign: i32) -> Result<Vec<Op>, String> {
    let mut sql = format!("SELECT to_jsonb(r)::text FROM (SELECT {} FROM {from_list}", spec.select_list);
    if let Some(p) = &spec.predicate {
        sql.push_str(&format!(" WHERE {p}"));
    }
    if let Some(g) = &spec.group_by {
        sql.push_str(&format!(" GROUP BY {g}"));
    }
    sql.push_str(") AS r");
    Ok(returning_text(&sql, args)?.into_iter().map(|row_json| Op::Row { sign, row_json }).collect())
}

// --- planning: single-table views --------------------------------------------

pub fn plan_single_insert(view: &ViewTarget, rel: &Relation, new: &Tuple) -> Result<Vec<Op>, String> {
    let src = values_source(rel, &view.spec.base_relname, 1)?;
    delta_rows(view.spec, &src, &row_args(new), 1)
}

pub fn plan_single_delete(view: &ViewTarget, rel: &Relation, key_kind: u8, old: &Tuple) -> Result<Planned, String> {
    let spec = view.spec;
    match spec.shape {
        Shape::Projection => {
            let mut conds = Vec::new();
            let mut values = Vec::new();
            for (pk, view_col) in spec.pk_columns.iter().zip(spec.pk_view_columns.iter()) {
                let idx = rel_column_index(rel, pk).ok_or_else(|| format!("primary key column {pk} missing"))?;
                let ty = rel.columns[idx].type_name.as_deref().ok_or("type not resolved")?;
                match &old[idx] {
                    ColumnValue::Text(s) => values.push(Some(s.clone())),
                    _ => {
                        return Ok(Planned::Stale(Stale(format!(
                            "primary key column {pk} missing from the decoded old row"
                        ))))
                    }
                }
                conds.push(format!("v.{} = ${}::{}", quote_identifier(view_col), values.len(), ty));
            }
            Ok(Planned::Ops(vec![Op::DeleteByKey { conds: conds.join(" AND "), values }]))
        }
        Shape::Aggregate => {
            if key_kind != b'O' {
                return Ok(Planned::Stale(Stale(
                    "old row not available; the base table needs REPLICA IDENTITY FULL".to_string(),
                )));
            }
            let src = values_source(rel, &spec.base_relname, 1)?;
            Ok(Planned::Ops(delta_rows(spec, &src, &row_args(old), -1)?))
        }
    }
}

// --- planning: join views ------------------------------------------------------

/// Delta rows of a join view for one row of relation `rel_index` (position
/// in `spec.relations`): the row joins the shadows of every other relation.
pub fn plan_join(
    view: &ViewTarget,
    rel_index: usize,
    rel: &Relation,
    row: &Tuple,
    sign: i32,
) -> Result<Vec<Op>, String> {
    let spec = view.spec;
    let mut from = Vec::with_capacity(spec.relations.len());
    for (i, r) in spec.relations.iter().enumerate() {
        if i == rel_index {
            from.push(values_source(rel, &r.alias, 1)?);
        } else {
            from.push(format!("{} AS {}", shadow::table_name(r.oid), quote_identifier(&r.alias)));
        }
    }
    delta_rows(spec, &from.join(", "), &row_args(row), sign)
}

// --- execution -----------------------------------------------------------------

/// Apply planned operations to the view table, in order, and return the
/// subscriber deltas.
pub fn execute(view: &ViewTarget, ops: &[Op]) -> Result<Vec<Delta>, String> {
    let mut deltas = Vec::new();
    for op in ops {
        match op {
            Op::DeleteByKey { conds, values } => {
                let args: Vec<DatumWithOid> = values.iter().map(|v| v.clone().into()).collect();
                let sql = format!("DELETE FROM {} AS v WHERE {conds} RETURNING to_jsonb(v)::text", view.name);
                for row_json in returning_text(&sql, &args)? {
                    deltas.push(Delta { op: 'D', row_json });
                }
            }
            Op::Row { sign, row_json } => match view.spec.shape {
                Shape::Projection => projection_row(view, *sign, row_json, &mut deltas)?,
                Shape::Aggregate => aggregate_row(view, *sign, row_json, &mut deltas)?,
            },
        }
    }
    Ok(deltas)
}

fn projection_row(view: &ViewTarget, sign: i32, row_json: &str, deltas: &mut Vec<Delta>) -> Result<(), String> {
    let args: [DatumWithOid; 1] = [row_json.into()];
    if sign > 0 {
        let sql = format!(
            "INSERT INTO {view} AS v SELECT * FROM jsonb_populate_record(NULL::{view}, $1::jsonb) \
             RETURNING to_jsonb(v)::text",
            view = view.name
        );
        for row_json in returning_text(&sql, &args)? {
            deltas.push(Delta { op: 'I', row_json });
        }
    } else {
        let conds: Vec<String> =
            view.spec.pk_view_columns.iter().map(|c| format!("v.{c} = d.{c}", c = quote_identifier(c))).collect();
        let sql = format!(
            "DELETE FROM {view} AS v USING jsonb_populate_record(NULL::{view}, $1::jsonb) AS d \
             WHERE {conds} RETURNING to_jsonb(v)::text",
            view = view.name,
            conds = conds.join(" AND ")
        );
        for row_json in returning_text(&sql, &args)? {
            deltas.push(Delta { op: 'D', row_json });
        }
    }
    Ok(())
}

/// Add (`sign = 1`) or remove (`sign = -1`) one group contribution, given as
/// a view-shaped row whose count and sum columns hold the contribution.
fn aggregate_row(view: &ViewTarget, sign: i32, row_json: &str, deltas: &mut Vec<Delta>) -> Result<(), String> {
    let spec = view.spec;
    let count = quote_identifier(spec.count_alias.as_deref().ok_or("aggregate view without count(*)")?);
    let args: [DatumWithOid; 1] = [row_json.into()];
    let cte = format!("WITH src AS (SELECT * FROM jsonb_populate_record(NULL::{}, $1::jsonb))", view.name);

    let join_on: Vec<String> = spec
        .columns
        .iter()
        .map(|c| format!("v.{a} IS NOT DISTINCT FROM src.{a}", a = quote_identifier(&c.alias)))
        .collect();
    let previous = returning_text(
        &format!(
            "{cte} SELECT to_jsonb(v)::text FROM {view} AS v JOIN src ON {on}",
            view = view.name,
            on = join_on.join(" AND ")
        ),
        &args,
    )?;

    let group_aliases: Vec<String> = spec.columns.iter().map(|c| quote_identifier(&c.alias)).collect();
    let mut insert_cols = group_aliases.clone();
    let mut insert_select = group_aliases.clone();
    let mut updates: Vec<String> = Vec::new();
    // count(*): the contribution carries the number of rows.
    insert_cols.push(count.clone());
    insert_select.push(format!("({sign}) * {count}"));
    updates.push(format!("{count} = v.{count} + EXCLUDED.{count}"));
    // count(expr): the number of non-NULL values.
    for c in &spec.counts {
        let a = quote_identifier(&c.alias);
        insert_cols.push(a.clone());
        insert_select.push(format!("({sign}) * {a}"));
        updates.push(format!("{a} = v.{a} + EXCLUDED.{a}"));
    }
    // sum(expr) with its hidden non-NULL counter. sum() ignores NULLs and is
    // NULL when nothing non-NULL contributed, so the stored sum is NULL exactly
    // when the counter is 0; the CASE keeps that atomic with the row update.
    for su in &spec.sums {
        let a = quote_identifier(&su.alias);
        let nn = quote_identifier(&su.counter);
        insert_cols.push(a.clone());
        insert_select.push(format!("({sign}) * {a}"));
        insert_cols.push(nn.clone());
        insert_select.push(format!("({sign}) * {nn}"));
        updates.push(format!(
            "{a} = CASE WHEN v.{nn} + EXCLUDED.{nn} = 0 THEN NULL \
             WHEN EXCLUDED.{a} IS NULL THEN v.{a} WHEN v.{a} IS NULL THEN EXCLUDED.{a} \
             ELSE v.{a} + EXCLUDED.{a} END"
        ));
        updates.push(format!("{nn} = v.{nn} + EXCLUDED.{nn}"));
    }
    let upsert = format!(
        "{cte} INSERT INTO {view} AS v ({cols}) SELECT {sel} FROM src \
         ON CONFLICT ({conflict}) DO UPDATE SET {updates} RETURNING to_jsonb(v)::text || ' ' || v.{count}::text",
        view = view.name,
        cols = insert_cols.join(", "),
        sel = insert_select.join(", "),
        conflict = group_aliases.join(", "),
        updates = updates.join(", "),
    );
    let returned = returning_text(&upsert, &args)?;
    let Some(returned) = returned.into_iter().next() else {
        return Ok(());
    };
    let (new_row, count_text) = returned.rsplit_once(' ').ok_or("unexpected RETURNING shape")?;
    let new_count: i64 = count_text.parse().map_err(|e| format!("bad count: {e}"))?;

    for row_json in previous {
        deltas.push(Delta { op: 'D', row_json });
    }
    if new_count > 0 {
        deltas.push(Delta { op: 'I', row_json: new_row.to_string() });
    } else {
        execute_cached(&format!("DELETE FROM {} AS v WHERE v.{count} <= 0", view.name), &[])
            .map_err(|e| format!("group cleanup failed: {e}"))?;
    }
    Ok(())
}

// --- netting -------------------------------------------------------------------

/// The identity of a view row: the group keys of an aggregate view, the
/// selected primary key columns of a single-table projection, the hidden
/// `_nabla_pk*` columns of a join projection.
fn identity_columns(spec: &ViewSpec) -> Vec<&str> {
    match spec.shape {
        Shape::Aggregate => spec.columns.iter().map(|c| c.alias.as_str()).collect(),
        Shape::Projection => spec.pk_view_columns.iter().map(|s| s.as_str()).collect(),
    }
}

fn visible(spec: &ViewSpec, row: &serde_json::Value) -> serde_json::Value {
    let mut row = row.clone();
    if let serde_json::Value::Object(map) = &mut row {
        for h in &spec.hidden_columns {
            map.remove(h);
        }
    }
    row
}

/// Net the deltas of one source transaction for one view: per identity key,
/// `D(before)` (the row carried by the key's first `D`, absent if its first
/// event is an `I`) then `I(after)` (the row of its last `I`, absent if its
/// last event is a `D`), nothing when the two are equal on visible columns.
/// Keys keep the order of their first appearance. Subscribers therefore only
/// ever see committed states, never the intermediate rows the per-change
/// maintenance passed through. The buffer is bounded by the transaction's
/// own delta count, which is already fully decoded in memory.
pub fn net(spec: &ViewSpec, deltas: Vec<Delta>) -> Vec<Delta> {
    struct State {
        before: Option<serde_json::Value>,
        after: Option<serde_json::Value>,
    }
    let identity = identity_columns(spec);
    let mut order: Vec<Vec<serde_json::Value>> = Vec::new();
    let mut states: Vec<State> = Vec::new();
    for d in deltas {
        let row: serde_json::Value = match serde_json::from_str(&d.row_json) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let key: Vec<serde_json::Value> =
            identity.iter().map(|c| row.get(*c).cloned().unwrap_or(serde_json::Value::Null)).collect();
        let index = match order.iter().position(|k| *k == key) {
            Some(i) => i,
            None => {
                order.push(key);
                states.push(State { before: if d.op == 'D' { Some(row.clone()) } else { None }, after: None });
                states.len() - 1
            }
        };
        let state = &mut states[index];
        state.after = if d.op == 'I' { Some(row) } else { None };
    }
    let mut out = Vec::new();
    for state in states {
        let before_visible = state.before.as_ref().map(|r| visible(spec, r));
        let after_visible = state.after.as_ref().map(|r| visible(spec, r));
        if before_visible == after_visible {
            continue;
        }
        if let Some(before) = state.before {
            out.push(Delta { op: 'D', row_json: before.to_string() });
        }
        if let Some(after) = state.after {
            out.push(Delta { op: 'I', row_json: after.to_string() });
        }
    }
    out
}

#[cfg(test)]
mod net_tests {
    use super::*;
    use crate::definition::{OutputColumn, ViewSpec};

    fn spec() -> ViewSpec {
        ViewSpec {
            shape: Shape::Aggregate,
            base_table: String::new(),
            base_relname: String::new(),
            predicate: None,
            columns: vec![OutputColumn { expr: "k".into(), alias: "k".into() }],
            pk_columns: vec![],
            pk_view_columns: vec![],
            count_alias: Some("n".into()),
            hidden_count: false,
            counts: vec![],
            sums: vec![],
            populate_sql: String::new(),
            relations: vec![],
            select_list: String::new(),
            group_by: None,
            visible_columns: vec!["k".into(), "n".into()],
            hidden_columns: vec!["_nabla_nn_0".into()],
        }
    }

    fn d(op: char, json: &str) -> Delta {
        Delta { op, row_json: json.to_string() }
    }

    #[test]
    fn nets_to_final_state() {
        let out =
            net(&spec(), vec![d('I', r#"{"k":1,"n":1}"#), d('D', r#"{"k":1,"n":1}"#), d('I', r#"{"k":1,"n":2}"#)]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].op, 'I');
        assert!(out[0].row_json.contains(r#""n":2"#));
    }

    #[test]
    fn insert_then_delete_is_silent_and_delete_keeps_before() {
        assert!(net(&spec(), vec![d('I', r#"{"k":1,"n":1}"#), d('D', r#"{"k":1,"n":1}"#)]).is_empty());
        let out =
            net(&spec(), vec![d('D', r#"{"k":2,"n":3}"#), d('I', r#"{"k":2,"n":1}"#), d('D', r#"{"k":2,"n":1}"#)]);
        assert_eq!(out.len(), 1);
        assert_eq!((out[0].op, out[0].row_json.contains(r#""n":3"#)), ('D', true));
    }

    #[test]
    fn hidden_only_changes_are_silent_and_keys_keep_order() {
        assert!(net(
            &spec(),
            vec![d('D', r#"{"k":1,"n":1,"_nabla_nn_0":0}"#), d('I', r#"{"k":1,"n":1,"_nabla_nn_0":1}"#)]
        )
        .is_empty());
        let out =
            net(&spec(), vec![d('D', r#"{"k":9,"n":1}"#), d('I', r#"{"k":9,"n":2}"#), d('D', r#"{"k":3,"n":5}"#)]);
        let ops: Vec<(char, i64)> = out
            .iter()
            .map(|x| (x.op, serde_json::from_str::<serde_json::Value>(&x.row_json).unwrap()["k"].as_i64().unwrap()))
            .collect();
        assert_eq!(ops, vec![('D', 9), ('I', 9), ('D', 3)]);
    }
}
