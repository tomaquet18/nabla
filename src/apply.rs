//! Delta computation for the two view shapes. Everything runs as SQL over SPI
//! inside the worker's apply transaction; decoded values are bound as text
//! parameters and cast to the base column types, never interpolated.

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::spi::quote_identifier;

use crate::definition::{Shape, ViewSpec};
use crate::pgoutput::{ColumnValue, Relation, Tuple};

/// A delta row produced for the subscriber log.
pub struct Delta {
    pub op: char,
    pub row_json: String,
}

/// Why a view can no longer be maintained incrementally.
pub struct Stale(pub String);

pub enum Outcome {
    Applied(Vec<Delta>),
    Stale(Stale),
}

/// A view with its runtime state for one apply transaction.
pub struct ViewTarget<'a> {
    pub name: &'a str,
    pub spec: &'a ViewSpec,
}

fn rel_column_index(rel: &Relation, name: &str) -> Option<usize> {
    rel.columns.iter().position(|c| c.name == name)
}

/// `(VALUES ($1::t1, $2::t2, ...)) AS relname(c1, c2, ...)`, using the
/// resolved type of each column so text values are cast correctly.
fn values_source(rel: &Relation, first_param: usize) -> Result<String, String> {
    let mut casts = Vec::with_capacity(rel.columns.len());
    let mut names = Vec::with_capacity(rel.columns.len());
    for (i, col) in rel.columns.iter().enumerate() {
        let ty = col
            .type_name
            .as_deref()
            .ok_or_else(|| format!("type of column {} not resolved", col.name))?;
        casts.push(format!("${}::{}", first_param + i, ty));
        names.push(quote_identifier(&col.name));
    }
    Ok(format!(
        "(VALUES ({})) AS {}({})",
        casts.join(", "),
        quote_identifier(&rel.name),
        names.join(", ")
    ))
}

fn row_args<'a>(tuple: &[ColumnValue]) -> Vec<DatumWithOid<'a>> {
    tuple
        .iter()
        .map(|v| match v {
            ColumnValue::Text(s) => Some(s.clone()).into(),
            _ => Option::<String>::None.into(),
        })
        .collect()
}

fn where_clause(spec: &ViewSpec) -> String {
    match &spec.predicate {
        Some(p) => format!(" WHERE {p}"),
        None => String::new(),
    }
}

/// Run a statement and return the first column of each returned row as text.
fn returning_text(sql: &str, args: &[DatumWithOid]) -> Result<Vec<String>, String> {
    Spi::connect_mut(|client| {
        let mut out = Vec::new();
        for row in client.update(sql, None, args)? {
            if let Some(s) = row.get::<String>(1)? {
                out.push(s);
            }
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .map_err(|e| format!("{sql}: {e}"))
}

/// Substitute unchanged-TOAST markers from the old tuple when available.
/// Columns the definition never mentions are set to NULL; anything else that
/// stays unknown makes the view stale.
pub fn resolve_unchanged(
    rel: &Relation,
    new: &Tuple,
    old: Option<&(u8, Tuple)>,
    definition_mentions: impl Fn(&str) -> bool,
) -> Result<Tuple, Stale> {
    let mut out = new.clone();
    for (i, v) in out.iter_mut().enumerate() {
        if *v != ColumnValue::Unchanged {
            continue;
        }
        let col = &rel.columns[i].name;
        if let Some((b'O', old_tuple)) = old {
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

pub fn apply_insert(view: &ViewTarget, rel: &Relation, new: &Tuple) -> Result<Outcome, String> {
    match view.spec.shape {
        Shape::Projection => projection_insert(view, rel, new),
        Shape::Aggregate => aggregate_adjust(view, rel, new, 1),
    }
}

pub fn apply_delete(view: &ViewTarget, rel: &Relation, key_kind: u8, old: &Tuple) -> Result<Outcome, String> {
    match view.spec.shape {
        Shape::Projection => projection_delete(view, rel, old),
        Shape::Aggregate => {
            if key_kind != b'O' {
                return Ok(Outcome::Stale(Stale(
                    "old row not available; the base table needs REPLICA IDENTITY FULL".to_string(),
                )));
            }
            aggregate_adjust(view, rel, old, -1)
        }
    }
}

fn projection_insert(view: &ViewTarget, rel: &Relation, new: &Tuple) -> Result<Outcome, String> {
    let spec = view.spec;
    let targets: Vec<String> = spec.columns.iter().map(|c| quote_identifier(&c.alias)).collect();
    let select: Vec<String> = spec
        .columns
        .iter()
        .map(|c| format!("{} AS {}", quote_identifier(&c.base), quote_identifier(&c.alias)))
        .collect();
    let sql = format!(
        "INSERT INTO {view} AS v ({targets}) SELECT {select} FROM {src}{where} RETURNING to_jsonb(v)::text",
        view = view.name,
        targets = targets.join(", "),
        select = select.join(", "),
        src = values_source(rel, 1)?,
        where = where_clause(spec),
    );
    let rows = returning_text(&sql, &row_args(new))?;
    Ok(Outcome::Applied(rows.into_iter().map(|row_json| Delta { op: 'I', row_json }).collect()))
}

fn projection_delete(view: &ViewTarget, rel: &Relation, old: &Tuple) -> Result<Outcome, String> {
    let spec = view.spec;
    let view_cols = spec.pk_view_columns();
    let mut conds = Vec::new();
    let mut args = Vec::new();
    for (pk, view_col) in spec.pk_columns.iter().zip(view_cols.iter()) {
        let idx = rel_column_index(rel, pk).ok_or_else(|| format!("primary key column {pk} missing"))?;
        let col = &rel.columns[idx];
        let ty = col.type_name.as_deref().ok_or("type not resolved")?;
        match &old[idx] {
            ColumnValue::Text(s) => args.push(Some(s.clone()).into()),
            _ => {
                return Ok(Outcome::Stale(Stale(format!(
                    "primary key column {pk} missing from the decoded old row"
                ))))
            }
        }
        conds.push(format!("v.{} = ${}::{}", quote_identifier(view_col), args.len(), ty));
    }
    let sql = format!(
        "DELETE FROM {view} AS v WHERE {conds} RETURNING to_jsonb(v)::text",
        view = view.name,
        conds = conds.join(" AND "),
    );
    let rows = returning_text(&sql, &args)?;
    Ok(Outcome::Applied(rows.into_iter().map(|row_json| Delta { op: 'D', row_json }).collect()))
}

/// Add (`sign = 1`) or remove (`sign = -1`) one base row's contribution to its group.
fn aggregate_adjust(view: &ViewTarget, rel: &Relation, row: &Tuple, sign: i32) -> Result<Outcome, String> {
    let spec = view.spec;
    let count = quote_identifier(spec.count_alias.as_deref().ok_or("aggregate view without count(*)")?);
    let src = values_source(rel, 1)?;
    let args = row_args(row);

    let group_select: Vec<String> = spec
        .columns
        .iter()
        .map(|c| format!("{} AS {}", quote_identifier(&c.base), quote_identifier(&c.alias)))
        .collect();
    let sum_select: Vec<String> = spec
        .sums
        .iter()
        .map(|s| format!("{} AS {}", quote_identifier(&s.base), quote_identifier(&s.alias)))
        .collect();
    let mut cte_select = group_select.clone();
    cte_select.extend(sum_select);
    let cte = format!(
        "WITH src AS (SELECT {} FROM {src}{})",
        cte_select.join(", "),
        where_clause(spec)
    );

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
    insert_cols.push(count.clone());
    insert_cols.extend(spec.sums.iter().map(|s| quote_identifier(&s.alias)));
    let mut insert_select = group_aliases.clone();
    insert_select.push(format!("{sign}"));
    insert_select.extend(spec.sums.iter().map(|s| format!("({sign}) * {}", quote_identifier(&s.alias))));
    let mut updates = vec![format!("{count} = v.{count} + EXCLUDED.{count}")];
    for s in &spec.sums {
        let a = quote_identifier(&s.alias);
        // sum() ignores NULLs: a NULL contribution leaves the running sum untouched.
        updates.push(format!(
            "{a} = CASE WHEN EXCLUDED.{a} IS NULL THEN v.{a} WHEN v.{a} IS NULL THEN EXCLUDED.{a} ELSE v.{a} + EXCLUDED.{a} END"
        ));
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
        // The row did not satisfy the predicate: no group is affected.
        return Ok(Outcome::Applied(Vec::new()));
    };
    let (row_json, count_text) = returned.rsplit_once(' ').ok_or("unexpected RETURNING shape")?;
    let new_count: i64 = count_text.parse().map_err(|e| format!("bad count: {e}"))?;

    let mut deltas = Vec::new();
    for row_json in previous {
        deltas.push(Delta { op: 'D', row_json });
    }
    if new_count > 0 {
        deltas.push(Delta { op: 'I', row_json: row_json.to_string() });
    } else {
        Spi::run(&format!("DELETE FROM {} AS v WHERE v.{count} <= 0", view.name))
            .map_err(|e| format!("group cleanup failed: {e}"))?;
    }
    Ok(Outcome::Applied(deltas))
}
