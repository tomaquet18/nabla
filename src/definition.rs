//! Parser for the two accepted view definition shapes.
//!
//! v0.1 matches the definition text with strict, case-insensitive regular
//! expressions. The module is deliberately isolated so it can be replaced by
//! a parse-tree walker without touching the rest of the extension. Known
//! limitations of the text matcher: quoted or mixed-case identifiers, SQL
//! comments, and schema-qualified column references are not supported;
//! keyword-looking words inside string literals of the predicate may be
//! misclassified as forbidden syntax.

use pgrx::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;

use crate::errors;

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Shape {
    Projection,
    Aggregate,
}

impl Shape {
    pub fn as_str(self) -> &'static str {
        match self {
            Shape::Projection => "projection",
            Shape::Aggregate => "aggregate",
        }
    }
}

/// A base column and the name it has in the view table.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ColumnRef {
    pub base: String,
    pub alias: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ViewSpec {
    pub shape: Shape,
    /// Schema-qualified base table name, lowercase, as used in generated SQL.
    pub base_table: String,
    /// Bare relation name; generated SQL aliases the decoded row with it so a
    /// predicate written as `orders.status = 'paid'` keeps working.
    pub base_relname: String,
    pub predicate: Option<String>,
    /// Projection: the select list. Aggregate: the group columns.
    pub columns: Vec<ColumnRef>,
    /// Base primary key columns (projection only).
    pub pk_columns: Vec<String>,
    /// View column holding count(*) (aggregate only).
    pub count_alias: Option<String>,
    /// sum(col) aggregates (aggregate only).
    pub sums: Vec<ColumnRef>,
}

impl ViewSpec {
    /// View column names for the base primary key, in PK order.
    pub fn pk_view_columns(&self) -> Vec<String> {
        self.pk_columns
            .iter()
            .map(|pk| {
                self.columns
                    .iter()
                    .find(|c| &c.base == pk)
                    .map(|c| c.alias.clone())
                    .unwrap_or_else(|| pk.clone())
            })
            .collect()
    }
}

/// Result of the pure text stage, before any catalog lookup.
#[derive(Debug)]
struct Parsed {
    table: String,
    predicate: Option<String>,
    columns: Vec<ColumnRef>,
    count_alias: Option<String>,
    sums: Vec<ColumnRef>,
    group_columns: Option<Vec<String>>,
}

/// Base table facts read from the catalog.
pub struct BaseTable {
    pub oid: u32,
    pub qualified: String,
    pub relname: String,
    pub replident: String,
    pub columns: Vec<String>,
    pub pk_columns: Vec<String>,
}

const IDENT: &str = r"[a-z_][a-z0-9_$]*";

fn re(cell: &'static OnceLock<Regex>, pattern: &str) -> &'static Regex {
    cell.get_or_init(|| Regex::new(pattern).expect("static regex"))
}

fn main_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    re(
        &RE,
        &format!(
            r"(?is)^\s*select\s+(?P<select>.+?)\s+from\s+(?P<table>{IDENT}(?:\.{IDENT})?)(?:\s+where\s+(?P<where>.+?))?(?:\s+group\s+by\s+(?P<group>.+?))?\s*;?\s*$"
        ),
    )
}

fn column_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    re(&RE, &format!(r"(?i)^(?P<col>{IDENT})(?:\s+as\s+(?P<alias>{IDENT}))?$"))
}

fn count_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    re(&RE, &format!(r"(?i)^count\s*\(\s*\*\s*\)(?:\s+as\s+(?P<alias>{IDENT}))?$"))
}

fn sum_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    re(&RE, &format!(r"(?i)^sum\s*\(\s*(?P<col>{IDENT})\s*\)(?:\s+as\s+(?P<alias>{IDENT}))?$"))
}

fn call_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    re(&RE, &format!(r"(?i)^(?P<fn>{IDENT})\s*\("))
}

/// Split on commas that are not nested inside parentheses.
fn split_top_level(text: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut depth = 0i32;
    let mut current = String::new();
    for ch in text.chars() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                items.push(current.trim().to_string());
                current.clear();
                continue;
            }
            _ => {}
        }
        current.push(ch);
    }
    items.push(current.trim().to_string());
    items
}

fn reject_forbidden_syntax(definition: &str) -> Result<(), String> {
    let checks: &[(&str, &str)] = &[
        (r"(?i)\bjoin\b", "JOIN is not supported; a view has exactly one base table"),
        (r"(?i)\bdistinct\b", "DISTINCT is not supported"),
        (r"(?i)\border\s+by\b", "ORDER BY is not supported"),
        (r"(?i)\blimit\b", "LIMIT is not supported"),
        (r"(?i)\boffset\b", "OFFSET is not supported"),
        (r"(?i)\bhaving\b", "HAVING is not supported"),
        (r"(?i)\b(union|intersect|except)\b", "set operations are not supported"),
        (r"(?i)\(\s*select\b", "subqueries are not supported"),
        (r"(?i)^\s*with\b", "common table expressions are not supported"),
        (r"(?i)\bover\s*\(", "window functions are not supported"),
        (r"(?i)\bfrom\s+[a-z_][a-z0-9_$.]*\s*,", "comma-separated FROM lists are not supported"),
    ];
    for (pattern, reason) in checks {
        if Regex::new(pattern).expect("static regex").is_match(definition) {
            return Err((*reason).to_string());
        }
    }
    Ok(())
}

fn parse_text(definition: &str) -> Result<Parsed, String> {
    reject_forbidden_syntax(definition)?;
    let caps = main_re()
        .captures(definition)
        .ok_or_else(|| "definition does not match either accepted shape".to_string())?;

    let table = caps["table"].to_lowercase();
    let predicate = caps.name("where").map(|m| m.as_str().trim().to_string());
    let group_columns = match caps.name("group") {
        Some(m) => {
            let mut cols = Vec::new();
            for item in split_top_level(m.as_str()) {
                let lowered = item.to_lowercase();
                if !Regex::new(&format!("^{IDENT}$")).expect("static regex").is_match(&lowered) {
                    return Err(format!("GROUP BY item '{item}' is not a bare column"));
                }
                cols.push(lowered);
            }
            Some(cols)
        }
        None => None,
    };

    let mut columns = Vec::new();
    let mut count_alias: Option<String> = None;
    let mut sums = Vec::new();
    let mut seen_aggregate = false;
    for item in split_top_level(&caps["select"]) {
        if item.is_empty() {
            return Err("empty select-list item".to_string());
        }
        if let Some(c) = count_re().captures(&item) {
            if count_alias.is_some() {
                return Err("count(*) may appear only once".to_string());
            }
            count_alias = Some(c.name("alias").map_or("count".to_string(), |m| m.as_str().to_lowercase()));
            seen_aggregate = true;
        } else if let Some(c) = sum_re().captures(&item) {
            sums.push(ColumnRef {
                base: c["col"].to_lowercase(),
                alias: c.name("alias").map_or("sum".to_string(), |m| m.as_str().to_lowercase()),
            });
            seen_aggregate = true;
        } else if let Some(c) = column_re().captures(&item) {
            if seen_aggregate {
                return Err(format!(
                    "column '{item}' appears after an aggregate; group columns must come first"
                ));
            }
            let base = c["col"].to_lowercase();
            let alias = c.name("alias").map_or(base.clone(), |m| m.as_str().to_lowercase());
            columns.push(ColumnRef { base, alias });
        } else if let Some(c) = call_re().captures(&item) {
            return Err(format!(
                "'{}' is not supported; only count(*) and sum(column) aggregates are accepted",
                &c["fn"]
            ));
        } else {
            return Err(format!(
                "select-list item '{item}' is not a bare column, count(*) or sum(column)"
            ));
        }
    }
    if columns.is_empty() {
        return Err("the select list must start with at least one bare column".to_string());
    }

    match (&group_columns, seen_aggregate) {
        (Some(_), false) => return Err("GROUP BY without count(*) is not supported".to_string()),
        (None, true) => return Err("aggregates require a GROUP BY clause".to_string()),
        (Some(group), true) => {
            if count_alias.is_none() {
                return Err("the aggregate shape requires count(*)".to_string());
            }
            let mut selected: Vec<&str> = columns.iter().map(|c| c.base.as_str()).collect();
            let mut grouped: Vec<&str> = group.iter().map(|g| g.as_str()).collect();
            selected.sort_unstable();
            grouped.sort_unstable();
            if selected != grouped {
                return Err("the GROUP BY list must equal the selected group columns".to_string());
            }
        }
        (None, false) => {}
    }

    let mut aliases: Vec<&String> = columns.iter().map(|c| &c.alias).collect();
    aliases.extend(count_alias.iter());
    aliases.extend(sums.iter().map(|s| &s.alias));
    let mut sorted = aliases.clone();
    sorted.sort();
    sorted.dedup();
    if sorted.len() != aliases.len() {
        return Err("output column names must be unique; add AS aliases".to_string());
    }

    Ok(Parsed { table, predicate, columns, count_alias, sums, group_columns })
}

/// Read the base table from the catalog. Raises if it does not exist or is
/// not an ordinary table.
pub fn lookup_base_table(table: &str) -> BaseTable {
    let args = [table.into()];
    let found = Spi::connect(|client| {
        let rows = client.select(
            "SELECT c.oid::int8, n.nspname::text, c.relname::text, c.relreplident::text, c.relkind::text \
             FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.oid = pg_catalog.to_regclass($1)",
            None,
            &args,
        )?;
        let mut out = None;
        for row in rows {
            out = Some((
                row.get::<i64>(1)?.unwrap_or_default(),
                row.get::<String>(2)?.unwrap_or_default(),
                row.get::<String>(3)?.unwrap_or_default(),
                row.get::<String>(4)?.unwrap_or_default(),
                row.get::<String>(5)?.unwrap_or_default(),
            ));
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .unwrap_or_else(|e| errors::invalid(format!("nabla: catalog lookup failed: {e}"), None));

    let (oid, nspname, relname, replident, relkind) = match found {
        Some(f) => f,
        None => errors::invalid(format!("nabla: base table \"{table}\" does not exist"), None),
    };
    if relkind != "r" {
        errors::unsupported_definition(format!(
            "\"{table}\" is not an ordinary table (relkind '{relkind}')"
        ));
    }

    let oid_arg = [oid.into()];
    let (columns, pk_columns) = Spi::connect(|client| {
        let mut columns = Vec::new();
        for row in client.select(
            "SELECT attname::text FROM pg_catalog.pg_attribute \
             WHERE attrelid = $1::oid AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
            None,
            &oid_arg,
        )? {
            columns.push(row.get::<String>(1)?.unwrap_or_default());
        }
        let mut pk = Vec::new();
        for row in client.select(
            "SELECT a.attname::text FROM pg_catalog.pg_index i \
             JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) ON true \
             JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum \
             WHERE i.indrelid = $1::oid AND i.indisprimary ORDER BY k.ord",
            None,
            &oid_arg,
        )? {
            pk.push(row.get::<String>(1)?.unwrap_or_default());
        }
        Ok::<_, pgrx::spi::Error>((columns, pk))
    })
    .unwrap_or_else(|e| errors::invalid(format!("nabla: catalog lookup failed: {e}"), None));

    BaseTable {
        oid: oid as u32,
        qualified: format!("{nspname}.{relname}"),
        relname,
        replident,
        columns,
        pk_columns,
    }
}

/// Parse and validate a definition. Raises a `nabla: unsupported view
/// definition` error for anything outside the two shapes, and lets PostgreSQL
/// report ordinary errors (unknown column, syntax) through EXPLAIN.
pub fn validate(definition: &str) -> (ViewSpec, BaseTable) {
    let parsed = match parse_text(definition) {
        Ok(p) => p,
        Err(reason) => errors::unsupported_definition(reason),
    };

    // Let the real parser and planner judge the predicate and column
    // references. Preparing (not running) keeps the transaction free of an
    // xid, which create_view needs to create the replication slot afterwards.
    if let Err(e) = Spi::connect(|client| client.prepare(definition, &[]).map(|_| ())) {
        errors::invalid(format!("nabla: definition failed to plan: {e}"), None);
    }

    let base = lookup_base_table(&parsed.table);
    let referenced = parsed
        .columns
        .iter()
        .map(|c| &c.base)
        .chain(parsed.sums.iter().map(|s| &s.base));
    for col in referenced {
        if !base.columns.contains(col) {
            errors::unsupported_definition(format!(
                "column '{col}' does not exist in {}",
                base.qualified
            ));
        }
    }

    let shape = if parsed.group_columns.is_some() { Shape::Aggregate } else { Shape::Projection };
    let mut pk_columns = Vec::new();
    if shape == Shape::Projection {
        if base.pk_columns.is_empty() {
            errors::unsupported_definition(format!(
                "{} has no primary key; the projection shape needs one to locate deleted rows",
                base.qualified
            ));
        }
        let missing: Vec<&String> = base
            .pk_columns
            .iter()
            .filter(|pk| !parsed.columns.iter().any(|c| &c.base == *pk))
            .collect();
        if !missing.is_empty() {
            let list = missing.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ");
            errors::unsupported_definition(format!(
                "the select list must include the primary key column(s) of {}: missing {list}",
                base.qualified
            ));
        }
        pk_columns = base.pk_columns.clone();
    }

    let spec = ViewSpec {
        shape,
        base_table: base.qualified.clone(),
        base_relname: base.relname.clone(),
        predicate: parsed.predicate,
        columns: parsed.columns,
        pk_columns,
        count_alias: parsed.count_alias,
        sums: parsed.sums,
    };
    (spec, base)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_projection() {
        let p = parse_text("SELECT id, k, amount AS amt FROM public.orders WHERE status = 'paid'").unwrap();
        assert_eq!(p.table, "public.orders");
        assert_eq!(p.predicate.as_deref(), Some("status = 'paid'"));
        assert_eq!(p.columns.len(), 3);
        assert_eq!(p.columns[2].alias, "amt");
        assert!(p.group_columns.is_none());
    }

    #[test]
    fn accepts_aggregate() {
        let p = parse_text(
            "select k, count(*) as n, sum(amount) as total from orders where status = 'paid' group by k",
        )
        .unwrap();
        assert_eq!(p.count_alias.as_deref(), Some("n"));
        assert_eq!(p.sums[0].alias, "total");
        assert_eq!(p.group_columns.unwrap(), vec!["k"]);
        assert_eq!(p.predicate.as_deref(), Some("status = 'paid'"));
    }

    #[test]
    fn rejects_other_shapes() {
        for bad in [
            "SELECT o.id FROM orders o JOIN customers c ON c.id = o.customer_id",
            "SELECT k, avg(amount) FROM orders GROUP BY k",
            "SELECT id FROM orders ORDER BY id",
            "SELECT id, count(*) FROM orders",
            "SELECT count(*), k FROM orders GROUP BY k",
            "SELECT k, count(*) FROM orders GROUP BY k, status",
            "SELECT id, sum(a), sum(b) FROM orders GROUP BY id",
            "SELECT id FROM orders WHERE id IN (SELECT 1)",
        ] {
            assert!(parse_text(bad).is_err(), "{bad}");
        }
    }
}
