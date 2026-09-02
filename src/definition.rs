//! Shape decisions for view definitions, made on PostgreSQL's own analyzed
//! `Query` tree.
//!
//! `validate` parses the definition with `raw_parser`, analyzes it with
//! `parse_analyze_fixedparams` (as the calling user, with their search_path,
//! so name resolution and permission checks are PostgreSQL's), then walks the
//! tree. Anything outside the two accepted shapes is rejected with a specific
//! reason. Expressions apply.rs needs as SQL text (predicate, output
//! expressions, sum arguments) are deparsed with `deparse_expression` against
//! a single-relation context, so column references come out as bare quoted
//! identifiers that bind against the `(VALUES ...) AS rel(cols)` row the
//! worker builds.
//!
//! Errors raised by PostgreSQL during parsing or analysis (syntax errors,
//! unknown columns, missing tables, permissions) propagate unchanged: the
//! pg_extern wrapper turns them into the caller's error, which is the message
//! we want.

use pgrx::pg_sys::{self, NodeTag};
use pgrx::prelude::*;
use pgrx::spi::quote_qualified_identifier;
use serde::{Deserialize, Serialize};
use std::ffi::{c_char, c_void, CStr, CString};

use crate::errors;

/// Name of the group-count column added when an aggregate definition has no
/// count(*). It is a real column of the view table and appears in deltas.
pub const HIDDEN_COUNT_COLUMN: &str = "_nabla_n";

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

/// One output column: a deparsed expression over the base table's columns and
/// the (unquoted) name it has in the view table.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct OutputColumn {
    pub expr: String,
    pub alias: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ViewSpec {
    pub shape: Shape,
    /// Quoted, schema-qualified base table name, ready for SQL.
    pub base_table: String,
    /// Bare (unquoted) relation name; apply.rs aliases the decoded row with it.
    pub base_relname: String,
    /// Deparsed WHERE clause, if any.
    pub predicate: Option<String>,
    /// Projection: every output column. Aggregate: the group keys.
    pub columns: Vec<OutputColumn>,
    /// Base primary key column names (projection only).
    pub pk_columns: Vec<String>,
    /// View column names carrying the primary key, in PK order (projection only).
    pub pk_view_columns: Vec<String>,
    /// View column carrying the group count (aggregate only).
    pub count_alias: Option<String>,
    /// True when `count_alias` is the hidden column nabla added.
    pub hidden_count: bool,
    /// sum() aggregates: `expr` is the deparsed argument.
    pub sums: Vec<OutputColumn>,
    /// SELECT that fills the view table (create and refresh).
    pub populate_sql: String,
}

/// Base table facts from the catalog.
pub struct BaseTable {
    pub oid: u32,
    pub qualified: String,
    pub replident: String,
    pub pk_columns: Vec<String>,
}

fn reject(reason: impl AsRef<str>) -> ! {
    errors::unsupported_definition(reason)
}

// --- raw pointer helpers -----------------------------------------------------

unsafe fn list_items(list: *mut pg_sys::List) -> Vec<*mut c_void> {
    if list.is_null() {
        return Vec::new();
    }
    (0..(*list).length as usize).map(|i| (*(*list).elements.add(i)).ptr_value).collect()
}

unsafe fn list_is_empty(list: *mut pg_sys::List) -> bool {
    list.is_null() || (*list).length == 0
}

unsafe fn tag(node: *const c_void) -> Option<NodeTag> {
    if node.is_null() {
        None
    } else {
        Some((*(node as *const pg_sys::Node)).type_)
    }
}

unsafe fn text(ptr: *const c_char) -> String {
    if ptr.is_null() {
        String::new()
    } else {
        CStr::from_ptr(ptr).to_string_lossy().into_owned()
    }
}

unsafe fn func_name(oid: pg_sys::Oid) -> String {
    text(pg_sys::get_func_name(oid))
}

// --- mutable function search -------------------------------------------------

struct MutableSearch {
    found: Option<String>,
}

/// expression_tree_walker callback: stop at the first non-IMMUTABLE function.
unsafe extern "C-unwind" fn find_mutable(node: *mut pg_sys::Node, ctx: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }
    let search = &mut *(ctx as *mut MutableSearch);
    let funcid = match (*node).type_ {
        NodeTag::T_FuncExpr => Some((*(node as *mut pg_sys::FuncExpr)).funcid),
        NodeTag::T_OpExpr | NodeTag::T_DistinctExpr | NodeTag::T_NullIfExpr => {
            Some((*(node as *mut pg_sys::OpExpr)).opfuncid)
        }
        NodeTag::T_ScalarArrayOpExpr => Some((*(node as *mut pg_sys::ScalarArrayOpExpr)).opfuncid),
        NodeTag::T_SQLValueFunction => {
            search.found = Some("current_date/current_time/current_user or similar".to_string());
            return true;
        }
        NodeTag::T_NextValueExpr => {
            search.found = Some("nextval".to_string());
            return true;
        }
        _ => None,
    };
    if let Some(oid) = funcid {
        if oid != pg_sys::InvalidOid && pg_sys::func_volatile(oid) != b'i' as c_char {
            search.found = Some(func_name(oid));
            return true;
        }
    }
    pg_sys::expression_tree_walker_impl(node, Some(find_mutable), ctx)
}

/// Reject when `node` uses anything that is not IMMUTABLE: the view must be a
/// pure function of the base table's rows.
unsafe fn require_immutable(node: *mut pg_sys::Node, place: &str) {
    if node.is_null() || !pg_sys::contain_mutable_functions(node) {
        return;
    }
    let mut search = MutableSearch { found: None };
    find_mutable(node, &mut search as *mut MutableSearch as *mut c_void);
    match search.found {
        Some(name) => reject(format!(
            "{place} uses \"{name}\", which is not IMMUTABLE; the view must be a pure function of the base table"
        )),
        None => reject(format!("{place} uses a function that is not IMMUTABLE")),
    }
}

// --- analysis ----------------------------------------------------------------

unsafe fn analyze(definition: &str) -> *mut pg_sys::Query {
    let source = CString::new(definition)
        .unwrap_or_else(|_| errors::invalid("nabla: the definition contains a NUL byte", None));
    let raw = pg_sys::raw_parser(source.as_ptr(), pg_sys::RawParseMode::RAW_PARSE_DEFAULT);
    let statements = list_items(raw);
    match statements.len() {
        0 => reject("the definition is empty"),
        1 => {}
        n => reject(format!("the definition must be exactly one SELECT statement, found {n}")),
    }
    let raw_stmt = statements[0] as *mut pg_sys::RawStmt;
    pg_sys::parse_analyze_fixedparams(raw_stmt, source.as_ptr(), std::ptr::null(), 0, std::ptr::null_mut())
}

unsafe fn reject_unsupported_clauses(query: *mut pg_sys::Query) {
    let q = &*query;
    if q.commandType != pg_sys::CmdType::CMD_SELECT {
        reject("only SELECT statements are supported");
    }
    if !q.setOperations.is_null() {
        reject("UNION, INTERSECT and EXCEPT are not supported");
    }
    if !list_is_empty(q.cteList) {
        reject("common table expressions (WITH) are not supported");
    }
    if q.hasSubLinks {
        reject("subqueries are not supported");
    }
    if q.hasWindowFuncs {
        reject("window functions are not supported");
    }
    if q.hasTargetSRFs {
        reject("set-returning functions are not supported");
    }
    if q.hasDistinctOn || !list_is_empty(q.distinctClause) {
        reject("DISTINCT is not supported");
    }
    if !list_is_empty(q.sortClause) {
        reject("ORDER BY is not supported");
    }
    if !q.limitCount.is_null() || !q.limitOffset.is_null() {
        reject("LIMIT and OFFSET are not supported");
    }
    if !q.havingQual.is_null() {
        reject("HAVING is not supported");
    }
    if !list_is_empty(q.groupingSets) {
        reject("GROUPING SETS, CUBE and ROLLUP are not supported");
    }
    if q.hasForUpdate || !list_is_empty(q.rowMarks) {
        reject("FOR UPDATE and FOR SHARE are not supported");
    }
}

/// The single base relation of the query, after rejecting every other kind
/// of range table entry.
unsafe fn single_relation(query: *mut pg_sys::Query) -> *mut pg_sys::RangeTblEntry {
    let mut relations = Vec::new();
    for item in list_items((*query).rtable) {
        let rte = item as *mut pg_sys::RangeTblEntry;
        if (*rte).lateral {
            reject("LATERAL is not supported");
        }
        match (*rte).rtekind {
            pg_sys::RTEKind::RTE_RELATION => relations.push(rte),
            pg_sys::RTEKind::RTE_JOIN => reject("joins are not supported; a view has exactly one base table"),
            pg_sys::RTEKind::RTE_SUBQUERY => reject("subqueries in FROM are not supported"),
            pg_sys::RTEKind::RTE_FUNCTION | pg_sys::RTEKind::RTE_TABLEFUNC => {
                reject("set-returning functions are not supported")
            }
            pg_sys::RTEKind::RTE_VALUES => reject("VALUES lists are not supported"),
            pg_sys::RTEKind::RTE_CTE => reject("common table expressions (WITH) are not supported"),
            _ => reject("a FROM clause with exactly one ordinary table is required"),
        }
    }
    match relations.len() {
        0 => reject("a FROM clause with exactly one ordinary table is required"),
        1 => relations[0],
        _ => reject("joins are not supported; a view has exactly one base table"),
    }
}

unsafe fn qualified_name(relid: pg_sys::Oid) -> (String, String) {
    let relname = text(pg_sys::get_rel_name(relid));
    let nspname = text(pg_sys::get_namespace_name(pg_sys::get_rel_namespace(relid)));
    (nspname, relname)
}

/// Catalog facts about the base table, read with read-only SPI so the calling
/// transaction stays free of an xid (create_view creates the slot afterwards).
fn lookup_base(relid: u32, nspname: &str, relname: &str) -> (BaseTable, Vec<i16>) {
    let qualified = quote_qualified_identifier(nspname, relname);
    let args = [(relid as i64).into()];
    Spi::connect(|client| {
        let mut replident = String::new();
        let mut has_children = false;
        let mut is_nabla_view = false;
        for row in client.select(
            "SELECT c.relreplident::text, \
                    EXISTS (SELECT 1 FROM pg_catalog.pg_inherits WHERE inhparent = c.oid), \
                    EXISTS (SELECT 1 FROM nabla.views WHERE pg_catalog.to_regclass(name) = c.oid) \
             FROM pg_catalog.pg_class c WHERE c.oid = $1::oid",
            Some(1),
            &args,
        )? {
            replident = row.get::<String>(1)?.unwrap_or_default();
            has_children = row.get::<bool>(2)?.unwrap_or(false);
            is_nabla_view = row.get::<bool>(3)?.unwrap_or(false);
        }
        if is_nabla_view {
            reject(format!("{qualified} is a nabla view; views cannot be built on other nabla views"));
        }
        if has_children {
            reject(format!("{qualified} has inheritance children, which logical decoding does not follow"));
        }
        let mut pk_columns = Vec::new();
        let mut pk_attnums = Vec::new();
        for row in client.select(
            "SELECT a.attnum::int4, a.attname::text FROM pg_catalog.pg_index i \
             JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) ON true \
             JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum \
             WHERE i.indrelid = $1::oid AND i.indisprimary ORDER BY k.ord",
            None,
            &args,
        )? {
            pk_attnums.push(row.get::<i32>(1)?.unwrap_or_default() as i16);
            pk_columns.push(row.get::<String>(2)?.unwrap_or_default());
        }
        Ok::<_, pgrx::spi::Error>((
            BaseTable { oid: relid, qualified, replident, pk_columns },
            pk_attnums,
        ))
    })
    .unwrap_or_else(|e| errors::invalid(format!("nabla: catalog lookup failed: {e}"), None))
}

unsafe fn check_relation_kind(rte: *mut pg_sys::RangeTblEntry, qualified: &str, nspname: &str) {
    match (*rte).relkind as u8 {
        b'r' => {}
        b'p' => reject(format!("{qualified} is a partitioned table; partitioned tables are not supported")),
        b'v' => reject(format!("{qualified} is a view; the base must be an ordinary table")),
        b'm' => reject(format!("{qualified} is a materialized view; the base must be an ordinary table")),
        b'f' => reject(format!("{qualified} is a foreign table; the base must be an ordinary table")),
        other => reject(format!(
            "{qualified} is not an ordinary table (relkind '{}')",
            other as char
        )),
    }
    if nspname == "nabla" {
        reject(format!("{qualified} is a nabla catalog table and cannot be a base table"));
    }
}

/// Deparser bound to the single base relation: column references print as
/// bare quoted identifiers.
struct Deparser {
    context: *mut pg_sys::List,
    _alias: CString,
}

impl Deparser {
    unsafe fn new(relid: pg_sys::Oid, relname: &str) -> Self {
        let alias = CString::new(relname).expect("relation name without NUL");
        let context = pg_sys::deparse_context_for(alias.as_ptr(), relid);
        Deparser { context, _alias: alias }
    }

    unsafe fn expr(&self, node: *mut pg_sys::Node) -> String {
        text(pg_sys::deparse_expression(node, self.context, false, false))
    }
}

unsafe fn is_plain_var(node: *mut pg_sys::Node, attnum: i16) -> bool {
    if tag(node as *const c_void) != Some(NodeTag::T_Var) {
        return false;
    }
    let var = &*(node as *mut pg_sys::Var);
    var.varno == 1 && var.varlevelsup == 0 && var.varattno == attnum
}

struct Target {
    node: *mut pg_sys::Node,
    alias: String,
    sortgroupref: u32,
    resjunk: bool,
}

unsafe fn targets(query: *mut pg_sys::Query) -> Vec<Target> {
    list_items((*query).targetList)
        .into_iter()
        .map(|item| {
            let tle = &*(item as *mut pg_sys::TargetEntry);
            let alias = if tle.resname.is_null() { "?column?".to_string() } else { text(tle.resname) };
            Target { node: tle.expr as *mut pg_sys::Node, alias, sortgroupref: tle.ressortgroupref, resjunk: tle.resjunk }
        })
        .collect()
}

unsafe fn group_refs(query: *mut pg_sys::Query) -> Vec<u32> {
    list_items((*query).groupClause)
        .into_iter()
        .map(|item| (*(item as *mut pg_sys::SortGroupClause)).tleSortGroupRef)
        .collect()
}

enum AggKind {
    Count,
    Sum(*mut pg_sys::Node),
}

/// Accept only count(*) and sum(<expression>) in their plain forms.
unsafe fn classify_aggregate(node: *mut pg_sys::Node, alias: &str) -> AggKind {
    let agg = &*(node as *mut pg_sys::Aggref);
    let name = func_name(agg.aggfnoid);
    if agg.aggkind as u8 != b'n' || !list_is_empty(agg.aggdirectargs) {
        reject(format!("ordered-set and hypothetical-set aggregates like \"{name}\" are not supported"));
    }
    if !list_is_empty(agg.aggdistinct) {
        reject(format!("DISTINCT inside aggregates is not supported (column \"{alias}\")"));
    }
    if !agg.aggfilter.is_null() {
        reject(format!("FILTER on aggregates is not supported (column \"{alias}\")"));
    }
    if !list_is_empty(agg.aggorder) {
        reject(format!("ORDER BY inside aggregates is not supported (column \"{alias}\")"));
    }
    let builtin = pg_sys::get_func_namespace(agg.aggfnoid) == pg_sys::Oid::from(pg_sys::PG_CATALOG_NAMESPACE);
    match (builtin, name.as_str()) {
        (true, "count") => {
            if !agg.aggstar {
                reject("count(expression) is not supported; use count(*)");
            }
            AggKind::Count
        }
        (true, "sum") => {
            let args = list_items(agg.args);
            if args.len() != 1 {
                reject("sum() must have exactly one argument");
            }
            let arg = (*(args[0] as *mut pg_sys::TargetEntry)).expr as *mut pg_sys::Node;
            require_immutable(arg, &format!("the argument of sum() in column \"{alias}\""));
            AggKind::Sum(arg)
        }
        _ => reject(format!(
            "aggregate \"{name}\" is not supported; only count(*) and sum(expression) are accepted"
        )),
    }
}

/// Parse, analyze and classify a definition. Raises `nabla: unsupported view
/// definition: <reason>` for anything outside the two shapes; PostgreSQL's own
/// errors (syntax, unknown column, permissions) propagate unchanged.
pub fn validate(definition: &str) -> (ViewSpec, BaseTable) {
    // SAFETY: every pointer comes from the parser in the current memory context
    // and is only read while it is alive; every FFI call is a documented
    // PostgreSQL entry point used the way the backend itself uses it.
    unsafe {
        let query = analyze(definition);
        reject_unsupported_clauses(query);
        let rte = single_relation(query);
        let relid = (*rte).relid;
        let (nspname, relname) = qualified_name(relid);
        let qualified = quote_qualified_identifier(&nspname, &relname);
        check_relation_kind(rte, &qualified, &nspname);
        let (base, pk_attnums) = lookup_base(relid.to_u32(), &nspname, &relname);

        let deparser = Deparser::new(relid, &relname);
        let quals = (*(*query).jointree).quals;
        require_immutable(quals, "the WHERE clause");
        let predicate = if quals.is_null() { None } else { Some(deparser.expr(quals)) };

        let all_targets = targets(query);
        let refs = group_refs(query);
        for t in all_targets.iter().filter(|t| t.resjunk) {
            if t.sortgroupref != 0 && refs.contains(&t.sortgroupref) {
                reject("every GROUP BY expression must also appear in the select list");
            }
        }
        let visible: Vec<&Target> = all_targets.iter().filter(|t| !t.resjunk).collect();
        if visible.is_empty() {
            reject("the select list is empty");
        }
        let mut seen = std::collections::HashSet::new();
        for t in &visible {
            if !seen.insert(t.alias.as_str()) {
                reject(format!("output column names must be unique; add AS aliases (duplicate: \"{}\")", t.alias));
            }
        }

        let has_aggs = (*query).hasAggs;
        let has_group = !refs.is_empty();
        let mut spec = ViewSpec {
            shape: if has_aggs { Shape::Aggregate } else { Shape::Projection },
            base_table: qualified.clone(),
            base_relname: relname.clone(),
            predicate,
            columns: Vec::new(),
            pk_columns: Vec::new(),
            pk_view_columns: Vec::new(),
            count_alias: None,
            hidden_count: false,
            sums: Vec::new(),
            populate_sql: String::new(),
        };
        // Output columns of the populate query, in definition order.
        let mut populate_targets: Vec<String> = Vec::new();
        let mut group_exprs: Vec<String> = Vec::new();

        if has_aggs {
            if !has_group {
                reject("aggregates require a GROUP BY clause");
            }
            for t in &visible {
                let quoted_alias = pgrx::spi::quote_identifier(&t.alias);
                if t.sortgroupref != 0 && refs.contains(&t.sortgroupref) {
                    require_immutable(t.node, &format!("the GROUP BY key \"{}\"", t.alias));
                    let expr = deparser.expr(t.node);
                    populate_targets.push(format!("{expr} AS {quoted_alias}"));
                    group_exprs.push(expr.clone());
                    spec.columns.push(OutputColumn { expr, alias: t.alias.clone() });
                } else if tag(t.node as *const c_void) == Some(NodeTag::T_Aggref) {
                    match classify_aggregate(t.node, &t.alias) {
                        AggKind::Count => {
                            if spec.count_alias.is_some() {
                                reject("count(*) may appear only once");
                            }
                            populate_targets.push(format!("count(*) AS {quoted_alias}"));
                            spec.count_alias = Some(t.alias.clone());
                        }
                        AggKind::Sum(arg) => {
                            let expr = deparser.expr(arg);
                            populate_targets.push(format!("sum({expr}) AS {quoted_alias}"));
                            spec.sums.push(OutputColumn { expr, alias: t.alias.clone() });
                        }
                    }
                } else if pg_sys::contain_agg_clause(t.node) {
                    reject(format!(
                        "column \"{}\" is an expression over an aggregate; alias the aggregate and compute outside the view",
                        t.alias
                    ));
                } else {
                    reject(format!("column \"{}\" is neither a GROUP BY key nor an accepted aggregate", t.alias));
                }
            }
            if spec.columns.is_empty() {
                reject("at least one GROUP BY key must be selected");
            }
            if spec.count_alias.is_none() {
                if seen.contains(HIDDEN_COUNT_COLUMN) {
                    reject(format!("the column name {HIDDEN_COUNT_COLUMN} is reserved for the group count"));
                }
                populate_targets.push(format!("count(*) AS {HIDDEN_COUNT_COLUMN}"));
                spec.count_alias = Some(HIDDEN_COUNT_COLUMN.to_string());
                spec.hidden_count = true;
            }
        } else {
            if has_group {
                reject("GROUP BY without an aggregate is not supported");
            }
            if pk_attnums.is_empty() {
                reject(format!(
                    "{qualified} has no primary key; a projection view needs one to locate deleted rows"
                ));
            }
            for t in &visible {
                require_immutable(t.node, &format!("column \"{}\"", t.alias));
                let expr = deparser.expr(t.node);
                populate_targets.push(format!("{expr} AS {}", pgrx::spi::quote_identifier(&t.alias)));
                spec.columns.push(OutputColumn { expr, alias: t.alias.clone() });
            }
            let mut missing = Vec::new();
            for (attnum, name) in pk_attnums.iter().zip(base.pk_columns.iter()) {
                match visible.iter().find(|t| is_plain_var(t.node, *attnum)) {
                    Some(t) => spec.pk_view_columns.push(t.alias.clone()),
                    None => missing.push(name.clone()),
                }
            }
            if !missing.is_empty() {
                reject(format!(
                    "the select list must include the primary key column(s) of {qualified} as plain columns: missing {}",
                    missing.join(", ")
                ));
            }
            spec.pk_columns = base.pk_columns.clone();
        }

        let mut populate = format!("SELECT {} FROM {qualified}", populate_targets.join(", "));
        if let Some(p) = &spec.predicate {
            populate.push_str(&format!(" WHERE {p}"));
        }
        if !group_exprs.is_empty() {
            populate.push_str(&format!(" GROUP BY {}", group_exprs.join(", ")));
        }
        spec.populate_sql = populate;
        (spec, base)
    }
}

