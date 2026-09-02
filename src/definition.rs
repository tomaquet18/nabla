// SPDX-License-Identifier: AGPL-3.0-or-later
//! Shape decisions for view definitions, made on PostgreSQL's own analyzed
//! `Query` tree.
//!
//! `validate` parses the definition with `raw_parser`, analyzes it with
//! `parse_analyze_fixedparams` (as the calling user, with their search_path,
//! so name resolution and permission checks are PostgreSQL's), then walks the
//! tree. Anything outside the two accepted shapes is rejected with a specific
//! reason. Expressions apply.rs needs as SQL text (predicate, output
//! expressions, aggregate arguments) are deparsed with `deparse_expression`:
//! single-table views use a one-relation context so columns come out bare;
//! join views use a context over the whole range table so every column comes
//! out as `alias.column`, matching the FROM list apply.rs builds from a
//! one-row VALUES for the changed table and shadow tables for the others.
//!
//! Errors raised by PostgreSQL during parsing or analysis (syntax errors,
//! unknown columns, missing tables, permissions) propagate unchanged: the
//! pg_extern wrapper turns them into the caller's error, which is the message
//! we want.

use pgrx::pg_sys::{self, NodeTag};
use pgrx::prelude::*;
use pgrx::spi::{quote_identifier, quote_qualified_identifier};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::ffi::{c_char, c_void, CStr, CString};

use crate::errors;

/// Name of the group-count column added when an aggregate definition has no
/// count(*). It is a real column of the view table and appears in deltas.
pub const HIDDEN_COUNT_COLUMN: &str = "_nabla_n";
/// Prefix of the hidden non-NULL counter that accompanies every sum():
/// `_nabla_nn_<n>`, where n is the 0-based position of that sum among the
/// view's aggregates in select-list order. Clients ignore `_nabla_*` columns.
pub const HIDDEN_SUM_COUNTER_PREFIX: &str = "_nabla_nn_";
/// Prefix of the hidden primary-key columns of a projection join view:
/// `_nabla_pk<rti>_<column>`, rti being the base relation's 1-based position
/// in the view's relation list (range-table order).
pub const HIDDEN_PK_PREFIX: &str = "_nabla_pk";

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

/// One output column: a deparsed expression over base columns and the
/// (unquoted) name it has in the view table.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct OutputColumn {
    pub expr: String,
    pub alias: String,
}

/// A sum() aggregate: the deparsed argument, the output column, and the
/// hidden column counting its non-NULL contributions (the stored sum is
/// NULL exactly when that counter is 0, matching PostgreSQL).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SumSpec {
    pub expr: String,
    pub alias: String,
    pub counter: String,
}

/// One base relation of a view, in range-table order.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct BaseRelation {
    pub oid: u32,
    /// 1-based position in the view's relation list.
    pub rti: usize,
    /// The alias the definition used for it (or the relation name), unquoted.
    pub alias: String,
    /// Quoted, schema-qualified name, ready for SQL.
    pub qualified: String,
    pub pk_columns: Vec<String>,
    /// All column names in attribute order.
    pub columns: Vec<String>,
    /// Types of `columns` (`format_type` text), parallel to it.
    #[serde(default)]
    pub column_types: Vec<String>,
    /// Columns this view reads from the relation (select list, group keys,
    /// quals and, for join views, the primary key), in attribute order.
    #[serde(default)]
    pub used_columns: Vec<String>,
    /// Types of `used_columns`, parallel to it; a mismatch with the decoded
    /// relation marks the view stale.
    #[serde(default)]
    pub used_column_types: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ViewSpec {
    pub shape: Shape,
    /// Quoted, schema-qualified name of the (first) base table.
    pub base_table: String,
    /// Bare (unquoted) relation name of the single base table; apply.rs
    /// aliases the decoded row with it for single-table views.
    pub base_relname: String,
    /// Deparsed WHERE clause (for joins: the conjunction of every ON and WHERE).
    pub predicate: Option<String>,
    /// Projection: every visible output column. Aggregate: the group keys.
    pub columns: Vec<OutputColumn>,
    /// Base primary key column names (single-table projection only).
    pub pk_columns: Vec<String>,
    /// View columns identifying a row of a projection view, in order:
    /// the selected PK columns (single table) or the hidden `_nabla_pk*`
    /// columns (join).
    pub pk_view_columns: Vec<String>,
    /// View column carrying the group count (aggregate only).
    pub count_alias: Option<String>,
    /// True when `count_alias` is the hidden column nabla added.
    pub hidden_count: bool,
    /// count(<expr>) aggregates: the output column is the non-NULL counter itself.
    pub counts: Vec<OutputColumn>,
    /// sum() aggregates with their hidden non-NULL counters.
    pub sums: Vec<SumSpec>,
    /// SELECT that fills the view table (create and refresh).
    pub populate_sql: String,
    /// Base relations in range-table order (one entry for single-table views).
    #[serde(default)]
    pub relations: Vec<BaseRelation>,
    /// The populate query's select list (visible and hidden columns).
    #[serde(default)]
    pub select_list: String,
    /// GROUP BY expressions of the populate query, if any.
    #[serde(default)]
    pub group_by: Option<String>,
    /// The definition's output column names, in order.
    #[serde(default)]
    pub visible_columns: Vec<String>,
    /// Maintenance columns nabla added (`_nabla_*`), stripped from delta rows by default.
    #[serde(default)]
    pub hidden_columns: Vec<String>,
}

impl ViewSpec {
    pub fn is_join(&self) -> bool {
        self.relations.len() > 1
    }
}

/// Base table facts from the catalog (single-table API checks).
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
/// pure function of the base tables' rows.
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

// --- unresolved Var search ---------------------------------------------------

struct VarCheck {
    allowed: Vec<i32>,
    bad: bool,
}

/// After flattening join aliases every Var must point at a base relation of
/// the current query level.
unsafe extern "C-unwind" fn find_foreign_var(node: *mut pg_sys::Node, ctx: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }
    let check = &mut *(ctx as *mut VarCheck);
    if (*node).type_ == NodeTag::T_Var {
        let var = &*(node as *mut pg_sys::Var);
        if var.varlevelsup != 0 || !check.allowed.contains(&var.varno) {
            check.bad = true;
            return true;
        }
        return false;
    }
    pg_sys::expression_tree_walker_impl(node, Some(find_foreign_var), ctx)
}

struct VarCollect {
    vars: Vec<(i32, i16)>,
}

unsafe extern "C-unwind" fn collect_vars(node: *mut pg_sys::Node, ctx: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }
    let collect = &mut *(ctx as *mut VarCollect);
    if (*node).type_ == NodeTag::T_Var {
        let var = &*(node as *mut pg_sys::Var);
        if var.varlevelsup == 0 {
            collect.vars.push((var.varno, var.varattno));
        }
        return false;
    }
    pg_sys::expression_tree_walker_impl(node, Some(collect_vars), ctx)
}

/// Every (varno, attno) referenced by `node`.
unsafe fn referenced_vars(node: *mut pg_sys::Node, out: &mut Vec<(i32, i16)>) {
    if node.is_null() {
        return;
    }
    let mut collect = VarCollect { vars: Vec::new() };
    collect_vars(node, &mut collect as *mut VarCollect as *mut c_void);
    out.extend(collect.vars);
}

unsafe fn require_base_vars(node: *mut pg_sys::Node, allowed: &[i32]) {
    if node.is_null() {
        return;
    }
    let mut check = VarCheck { allowed: allowed.to_vec(), bad: false };
    find_foreign_var(node, &mut check as *mut VarCheck as *mut c_void);
    if check.bad {
        reject("a column reference could not be resolved to one of the joined tables");
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

struct RelEntry {
    /// 1-based index in the query's range table (what Vars use as varno).
    rt_index: i32,
    rte: *mut pg_sys::RangeTblEntry,
}

/// The base relations of the query, after rejecting every other kind of
/// range table entry. Join entries (from explicit JOIN syntax) are allowed.
unsafe fn collect_relations(query: *mut pg_sys::Query) -> Vec<RelEntry> {
    let mut relations = Vec::new();
    for (i, item) in list_items((*query).rtable).into_iter().enumerate() {
        let rte = item as *mut pg_sys::RangeTblEntry;
        if (*rte).lateral {
            reject("LATERAL is not supported");
        }
        match (*rte).rtekind {
            pg_sys::RTEKind::RTE_RELATION => relations.push(RelEntry { rt_index: i as i32 + 1, rte }),
            pg_sys::RTEKind::RTE_JOIN => {}
            pg_sys::RTEKind::RTE_SUBQUERY => reject("subqueries in FROM are not supported"),
            pg_sys::RTEKind::RTE_FUNCTION | pg_sys::RTEKind::RTE_TABLEFUNC => {
                reject("set-returning functions are not supported")
            }
            pg_sys::RTEKind::RTE_VALUES => reject("VALUES lists are not supported"),
            pg_sys::RTEKind::RTE_CTE => reject("common table expressions (WITH) are not supported"),
            _ => reject("a FROM clause with one or more ordinary tables is required"),
        }
    }
    if relations.is_empty() {
        reject("a FROM clause with one or more ordinary tables is required");
    }
    relations
}

/// Walk the join tree: reject anything but inner joins and collect every
/// ON and WHERE qual. With only inner joins, cross product plus the
/// conjunction of all quals is equivalent to the original nesting.
unsafe fn collect_quals(node: *mut pg_sys::Node, out: &mut Vec<*mut pg_sys::Node>) {
    match tag(node as *const c_void) {
        Some(NodeTag::T_FromExpr) => {
            let from = &*(node as *mut pg_sys::FromExpr);
            for item in list_items(from.fromlist) {
                collect_quals(item as *mut pg_sys::Node, out);
            }
            if !from.quals.is_null() {
                out.push(from.quals);
            }
        }
        Some(NodeTag::T_JoinExpr) => {
            let join = &*(node as *mut pg_sys::JoinExpr);
            let kind = match join.jointype {
                pg_sys::JoinType::JOIN_INNER => None,
                pg_sys::JoinType::JOIN_LEFT => Some("LEFT JOIN"),
                pg_sys::JoinType::JOIN_RIGHT => Some("RIGHT JOIN"),
                pg_sys::JoinType::JOIN_FULL => Some("FULL JOIN"),
                pg_sys::JoinType::JOIN_SEMI => Some("SEMI JOIN"),
                pg_sys::JoinType::JOIN_ANTI | pg_sys::JoinType::JOIN_RIGHT_ANTI => Some("ANTI JOIN"),
                _ => Some("this join type"),
            };
            if let Some(kind) = kind {
                reject(format!("{kind} is not supported; only inner joins are"));
            }
            collect_quals(join.larg, out);
            collect_quals(join.rarg, out);
            if !join.quals.is_null() {
                out.push(join.quals);
            }
        }
        _ => {}
    }
}

unsafe fn qualified_name(relid: pg_sys::Oid) -> (String, String) {
    let relname = text(pg_sys::get_rel_name(relid));
    let nspname = text(pg_sys::get_namespace_name(pg_sys::get_rel_namespace(relid)));
    (nspname, relname)
}

/// Catalog facts about a base table, read with read-only SPI so the calling
/// transaction stays free of an xid (create_view creates the slot afterwards).
fn lookup_base(relid: u32, nspname: &str, relname: &str) -> (BaseTable, Vec<i16>, Vec<(String, String)>) {
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
        let mut columns = Vec::new();
        for row in client.select(
            "SELECT attname::text, pg_catalog.format_type(atttypid, atttypmod) FROM pg_catalog.pg_attribute \
             WHERE attrelid = $1::oid AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
            None,
            &args,
        )? {
            columns.push((row.get::<String>(1)?.unwrap_or_default(), row.get::<String>(2)?.unwrap_or_default()));
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
            columns,
        ))
    })
    .unwrap_or_else(|e| errors::invalid(format!("nabla: catalog lookup failed: {e}"), None))
}

unsafe fn check_relation_kind(rte: *mut pg_sys::RangeTblEntry, qualified: &str, nspname: &str) {
    if nspname == "nabla_store" {
        reject(format!("{qualified} is a nabla storage table and cannot be a base table"));
    }
    let relid = (*rte).relid.to_u32() as i64;
    let is_nabla_view = Spi::connect(|client| {
        client
            .select("SELECT EXISTS (SELECT 1 FROM nabla.views WHERE relid = $1::oid)", Some(1), &[relid.into()])?
            .first()
            .get_one::<bool>()
    })
    .unwrap_or(None)
    .unwrap_or(false);
    if is_nabla_view {
        reject(format!("{qualified} is a nabla view; views cannot be built on other nabla views"));
    }
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
    if nspname == crate::shadow::SCHEMA {
        reject(format!("{qualified} is a nabla shadow table and cannot be a base table"));
    }
}

/// PostgreSQL's deparser bound to the definition's range table.
struct Deparser {
    context: *mut pg_sys::List,
    /// Prefix every column with its relation alias (needed for joins).
    prefix: bool,
}

impl Deparser {
    /// Single relation: columns print as bare quoted identifiers.
    unsafe fn single(relid: pg_sys::Oid, relname: &str) -> Self {
        let alias = CString::new(relname).expect("relation name without NUL");
        let context = pg_sys::deparse_context_for(alias.as_ptr(), relid);
        // The alias string is copied by deparse_context_for.
        Deparser { context, prefix: false }
    }

    /// Whole range table: `deparse_context_for_plan_tree` is what EXPLAIN
    /// uses; it only reads `rtable` from the plan, so a bare PlannedStmt
    /// carrying the analyzed query's range table is enough.
    unsafe fn for_rtable(rtable: *mut pg_sys::List, names: &[String]) -> Self {
        let pstmt = pg_sys::palloc0(std::mem::size_of::<pg_sys::PlannedStmt>()) as *mut pg_sys::PlannedStmt;
        (*pstmt).type_ = NodeTag::T_PlannedStmt;
        (*pstmt).commandType = pg_sys::CmdType::CMD_SELECT;
        (*pstmt).rtable = rtable;
        let mut list: *mut pg_sys::List = std::ptr::null_mut();
        for name in names {
            let c = CString::new(name.as_str()).expect("alias without NUL");
            // rtable_names is a List of plain C strings (as EXPLAIN builds it).
            list = pg_sys::lappend(list, pg_sys::pstrdup(c.as_ptr()) as *mut c_void);
        }
        let context = pg_sys::deparse_context_for_plan_tree(pstmt, list);
        Deparser { context, prefix: true }
    }

    unsafe fn expr(&self, node: *mut pg_sys::Node) -> String {
        text(pg_sys::deparse_expression(node, self.context, self.prefix, false))
    }
}

unsafe fn is_plain_var(node: *mut pg_sys::Node, varno: i32, attnum: i16) -> bool {
    if tag(node as *const c_void) != Some(NodeTag::T_Var) {
        return false;
    }
    let var = &*(node as *mut pg_sys::Var);
    var.varno == varno && var.varlevelsup == 0 && var.varattno == attnum
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
    CountExpr(*mut pg_sys::Node),
    Sum(*mut pg_sys::Node),
}

/// Accept only count(*), count(<expression>) and sum(<expression>) in their plain forms.
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
            if agg.aggstar {
                return AggKind::Count;
            }
            let args = list_items(agg.args);
            if args.len() != 1 {
                reject("count() must be count(*) or count(expression)");
            }
            let arg = (*(args[0] as *mut pg_sys::TargetEntry)).expr as *mut pg_sys::Node;
            require_immutable(arg, &format!("the argument of count() in column \"{alias}\""));
            AggKind::CountExpr(arg)
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
            "aggregate \"{name}\" is not supported; only count(*), count(expression) and sum(expression) are accepted"
        )),
    }
}

/// Parse, analyze and classify a definition. Raises `nabla: unsupported view
/// definition: <reason>` for anything outside the two shapes; PostgreSQL's own
/// errors (syntax, unknown column, permissions) propagate unchanged. Returns
/// the spec and the catalog facts of the first base table.
pub fn validate(definition: &str) -> (ViewSpec, BaseTable) {
    // SAFETY: every pointer comes from the parser in the current memory context
    // and is only read while it is alive; every FFI call is a documented
    // PostgreSQL entry point used the way the backend itself uses it.
    unsafe {
        let query = analyze(definition);
        reject_unsupported_clauses(query);
        let entries = collect_relations(query);
        let is_join = entries.len() > 1;

        // Base relations: catalog facts, kind checks, self-join rejection.
        let mut relations: Vec<BaseRelation> = Vec::new();
        let mut first_base: Option<BaseTable> = None;
        let mut first_pk_attnums: Vec<i16> = Vec::new();
        let mut seen_oids: HashSet<u32> = HashSet::new();
        for (i, entry) in entries.iter().enumerate() {
            let relid = (*entry.rte).relid;
            let (nspname, relname) = qualified_name(relid);
            let qualified = quote_qualified_identifier(&nspname, &relname);
            check_relation_kind(entry.rte, &qualified, &nspname);
            if !seen_oids.insert(relid.to_u32()) {
                reject(format!("table {qualified} is referenced twice; self-joins are not supported"));
            }
            let (base, pk_attnums, columns_typed) = lookup_base(relid.to_u32(), &nspname, &relname);
            let columns: Vec<String> = columns_typed.iter().map(|(n, _)| n.clone()).collect();
            let column_types: Vec<String> = columns_typed.iter().map(|(_, t)| t.clone()).collect();
            if is_join && pk_attnums.is_empty() {
                reject(format!("{qualified} has no primary key; every table in a join view needs one"));
            }
            let eref = (*entry.rte).eref;
            let alias = if eref.is_null() { relname.clone() } else { text((*eref).aliasname) };
            relations.push(BaseRelation {
                oid: relid.to_u32(),
                rti: i + 1,
                alias,
                qualified,
                pk_columns: base.pk_columns.clone(),
                columns,
                column_types,
                used_columns: Vec::new(),
                used_column_types: Vec::new(),
            });
            if i == 0 {
                first_pk_attnums = pk_attnums;
                first_base = Some(base);
            }
        }
        let base = first_base.expect("at least one relation");
        let base_relname = text(pg_sys::get_rel_name(pg_sys::Oid::from(base.oid)));
        let allowed: Vec<i32> = entries.iter().map(|e| e.rt_index).collect();

        // Quals: WHERE for a single table; every ON plus WHERE for joins.
        let mut quals: Vec<*mut pg_sys::Node> = Vec::new();
        collect_quals((*query).jointree as *mut pg_sys::Node, &mut quals);

        // Join alias Vars (USING, SELECT * over a join) become base Vars.
        if is_join {
            (*query).targetList =
                pg_sys::flatten_join_alias_vars(std::ptr::null_mut(), query, (*query).targetList as *mut pg_sys::Node)
                    as *mut pg_sys::List;
            for q in quals.iter_mut() {
                *q = pg_sys::flatten_join_alias_vars(std::ptr::null_mut(), query, *q);
            }
        }

        let deparser = if is_join {
            let names: Vec<String> = list_items((*query).rtable)
                .into_iter()
                .map(|item| {
                    let rte = item as *mut pg_sys::RangeTblEntry;
                    let eref = (*rte).eref;
                    if eref.is_null() { String::from("t") } else { text((*eref).aliasname) }
                })
                .collect();
            Deparser::for_rtable((*query).rtable, &names)
        } else {
            Deparser::single(pg_sys::Oid::from(base.oid), &base_relname)
        };

        let mut var_refs: Vec<(i32, i16)> = Vec::new();
        for q in &quals {
            referenced_vars(*q, &mut var_refs);
        }
        let mut predicate_parts = Vec::new();
        let qual_place = if is_join { "the WHERE or ON clause" } else { "the WHERE clause" };
        for q in &quals {
            require_immutable(*q, qual_place);
            require_base_vars(*q, &allowed);
            predicate_parts.push(deparser.expr(*q));
        }
        let predicate = match predicate_parts.len() {
            0 => None,
            1 => Some(predicate_parts.remove(0)),
            _ => Some(predicate_parts.iter().map(|p| format!("({p})")).collect::<Vec<_>>().join(" AND ")),
        };

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
        let mut seen = HashSet::new();
        for t in &visible {
            if t.alias.starts_with("_nabla_") {
                reject(format!("column names starting with _nabla_ are reserved (\"{}\")", t.alias));
            }
            if !seen.insert(t.alias.as_str()) {
                reject(format!("output column names must be unique; add AS aliases (duplicate: \"{}\")", t.alias));
            }
            require_base_vars(t.node, &allowed);
        }

        for t in &visible {
            referenced_vars(t.node, &mut var_refs);
        }
        for (i, entry) in entries.iter().enumerate() {
            let rel = &mut relations[i];
            let mut used: HashSet<String> = HashSet::new();
            for (varno, attno) in &var_refs {
                if *varno != entry.rt_index {
                    continue;
                }
                if *attno <= 0 {
                    reject("whole-row references are not supported");
                }
                used.insert(text(pg_sys::get_attname((*entry.rte).relid, *attno, false)));
            }
            if is_join {
                used.extend(rel.pk_columns.iter().cloned());
            }
            for (name, ty) in rel.columns.iter().zip(rel.column_types.iter()) {
                if used.contains(name) {
                    rel.used_columns.push(name.clone());
                    rel.used_column_types.push(ty.clone());
                }
            }
        }

        let has_aggs = (*query).hasAggs;
        let has_group = !refs.is_empty();
        let mut spec = ViewSpec {
            shape: if has_aggs { Shape::Aggregate } else { Shape::Projection },
            base_table: base.qualified.clone(),
            base_relname: base_relname.clone(),
            predicate,
            columns: Vec::new(),
            pk_columns: Vec::new(),
            pk_view_columns: Vec::new(),
            count_alias: None,
            hidden_count: false,
            counts: Vec::new(),
            sums: Vec::new(),
            populate_sql: String::new(),
            relations: relations.clone(),
            select_list: String::new(),
            group_by: None,
            visible_columns: visible.iter().map(|t| t.alias.clone()).collect(),
            hidden_columns: Vec::new(),
        };
        // Output columns of the populate query, in definition order.
        let mut populate_targets: Vec<String> = Vec::new();
        let mut group_exprs: Vec<String> = Vec::new();

        if has_aggs {
            if !has_group {
                reject("aggregates require a GROUP BY clause");
            }
            // Hidden columns go after the user's columns in the view table.
            let mut hidden_targets: Vec<String> = Vec::new();
            let mut agg_index = 0usize;
            for t in &visible {
                let quoted_alias = quote_identifier(&t.alias);
                if t.sortgroupref != 0 && refs.contains(&t.sortgroupref) {
                    require_immutable(t.node, &format!("the GROUP BY key \"{}\"", t.alias));
                    let expr = deparser.expr(t.node);
                    populate_targets.push(format!("{expr} AS {quoted_alias}"));
                    group_exprs.push(expr.clone());
                    spec.columns.push(OutputColumn { expr, alias: t.alias.clone() });
                } else if tag(t.node as *const c_void) == Some(NodeTag::T_Aggref) {
                    let index = agg_index;
                    agg_index += 1;
                    match classify_aggregate(t.node, &t.alias) {
                        AggKind::Count => {
                            if spec.count_alias.is_some() {
                                reject("count(*) may appear only once");
                            }
                            populate_targets.push(format!("count(*) AS {quoted_alias}"));
                            spec.count_alias = Some(t.alias.clone());
                        }
                        AggKind::CountExpr(arg) => {
                            let expr = deparser.expr(arg);
                            populate_targets.push(format!("count({expr}) AS {quoted_alias}"));
                            spec.counts.push(OutputColumn { expr, alias: t.alias.clone() });
                        }
                        AggKind::Sum(arg) => {
                            let expr = deparser.expr(arg);
                            let counter = format!("{HIDDEN_SUM_COUNTER_PREFIX}{index}");
                            populate_targets.push(format!("sum({expr}) AS {quoted_alias}"));
                            hidden_targets.push(format!("count({expr}) AS {counter}"));
                            spec.hidden_columns.push(counter.clone());
                            spec.sums.push(SumSpec { expr, alias: t.alias.clone(), counter });
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
            populate_targets.extend(hidden_targets);
            if spec.count_alias.is_none() {
                populate_targets.push(format!("count(*) AS {HIDDEN_COUNT_COLUMN}"));
                spec.count_alias = Some(HIDDEN_COUNT_COLUMN.to_string());
                spec.hidden_columns.push(HIDDEN_COUNT_COLUMN.to_string());
                spec.hidden_count = true;
            }
        } else {
            if has_group {
                reject("GROUP BY without an aggregate is not supported");
            }
            for t in &visible {
                require_immutable(t.node, &format!("column \"{}\"", t.alias));
                let expr = deparser.expr(t.node);
                populate_targets.push(format!("{expr} AS {}", quote_identifier(&t.alias)));
                spec.columns.push(OutputColumn { expr, alias: t.alias.clone() });
            }
            if is_join {
                // Row identity of a join row: every base table's primary key,
                // carried in hidden columns so the user's select list is free.
                for rel in &relations {
                    for col in &rel.pk_columns {
                        let hidden = format!("{HIDDEN_PK_PREFIX}{}_{}", rel.rti, col);
                        populate_targets.push(format!(
                            "{}.{} AS {}",
                            quote_identifier(&rel.alias),
                            quote_identifier(col),
                            quote_identifier(&hidden)
                        ));
                        spec.hidden_columns.push(hidden.clone());
                        spec.pk_view_columns.push(hidden);
                    }
                }
            } else {
                if first_pk_attnums.is_empty() {
                    reject(format!(
                        "{} has no primary key; a projection view needs one to locate deleted rows",
                        base.qualified
                    ));
                }
                let mut missing = Vec::new();
                for (attnum, name) in first_pk_attnums.iter().zip(base.pk_columns.iter()) {
                    match visible.iter().find(|t| is_plain_var(t.node, entries[0].rt_index, *attnum)) {
                        Some(t) => spec.pk_view_columns.push(t.alias.clone()),
                        None => missing.push(name.clone()),
                    }
                }
                if !missing.is_empty() {
                    reject(format!(
                        "the select list must include the primary key column(s) of {} as plain columns: missing {}",
                        base.qualified,
                        missing.join(", ")
                    ));
                }
                spec.pk_columns = base.pk_columns.clone();
            }
        }

        let from_list = if is_join {
            relations
                .iter()
                .map(|r| format!("{} AS {}", r.qualified, quote_identifier(&r.alias)))
                .collect::<Vec<_>>()
                .join(", ")
        } else {
            base.qualified.clone()
        };
        spec.select_list = populate_targets.join(", ");
        let mut populate = format!("SELECT {} FROM {from_list}", spec.select_list);
        if let Some(p) = &spec.predicate {
            populate.push_str(&format!(" WHERE {p}"));
        }
        if !group_exprs.is_empty() {
            let group_by = group_exprs.join(", ");
            populate.push_str(&format!(" GROUP BY {group_by}"));
            spec.group_by = Some(group_by);
        }
        spec.populate_sql = populate;
        (spec, base)
    }
}
