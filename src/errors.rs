//! Error reporting helpers. Every user-facing error is prefixed with "nabla:".
//!
//! Conditions a client must branch on carry nabla-specific SQLSTATEs:
//!
//! | code  | condition                                   |
//! |-------|---------------------------------------------|
//! | NB001 | subscriber lagged behind retention           |
//! | NB002 | view is stale                                |
//! | NB003 | view epoch changed (refresh)                 |
//! | NB004 | unsupported view definition                  |
//! | NB005 | direct write to a nabla-managed table (SQL)  |
//!
//! pgrx 0.17's `ErrorReport` only accepts the `PgSqlErrorCode` enum, so the
//! custom codes go through PostgreSQL's ereport entry points directly, the
//! same way pgrx itself reports.

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::*;
use std::ffi::{c_char, c_int, CString};

pub const SQLSTATE_LAGGED: &str = "NB001";
pub const SQLSTATE_STALE: &str = "NB002";
pub const SQLSTATE_EPOCH_CHANGED: &str = "NB003";
pub const SQLSTATE_UNSUPPORTED_DEFINITION: &str = "NB004";

pub const SHAPES_HINT: &str = "Accepted shapes: \
(1) projection: SELECT expr [AS alias][, ...] FROM table [JOIN ...] [WHERE predicate], where a single \
table's primary key columns are selected as plain columns; \
(2) aggregate: SELECT key [AS alias][, ...], count(*) [AS a][, sum(expr) [AS b]]... FROM table \
[JOIN ...] [WHERE predicate] GROUP BY key[, ...]. \
Expressions may only use IMMUTABLE functions and operators over the base tables' columns.";

extern "C-unwind" {
    fn errstart(elevel: c_int, domain: *const c_char) -> bool;
    fn errcode(sqlerrcode: c_int) -> c_int;
    fn errmsg(fmt: *const c_char, ...) -> c_int;
    fn errdetail(fmt: *const c_char, ...) -> c_int;
    fn errhint(fmt: *const c_char, ...) -> c_int;
    fn errfinish(filename: *const c_char, lineno: c_int, funcname: *const c_char);
}

/// PostgreSQL's MAKE_SQLSTATE: five characters packed six bits each.
fn make_sqlstate(code: &str) -> c_int {
    let bytes = code.as_bytes();
    assert!(bytes.len() == 5, "SQLSTATE must have five characters");
    bytes
        .iter()
        .enumerate()
        .map(|(i, b)| (((*b as i32) - b'0' as i32) & 0x3F) << (6 * i))
        .sum()
}

/// Raise an ERROR with an arbitrary five-character SQLSTATE.
pub fn raise_sqlstate(code: &str, message: &str, detail: Option<&str>, hint: Option<&str>) -> ! {
    let message = CString::new(message).unwrap_or_default();
    let detail = detail.map(|d| CString::new(d).unwrap_or_default());
    let hint = hint.map(|h| CString::new(h).unwrap_or_default());
    // SAFETY: mirrors pgrx's own report path; errfinish longjmps for ERROR.
    unsafe {
        if errstart(PgLogLevel::ERROR as c_int, std::ptr::null()) {
            errcode(make_sqlstate(code));
            errmsg(c"%s".as_ptr(), message.as_ptr());
            if let Some(d) = &detail {
                errdetail(c"%s".as_ptr(), d.as_ptr());
            }
            if let Some(h) = &hint {
                errhint(c"%s".as_ptr(), h.as_ptr());
            }
            errfinish(c"nabla".as_ptr(), 0, c"nabla".as_ptr());
        }
    }
    unreachable!("errfinish returned for an ERROR")
}

pub fn raise(code: PgSqlErrorCode, message: impl Into<String>, hint: Option<&str>) -> ! {
    let mut report = ErrorReport::new(code, message, pgrx::pg_sys::function_name!());
    if let Some(hint) = hint {
        report = report.set_hint(hint);
    }
    report.report(PgLogLevel::ERROR);
    unreachable!()
}

/// NB004
pub fn unsupported_definition(reason: impl AsRef<str>) -> ! {
    raise_sqlstate(
        SQLSTATE_UNSUPPORTED_DEFINITION,
        &format!("nabla: unsupported view definition: {}", reason.as_ref()),
        None,
        Some(SHAPES_HINT),
    )
}

/// NB001
pub fn lagged(name: &str, oldest_retained_seq: i64) -> ! {
    raise_sqlstate(
        SQLSTATE_LAGGED,
        &format!("nabla: subscriber lagged behind retention for view \"{name}\""),
        Some(&format!("oldest retained seq is {oldest_retained_seq}")),
        Some("resync from the view and continue from nabla.status(...).current_seq"),
    )
}

/// NB002
pub fn stale(name: &str, reason: Option<&str>) -> ! {
    raise_sqlstate(
        SQLSTATE_STALE,
        &format!("nabla: view \"{name}\" is stale"),
        Some(reason.unwrap_or("reason not recorded")),
        Some(&format!("run nabla.refresh('{name}') after fixing the cause")),
    )
}

/// NB003
pub fn epoch_changed(name: &str, from: i32, to: i32) -> ! {
    raise_sqlstate(
        SQLSTATE_EPOCH_CHANGED,
        &format!("nabla: view \"{name}\" epoch changed"),
        Some(&format!("epoch {from} -> {to}")),
        Some("resync from the view"),
    )
}

pub fn invalid(message: impl Into<String>, hint: Option<&str>) -> ! {
    raise(PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE, message, hint)
}

pub fn prerequisite(message: impl Into<String>, hint: Option<&str>) -> ! {
    raise(PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, message, hint)
}
