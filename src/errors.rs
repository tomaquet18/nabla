//! Error reporting helpers. Every user-facing error is prefixed with "nabla:".

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::*;

pub const SHAPES_HINT: &str = "Accepted shapes: (1) projection: SELECT expr [AS alias][, ...] FROM table [WHERE predicate], where the primary key columns are selected as plain columns; (2) aggregate: SELECT key [AS alias][, ...], count(*) [AS a][, sum(expr) [AS b]]... FROM table [WHERE predicate] GROUP BY key[, ...]. Expressions may only use IMMUTABLE functions and operators over the base table's columns.";

pub fn raise(code: PgSqlErrorCode, message: impl Into<String>, hint: Option<&str>) -> ! {
    let mut report = ErrorReport::new(code, message, pgrx::pg_sys::function_name!());
    if let Some(hint) = hint {
        report = report.set_hint(hint);
    }
    report.report(PgLogLevel::ERROR);
    unreachable!()
}

pub fn unsupported_definition(reason: impl AsRef<str>) -> ! {
    raise(
        PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
        format!("nabla: unsupported view definition: {}", reason.as_ref()),
        Some(SHAPES_HINT),
    )
}

pub fn invalid(message: impl Into<String>, hint: Option<&str>) -> ! {
    raise(PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE, message, hint)
}

pub fn prerequisite(message: impl Into<String>, hint: Option<&str>) -> ! {
    raise(PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, message, hint)
}
