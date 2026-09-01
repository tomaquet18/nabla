//! Error reporting helpers. Every user-facing error is prefixed with "nabla:".

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::*;

pub const SHAPES_HINT: &str = "Accepted shapes: \
(1) projection: SELECT col[, col ...] FROM table [WHERE predicate], where every col is a bare column \
(optionally AS alias) and the primary key columns are included; \
(2) aggregate: SELECT groupcol[, ...], count(*) [AS a][, sum(col) [AS b]]... FROM table [WHERE predicate] \
GROUP BY groupcol[, ...].";

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
