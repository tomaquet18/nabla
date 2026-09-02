//! nabla: incrementally maintained views for PostgreSQL that never block writers.
//!
//! Changes are captured through logical decoding (a replication slot consumed
//! by a background worker), applied to view tables in commit order, and
//! published to subscribers as delta rows from a bounded durable log.

use pgrx::prelude::*;

mod api;
mod apply;
mod definition;
mod errors;
mod guc;
mod idle;
mod lsn;
mod pgoutput;
mod shadow;
mod worker;

::pgrx::pg_module_magic!();

extension_sql_file!("../sql/bootstrap.sql", name = "bootstrap", bootstrap);

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    guc::register();
    idle::register();
    worker::register();
}

