// SPDX-License-Identifier: AGPL-3.0-only
//! Configuration parameters, all under the `nabla.` prefix.

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CString;

/// Database the background worker connects to. Empty means the worker idles.
pub static DATABASE: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
/// How often the worker polls the replication slot.
pub static POLL_INTERVAL_MS: GucSetting<i32> = GucSetting::<i32>::new(100);
/// Maximum delta-log rows kept per view.
pub static RETAIN_DELTAS: GucSetting<i32> = GucSetting::<i32>::new(100_000);
/// Maximum WAL the slot may retain before the worker drops it and marks views stale.
/// pgrx 0.17 only exposes 32-bit integer GUCs, so the cap tops out just under 2 GiB.
pub static MAX_SLOT_LAG_BYTES: GucSetting<i32> = GucSetting::<i32>::new(1 << 30);
/// Consecutive failed applies of one source transaction before a view goes stale.
pub static MAX_APPLY_FAILURES: GucSetting<i32> = GucSetting::<i32>::new(3);
/// Changes decoded per poll; every complete source transaction of the peek is
/// applied in one worker transaction (see the round argument in worker.rs).
pub static BATCH_CHANGES: GucSetting<i32> = GucSetting::<i32>::new(5000);
/// Test hook: hold the population snapshot for this long before building.
pub static DEBUG_POPULATE_DELAY_MS: GucSetting<i32> = GucSetting::<i32>::new(0);
/// Session flag that lets the worker and nabla.refresh write to view tables.
pub static INTERNAL_WRITE: GucSetting<bool> = GucSetting::<bool>::new(false);

pub fn register() {
    GucRegistry::define_string_guc(
        c"nabla.database",
        c"Database the nabla background worker maintains views in.",
        c"The worker connects to this database at startup. When unset, the worker idles.",
        &DATABASE,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"nabla.poll_interval_ms",
        c"Interval between replication slot polls, in milliseconds.",
        c"",
        &POLL_INTERVAL_MS,
        1,
        60_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"nabla.retain_deltas",
        c"Maximum number of delta rows retained per view.",
        c"Subscribers that fall further behind receive an error and must resync.",
        &RETAIN_DELTAS,
        1,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"nabla.max_slot_lag_bytes",
        c"Maximum WAL bytes the nabla replication slot may retain.",
        c"When exceeded, the worker marks all views stale and drops the slot.",
        &MAX_SLOT_LAG_BYTES,
        1_048_576,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"nabla.max_apply_failures",
        c"Failed apply attempts of one transaction before a view is marked stale.",
        c"Each failure is retried on the next poll; the other views are not blocked.",
        &MAX_APPLY_FAILURES,
        1,
        1_000_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"nabla.batch_changes",
        c"Maximum decoded changes per worker poll (upto_nchanges of the peek).",
        c"All complete source transactions of one peek are applied in one worker transaction.",
        &BATCH_CHANGES,
        1,
        1_000_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"nabla.debug_populate_delay_ms",
        c"Test hook: delay before a view is populated, while the consistent snapshot is held.",
        c"Leave at 0 outside test suites.",
        &DEBUG_POPULATE_DELAY_MS,
        0,
        600_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_bool_guc(
        c"nabla.internal_write",
        c"Allows writes to nabla view tables in the current transaction.",
        c"Set locally by the nabla worker and by nabla.refresh. Not meant for users.",
        &INTERNAL_WRITE,
        GucContext::Userset,
        GucFlags::empty(),
    );
}

/// The configured worker database, or `None` when unset or empty.
pub fn database() -> Option<String> {
    DATABASE
        .get()
        .and_then(|c| c.into_string().ok())
        .filter(|s| !s.trim().is_empty())
}
