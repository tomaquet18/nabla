// SPDX-License-Identifier: AGPL-3.0-only
//! The idle window: a WAL range the worker has verified to contain no
//! published change, shared with backends through shared memory.
//!
//! The worker's own bookkeeping commits (frontier updates, delta rows)
//! advance `pg_current_wal_lsn()` but can never affect a view, and the worker
//! deliberately does not chase its own WAL (it would loop). Without this
//! window a client calling `nabla.wait_for(view, pg_current_wal_lsn())` right
//! after such a commit, with no user write in between, would wait forever.
//! When a poll finds no decodable change and no WAL beyond the worker's last
//! commit, the worker publishes `(from, to]`: every live view whose frontier
//! is at least `from` also reflects the base tables at `to`.

use pgrx::prelude::*;
use pgrx::shmem::PGRXSharedMemory;
use pgrx::{pg_shmem_init, PgLwLock};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Copy, Clone, Default)]
pub struct IdleWindow {
    pub from: u64,
    pub to: u64,
}

unsafe impl PGRXSharedMemory for IdleWindow {}

static WINDOW: PgLwLock<IdleWindow> = unsafe { PgLwLock::new(c"nabla_idle_window") };
/// Set in the postmaster when the shared segment was requested; inherited by
/// every backend. Without shared_preload_libraries there is no window.
static INITIALIZED: AtomicBool = AtomicBool::new(false);

#[allow(unexpected_cfgs)] // pg_shmem_init! mentions every PostgreSQL feature flag
pub fn register() {
    if unsafe { pg_sys::process_shared_preload_libraries_in_progress } {
        pg_shmem_init!(WINDOW);
        INITIALIZED.store(true, Ordering::SeqCst);
    }
}

/// Worker side: WAL in `(from, to]` holds no published change.
pub fn publish(from: u64, to: u64) {
    if INITIALIZED.load(Ordering::SeqCst) {
        *WINDOW.exclusive() = IdleWindow { from, to };
    }
}

/// The frontier a live view effectively reflects, given its stored frontier.
pub fn effective_frontier(frontier: u64) -> u64 {
    if !INITIALIZED.load(Ordering::SeqCst) {
        return frontier;
    }
    let window = *WINDOW.share();
    if window.from != 0 && frontier >= window.from && window.to > frontier {
        window.to
    } else {
        frontier
    }
}
