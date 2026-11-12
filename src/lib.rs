//! ferrodb — a multi-threaded, Redis-compatible in-memory database.
//!
//! Zero dependencies: everything (protocol, storage, persistence, pub/sub,
//! glob matching) is built on the Rust standard library.

pub mod aof;
pub mod commands;
pub mod glob;
pub mod pubsub;
pub mod resp;
pub mod server;
pub mod store;

use std::sync::{Mutex, MutexGuard};

/// Lock a mutex, recovering the data if another thread panicked while
/// holding it. One buggy request should never take down the whole server.
pub fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub fn log(msg: &str) {
    let secs = store::clock_ms() as f64 / 1000.0;
    eprintln!("[ferrodb {secs:.3}] {msg}");
}
