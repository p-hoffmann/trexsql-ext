//! Local connections for operations that need direct `&Connection` access
//! (e.g. registering ArrowVTab, using appender). These can't go through
//! the C ABI string-based interface.
//!
//! Reads go through `trex_pool_client::read_arrow()` when possible.
//! Only complex operations that need closures use these local connections.

use duckdb::Connection;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

static LOCAL_CONNS: OnceLock<Vec<Mutex<Connection>>> = OnceLock::new();
static NEXT: AtomicUsize = AtomicUsize::new(0);

pub fn init(connection: &Connection, pool_size: usize) -> Result<(), String> {
    let mut conns = Vec::with_capacity(pool_size);
    for i in 0..pool_size {
        conns.push(Mutex::new(
            connection
                .try_clone()
                .map_err(|e| format!("local conn clone {i}: {e}"))?,
        ));
    }
    LOCAL_CONNS
        .set(conns)
        .map_err(|_| "local connections already initialized".to_string())
}

/// Run a closure with direct access to a local connection (round-robin).
pub fn with_connection<F, R>(f: F) -> Result<R, String>
where
    F: FnOnce(&Connection) -> Result<R, String>,
{
    let conns = LOCAL_CONNS
        .get()
        .ok_or("local connections not initialized")?;
    let idx = NEXT.fetch_add(1, Ordering::Relaxed) % conns.len();
    let guard = conns[idx]
        .lock()
        .map_err(|e| format!("local conn lock: {e}"))?;
    f(&guard)
}
