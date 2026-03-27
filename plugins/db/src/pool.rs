//! Thin wrappers around `trex_pool_client` session-based API for one-off queries.

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use std::sync::Arc;

/// Execute a one-off SQL query via a short-lived session, returning Arrow batches.
pub fn execute(sql: &str) -> Result<(Arc<Schema>, Vec<RecordBatch>), String> {
    let sid = trex_pool_client::create_session()?;
    let result = trex_pool_client::session_execute(sid, sql);
    let _ = trex_pool_client::destroy_session(sid);
    result
}

/// Execute a one-off SQL query via a short-lived session, returning Arrow batches.
/// Alias with explicit schema ref return for callers that need `SchemaRef`.
pub fn read_arrow(sql: &str) -> Result<(SchemaRef, Vec<RecordBatch>), String> {
    execute(sql)
}

/// Execute a one-off write statement via a short-lived session, discarding results.
pub fn write(sql: &str) -> Result<(), String> {
    let sid = trex_pool_client::create_session()?;
    let _ = trex_pool_client::session_execute(sid, sql);
    let _ = trex_pool_client::destroy_session(sid);
    Ok(())
}
