//! DuckDB connection management — local executor + pool writes.

use crate::query_executor::QueryExecutor;
use duckdb::Connection;
use std::sync::{Arc, Mutex, OnceLock};
use tracing::warn;

static QUERY_EXECUTOR: OnceLock<Arc<QueryExecutor>> = OnceLock::new();
static STREAMING_POOL: OnceLock<StreamingPool> = OnceLock::new();
static SHARED_CONN: OnceLock<Arc<Mutex<Connection>>> = OnceLock::new();

pub struct StreamingPool {
  connections: Mutex<Vec<Connection>>,
}

impl StreamingPool {
  fn new(connection: &Connection, pool_size: usize) -> Result<Self, String> {
    let mut connections = Vec::with_capacity(pool_size);
    for i in 0..pool_size {
      connections.push(
        connection
          .try_clone()
          .map_err(|e| format!("streaming pool clone {i}: {e}"))?,
      );
    }
    Ok(Self {
      connections: Mutex::new(connections),
    })
  }

  pub fn acquire(&self) -> Option<Connection> {
    match self.connections.lock() {
      Ok(mut pool) => pool.pop(),
      Err(poisoned) => {
        warn!("streaming pool lock poisoned on acquire, recovering");
        poisoned.into_inner().pop()
      }
    }
  }

  pub fn release(&self, conn: Connection) {
    match self.connections.lock() {
      Ok(mut pool) => pool.push(conn),
      Err(poisoned) => {
        warn!("streaming pool lock poisoned on release, recovering");
        poisoned.into_inner().push(conn);
      }
    }
  }
}

pub fn init_query_executor(
  connection: &Connection,
  pool_size: usize,
) -> Result<(), String> {
  let executor = QueryExecutor::new(connection, pool_size)?;
  QUERY_EXECUTOR
    .set(Arc::new(executor))
    .map_err(|_| "executor already initialized".into())
}

pub fn get_query_executor() -> Option<Arc<QueryExecutor>> {
  QUERY_EXECUTOR.get().cloned()
}

pub fn init_streaming_pool(
  connection: &Connection,
  pool_size: usize,
) -> Result<(), String> {
  let pool = StreamingPool::new(connection, pool_size)?;
  STREAMING_POOL
    .set(pool)
    .map_err(|_| "streaming pool already initialized".into())
}

pub fn get_streaming_pool() -> Option<&'static StreamingPool> {
  STREAMING_POOL.get()
}

pub fn init_connection(connection: &Connection) -> Result<(), String> {
  let cloned = connection
    .try_clone()
    .map_err(|e| format!("shared conn clone: {e}"))?;
  SHARED_CONN
    .set(Arc::new(Mutex::new(cloned)))
    .map_err(|_| "shared connection already initialized".into())
}

pub fn get_connection() -> Option<Arc<Mutex<Connection>>> {
  SHARED_CONN.get().cloned()
}
