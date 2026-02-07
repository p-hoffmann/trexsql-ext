//! Thread pool executor for parallel DuckDB query execution.

use crossbeam_channel::{unbounded, Receiver, Sender};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::{params, Connection};
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

pub struct QueryRequest {
    pub query: String,
    pub response_tx: tokio::sync::oneshot::Sender<QueryResult>,
}

pub enum QueryResult {
    Select {
        schema: Arc<duckdb::arrow::datatypes::Schema>,
        batches: Vec<RecordBatch>,
    },
    Execute {
        rows_affected: usize,
    },
    Error(String),
}

struct Worker {
    handle: Option<JoinHandle<()>>,
}

/// Distributes queries across a pool of worker threads with pre-cloned connections.
pub struct QueryExecutor {
    sender: Option<Sender<QueryRequest>>,
    workers: Vec<Worker>,
}

impl Drop for QueryExecutor {
    fn drop(&mut self) {
        // Drop sender to signal workers to exit
        self.sender.take();
        // Wait for workers to finish
        for worker in &mut self.workers {
            if let Some(handle) = worker.handle.take() {
                let _ = handle.join();
            }
        }
    }
}

impl QueryExecutor {
    /// Creates executor pool. Must be called from the connection's origin thread.
    pub fn new(connection: &Connection, pool_size: usize) -> Result<Self, String> {
        let mut connections = Vec::with_capacity(pool_size);
        for i in 0..pool_size {
            connections.push(
                connection
                    .try_clone()
                    .map_err(|e| format!("connection clone {i}: {e}"))?,
            );
        }

        let (sender, receiver): (Sender<QueryRequest>, Receiver<QueryRequest>) = unbounded();

        let mut workers = Vec::with_capacity(pool_size);
        for (i, conn) in connections.into_iter().enumerate() {
            let rx = receiver.clone();
            let handle = thread::Builder::new()
                .name(format!("pgwire-executor-{i}"))
                .spawn(move || worker_loop(conn, rx))
                .map_err(|e| format!("spawn worker {i}: {e}"))?;
            workers.push(Worker { handle: Some(handle) });
        }

        Ok(Self { sender: Some(sender), workers })
    }

    pub fn submit(&self, query: String) -> tokio::sync::oneshot::Receiver<QueryResult> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        let sender = match &self.sender {
            Some(s) => s,
            None => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                let _ = tx.send(QueryResult::Error("executor shutdown".into()));
                return rx;
            }
        };

        if let Err(e) = sender.send(QueryRequest { query, response_tx }) {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let _ = tx.send(QueryResult::Error(format!("executor closed: {e}")));
            return rx;
        }

        response_rx
    }

    pub fn pool_size(&self) -> usize {
        self.workers.len()
    }
}

fn worker_loop(conn: Connection, receiver: Receiver<QueryRequest>) {
    while let Ok(req) = receiver.recv() {
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            execute_query(&conn, &req.query)
        }));
        let query_result = match result {
            Ok(r) => r,
            Err(panic_err) => {
                let msg = extract_panic_message(panic_err);
                eprintln!("pgwire query panicked: {msg}");
                QueryResult::Error(format!("query panicked: {msg}"))
            }
        };
        let _ = req.response_tx.send(query_result);
    }
}

fn extract_panic_message(err: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = err.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

fn execute_query(conn: &Connection, query: &str) -> QueryResult {
    let trimmed = query.trim();
    let upper = trimmed.to_uppercase();

    if is_result_returning_query(&upper) {
        match execute_select(conn, trimmed) {
            Ok((schema, batches)) => QueryResult::Select { schema, batches },
            Err(e) => QueryResult::Error(e),
        }
    } else {
        match execute_non_select(conn, trimmed) {
            Ok(rows) => QueryResult::Execute { rows_affected: rows },
            Err(e) => QueryResult::Error(e),
        }
    }
}

fn is_result_returning_query(upper: &str) -> bool {
    upper.starts_with("SELECT")
        || upper.starts_with("WITH")
        || upper.starts_with("SHOW")
        || upper.starts_with("DESCRIBE")
        || upper.starts_with("EXPLAIN")
        || upper.starts_with("TABLE")
        || upper.starts_with("VALUES")
        || upper.starts_with("FROM")
        || upper.starts_with("PRAGMA")
}

fn execute_select(
    conn: &Connection,
    query: &str,
) -> Result<(Arc<duckdb::arrow::datatypes::Schema>, Vec<RecordBatch>), String> {
    let mut stmt = conn.prepare(query).map_err(|e| format!("prepare: {e}"))?;
    let arrow_result = stmt.query_arrow(params![]).map_err(|e| format!("query: {e}"))?;
    Ok((arrow_result.get_schema(), arrow_result.collect()))
}

fn execute_non_select(conn: &Connection, query: &str) -> Result<usize, String> {
    let upper = query.to_uppercase();
    if upper.starts_with("SET")
        && (upper.contains("EXTRA_FLOAT_DIGITS") || upper.contains("APPLICATION_NAME"))
    {
        return Ok(0);
    }

    conn.execute_batch(query).map_err(|e| format!("exec: {e}"))?;
    Ok(0)
}
