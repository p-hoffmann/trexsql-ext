//! Shared DuckDB connection pool with serialized write queue.
//!
//! This crate is a DuckDB extension (`pool.trex`) loaded before all other
//! extensions. It initialises the shared pool in its `extension_entrypoint`
//! and exports `#[no_mangle] extern "C"` functions that consumer extensions
//! discover via `dlsym` at runtime.
//!
//! Consumer extensions depend on the `trex-pool-client` rlib, which provides
//! safe Rust wrappers around the C ABI.

use crossbeam_channel::{unbounded, Receiver, Sender};
pub use duckdb;
pub use duckdb::arrow;
use duckdb::arrow::datatypes::Schema;
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;
use std::collections::HashMap;
use std::os::raw::c_void;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use tracing::warn;

// ── Result types ─────────────────────────────────────────────────────────────

/// Result of a read query — Arrow RecordBatches.
pub enum PoolResult {
    Rows {
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
    },
    Executed,
    Error(String),
}

/// Result of a write query.
pub enum WriteResult {
    Ok,
    Error(String),
}

// ── Internal request types ───────────────────────────────────────────────────

struct ReadRequest {
    sql: String,
    response_tx: std::sync::mpsc::SyncSender<PoolResult>,
}

struct WriteRequest {
    sql: String,
    params: Vec<String>,
    response_tx: std::sync::mpsc::SyncSender<WriteResult>,
}

// ── Connection handle for direct/streaming access ────────────────────────────

/// Opaque handle returned by [`acquire_direct`]. Must be returned via
/// [`release_direct`] when the caller is done.
pub struct ConnectionHandle {
    index: usize,
}

// ── SharedPool ───────────────────────────────────────────────────────────────

struct Worker {
    _handle: JoinHandle<()>,
}

struct SharedPool {
    read_senders: Vec<Sender<ReadRequest>>,
    #[allow(dead_code)]
    read_workers: Vec<Worker>,
    next_read: AtomicUsize,

    write_sender: Sender<WriteRequest>,
    #[allow(dead_code)]
    write_worker: Worker,

    direct_connections: Vec<Mutex<Option<Connection>>>,
    next_direct: AtomicUsize,
}

static POOL: OnceLock<Arc<SharedPool>> = OnceLock::new();

impl SharedPool {
    fn new(base_conn: &Connection, read_pool_size: usize) -> Result<Self, String> {
        if read_pool_size == 0 {
            return Err("read_pool_size must be > 0".to_string());
        }

        // --- Read workers ---
        let mut read_senders = Vec::with_capacity(read_pool_size);
        let mut read_workers = Vec::with_capacity(read_pool_size);

        for i in 0..read_pool_size {
            let conn = base_conn
                .try_clone()
                .map_err(|e| format!("read worker clone {i}: {e}"))?;
            let (tx, rx): (Sender<ReadRequest>, Receiver<ReadRequest>) = unbounded();
            read_senders.push(tx);

            let handle = thread::Builder::new()
                .name(format!("trex-pool-read-{i}"))
                .spawn(move || read_worker_loop(conn, rx))
                .map_err(|e| format!("spawn read worker {i}: {e}"))?;
            read_workers.push(Worker { _handle: handle });
        }

        // --- Write worker (single, serialized) ---
        let write_conn = base_conn
            .try_clone()
            .map_err(|e| format!("write worker clone: {e}"))?;
        let (write_tx, write_rx): (Sender<WriteRequest>, Receiver<WriteRequest>) = unbounded();

        let write_handle = thread::Builder::new()
            .name("trex-pool-write".into())
            .spawn(move || write_worker_loop(write_conn, write_rx))
            .map_err(|e| format!("spawn write worker: {e}"))?;

        // --- Direct connections (for with_connection and streaming) ---
        let direct_count = read_pool_size;
        let mut direct_connections = Vec::with_capacity(direct_count);
        for i in 0..direct_count {
            let conn = base_conn
                .try_clone()
                .map_err(|e| format!("direct conn clone {i}: {e}"))?;
            direct_connections.push(Mutex::new(Some(conn)));
        }

        Ok(Self {
            read_senders,
            read_workers,
            next_read: AtomicUsize::new(0),
            write_sender: write_tx,
            write_worker: Worker {
                _handle: write_handle,
            },
            direct_connections,
            next_direct: AtomicUsize::new(0),
        })
    }
}

// ── Worker loops ─────────────────────────────────────────────────────────────

fn read_worker_loop(conn: Connection, receiver: Receiver<ReadRequest>) {
    while let Ok(req) = receiver.recv() {
        let result = panic::catch_unwind(AssertUnwindSafe(|| execute_read(&conn, &req.sql)));
        let pool_result = match result {
            Ok(r) => r,
            Err(panic_err) => {
                let msg = extract_panic_message(panic_err);
                warn!(error = %msg, "read query panicked");
                PoolResult::Error(format!("query panicked: {msg}"))
            }
        };
        let _ = req.response_tx.send(pool_result);
    }
}

fn write_worker_loop(conn: Connection, receiver: Receiver<WriteRequest>) {
    while let Ok(req) = receiver.recv() {
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            if req.params.is_empty() {
                execute_write(&conn, &req.sql)
            } else {
                execute_write_params(&conn, &req.sql, &req.params)
            }
        }));
        let write_result = match result {
            Ok(r) => r,
            Err(panic_err) => {
                let msg = extract_panic_message(panic_err);
                warn!(error = %msg, "write query panicked");
                WriteResult::Error(format!("query panicked: {msg}"))
            }
        };
        let _ = req.response_tx.send(write_result);
    }
}

// ── Query execution ──────────────────────────────────────────────────────────

fn execute_read(conn: &Connection, sql: &str) -> PoolResult {
    match conn.prepare(sql) {
        Ok(mut stmt) => match stmt.query_arrow(duckdb::params![]) {
            Ok(arrow_result) => {
                let schema = arrow_result.get_schema();
                let batches: Vec<RecordBatch> = arrow_result.collect();
                PoolResult::Rows { schema, batches }
            }
            Err(e) => PoolResult::Error(format!("query exec: {e}")),
        },
        Err(e) => PoolResult::Error(format!("prepare: {e}")),
    }
}

fn execute_write(conn: &Connection, sql: &str) -> WriteResult {
    match conn.execute_batch(sql) {
        Ok(()) => WriteResult::Ok,
        Err(e) => WriteResult::Error(format!("exec: {e}")),
    }
}

fn execute_read_params(conn: &Connection, sql: &str, params: &[String]) -> PoolResult {
    match conn.prepare(sql) {
        Ok(mut stmt) => {
            let param_refs: Vec<&dyn duckdb::types::ToSql> =
                params.iter().map(|s| s as &dyn duckdb::types::ToSql).collect();
            match stmt.query_arrow(param_refs.as_slice()) {
                Ok(arrow_result) => {
                    let schema = arrow_result.get_schema();
                    let batches: Vec<RecordBatch> = arrow_result.collect();
                    PoolResult::Rows { schema, batches }
                }
                Err(e) => PoolResult::Error(format!("query exec: {e}")),
            }
        }
        Err(e) => PoolResult::Error(format!("prepare: {e}")),
    }
}

fn execute_write_params(conn: &Connection, sql: &str, params: &[String]) -> WriteResult {
    match conn.prepare(sql) {
        Ok(mut stmt) => {
            let param_refs: Vec<&dyn duckdb::types::ToSql> =
                params.iter().map(|s| s as &dyn duckdb::types::ToSql).collect();
            match stmt.execute(param_refs.as_slice()) {
                Ok(_) => WriteResult::Ok,
                Err(e) => WriteResult::Error(format!("exec: {e}")),
            }
        }
        Err(e) => WriteResult::Error(format!("prepare: {e}")),
    }
}

// ── Query classification ─────────────────────────────────────────────────────

/// Returns `true` if the SQL statement is expected to return result rows.
pub fn is_result_returning_query(sql: &str) -> bool {
    let upper = sql.trim().to_uppercase();
    upper.starts_with("SELECT")
        || upper.starts_with("WITH")
        || upper.starts_with("SHOW")
        || upper.starts_with("DESCRIBE")
        || upper.starts_with("EXPLAIN")
        || upper.starts_with("TABLE")
        || upper.starts_with("VALUES")
        || upper.starts_with("FROM")
        || (upper.starts_with("PRAGMA") && !is_action_pragma(&upper))
}

fn is_action_pragma(upper: &str) -> bool {
    let after = upper["PRAGMA".len()..].trim_start();
    after.starts_with("CREATE_FTS_INDEX")
        || after.starts_with("DROP_FTS_INDEX")
        || after.starts_with("COPY_DATABASE")
        || after.starts_with("IMPORT_DATABASE")
}

// ── JSON serialization ───────────────────────────────────────────────────────

/// Convert Arrow RecordBatches to a JSON array string.
pub fn record_batches_to_json(batches: &[RecordBatch]) -> String {
    let mut rows: Vec<serde_json::Value> = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        for r in 0..batch.num_rows() {
            let mut obj = serde_json::Map::with_capacity(batch.num_columns());
            for (i, field) in schema.fields().iter().enumerate() {
                let col = batch.column(i);
                let v = column_value_to_json(col.as_ref(), r, field.data_type());
                obj.insert(field.name().clone(), v);
            }
            rows.push(serde_json::Value::Object(obj));
        }
    }
    serde_json::to_string(&rows).unwrap_or_else(|_| "[]".to_string())
}

fn column_value_to_json(
    array: &dyn duckdb::arrow::array::Array,
    row: usize,
    dt: &duckdb::arrow::datatypes::DataType,
) -> serde_json::Value {
    use duckdb::arrow::array::*;
    use duckdb::arrow::datatypes::{DataType, TimeUnit};
    use serde_json::Value as JV;

    if array.is_null(row) {
        return JV::Null;
    }
    match dt {
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            JV::String(a.value(row).to_string())
        }
        DataType::LargeUtf8 => {
            let a = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            JV::String(a.value(row).to_string())
        }
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            JV::from(a.value(row))
        }
        DataType::Int8 => {
            let a = array.as_any().downcast_ref::<Int8Array>().unwrap();
            JV::from(a.value(row) as i64)
        }
        DataType::Int16 => {
            let a = array.as_any().downcast_ref::<Int16Array>().unwrap();
            JV::from(a.value(row) as i64)
        }
        DataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
            JV::from(a.value(row) as i64)
        }
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
            JV::from(a.value(row))
        }
        DataType::UInt8 => {
            let a = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            JV::from(a.value(row) as u64)
        }
        DataType::UInt16 => {
            let a = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            JV::from(a.value(row) as u64)
        }
        DataType::UInt32 => {
            let a = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            JV::from(a.value(row) as u64)
        }
        DataType::UInt64 => {
            let a = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            JV::from(a.value(row))
        }
        DataType::Float32 => {
            let a = array.as_any().downcast_ref::<Float32Array>().unwrap();
            JV::from(a.value(row) as f64)
        }
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
            JV::from(a.value(row))
        }
        DataType::Decimal128(_, scale) => {
            let a = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let value = a.value(row) as f64 / 10_f64.powi(*scale as i32);
            JV::from(value)
        }
        DataType::Date32 => {
            let a = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = a.value(row);
            let ts = days as i64 * 86400;
            let dt = chrono::DateTime::from_timestamp(ts, 0)
                .unwrap_or(chrono::DateTime::UNIX_EPOCH);
            JV::String(dt.format("%Y-%m-%d").to_string())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let a = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let micros = a.value(row);
            let dt = chrono::DateTime::from_timestamp_micros(micros)
                .unwrap_or(chrono::DateTime::UNIX_EPOCH);
            JV::String(dt.to_rfc3339())
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let a = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let millis = a.value(row);
            let dt = chrono::DateTime::from_timestamp_millis(millis)
                .unwrap_or(chrono::DateTime::UNIX_EPOCH);
            JV::String(dt.to_rfc3339())
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let a = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            let secs = a.value(row);
            let dt = chrono::DateTime::from_timestamp(secs, 0)
                .unwrap_or(chrono::DateTime::UNIX_EPOCH);
            JV::String(dt.to_rfc3339())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let a = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let nanos = a.value(row);
            let dt = chrono::DateTime::from_timestamp_nanos(nanos);
            JV::String(dt.to_rfc3339())
        }
        // Fallback: convert to string representation
        _ => JV::String(format!("{:?}", array.as_any())),
    }
}

// ── Public API ───────────────────────────────────────────────────────────────

/// Initialise the shared pool from an existing Connection.
/// Clones the connection for each worker thread.
///
/// `read_pool_size` controls the number of parallel read workers.
/// There is always exactly one serialised write worker.
/// Also initialises the streaming pool and connection provider.
pub fn init_from_connection(conn: &Connection, read_pool_size: usize) -> Result<(), String> {
    let pool = SharedPool::new(conn, read_pool_size)?;
    POOL.set(Arc::new(pool))
        .map_err(|_| "pool already initialised".to_string())?;

    // Initialize streaming pool
    let streaming = StreamingPool::new(conn, read_pool_size)?;
    let _ = STREAMING_POOL.set(streaming);

    // Initialize shared connection provider
    let shared_conn = conn
        .try_clone()
        .map_err(|e| format!("shared conn clone: {e}"))?;
    let _ = CONNECTION_PROVIDER.set(Arc::new(Mutex::new(shared_conn)));

    Ok(())
}

/// Initialise the shared pool from a raw `duckdb_database` handle.
///
/// # Safety
///
/// `db_ptr` must be a valid `duckdb_database` handle that outlives the pool.
pub unsafe fn init(db_ptr: *mut c_void, read_pool_size: usize) -> Result<(), String> {
    let base_conn = Connection::open_from_raw(db_ptr.cast())
        .map_err(|e| format!("open_from_raw: {e}"))?;
    let pool = SharedPool::new(&base_conn, read_pool_size)?;
    // Intentionally leak base_conn — it must outlive the pool (which is 'static).
    std::mem::forget(base_conn);
    POOL.set(Arc::new(pool))
        .map_err(|_| "pool already initialised".to_string())
}

fn get_pool() -> Result<&'static Arc<SharedPool>, String> {
    POOL.get().ok_or_else(|| "pool not initialised".to_string())
}

// ── Read API (Arrow) ─────────────────────────────────────────────────────────

/// Submit a read query and block, returning Arrow Schema + RecordBatches.
pub fn read_arrow(sql: &str) -> Result<(Arc<Schema>, Vec<RecordBatch>), String> {
    let pool = get_pool()?;
    let idx = pool.next_read.fetch_add(1, Ordering::Relaxed) % pool.read_senders.len();
    let sender = &pool.read_senders[idx];

    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    sender
        .send(ReadRequest {
            sql: sql.to_string(),
            response_tx: tx,
        })
        .map_err(|e| format!("read channel closed: {e}"))?;

    match rx.recv() {
        Ok(PoolResult::Rows { schema, batches }) => Ok((schema, batches)),
        Ok(PoolResult::Executed) => Ok((Arc::new(Schema::empty()), vec![])),
        Ok(PoolResult::Error(e)) => Err(e),
        Err(e) => Err(format!("read response channel closed: {e}")),
    }
}

/// Submit a read query and block, returning a JSON string.
pub fn read(sql: &str) -> Result<String, String> {
    let (_, batches) = read_arrow(sql)?;
    Ok(record_batches_to_json(&batches))
}

/// Submit a read query to a specific worker (pinned connection).
pub fn read_on(worker_id: usize, sql: &str) -> Result<String, String> {
    let pool = get_pool()?;
    let idx = worker_id % pool.read_senders.len();
    let sender = &pool.read_senders[idx];

    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    sender
        .send(ReadRequest {
            sql: sql.to_string(),
            response_tx: tx,
        })
        .map_err(|e| format!("read channel closed: {e}"))?;

    match rx.recv() {
        Ok(PoolResult::Rows { batches, .. }) => Ok(record_batches_to_json(&batches)),
        Ok(PoolResult::Executed) => Ok("[]".to_string()),
        Ok(PoolResult::Error(e)) => Err(e),
        Err(e) => Err(format!("read response channel closed: {e}")),
    }
}

/// Submit a read query to a specific worker, returning Arrow RecordBatches.
pub fn read_arrow_on(worker_id: usize, sql: &str) -> Result<(Arc<Schema>, Vec<RecordBatch>), String> {
    let pool = get_pool()?;
    let idx = worker_id % pool.read_senders.len();
    let sender = &pool.read_senders[idx];

    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    sender
        .send(ReadRequest {
            sql: sql.to_string(),
            response_tx: tx,
        })
        .map_err(|e| format!("read channel closed: {e}"))?;

    match rx.recv() {
        Ok(PoolResult::Rows { schema, batches }) => Ok((schema, batches)),
        Ok(PoolResult::Executed) => Ok((Arc::new(Schema::empty()), vec![])),
        Ok(PoolResult::Error(e)) => Err(e),
        Err(e) => Err(format!("read response channel closed: {e}")),
    }
}

// ── Write API ────────────────────────────────────────────────────────────────

/// Submit a write query through the serialised write queue. Blocks until done.
pub fn write(sql: &str) -> Result<(), String> {
    write_params(sql, &[])
}

/// Submit a parameterized write query through the serialised write queue.
pub fn write_params(sql: &str, params: &[String]) -> Result<(), String> {
    let pool = get_pool()?;

    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    pool.write_sender
        .send(WriteRequest {
            sql: sql.to_string(),
            params: params.to_vec(),
            response_tx: tx,
        })
        .map_err(|e| format!("write channel closed: {e}"))?;

    match rx.recv() {
        Ok(WriteResult::Ok) => Ok(()),
        Ok(WriteResult::Error(e)) => Err(e),
        Err(e) => Err(format!("write response channel closed: {e}")),
    }
}

// ── Auto-classify API ────────────────────────────────────────────────────────

/// Auto-classify the SQL as read or write, then route accordingly.
/// Returns JSON for reads, "[]" for writes.
pub fn execute(sql: &str) -> Result<String, String> {
    if is_result_returning_query(sql) {
        read(sql)
    } else {
        write(sql).map(|()| "[]".to_string())
    }
}

/// Auto-classify the SQL as read or write, returning Arrow for reads.
pub fn execute_arrow(sql: &str) -> PoolResult {
    if is_result_returning_query(sql) {
        match read_arrow(sql) {
            Ok((schema, batches)) => PoolResult::Rows { schema, batches },
            Err(e) => PoolResult::Error(e),
        }
    } else {
        match write(sql) {
            Ok(()) => PoolResult::Executed,
            Err(e) => PoolResult::Error(e),
        }
    }
}

// ── Direct connection API ────────────────────────────────────────────────────

/// Run a closure with direct access to a pooled connection (round-robin).
/// The closure runs on the calling thread with a Mutex-locked connection.
pub fn with_connection<F, R>(f: F) -> Result<R, String>
where
    F: FnOnce(&Connection) -> Result<R, String>,
{
    let pool = get_pool()?;
    let idx = pool
        .next_direct
        .fetch_add(1, Ordering::Relaxed)
        % pool.direct_connections.len();
    let slot = &pool.direct_connections[idx];
    let guard = slot
        .lock()
        .map_err(|e| format!("direct conn lock: {e}"))?;
    let conn = guard.as_ref().ok_or("direct connection not available")?;
    f(conn)
}

/// Acquire a direct connection handle for exclusive use (e.g. streaming queries).
/// Returns `None` if all direct connections are currently in use.
/// The caller **must** return the handle via [`release_direct`].
///
/// While held, use [`execute_direct`] to run queries on this connection.
pub fn acquire_direct() -> Result<Option<ConnectionHandle>, String> {
    let pool = get_pool()?;
    for (i, slot) in pool.direct_connections.iter().enumerate() {
        if let Ok(guard) = slot.try_lock() {
            if guard.is_some() {
                return Ok(Some(ConnectionHandle { index: i }));
            }
        }
    }
    Ok(None)
}

/// Execute SQL on a direct connection identified by handle.
pub fn execute_direct(handle: &ConnectionHandle, sql: &str) -> Result<String, String> {
    let pool = get_pool()?;
    let slot = pool
        .direct_connections
        .get(handle.index)
        .ok_or("invalid connection handle")?;
    let guard = slot.lock().map_err(|e| format!("direct conn lock: {e}"))?;
    let conn = guard.as_ref().ok_or("direct connection not available")?;

    if is_result_returning_query(sql) {
        match execute_read(conn, sql) {
            PoolResult::Rows { batches, .. } => Ok(record_batches_to_json(&batches)),
            PoolResult::Executed => Ok("[]".to_string()),
            PoolResult::Error(e) => Err(e),
        }
    } else {
        match execute_write(conn, sql) {
            WriteResult::Ok => Ok("[]".to_string()),
            WriteResult::Error(e) => Err(e),
        }
    }
}

/// Release a previously acquired direct connection handle.
pub fn release_direct(_handle: ConnectionHandle) -> Result<(), String> {
    Ok(())
}

// ── Streaming pool ───────────────────────────────────────────────────────────

static STREAMING_POOL: OnceLock<StreamingPool> = OnceLock::new();

/// Pool of connections for streaming/cursor use. Connections can be acquired
/// and released individually, unlike the channel-based read workers.
pub struct StreamingPool {
    connections: Mutex<Vec<Connection>>,
}

impl StreamingPool {
    fn new(base_conn: &Connection, pool_size: usize) -> Result<Self, String> {
        let mut connections = Vec::with_capacity(pool_size);
        for i in 0..pool_size {
            connections.push(
                base_conn
                    .try_clone()
                    .map_err(|e| format!("streaming pool clone {i}: {e}"))?,
            );
        }
        Ok(Self {
            connections: Mutex::new(connections),
        })
    }

    /// Acquire a connection from the streaming pool. Returns `None` if all are in use.
    pub fn acquire(&self) -> Option<Connection> {
        match self.connections.lock() {
            Ok(mut pool) => pool.pop(),
            Err(poisoned) => poisoned.into_inner().pop(),
        }
    }

    /// Return a connection to the streaming pool.
    pub fn release(&self, conn: Connection) {
        match self.connections.lock() {
            Ok(mut pool) => pool.push(conn),
            Err(poisoned) => poisoned.into_inner().push(conn),
        }
    }
}

/// Get the shared streaming pool (initialized alongside the main pool).
pub fn get_streaming_pool() -> Option<&'static StreamingPool> {
    STREAMING_POOL.get()
}

// ── Connection provider ──────────────────────────────────────────────────────

static CONNECTION_PROVIDER: OnceLock<Arc<Mutex<Connection>>> = OnceLock::new();

/// Get a shared connection from the pool (for general purpose use).
pub fn get_connection() -> Option<Arc<Mutex<Connection>>> {
    CONNECTION_PROVIDER.get().cloned()
}

// ── Pool management ──────────────────────────────────────────────────────────

/// Force a trivial query on every read worker to refresh the DuckDB catalog
/// after DDL changes. Call this after write operations that change schema.
pub fn sync_all() -> Result<(), String> {
    let pool = get_pool()?;
    let mut receivers = Vec::with_capacity(pool.read_senders.len());

    for sender in &pool.read_senders {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let _ = sender.send(ReadRequest {
            sql: "SELECT 1".to_string(),
            response_tx: tx,
        });
        receivers.push(rx);
    }

    for rx in receivers {
        let _ = rx.recv();
    }
    Ok(())
}

/// Return the number of read workers in the pool.
pub fn read_pool_size() -> Result<usize, String> {
    let pool = get_pool()?;
    Ok(pool.read_senders.len())
}

/// Return the next read worker id (round-robin).
pub fn next_read_worker_id() -> Result<usize, String> {
    let pool = get_pool()?;
    Ok(pool.next_read.fetch_add(1, Ordering::Relaxed) % pool.read_senders.len())
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn extract_panic_message(err: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = err.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

// ── Session API with automatic transaction detection ─────────────────────────

struct SessionState {
    /// Index into `SharedPool::direct_connections`. `Some` = in transaction.
    direct_conn_index: Option<usize>,
}

static SESSIONS: OnceLock<Mutex<HashMap<u64, SessionState>>> = OnceLock::new();
static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

fn sessions() -> &'static Mutex<HashMap<u64, SessionState>> {
    SESSIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Create a new session. Returns a unique session ID.
pub fn create_session() -> u64 {
    let id = NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed);
    sessions()
        .lock()
        .expect("sessions lock poisoned")
        .insert(id, SessionState { direct_conn_index: None });
    id
}

/// Destroy a session. If a transaction is active, it is rolled back.
pub fn destroy_session(session_id: u64) {
    let mut map = sessions().lock().expect("sessions lock poisoned");
    if let Some(state) = map.remove(&session_id) {
        if let Some(idx) = state.direct_conn_index {
            // Auto-rollback the dangling transaction.
            if let Ok(pool) = get_pool() {
                if let Some(slot) = pool.direct_connections.get(idx) {
                    if let Ok(guard) = slot.lock() {
                        if let Some(conn) = guard.as_ref() {
                            let _ = conn.execute_batch("ROLLBACK;");
                        }
                    }
                }
            }
        }
    }
}

fn is_begin(sql: &str) -> bool {
    let upper = sql.trim().to_uppercase();
    upper.starts_with("BEGIN")
}

fn is_commit_or_rollback(sql: &str) -> bool {
    let upper = sql.trim().to_uppercase();
    upper.starts_with("COMMIT") || upper.starts_with("ROLLBACK")
}

/// Execute SQL within a session. See [`session_execute_params`] for details.
pub fn session_execute(session_id: u64, sql: &str) -> PoolResult {
    session_execute_params(session_id, sql, &[])
}

/// Execute parameterized SQL within a session. Automatically detects
/// `BEGIN`/`COMMIT`/`ROLLBACK` and pins the session to a direct connection
/// for the duration of the transaction. Outside a transaction, queries are
/// routed normally (reads → read workers, writes → write worker).
pub fn session_execute_params(session_id: u64, sql: &str, params: &[String]) -> PoolResult {
    let mut map = sessions().lock().expect("sessions lock poisoned");
    if !map.contains_key(&session_id) {
        return PoolResult::Error(format!("session {session_id} not found"));
    }

    let in_txn = map[&session_id].direct_conn_index;

    if let Some(idx) = in_txn {
        // Session is in a transaction — execute on pinned direct connection.
        let is_end = is_commit_or_rollback(sql);
        if is_end {
            if let Some(state) = map.get_mut(&session_id) {
                state.direct_conn_index = None;
            }
        }
        drop(map);
        execute_on_direct_params(idx, sql, params)
    } else if is_begin(sql) {
        // Acquire a direct connection for the transaction.
        // Retry with backoff if all direct connections are currently in use.
        drop(map);
        let pool = match get_pool() {
            Ok(p) => p,
            Err(e) => return PoolResult::Error(e),
        };
        let max_retries = 50; // 50 * 20ms = 1s max wait
        let mut found_idx = None;
        for _ in 0..max_retries {
            let m = sessions().lock().expect("sessions lock poisoned");
            let held: std::collections::HashSet<usize> = m
                .values()
                .filter_map(|s| s.direct_conn_index)
                .collect();
            for i in 0..pool.direct_connections.len() {
                if !held.contains(&i) {
                    found_idx = Some(i);
                    break;
                }
            }
            drop(m);
            if found_idx.is_some() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let idx = match found_idx {
            Some(i) => i,
            None => {
                return PoolResult::Error(
                    "no connections available for transaction (timeout)".to_string(),
                );
            }
        };
        let mut map = sessions().lock().expect("sessions lock poisoned");
        if let Some(state) = map.get_mut(&session_id) {
            state.direct_conn_index = Some(idx);
        }
        drop(map);
        execute_on_direct(idx, sql)
    } else {
        // Not in a transaction — route normally.
        drop(map);
        if params.is_empty() {
            if is_result_returning_query(sql) {
                match read_arrow(sql) {
                    Ok((schema, batches)) => PoolResult::Rows { schema, batches },
                    Err(e) => PoolResult::Error(e),
                }
            } else {
                match write(sql) {
                    Ok(()) => PoolResult::Executed,
                    Err(e) => PoolResult::Error(e),
                }
            }
        } else {
            // Parameterized queries need a direct connection (read/write workers
            // don't support params through the channel API).
            let pool = match get_pool() {
                Ok(p) => p,
                Err(e) => return PoolResult::Error(e),
            };
            let idx = pool.next_direct.fetch_add(1, Ordering::Relaxed)
                % pool.direct_connections.len();
            execute_on_direct_params(idx, sql, params)
        }
    }
}

/// Execute SQL on a direct connection by index, supporting both reads and writes.
fn execute_on_direct(idx: usize, sql: &str) -> PoolResult {
    execute_on_direct_params(idx, sql, &[])
}

/// Execute parameterized SQL on a direct connection by index.
fn execute_on_direct_params(idx: usize, sql: &str, params: &[String]) -> PoolResult {
    let pool = match get_pool() {
        Ok(p) => p,
        Err(e) => return PoolResult::Error(e),
    };
    let slot = match pool.direct_connections.get(idx) {
        Some(s) => s,
        None => return PoolResult::Error(format!("invalid direct connection index {idx}")),
    };
    let guard = match slot.lock() {
        Ok(g) => g,
        Err(e) => return PoolResult::Error(format!("direct conn lock: {e}")),
    };
    let conn = match guard.as_ref() {
        Some(c) => c,
        None => return PoolResult::Error("direct connection not available".to_string()),
    };

    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        if is_result_returning_query(sql) {
            if params.is_empty() {
                execute_read(conn, sql)
            } else {
                execute_read_params(conn, sql, params)
            }
        } else if params.is_empty() {
            match execute_write(conn, sql) {
                WriteResult::Ok => PoolResult::Executed,
                WriteResult::Error(e) => PoolResult::Error(e),
            }
        } else {
            match execute_write_params(conn, sql, params) {
                WriteResult::Ok => PoolResult::Executed,
                WriteResult::Error(e) => PoolResult::Error(e),
            }
        }
    }));

    match result {
        Ok(r) => r,
        Err(panic_err) => {
            let msg = extract_panic_message(panic_err);
            PoolResult::Error(format!("query panicked: {msg}"))
        }
    }
}

// ── DuckDB extension entrypoint ──────────────────────────────────────────────

const DEFAULT_POOL_SIZE: usize = 4;

#[duckdb_loadable_macros::duckdb_entrypoint_c_api(ext_name = "pool")]
pub unsafe fn extension_entrypoint(con: Connection) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let pool_size: usize = std::env::var("TREX_POOL_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_POOL_SIZE)
        .max(1);

    init_from_connection(&con, pool_size)
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    Ok(())
}

// ── C ABI exports ────────────────────────────────────────────────────────────
//
// These are discovered by consumer extensions via dlsym(RTLD_DEFAULT, ...).
// Each function uses simple C types at the boundary (pointers, lengths, ints).

/// Opaque result handle returned by read functions. Must be freed with
/// `trex_pool_result_free`.
pub struct CPoolResult {
    json: String,
    error: Option<String>,
}

/// Execute a read query and return a result handle. Caller must free with
/// `trex_pool_result_free`. Returns null on channel error.
#[no_mangle]
pub extern "C" fn trex_pool_read(
    sql_ptr: *const u8,
    sql_len: usize,
) -> *mut CPoolResult {
    let sql = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(sql_ptr, sql_len)) };
    let result = match read(sql) {
        Ok(json) => CPoolResult { json, error: None },
        Err(e) => CPoolResult { json: String::new(), error: Some(e) },
    };
    Box::into_raw(Box::new(result))
}

/// Execute a write query. Returns 0 on success, 1 on error.
/// On error, the error message is written to `err_ptr`/`err_len`.
#[no_mangle]
pub extern "C" fn trex_pool_write(
    sql_ptr: *const u8,
    sql_len: usize,
    err_ptr: *mut *const u8,
    err_len: *mut usize,
) -> i32 {
    let sql = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(sql_ptr, sql_len)) };
    match write(sql) {
        Ok(()) => 0,
        Err(e) => {
            if !err_ptr.is_null() {
                let leaked = e.into_bytes().leak();
                unsafe {
                    *err_ptr = leaked.as_ptr();
                    *err_len = leaked.len();
                }
            }
            1
        }
    }
}

/// Execute SQL with auto-classification (read vs write). Returns result handle.
#[no_mangle]
pub extern "C" fn trex_pool_execute(
    sql_ptr: *const u8,
    sql_len: usize,
) -> *mut CPoolResult {
    let sql = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(sql_ptr, sql_len)) };
    let result = match execute(sql) {
        Ok(json) => CPoolResult { json, error: None },
        Err(e) => CPoolResult { json: String::new(), error: Some(e) },
    };
    Box::into_raw(Box::new(result))
}

/// Check if the result is an error. Returns 1 if error, 0 if success.
#[no_mangle]
pub extern "C" fn trex_pool_result_is_error(result: *const CPoolResult) -> i32 {
    if result.is_null() { return 1; }
    let r = unsafe { &*result };
    if r.error.is_some() { 1 } else { 0 }
}

/// Get the JSON data from a result. Sets `out_ptr` and `out_len`.
#[no_mangle]
pub extern "C" fn trex_pool_result_json(
    result: *const CPoolResult,
    out_ptr: *mut *const u8,
    out_len: *mut usize,
) {
    if result.is_null() { return; }
    let r = unsafe { &*result };
    let bytes = r.json.as_bytes();
    unsafe {
        *out_ptr = bytes.as_ptr();
        *out_len = bytes.len();
    }
}

/// Get the error message from a result. Sets `out_ptr` and `out_len`.
#[no_mangle]
pub extern "C" fn trex_pool_result_error(
    result: *const CPoolResult,
    out_ptr: *mut *const u8,
    out_len: *mut usize,
) {
    if result.is_null() { return; }
    let r = unsafe { &*result };
    if let Some(ref e) = r.error {
        unsafe {
            *out_ptr = e.as_ptr();
            *out_len = e.len();
        }
    }
}

/// Free a result handle.
#[no_mangle]
pub extern "C" fn trex_pool_result_free(result: *mut CPoolResult) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result)); }
    }
}

/// Check if SQL is a result-returning query. Returns 1 if true, 0 if false.
#[no_mangle]
pub extern "C" fn trex_pool_is_read_query(
    sql_ptr: *const u8,
    sql_len: usize,
) -> i32 {
    let sql = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(sql_ptr, sql_len)) };
    if is_result_returning_query(sql) { 1 } else { 0 }
}

/// Force catalog sync across all read workers. Returns 0 on success.
#[no_mangle]
pub extern "C" fn trex_pool_sync_all() -> i32 {
    match sync_all() {
        Ok(()) => 0,
        Err(_) => 1,
    }
}

/// Get the read pool size. Returns 0 if pool not initialized.
#[no_mangle]
pub extern "C" fn trex_pool_read_pool_size() -> usize {
    read_pool_size().unwrap_or(0)
}

/// Run a closure on a direct connection. The C ABI exposes this as
/// execute-on-direct-connection with SQL string.
#[no_mangle]
pub extern "C" fn trex_pool_with_connection_execute(
    sql_ptr: *const u8,
    sql_len: usize,
) -> *mut CPoolResult {
    let sql = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(sql_ptr, sql_len)) };
    let result = with_connection(|conn| {
        conn.execute_batch(sql).map_err(|e| format!("exec: {e}"))
    });
    let cresult = match result {
        Ok(()) => CPoolResult { json: "[]".to_string(), error: None },
        Err(e) => CPoolResult { json: String::new(), error: Some(e) },
    };
    Box::into_raw(Box::new(cresult))
}

/// Opaque handle for Arrow IPC result. Contains serialized IPC stream bytes.
/// Must be freed with `trex_pool_arrow_result_free`.
pub struct CArrowResult {
    data: Vec<u8>,
    error: Option<String>,
}

/// Execute a read query and return Arrow IPC serialized bytes.
/// Returns null on allocation failure. Caller must free with `trex_pool_arrow_result_free`.
#[no_mangle]
pub extern "C" fn trex_pool_read_arrow_ipc(
    sql_ptr: *const u8,
    sql_len: usize,
) -> *mut CArrowResult {
    let sql = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(sql_ptr, sql_len)) };
    let result = read_arrow(sql);
    let cresult = match result {
        Ok((schema, batches)) => {
            match serialize_arrow_ipc(&schema, &batches) {
                Ok(data) => CArrowResult { data, error: None },
                Err(e) => CArrowResult { data: Vec::new(), error: Some(e) },
            }
        }
        Err(e) => CArrowResult { data: Vec::new(), error: Some(e) },
    };
    Box::into_raw(Box::new(cresult))
}

/// Execute SQL with auto-classification, returning Arrow IPC for reads.
#[no_mangle]
pub extern "C" fn trex_pool_execute_arrow_ipc(
    sql_ptr: *const u8,
    sql_len: usize,
) -> *mut CArrowResult {
    let sql = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(sql_ptr, sql_len)) };
    if is_result_returning_query(sql) {
        trex_pool_read_arrow_ipc(sql_ptr, sql_len)
    } else {
        let cresult = match write(sql) {
            Ok(()) => CArrowResult { data: Vec::new(), error: None },
            Err(e) => CArrowResult { data: Vec::new(), error: Some(e) },
        };
        Box::into_raw(Box::new(cresult))
    }
}

/// Check if the Arrow result is an error.
#[no_mangle]
pub extern "C" fn trex_pool_arrow_result_is_error(result: *const CArrowResult) -> i32 {
    if result.is_null() { return 1; }
    let r = unsafe { &*result };
    if r.error.is_some() { 1 } else { 0 }
}

/// Get the Arrow IPC bytes from a result. Sets `out_ptr` and `out_len`.
/// Returns 0 on success, 1 if no data (write result or error).
#[no_mangle]
pub extern "C" fn trex_pool_arrow_result_data(
    result: *const CArrowResult,
    out_ptr: *mut *const u8,
    out_len: *mut usize,
) -> i32 {
    if result.is_null() { return 1; }
    let r = unsafe { &*result };
    if r.data.is_empty() {
        return 1;
    }
    unsafe {
        *out_ptr = r.data.as_ptr();
        *out_len = r.data.len();
    }
    0
}

/// Get the error message from an Arrow result.
#[no_mangle]
pub extern "C" fn trex_pool_arrow_result_error(
    result: *const CArrowResult,
    out_ptr: *mut *const u8,
    out_len: *mut usize,
) {
    if result.is_null() { return; }
    let r = unsafe { &*result };
    if let Some(ref e) = r.error {
        unsafe {
            *out_ptr = e.as_ptr();
            *out_len = e.len();
        }
    }
}

/// Free an Arrow result handle.
#[no_mangle]
pub extern "C" fn trex_pool_arrow_result_free(result: *mut CArrowResult) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result)); }
    }
}

/// Serialize Arrow RecordBatches to IPC stream format.
fn serialize_arrow_ipc(
    schema: &Arc<Schema>,
    batches: &[RecordBatch],
) -> Result<Vec<u8>, String> {
    use arrow_ipc::writer::StreamWriter;

    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, schema)
        .map_err(|e| format!("ipc writer init: {e}"))?;

    for batch in batches {
        writer.write(batch).map_err(|e| format!("ipc write batch: {e}"))?;
    }
    writer.finish().map_err(|e| format!("ipc finish: {e}"))?;

    Ok(buf)
}

/// Execute multiple SQL statements in a single transaction on one connection.
/// `sqls_ptr` points to a null-terminated array of (ptr, len) pairs.
/// Returns 0 on success, result handle with error on failure.
#[no_mangle]
pub extern "C" fn trex_pool_execute_transaction(
    sqls_ptr: *const *const u8,
    sqls_lens: *const usize,
    count: usize,
) -> *mut CPoolResult {
    let sqls: Vec<&str> = unsafe {
        let ptrs = std::slice::from_raw_parts(sqls_ptr, count);
        let lens = std::slice::from_raw_parts(sqls_lens, count);
        ptrs.iter()
            .zip(lens.iter())
            .map(|(&p, &l)| std::str::from_utf8_unchecked(std::slice::from_raw_parts(p, l)))
            .collect()
    };

    let result = with_connection(|conn| {
        conn.execute_batch("BEGIN TRANSACTION;")
            .map_err(|e| format!("begin: {e}"))?;

        for sql in &sqls {
            if let Err(e) = conn.execute_batch(sql) {
                let _ = conn.execute_batch("ROLLBACK;");
                return Err(format!("transaction failed: {e}"));
            }
        }

        conn.execute_batch("COMMIT;")
            .map_err(|e| format!("commit: {e}"))?;
        Ok(())
    });

    let cresult = match result {
        Ok(()) => CPoolResult {
            json: "[]".to_string(),
            error: None,
        },
        Err(e) => CPoolResult {
            json: String::new(),
            error: Some(e),
        },
    };
    Box::into_raw(Box::new(cresult))
}

// ── Session C ABI exports ────────────────────────────────────────────────────

/// Create a new session. Returns a unique session ID (always > 0).
#[no_mangle]
pub extern "C" fn trex_pool_session_create() -> u64 {
    create_session()
}

/// Execute SQL within a session, returning Arrow IPC bytes.
/// Auto-detects transactions (BEGIN/COMMIT/ROLLBACK).
#[no_mangle]
pub extern "C" fn trex_pool_session_execute_arrow(
    session_id: u64,
    sql_ptr: *const u8,
    sql_len: usize,
) -> *mut CArrowResult {
    trex_pool_session_execute_params_arrow(
        session_id, sql_ptr, sql_len,
        std::ptr::null(), std::ptr::null(), 0,
    )
}

/// Execute parameterized SQL within a session, returning Arrow IPC bytes.
#[no_mangle]
pub extern "C" fn trex_pool_session_execute_params_arrow(
    session_id: u64,
    sql_ptr: *const u8,
    sql_len: usize,
    params_ptrs: *const *const u8,
    params_lens: *const usize,
    params_count: usize,
) -> *mut CArrowResult {
    let sql = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(sql_ptr, sql_len)) };
    let params: Vec<String> = if params_count > 0 && !params_ptrs.is_null() && !params_lens.is_null() {
        unsafe {
            let ptrs = std::slice::from_raw_parts(params_ptrs, params_count);
            let lens = std::slice::from_raw_parts(params_lens, params_count);
            ptrs.iter().zip(lens.iter()).map(|(&p, &l)| {
                std::str::from_utf8_unchecked(std::slice::from_raw_parts(p, l)).to_string()
            }).collect()
        }
    } else {
        Vec::new()
    };
    let result = session_execute_params(session_id, sql, &params);
    let cresult = match result {
        PoolResult::Rows { schema, batches } => {
            match serialize_arrow_ipc(&schema, &batches) {
                Ok(data) => CArrowResult { data, error: None },
                Err(e) => CArrowResult { data: Vec::new(), error: Some(e) },
            }
        }
        PoolResult::Executed => CArrowResult { data: Vec::new(), error: None },
        PoolResult::Error(e) => CArrowResult { data: Vec::new(), error: Some(e) },
    };
    Box::into_raw(Box::new(cresult))
}

/// Destroy a session. Auto-rollback if a transaction is active.
#[no_mangle]
pub extern "C" fn trex_pool_session_destroy(session_id: u64) {
    destroy_session(session_id);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_classification() {
        assert!(is_result_returning_query("SELECT 1"));
        assert!(is_result_returning_query("  select * from t"));
        assert!(is_result_returning_query("WITH cte AS (SELECT 1) SELECT * FROM cte"));
        assert!(is_result_returning_query("SHOW TABLES"));
        assert!(is_result_returning_query("DESCRIBE t"));
        assert!(is_result_returning_query("EXPLAIN SELECT 1"));
        assert!(is_result_returning_query("FROM t"));
        assert!(is_result_returning_query("VALUES (1, 2)"));
        assert!(is_result_returning_query("PRAGMA table_info('t')"));

        assert!(!is_result_returning_query("INSERT INTO t VALUES (1)"));
        assert!(!is_result_returning_query("UPDATE t SET x = 1"));
        assert!(!is_result_returning_query("DELETE FROM t"));
        assert!(!is_result_returning_query("CREATE TABLE t (x INT)"));
        assert!(!is_result_returning_query("DROP TABLE t"));
        assert!(!is_result_returning_query("ALTER TABLE t ADD COLUMN y INT"));
        assert!(!is_result_returning_query("PRAGMA CREATE_FTS_INDEX('t', 'id', 'text')"));
    }

    #[test]
    fn test_transaction_detection() {
        assert!(is_begin("BEGIN"));
        assert!(is_begin("BEGIN TRANSACTION"));
        assert!(is_begin("  begin transaction"));
        assert!(!is_begin("SELECT 1"));
        assert!(!is_begin("INSERT INTO t VALUES (1)"));

        assert!(is_commit_or_rollback("COMMIT"));
        assert!(is_commit_or_rollback("ROLLBACK"));
        assert!(is_commit_or_rollback("  commit"));
        assert!(is_commit_or_rollback("  rollback;"));
        assert!(!is_commit_or_rollback("SELECT 1"));
        assert!(!is_commit_or_rollback("BEGIN"));
    }
}
