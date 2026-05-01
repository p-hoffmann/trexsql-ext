//! Safe Rust client for the trex_pool DuckDB extension.
//!
//! This is an rlib (statically linked into each consumer cdylib) that
//! discovers the pool's C ABI functions via `dlsym(RTLD_DEFAULT, ...)`
//! at first use. Supports both JSON and Arrow IPC result formats.

pub use arrow_array;
pub use arrow_ipc;
pub use arrow_schema;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use std::sync::{Arc, OnceLock};

type FnRead = unsafe extern "C" fn(*const u8, usize) -> *mut Opaque;
type FnWrite = unsafe extern "C" fn(*const u8, usize, *mut *const u8, *mut usize) -> i32;
type FnExecute = unsafe extern "C" fn(*const u8, usize) -> *mut Opaque;
type FnResultIsError = unsafe extern "C" fn(*const Opaque) -> i32;
type FnResultJson = unsafe extern "C" fn(*const Opaque, *mut *const u8, *mut usize);
type FnResultError = unsafe extern "C" fn(*const Opaque, *mut *const u8, *mut usize);
type FnResultFree = unsafe extern "C" fn(*mut Opaque);
type FnIsReadQuery = unsafe extern "C" fn(*const u8, usize) -> i32;
type FnSyncAll = unsafe extern "C" fn() -> i32;
type FnPoolSize = unsafe extern "C" fn() -> usize;
type FnWithConnExec = unsafe extern "C" fn(*const u8, usize) -> *mut Opaque;
type FnExecTransaction = unsafe extern "C" fn(*const *const u8, *const usize, usize) -> *mut Opaque;

type FnSessionCreate = unsafe extern "C" fn() -> u64;
type FnSessionExecuteArrow = unsafe extern "C" fn(u64, *const u8, usize) -> *mut Opaque;
type FnSessionExecuteParamsArrow = unsafe extern "C" fn(u64, *const u8, usize, *const *const u8, *const usize, usize) -> *mut Opaque;
type FnSessionDestroy = unsafe extern "C" fn(u64);

type FnReadArrowIpc = unsafe extern "C" fn(*const u8, usize) -> *mut Opaque;
type FnExecArrowIpc = unsafe extern "C" fn(*const u8, usize) -> *mut Opaque;
type FnArrowIsError = unsafe extern "C" fn(*const Opaque) -> i32;
type FnArrowData = unsafe extern "C" fn(*const Opaque, *mut *const u8, *mut usize) -> i32;
type FnArrowError = unsafe extern "C" fn(*const Opaque, *mut *const u8, *mut usize);
type FnArrowFree = unsafe extern "C" fn(*mut Opaque);

/// Opaque pointer for C ABI result handles.
#[repr(C)]
pub struct Opaque {
    _opaque: [u8; 0],
}

struct PoolFns {
    read: FnRead,
    write: FnWrite,
    execute: FnExecute,
    result_is_error: FnResultIsError,
    result_json: FnResultJson,
    result_error: FnResultError,
    result_free: FnResultFree,
    is_read_query: FnIsReadQuery,
    sync_all: FnSyncAll,
    pool_size: FnPoolSize,
    with_conn_exec: FnWithConnExec,
    exec_transaction: FnExecTransaction,
    // Sessions
    session_create: FnSessionCreate,
    session_create_persistent: FnSessionCreate,
    session_execute_arrow: FnSessionExecuteArrow,
    session_execute_params_arrow: FnSessionExecuteParamsArrow,
    session_destroy: FnSessionDestroy,
    // Arrow IPC
    read_arrow_ipc: FnReadArrowIpc,
    exec_arrow_ipc: FnExecArrowIpc,
    arrow_is_error: FnArrowIsError,
    arrow_data: FnArrowData,
    arrow_error: FnArrowError,
    arrow_free: FnArrowFree,
}

static POOL_FNS: OnceLock<Option<PoolFns>> = OnceLock::new();

fn get_fns() -> Result<&'static PoolFns, String> {
    let fns = POOL_FNS.get_or_init(|| unsafe { discover_pool_fns() });
    fns.as_ref()
        .ok_or_else(|| "trex_pool extension not loaded".to_string())
}

unsafe fn discover_pool_fns() -> Option<PoolFns> {
    // DuckDB loads extensions with RTLD_LOCAL, so symbols aren't visible via
    // RTLD_DEFAULT. We need to find the pool.trex handle and promote it to
    // RTLD_GLOBAL, or search for symbols in all loaded libraries.
    //
    // Strategy: try RTLD_DEFAULT first (works if pool was loaded with RTLD_GLOBAL).
    // If that fails, try to dlopen the pool library with RTLD_NOLOAD to get its
    // handle, then look up symbols from that handle.
    let handle = {
        let test = libc::dlsym(
            libc::RTLD_DEFAULT,
            b"trex_pool_read\0".as_ptr() as *const _,
        );
        if !test.is_null() {
            libc::RTLD_DEFAULT
        } else {
            // DuckDB loads extensions with RTLD_LOCAL using the full path,
            // so we can't find them by short name. Scan /proc/self/maps to
            // find the loaded pool library, then dlopen it with RTLD_NOLOAD.
            let mut found = std::ptr::null_mut();
            if let Ok(maps) = std::fs::read_to_string("/proc/self/maps") {
                for line in maps.lines() {
                    // Look for any loaded library containing "pool" in its path
                    // that ends with .trex or .so
                    if let Some(path_start) = line.find('/') {
                        let path = &line[path_start..];
                        let basename = path.rsplit('/').next().unwrap_or("");
                        if (basename.starts_with("pool.") || basename.starts_with("libpool."))
                            && (basename.ends_with(".trex") || basename.ends_with(".so"))
                        {
                            let c_path = std::ffi::CString::new(path).ok();
                            if let Some(ref cp) = c_path {
                                let h = libc::dlopen(
                                    cp.as_ptr(),
                                    libc::RTLD_NOLOAD | libc::RTLD_NOW,
                                );
                                if !h.is_null() {
                                    found = h;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            if found.is_null() {
                return None;
            }
            found
        }
    };

    macro_rules! sym {
        ($name:expr) => {{
            let name = concat!($name, "\0");
            let ptr = libc::dlsym(handle, name.as_ptr() as *const _);
            if ptr.is_null() {
                return None;
            }
            std::mem::transmute(ptr)
        }};
    }

    Some(PoolFns {
        read: sym!("trex_pool_read"),
        write: sym!("trex_pool_write"),
        execute: sym!("trex_pool_execute"),
        result_is_error: sym!("trex_pool_result_is_error"),
        result_json: sym!("trex_pool_result_json"),
        result_error: sym!("trex_pool_result_error"),
        result_free: sym!("trex_pool_result_free"),
        is_read_query: sym!("trex_pool_is_read_query"),
        sync_all: sym!("trex_pool_sync_all"),
        pool_size: sym!("trex_pool_read_pool_size"),
        with_conn_exec: sym!("trex_pool_with_connection_execute"),
        exec_transaction: sym!("trex_pool_execute_transaction"),
        session_create: sym!("trex_pool_session_create"),
        session_create_persistent: sym!("trex_pool_session_create_persistent"),
        session_execute_arrow: sym!("trex_pool_session_execute_arrow"),
        session_execute_params_arrow: sym!("trex_pool_session_execute_params_arrow"),
        session_destroy: sym!("trex_pool_session_destroy"),
        read_arrow_ipc: sym!("trex_pool_read_arrow_ipc"),
        exec_arrow_ipc: sym!("trex_pool_execute_arrow_ipc"),
        arrow_is_error: sym!("trex_pool_arrow_result_is_error"),
        arrow_data: sym!("trex_pool_arrow_result_data"),
        arrow_error: sym!("trex_pool_arrow_result_error"),
        arrow_free: sym!("trex_pool_arrow_result_free"),
    })
}


fn json_result_to_string(fns: &PoolFns, result: *mut Opaque) -> Result<String, String> {
    if result.is_null() {
        return Err("null result from pool".to_string());
    }
    unsafe {
        if (fns.result_is_error)(result) != 0 {
            let err = read_error_str(|p, l| (fns.result_error)(result, p, l));
            (fns.result_free)(result);
            Err(err)
        } else {
            let mut ptr: *const u8 = std::ptr::null();
            let mut len: usize = 0;
            (fns.result_json)(result, &mut ptr, &mut len);
            let json = if !ptr.is_null() && len > 0 {
                std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len)).to_string()
            } else {
                "[]".to_string()
            };
            (fns.result_free)(result);
            Ok(json)
        }
    }
}

fn arrow_result_to_batches(
    fns: &PoolFns,
    result: *mut Opaque,
) -> Result<(Arc<Schema>, Vec<RecordBatch>), String> {
    if result.is_null() {
        return Err("null result from pool".to_string());
    }
    unsafe {
        if (fns.arrow_is_error)(result) != 0 {
            let err = read_error_str(|p, l| (fns.arrow_error)(result, p, l));
            (fns.arrow_free)(result);
            return Err(err);
        }

        let mut ptr: *const u8 = std::ptr::null();
        let mut len: usize = 0;
        let rc = (fns.arrow_data)(result, &mut ptr, &mut len);

        if rc != 0 || ptr.is_null() || len == 0 {
            // No data (write result)
            (fns.arrow_free)(result);
            return Ok((Arc::new(Schema::empty()), vec![]));
        }

        let ipc_bytes = std::slice::from_raw_parts(ptr, len).to_vec();
        (fns.arrow_free)(result);

        deserialize_arrow_ipc(&ipc_bytes)
    }
}

unsafe fn read_error_str(
    f: impl FnOnce(*mut *const u8, *mut usize),
) -> String {
    let mut ptr: *const u8 = std::ptr::null();
    let mut len: usize = 0;
    f(&mut ptr, &mut len);
    if !ptr.is_null() && len > 0 {
        std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len)).to_string()
    } else {
        "unknown pool error".to_string()
    }
}

fn deserialize_arrow_ipc(
    data: &[u8],
) -> Result<(Arc<Schema>, Vec<RecordBatch>), String> {
    use arrow_ipc::reader::StreamReader;
    use std::io::Cursor;

    let cursor = Cursor::new(data);
    let reader =
        StreamReader::try_new(cursor, None).map_err(|e| format!("ipc reader init: {e}"))?;
    let schema = reader.schema();
    let batches: Result<Vec<_>, _> = reader.collect();
    let batches = batches.map_err(|e| format!("ipc read batch: {e}"))?;
    Ok((schema, batches))
}


/// Execute a read query and return JSON result.
pub fn read(sql: &str) -> Result<String, String> {
    let fns = get_fns()?;
    let result = unsafe { (fns.read)(sql.as_ptr(), sql.len()) };
    json_result_to_string(fns, result)
}

/// Execute a write query through the serialized write queue.
pub fn write(sql: &str) -> Result<(), String> {
    let fns = get_fns()?;
    let mut err_ptr: *const u8 = std::ptr::null();
    let mut err_len: usize = 0;
    let rc = unsafe { (fns.write)(sql.as_ptr(), sql.len(), &mut err_ptr, &mut err_len) };
    if rc == 0 {
        Ok(())
    } else {
        let msg = if !err_ptr.is_null() && err_len > 0 {
            unsafe {
                std::str::from_utf8_unchecked(std::slice::from_raw_parts(err_ptr, err_len))
            }
            .to_string()
        } else {
            "write failed".to_string()
        };
        Err(msg)
    }
}

/// Auto-classify SQL as read or write and execute accordingly (JSON result).
pub fn execute(sql: &str) -> Result<String, String> {
    let fns = get_fns()?;
    let result = unsafe { (fns.execute)(sql.as_ptr(), sql.len()) };
    json_result_to_string(fns, result)
}


/// Execute a read query and return Arrow RecordBatches (via IPC).
pub fn read_arrow(
    sql: &str,
) -> Result<(Arc<Schema>, Vec<RecordBatch>), String> {
    let fns = get_fns()?;
    let result = unsafe { (fns.read_arrow_ipc)(sql.as_ptr(), sql.len()) };
    arrow_result_to_batches(fns, result)
}

/// Auto-classify SQL and execute, returning Arrow RecordBatches for reads.
pub fn execute_arrow(
    sql: &str,
) -> Result<ArrowExecuteResult, String> {
    let fns = get_fns()?;
    let result = unsafe { (fns.exec_arrow_ipc)(sql.as_ptr(), sql.len()) };

    if result.is_null() {
        return Err("null result from pool".to_string());
    }

    unsafe {
        if (fns.arrow_is_error)(result) != 0 {
            let err = read_error_str(|p, l| (fns.arrow_error)(result, p, l));
            (fns.arrow_free)(result);
            return Err(err);
        }

        let mut ptr: *const u8 = std::ptr::null();
        let mut len: usize = 0;
        let rc = (fns.arrow_data)(result, &mut ptr, &mut len);

        if rc != 0 || ptr.is_null() || len == 0 {
            // Write result (no data)
            (fns.arrow_free)(result);
            return Ok(ArrowExecuteResult::Executed);
        }

        let ipc_bytes = std::slice::from_raw_parts(ptr, len).to_vec();
        (fns.arrow_free)(result);

        let (schema, batches) = deserialize_arrow_ipc(&ipc_bytes)?;
        Ok(ArrowExecuteResult::Rows { schema, batches })
    }
}

/// Result of an auto-classified Arrow query.
pub enum ArrowExecuteResult {
    Rows {
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
    },
    Executed,
}


/// Check if SQL is a result-returning query.
pub fn is_result_returning_query(sql: &str) -> bool {
    if let Ok(fns) = get_fns() {
        unsafe { (fns.is_read_query)(sql.as_ptr(), sql.len()) == 1 }
    } else {
        let upper = sql.trim().to_uppercase();
        upper.starts_with("SELECT")
            || upper.starts_with("WITH")
            || upper.starts_with("SHOW")
    }
}

/// Force catalog sync across all read workers.
pub fn sync_all() -> Result<(), String> {
    let fns = get_fns()?;
    let rc = unsafe { (fns.sync_all)() };
    if rc == 0 {
        Ok(())
    } else {
        Err("sync_all failed".to_string())
    }
}

/// Get the read pool size.
pub fn read_pool_size() -> Result<usize, String> {
    let fns = get_fns()?;
    let size = unsafe { (fns.pool_size)() };
    if size == 0 {
        Err("pool not initialized".to_string())
    } else {
        Ok(size)
    }
}

/// Execute SQL on a direct connection (for transactional use).
pub fn with_connection_execute(sql: &str) -> Result<(), String> {
    let fns = get_fns()?;
    let result = unsafe { (fns.with_conn_exec)(sql.as_ptr(), sql.len()) };
    json_result_to_string(fns, result).map(|_| ())
}

/// Execute multiple SQL statements in a single transaction on one connection.
/// All statements run within BEGIN/COMMIT. On error, ROLLBACK is issued.
pub fn execute_transaction(sqls: &[&str]) -> Result<(), String> {
    let fns = get_fns()?;
    let ptrs: Vec<*const u8> = sqls.iter().map(|s| s.as_ptr()).collect();
    let lens: Vec<usize> = sqls.iter().map(|s| s.len()).collect();
    let result = unsafe {
        (fns.exec_transaction)(ptrs.as_ptr(), lens.as_ptr(), sqls.len())
    };
    json_result_to_string(fns, result).map(|_| ())
}


pub fn create_session() -> Result<u64, String> {
    let fns = get_fns()?;
    let id = unsafe { (fns.session_create)() };
    Ok(id)
}

/// Lazy-pins a direct conn on the first stateful query; held until
/// [`destroy_session`]. Use only when later queries depend on
/// connection-local state.
pub fn create_persistent_session() -> Result<u64, String> {
    let fns = get_fns()?;
    let id = unsafe { (fns.session_create_persistent)() };
    Ok(id)
}

/// Execute SQL within a session, returning Arrow RecordBatches.
/// Transactions are detected automatically — `BEGIN` pins the session to a
/// dedicated connection; `COMMIT`/`ROLLBACK` releases it.
pub fn session_execute(
    session_id: u64,
    sql: &str,
) -> Result<(Arc<Schema>, Vec<RecordBatch>), String> {
    let fns = get_fns()?;
    let result = unsafe {
        (fns.session_execute_arrow)(session_id, sql.as_ptr(), sql.len())
    };
    arrow_result_to_batches(fns, result)
}

/// Execute parameterized SQL within a session, returning Arrow RecordBatches.
pub fn session_execute_params(
    session_id: u64,
    sql: &str,
    params: &[String],
) -> Result<(Arc<Schema>, Vec<RecordBatch>), String> {
    let fns = get_fns()?;
    let ptrs: Vec<*const u8> = params.iter().map(|s| s.as_ptr()).collect();
    let lens: Vec<usize> = params.iter().map(|s| s.len()).collect();
    let result = unsafe {
        (fns.session_execute_params_arrow)(
            session_id,
            sql.as_ptr(), sql.len(),
            ptrs.as_ptr(), lens.as_ptr(), params.len(),
        )
    };
    arrow_result_to_batches(fns, result)
}

/// Destroy a session. If a transaction is still active, it is rolled back.
pub fn destroy_session(session_id: u64) -> Result<(), String> {
    let fns = get_fns()?;
    unsafe { (fns.session_destroy)(session_id) };
    Ok(())
}
