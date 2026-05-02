//! Safe Rust client for the trex_pool DuckDB extension.
//!
//! This is an rlib (statically linked into each consumer cdylib) that
//! discovers the pool's C ABI functions via `dlsym(RTLD_DEFAULT, ...)`
//! at first use. The pool exposes a session-leasing facility — sessions
//! own a Connection from creation until destroy.

pub use arrow_array;
pub use arrow_ipc;
pub use arrow_schema;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use std::sync::{Arc, OnceLock};

type FnSessionCreate = unsafe extern "C" fn() -> u64;
type FnSessionExecuteArrow = unsafe extern "C" fn(u64, *const u8, usize) -> *mut Opaque;
type FnSessionExecuteParamsArrow = unsafe extern "C" fn(u64, *const u8, usize, *const *const u8, *const usize, usize) -> *mut Opaque;
type FnSessionDestroy = unsafe extern "C" fn(u64);

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
    session_create: FnSessionCreate,
    session_execute_arrow: FnSessionExecuteArrow,
    session_execute_params_arrow: FnSessionExecuteParamsArrow,
    session_destroy: FnSessionDestroy,
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
    // If that fails, scan /proc/self/maps for the loaded pool library and dlopen
    // it with RTLD_NOLOAD to get its handle, then look up symbols from that
    // handle.
    let handle = {
        let test = libc::dlsym(
            libc::RTLD_DEFAULT,
            b"trex_pool_session_create\0".as_ptr() as *const _,
        );
        if !test.is_null() {
            libc::RTLD_DEFAULT
        } else {
            let mut found = std::ptr::null_mut();
            if let Ok(maps) = std::fs::read_to_string("/proc/self/maps") {
                for line in maps.lines() {
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
        session_create: sym!("trex_pool_session_create"),
        session_execute_arrow: sym!("trex_pool_session_execute_arrow"),
        session_execute_params_arrow: sym!("trex_pool_session_execute_params_arrow"),
        session_destroy: sym!("trex_pool_session_destroy"),
        arrow_is_error: sym!("trex_pool_arrow_result_is_error"),
        arrow_data: sym!("trex_pool_arrow_result_data"),
        arrow_error: sym!("trex_pool_arrow_result_error"),
        arrow_free: sym!("trex_pool_arrow_result_free"),
    })
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


/// Lease a Connection from the pool and create a session bound to it.
/// Blocks if the pool is exhausted.
pub fn create_session() -> Result<u64, String> {
    let fns = get_fns()?;
    let id = unsafe { (fns.session_create)() };
    if id == 0 {
        Err("create_session failed".to_string())
    } else {
        Ok(id)
    }
}

/// Execute SQL within a session, returning Arrow RecordBatches.
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

/// Destroy a session: cleanup its Connection and return it to the pool.
pub fn destroy_session(session_id: u64) -> Result<(), String> {
    let fns = get_fns()?;
    unsafe { (fns.session_destroy)(session_id) };
    Ok(())
}
