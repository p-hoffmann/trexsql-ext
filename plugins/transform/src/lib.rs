extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

mod compile;
mod dag;
mod freshness;
mod parser;
mod plan;
mod project;
mod run;
mod seed;
mod state;
mod test;

use duckdb::Connection;
use libduckdb_sys as ffi;
use std::cell::Cell;
use std::error::Error;

// ── Database access via shared trex_pool (session-based) ─────────────────────

thread_local! {
    /// When set, `execute_sql` and `query_sql` route through this session.
    static ACTIVE_SESSION: Cell<Option<u64>> = const { Cell::new(None) };
}

/// Set the thread-local active session. Returns the previous value.
pub fn set_active_session(session_id: Option<u64>) -> Option<u64> {
    ACTIVE_SESSION.with(|c| c.replace(session_id))
}

fn active_session() -> Option<u64> {
    ACTIVE_SESSION.with(|c| c.get())
}

pub fn execute_sql(sql: &str) -> Result<(), Box<dyn Error>> {
    if let Some(sid) = active_session() {
        trex_pool_client::session_execute(sid, sql)
            .map(|_| ())
            .map_err(|e| -> Box<dyn Error> { e.into() })
    } else {
        let sid = trex_pool_client::create_session()
            .map_err(|e| -> Box<dyn Error> { e.into() })?;
        let result = trex_pool_client::session_execute(sid, sql).map(|_| ());
        let _ = trex_pool_client::destroy_session(sid);
        result.map_err(|e| -> Box<dyn Error> { e.into() })
    }
}

pub struct QueryRow {
    pub columns: Vec<String>,
}

pub fn query_sql(sql: &str) -> Result<Vec<QueryRow>, Box<dyn Error>> {
    let (_schema, batches) = if let Some(sid) = active_session() {
        trex_pool_client::session_execute(sid, sql)
            .map_err(|e| -> Box<dyn Error> { e.into() })?
    } else {
        let sid = trex_pool_client::create_session()
            .map_err(|e| -> Box<dyn Error> { e.into() })?;
        let result = trex_pool_client::session_execute(sid, sql);
        let _ = trex_pool_client::destroy_session(sid);
        result.map_err(|e| -> Box<dyn Error> { e.into() })?
    };

    let mut rows = Vec::new();
    for batch in &batches {
        for r in 0..batch.num_rows() {
            let mut columns = Vec::new();
            for c in 0..batch.num_columns() {
                let col = batch.column(c);
                let val = if col.is_null(r) {
                    String::new()
                } else {
                    arrow_value_to_string(col.as_ref(), r)
                };
                columns.push(val);
            }
            rows.push(QueryRow { columns });
        }
    }
    Ok(rows)
}

fn arrow_value_to_string(array: &dyn trex_pool_client::arrow_array::Array, row: usize) -> String {
    use trex_pool_client::arrow_array::*;
    use trex_pool_client::arrow_schema::DataType;

    match array.data_type() {
        DataType::Utf8 => array.as_any().downcast_ref::<StringArray>().unwrap().value(row).to_string(),
        DataType::LargeUtf8 => array.as_any().downcast_ref::<LargeStringArray>().unwrap().value(row).to_string(),
        DataType::Int32 => array.as_any().downcast_ref::<Int32Array>().unwrap().value(row).to_string(),
        DataType::Int64 => array.as_any().downcast_ref::<Int64Array>().unwrap().value(row).to_string(),
        DataType::UInt64 => array.as_any().downcast_ref::<UInt64Array>().unwrap().value(row).to_string(),
        DataType::Boolean => array.as_any().downcast_ref::<BooleanArray>().unwrap().value(row).to_string(),
        _ => format!("{:?}", array.data_type()),
    }
}

pub fn escape_sql_ident(s: &str) -> String {
    s.replace('"', "\"\"")
}

pub fn escape_sql_str(s: &str) -> String {
    s.replace('\'', "''")
}

// ── Extension Entrypoint ─────────────────────────────────────────────────────

unsafe fn extension_entrypoint(connection: Connection) -> Result<(), Box<dyn Error>> {
    connection.register_table_function::<compile::CompileVTab>("trex_transform_compile")?;
    connection.register_table_function::<plan::PlanVTab>("trex_transform_plan")?;
    connection.register_table_function::<run::RunVTab>("trex_transform_run")?;
    connection.register_table_function::<seed::SeedVTab>("trex_transform_seed")?;
    connection.register_table_function::<test::TestVTab>("trex_transform_test")?;
    connection.register_table_function::<freshness::FreshnessVTab>("trex_transform_freshness")?;
    Ok(())
}

unsafe fn transform_init_c_api_internal(
    info: ffi::duckdb_extension_info,
    access: *const ffi::duckdb_extension_access,
) -> Result<bool, Box<dyn Error>> {
    let have_api_struct = ffi::duckdb_rs_extension_api_init(info, access, "v1.3.2").unwrap();

    if !have_api_struct {
        return Ok(false);
    }

    let db: ffi::duckdb_database = *(*access).get_database.unwrap()(info);

    // Pool already initialized by db plugin
    let connection = Connection::open_from_raw(db.cast())?;
    extension_entrypoint(connection)?;

    Ok(true)
}

#[no_mangle]
pub unsafe extern "C" fn transform_init_c_api(
    info: ffi::duckdb_extension_info,
    access: *const ffi::duckdb_extension_access,
) -> bool {
    let init_result = transform_init_c_api_internal(info, access);

    if let Err(x) = init_result {
        let error_c_string = std::ffi::CString::new(x.to_string());
        match error_c_string {
            Ok(e) => {
                (*access).set_error.unwrap()(info, e.as_ptr());
            }
            Err(_) => {
                let error_msg =
                    c"An error occurred but the extension failed to allocate an error string";
                (*access).set_error.unwrap()(info, error_msg.as_ptr());
            }
        }
        return false;
    }

    init_result.unwrap()
}
