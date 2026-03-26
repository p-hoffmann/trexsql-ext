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
use std::error::Error;

// ── Database access via shared trex_pool ─────────────────────────────────────

pub fn execute_sql(sql: &str) -> Result<(), Box<dyn Error>> {
    trex_pool_client::write(sql).map_err(|e| -> Box<dyn Error> { e.into() })
}

pub struct QueryRow {
    pub columns: Vec<String>,
}

pub fn query_sql(sql: &str) -> Result<Vec<QueryRow>, Box<dyn Error>> {
    let json_str = trex_pool_client::read(sql).map_err(|e| -> Box<dyn Error> { e.into() })?;
    let parsed: Vec<serde_json::Value> =
        serde_json::from_str(&json_str).map_err(|e| -> Box<dyn Error> { e.into() })?;

    let mut rows = Vec::new();
    for obj in parsed {
        if let serde_json::Value::Object(map) = obj {
            let columns: Vec<String> = map
                .values()
                .map(|v| match v {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Null => String::new(),
                    other => other.to_string(),
                })
                .collect();
            rows.push(QueryRow { columns });
        }
    }
    Ok(rows)
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
