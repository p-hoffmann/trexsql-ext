extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

mod flight_server;
mod logging;
mod server_registry;

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab, arrow::WritableVector},
    vscalar::{VScalar, ScalarFunctionSignature},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use std::{
    error::Error,
    ffi::CString,
    sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex, OnceLock as OnceCell},
};

static SHARED_CONNECTION: OnceCell<Arc<Mutex<Connection>>> = OnceCell::new();

pub fn store_shared_connection(connection: &Connection) -> Result<(), Box<dyn Error>> {
    let cloned = connection
        .try_clone()
        .map_err(|e| format!("Failed to clone connection: {}", e))?;

    SHARED_CONNECTION
        .set(Arc::new(Mutex::new(cloned)))
        .map_err(|_| "Connection already stored")?;

    Ok(())
}

pub fn get_shared_connection() -> Option<Arc<Mutex<Connection>>> {
    SHARED_CONNECTION.get().cloned()
}

fn validate_port(port_i32: i32) -> std::result::Result<u16, Box<dyn std::error::Error>> {
    if port_i32 < 1 || port_i32 > 65535 {
        return Err(format!("Port must be between 1 and 65535, got {}", port_i32).into());
    }
    Ok(port_i32 as u16)
}

struct FlightVersionScalar;

impl VScalar for FlightVersionScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() > 0 {
            let flat_vector = output.flat_vector();
            flat_vector.insert(0, "flight extension v0.1.0");
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

struct StartFlightServerScalar;

impl VScalar for StartFlightServerScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let host_vector = input.flat_vector(0);
        let port_vector = input.flat_vector(1);

        let host_slice =
            host_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let port_slice = port_vector.as_slice_with_len::<i32>(input.len());

        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let host = duckdb::types::DuckString::new(&mut { host_slice[0] })
            .as_str()
            .to_string();
        let port = validate_port(port_slice[0])?;

        let response = match flight_server::start_flight_server(host, port, false) {
            Ok(msg) => msg,
            Err(err) => format!("Error: {}", err),
        };

        let flat_vector = output.flat_vector();
        flat_vector.insert(0, &response);
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeId::Varchar.into(),
                LogicalTypeId::Integer.into(),
            ],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

struct StartFlightServerTlsScalar;

impl VScalar for StartFlightServerTlsScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let host_vector = input.flat_vector(0);
        let port_vector = input.flat_vector(1);
        let cert_vector = input.flat_vector(2);
        let key_vector = input.flat_vector(3);
        let ca_vector = input.flat_vector(4);

        let host_slice =
            host_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let port_slice = port_vector.as_slice_with_len::<i32>(input.len());
        let cert_slice =
            cert_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let key_slice =
            key_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let ca_slice =
            ca_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());

        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let host = duckdb::types::DuckString::new(&mut { host_slice[0] })
            .as_str()
            .to_string();
        let port = validate_port(port_slice[0])?;
        let cert_path = duckdb::types::DuckString::new(&mut { cert_slice[0] })
            .as_str()
            .to_string();
        let key_path = duckdb::types::DuckString::new(&mut { key_slice[0] })
            .as_str()
            .to_string();
        let ca_cert_path = duckdb::types::DuckString::new(&mut { ca_slice[0] })
            .as_str()
            .to_string();

        let response = match flight_server::start_flight_server_with_tls(
            host,
            port,
            &cert_path,
            &key_path,
            &ca_cert_path,
        ) {
            Ok(msg) => msg,
            Err(err) => format!("Error: {}", err),
        };

        let flat_vector = output.flat_vector();
        flat_vector.insert(0, &response);
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeId::Varchar.into(),
                LogicalTypeId::Integer.into(),
                LogicalTypeId::Varchar.into(),
                LogicalTypeId::Varchar.into(),
                LogicalTypeId::Varchar.into(),
            ],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

struct StopFlightServerScalar;

impl VScalar for StopFlightServerScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let host_vector = input.flat_vector(0);
        let port_vector = input.flat_vector(1);

        let host_slice =
            host_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let port_slice = port_vector.as_slice_with_len::<i32>(input.len());

        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let host = duckdb::types::DuckString::new(&mut { host_slice[0] })
            .as_str()
            .to_string();
        let port = validate_port(port_slice[0])?;

        let response = match flight_server::stop_flight_server(&host, port) {
            Ok(msg) => msg,
            Err(err) => format!("Error: {}", err),
        };

        let flat_vector = output.flat_vector();
        flat_vector.insert(0, &response);
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeId::Varchar.into(),
                LogicalTypeId::Integer.into(),
            ],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

struct FlightServerStatusTable;

#[repr(C)]
struct FlightServerStatusBindData {}

#[repr(C)]
struct FlightServerStatusInitData {
    done: AtomicBool,
}

impl VTab for FlightServerStatusTable {
    type InitData = FlightServerStatusInitData;
    type BindData = FlightServerStatusBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("hostname", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("port", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "uptime_seconds",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "tls_enabled",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        Ok(FlightServerStatusBindData {})
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(FlightServerStatusInitData {
            done: AtomicBool::new(false),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();

        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }

        let servers_info = server_registry::ServerRegistry::instance().get_servers_info();

        if servers_info.is_empty() {
            output.set_len(0);
            return Ok(());
        }

        let chunk_size = servers_info.len();
        let hostname_vector = output.flat_vector(0);
        let port_vector = output.flat_vector(1);
        let uptime_vector = output.flat_vector(2);
        let tls_vector = output.flat_vector(3);

        for (i, (hostname, port, uptime_secs, tls_enabled)) in servers_info.iter().enumerate() {
            let hostname_cstring = CString::new(hostname.clone())?;
            hostname_vector.insert(i, hostname_cstring);

            let port_cstring = CString::new(port.to_string())?;
            port_vector.insert(i, port_cstring);

            let uptime_cstring = CString::new(uptime_secs.to_string())?;
            uptime_vector.insert(i, uptime_cstring);

            let tls_cstring = CString::new(tls_enabled.to_string())?;
            tls_vector.insert(i, tls_cstring);
        }

        output.set_len(chunk_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    store_shared_connection(&con)?;

    con.register_scalar_function::<FlightVersionScalar>("flight_version")
        .expect("Failed to register flight_version scalar function");

    con.register_scalar_function::<StartFlightServerScalar>("start_flight_server")
        .expect("Failed to register start_flight_server function");

    con.register_scalar_function::<StartFlightServerTlsScalar>("start_flight_server_tls")
        .expect("Failed to register start_flight_server_tls function");

    con.register_scalar_function::<StopFlightServerScalar>("stop_flight_server")
        .expect("Failed to register stop_flight_server function");

    con.register_table_function::<FlightServerStatusTable>("flight_server_status")
        .expect("Failed to register flight_server_status function");

    Ok(())
}
