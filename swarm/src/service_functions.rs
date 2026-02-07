// ---------------------------------------------------------------------------
// Service visibility SQL functions
// ---------------------------------------------------------------------------

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    vscalar::{ScalarFunctionSignature, VScalar},
};
use duckdb::vtab::arrow::WritableVector;
use std::{
    ffi::CString,
    sync::atomic::{AtomicBool, Ordering},
};

use crate::gossip::GossipRegistry;

// ---------------------------------------------------------------------------
// ServiceInfo – parsed JSON from gossip `service:*` keys
// ---------------------------------------------------------------------------

/// Represents a service advertisement stored in gossip as JSON.
///
/// Expected JSON shape:
/// ```json
/// {"host":"0.0.0.0","port":50051,"status":"running","uptime":3600,"config":{}}
/// ```
#[derive(Debug, Clone)]
pub struct ServiceInfo {
    pub host: String,
    pub port: String,
    pub status: String,
    pub uptime_seconds: String,
    pub config: String,
}

/// Parse a JSON value from a gossip `service:*` key into a `ServiceInfo`.
///
/// Tolerant of missing or differently-typed fields — all fields fall back to
/// sensible defaults so that partially-formed advertisements still produce
/// rows.
pub fn parse_service_json(json: &str) -> Option<ServiceInfo> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    let obj = v.as_object()?;

    let host = obj
        .get("host")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let port = match obj.get("port") {
        Some(serde_json::Value::Number(n)) => n.to_string(),
        Some(serde_json::Value::String(s)) => s.clone(),
        _ => String::new(),
    };

    let status = obj
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    let uptime_seconds = match obj.get("uptime") {
        Some(serde_json::Value::Number(n)) => n.to_string(),
        Some(serde_json::Value::String(s)) => s.clone(),
        _ => "0".to_string(),
    };

    let config = obj
        .get("config")
        .map(|v| v.to_string())
        .unwrap_or_else(|| "{}".to_string());

    Some(ServiceInfo {
        host,
        port,
        status,
        uptime_seconds,
        config,
    })
}

// ---------------------------------------------------------------------------
// Helper: known start-service SQL mapping
// ---------------------------------------------------------------------------

/// Return the SQL statement that starts the given service extension, or `None`
/// if the extension name is not in the known mapping.
pub fn get_start_service_sql(extension: &str, host: &str, port: u16, password: &str) -> Option<String> {
    match extension {
        "flight" => Some(format!(
            "SELECT start_flight_server('{host}', {port})"
        )),
        "pgwire" => Some(format!(
            "SELECT start_pgwire_server('{host}', {port}, '{password}', '')"
        )),
        "trexas" => Some(format!(
            "SELECT start_trexas_server('{host}', {port})"
        )),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Table function: swarm_services()
// ---------------------------------------------------------------------------

pub struct SwarmServicesTable;

#[repr(C)]
pub struct SwarmServicesBindData {}

#[repr(C)]
pub struct SwarmServicesInitData {
    done: AtomicBool,
}

impl VTab for SwarmServicesTable {
    type InitData = SwarmServicesInitData;
    type BindData = SwarmServicesBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("node_name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "service_name",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column("host", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("port", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("status", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "uptime_seconds",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column("config", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(SwarmServicesBindData {})
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(SwarmServicesInitData {
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

        let nodes = match GossipRegistry::instance().get_node_key_values() {
            Ok(nodes) => nodes,
            Err(_) => {
                output.set_len(0);
                return Ok(());
            }
        };

        // Collect service rows: one row per (node, service:*) key.
        struct ServiceRow {
            node_name: String,
            service_name: String,
            host: String,
            port: String,
            status: String,
            uptime_seconds: String,
            config: String,
        }

        let mut rows: Vec<ServiceRow> = Vec::new();

        for node in &nodes {
            for (key, value) in &node.key_values {
                if let Some(service_name) = key.strip_prefix("service:") {
                    let info = parse_service_json(value);
                    let (host, port, status, uptime, config) = match info {
                        Some(si) => (
                            si.host,
                            si.port,
                            si.status,
                            si.uptime_seconds,
                            si.config,
                        ),
                        None => (
                            String::new(),
                            String::new(),
                            "unknown".to_string(),
                            "0".to_string(),
                            "{}".to_string(),
                        ),
                    };

                    rows.push(ServiceRow {
                        node_name: node.node_name.clone(),
                        service_name: service_name.to_string(),
                        host,
                        port,
                        status,
                        uptime_seconds: uptime,
                        config,
                    });
                }
            }
        }

        if rows.is_empty() {
            output.set_len(0);
            return Ok(());
        }

        let chunk_size = rows.len();
        let node_name_vec = output.flat_vector(0);
        let service_name_vec = output.flat_vector(1);
        let host_vec = output.flat_vector(2);
        let port_vec = output.flat_vector(3);
        let status_vec = output.flat_vector(4);
        let uptime_vec = output.flat_vector(5);
        let config_vec = output.flat_vector(6);

        for (i, row) in rows.iter().enumerate() {
            node_name_vec.insert(i, CString::new(row.node_name.clone())?);
            service_name_vec.insert(i, CString::new(row.service_name.clone())?);
            host_vec.insert(i, CString::new(row.host.clone())?);
            port_vec.insert(i, CString::new(row.port.clone())?);
            status_vec.insert(i, CString::new(row.status.clone())?);
            uptime_vec.insert(i, CString::new(row.uptime_seconds.clone())?);
            config_vec.insert(i, CString::new(row.config.clone())?);
        }

        output.set_len(chunk_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

// ---------------------------------------------------------------------------
// Scalar: swarm_start_service(extension, host, port)
// ---------------------------------------------------------------------------

pub struct SwarmStartServiceScalar;

impl VScalar for SwarmStartServiceScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        // Read parameters
        let ext_vector = input.flat_vector(0);
        let host_vector = input.flat_vector(1);
        let port_vector = input.flat_vector(2);

        let ext_slice =
            ext_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let host_slice =
            host_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let port_slice = port_vector.as_slice_with_len::<i32>(input.len());

        let extension = duckdb::types::DuckString::new(&mut { ext_slice[0] })
            .as_str()
            .to_string();
        let host = duckdb::types::DuckString::new(&mut { host_slice[0] })
            .as_str()
            .to_string();
        let port_raw = port_slice[0];
        if port_raw < 0 || port_raw > 65535 {
            return Err(format!("Port {} out of valid range (0-65535)", port_raw).into());
        }
        let port = port_raw as u16;

        // Read optional password (4th parameter)
        let password = if input.num_columns() > 3 {
            let pw_vector = input.flat_vector(3);
            let pw_slice =
                pw_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
            duckdb::types::DuckString::new(&mut { pw_slice[0] })
                .as_str()
                .to_string()
        } else {
            String::new()
        };

        // Look up known SQL for this service extension
        let sql = match get_start_service_sql(&extension, &host, port, &password) {
            Some(sql) => sql,
            None => {
                let msg = format!(
                    "Unknown service extension '{}'. Known: flight, pgwire, trexas",
                    extension
                );
                let flat = output.flat_vector();
                flat.insert(0, &msg);
                return Ok(());
            }
        };

        // Execute the start SQL via the shared connection
        let conn_arc = crate::get_shared_connection().ok_or("No shared connection")?;
        let conn = conn_arc
            .lock()
            .map_err(|e| format!("Lock error: {e}"))?;

        if let Err(e) = conn.execute_batch(&sql) {
            let msg = format!("Failed to start {}: {}", extension, e);
            let flat = output.flat_vector();
            flat.insert(0, &msg);
            return Ok(());
        }

        // Build the service JSON and publish to gossip
        let service_json = serde_json::json!({
            "host": host,
            "port": port,
            "status": "running",
            "uptime": 0,
            "config": {}
        })
        .to_string();

        let gossip_key = format!("service:{}", extension);
        let gossip_result = GossipRegistry::instance().set_key(&gossip_key, &service_json);

        let response = match gossip_result {
            Ok(()) => format!(
                "Service '{}' started on {}:{} and registered in gossip",
                extension, host, port
            ),
            Err(e) => format!(
                "Service '{}' started on {}:{} but gossip registration failed: {}",
                extension, host, port, e
            ),
        };

        let flat = output.flat_vector();
        flat.insert(0, &response);
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![
            ScalarFunctionSignature::exact(
                vec![
                    LogicalTypeId::Varchar.into(), // extension
                    LogicalTypeId::Varchar.into(), // host
                    LogicalTypeId::Integer.into(), // port
                ],
                LogicalTypeId::Varchar.into(),
            ),
            ScalarFunctionSignature::exact(
                vec![
                    LogicalTypeId::Varchar.into(), // extension
                    LogicalTypeId::Varchar.into(), // host
                    LogicalTypeId::Integer.into(), // port
                    LogicalTypeId::Varchar.into(), // password
                ],
                LogicalTypeId::Varchar.into(),
            ),
        ]
    }
}

// ---------------------------------------------------------------------------
// Scalar: swarm_stop_service(extension)
// ---------------------------------------------------------------------------

pub struct SwarmStopServiceScalar;

impl VScalar for SwarmStopServiceScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let ext_vector = input.flat_vector(0);
        let ext_slice =
            ext_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let extension = duckdb::types::DuckString::new(&mut { ext_slice[0] })
            .as_str()
            .to_string();

        // Mark the service as stopped in gossip.  We update the JSON to
        // reflect a "stopped" status rather than removing the key entirely,
        // so that `swarm_services()` can still show the entry until it is
        // garbage-collected by gossip TTL.
        let gossip_key = format!("service:{}", extension);

        // Try to read the existing service JSON so we can preserve host/port
        // for display.  If the key does not exist we still mark it stopped.
        let existing_json = Self::read_own_service_key(&gossip_key);

        let stopped_json = match existing_json {
            Some(info) => serde_json::json!({
                "host": info.host,
                "port": info.port,
                "status": "stopped",
                "uptime": 0,
                "config": {}
            })
            .to_string(),
            None => serde_json::json!({
                "host": "",
                "port": "",
                "status": "stopped",
                "uptime": 0,
                "config": {}
            })
            .to_string(),
        };

        let gossip_result = GossipRegistry::instance().set_key(&gossip_key, &stopped_json);

        let response = match gossip_result {
            Ok(()) => format!(
                "Service '{}' marked as stopped in gossip",
                extension
            ),
            Err(e) => format!(
                "Failed to update gossip for service '{}': {}",
                extension, e
            ),
        };

        let flat = output.flat_vector();
        flat.insert(0, &response);
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into()], // extension
            LogicalTypeId::Varchar.into(),
        )]
    }
}

impl SwarmStopServiceScalar {
    /// Attempt to read the current service info for the given gossip key from
    /// this node's own state.  Returns `None` if gossip is not running or the
    /// key does not exist.
    fn read_own_service_key(gossip_key: &str) -> Option<ServiceInfo> {
        let nodes = GossipRegistry::instance().get_node_key_values().ok()?;
        // The first entry whose node_name matches our own config, or simply
        // look through all nodes for a matching key on *our* node.  Since
        // `get_self_config` gives us our node_id, we use that to filter.
        let self_config = GossipRegistry::instance().get_self_config().ok()?;
        let self_node_id = self_config
            .iter()
            .find(|(k, _)| k == "node_id")
            .map(|(_, v)| v.clone())?;

        for node in &nodes {
            if node.node_id == self_node_id {
                for (key, value) in &node.key_values {
                    if key == gossip_key {
                        return parse_service_json(value);
                    }
                }
            }
        }
        None
    }
}

// ---------------------------------------------------------------------------
// Scalar: swarm_load(extension)
// ---------------------------------------------------------------------------

pub struct SwarmLoadScalar;

impl VScalar for SwarmLoadScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let ext_vector = input.flat_vector(0);
        let ext_slice =
            ext_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let extension = duckdb::types::DuckString::new(&mut { ext_slice[0] })
            .as_str()
            .to_string();

        let load_sql = format!("LOAD '{}.trex'", extension);

        let conn_arc = crate::get_shared_connection().ok_or("No shared connection")?;
        let conn = conn_arc
            .lock()
            .map_err(|e| format!("Lock error: {e}"))?;

        let response = match conn.execute_batch(&load_sql) {
            Ok(()) => format!("Extension '{}' loaded successfully", extension),
            Err(e) => format!("Failed to load extension '{}': {}", extension, e),
        };

        let flat = output.flat_vector();
        flat.insert(0, &response);
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into()], // extension name
            LogicalTypeId::Varchar.into(),
        )]
    }
}

// ---------------------------------------------------------------------------
// Scalar: swarm_register_service(name, host, port)
// ---------------------------------------------------------------------------

pub struct SwarmRegisterServiceScalar;

impl VScalar for SwarmRegisterServiceScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let name_vector = input.flat_vector(0);
        let host_vector = input.flat_vector(1);
        let port_vector = input.flat_vector(2);

        let name_slice =
            name_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let host_slice =
            host_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let port_slice = port_vector.as_slice_with_len::<i32>(input.len());

        let name = duckdb::types::DuckString::new(&mut { name_slice[0] })
            .as_str()
            .to_string();
        let host = duckdb::types::DuckString::new(&mut { host_slice[0] })
            .as_str()
            .to_string();
        let port_raw = port_slice[0];
        if port_raw < 0 || port_raw > 65535 {
            return Err(format!("Port {} out of valid range (0-65535)", port_raw).into());
        }
        let port = port_raw as u16;

        // Build and publish the service JSON to gossip — no actual server is
        // started.  This allows ad-hoc registration of external services.
        let service_json = serde_json::json!({
            "host": host,
            "port": port,
            "status": "running",
            "uptime": 0,
            "config": {}
        })
        .to_string();

        let gossip_key = format!("service:{}", name);
        let gossip_result = GossipRegistry::instance().set_key(&gossip_key, &service_json);

        // When a flight service is registered, also advertise local tables
        // and start the catalog refresh thread so that swarm_tables() and
        // swarm_query() work in the manual startup flow.
        if name == "flight" {
            let _ = crate::catalog::advertise_local_tables();
            let _ = crate::catalog::start_catalog_refresh();
        }

        let response = match gossip_result {
            Ok(()) => format!(
                "Service '{}' registered at {}:{} in gossip",
                name, host, port
            ),
            Err(e) => format!(
                "Failed to register service '{}': {}",
                name, e
            ),
        };

        let flat = output.flat_vector();
        flat.insert(0, &response);
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeId::Varchar.into(), // service name
                LogicalTypeId::Varchar.into(), // host
                LogicalTypeId::Integer.into(), // port
            ],
            LogicalTypeId::Varchar.into(),
        )]
    }
}
