extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

pub mod logging;
pub mod config;
pub mod gossip;
pub mod catalog;
pub mod flight_client;
pub mod aggregation;
pub mod coordinator;
pub mod orchestrator;
pub mod service_functions;

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

use gossip::GossipRegistry;

// ---------------------------------------------------------------------------
// Shared DuckDB connection
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Scalar: swarm_start(host, port, cluster_id)
// Scalar: swarm_start(host, port, cluster_id, seeds)
// ---------------------------------------------------------------------------

struct SwarmStartScalar;

impl VScalar for SwarmStartScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let host_vector = input.flat_vector(0);
        let port_vector = input.flat_vector(1);
        let cluster_vector = input.flat_vector(2);

        let host_slice =
            host_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let port_slice = port_vector.as_slice_with_len::<i32>(input.len());
        let cluster_slice =
            cluster_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());

        let host = duckdb::types::DuckString::new(&mut { host_slice[0] })
            .as_str()
            .to_string();
        let port_raw = port_slice[0];
        if port_raw < 0 || port_raw > 65535 {
            return Err(format!("Port {} out of valid range (0-65535)", port_raw).into());
        }
        let port = port_raw as u16;
        let cluster_id = duckdb::types::DuckString::new(&mut { cluster_slice[0] })
            .as_str()
            .to_string();

        // Default node name from host:port
        let node_name = format!("node-{}:{}", host, port);

        let response = match GossipRegistry::instance().start(
            &host,
            port,
            &cluster_id,
            &node_name,
            "true",
            vec![],
        ) {
            Ok(_node_id) => format!(
                "Swarm started on {}:{} (cluster: {})",
                host, port, cluster_id
            ),
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
            ],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

// ---------------------------------------------------------------------------
// Scalar: swarm_start_with_seeds(host, port, cluster_id, seeds)
// ---------------------------------------------------------------------------

struct SwarmStartWithSeedsScalar;

impl VScalar for SwarmStartWithSeedsScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let host_vector = input.flat_vector(0);
        let port_vector = input.flat_vector(1);
        let cluster_vector = input.flat_vector(2);
        let seeds_vector = input.flat_vector(3);

        let host_slice =
            host_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let port_slice = port_vector.as_slice_with_len::<i32>(input.len());
        let cluster_slice =
            cluster_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let seeds_slice =
            seeds_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());

        let host = duckdb::types::DuckString::new(&mut { host_slice[0] })
            .as_str()
            .to_string();
        let port_raw = port_slice[0];
        if port_raw < 0 || port_raw > 65535 {
            return Err(format!("Port {} out of valid range (0-65535)", port_raw).into());
        }
        let port = port_raw as u16;
        let cluster_id = duckdb::types::DuckString::new(&mut { cluster_slice[0] })
            .as_str()
            .to_string();
        let seeds_str = duckdb::types::DuckString::new(&mut { seeds_slice[0] })
            .as_str()
            .to_string();

        let seeds: Vec<String> = seeds_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let seed_count = seeds.len();
        let node_name = format!("node-{}:{}", host, port);

        let response = match GossipRegistry::instance().start(
            &host,
            port,
            &cluster_id,
            &node_name,
            "true",
            seeds,
        ) {
            Ok(_node_id) => format!(
                "Swarm started on {}:{} (cluster: {}, seeds: {})",
                host, port, cluster_id, seed_count
            ),
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
            ],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

// ---------------------------------------------------------------------------
// Scalar: swarm_stop()
// ---------------------------------------------------------------------------

struct SwarmStopScalar;

impl VScalar for SwarmStopScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let response = match GossipRegistry::instance().stop() {
            Ok(_) => "Swarm stopped".to_string(),
            Err(err) => format!("Error: {}", err),
        };

        let flat_vector = output.flat_vector();
        flat_vector.insert(0, &response);
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

// ---------------------------------------------------------------------------
// Scalar: swarm_set(key, value)
// ---------------------------------------------------------------------------

struct SwarmSetScalar;

impl VScalar for SwarmSetScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let key_vector = input.flat_vector(0);
        let value_vector = input.flat_vector(1);

        let key_slice =
            key_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let value_slice =
            value_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());

        let key = duckdb::types::DuckString::new(&mut { key_slice[0] })
            .as_str()
            .to_string();
        let value = duckdb::types::DuckString::new(&mut { value_slice[0] })
            .as_str()
            .to_string();

        let response = match GossipRegistry::instance().set_key(&key, &value) {
            Ok(()) => {
                if key == "data_node" {
                    if value == "true" {
                        // Becoming a data node: start catalog advertising
                        let _ = catalog::advertise_local_tables();
                        let _ = catalog::start_catalog_refresh();
                    } else if value == "false" {
                        // Leaving data role: remove catalog keys and stop refresh
                        catalog::stop_catalog_refresh();
                        let _ = catalog::remove_catalog_keys();
                    }
                }
                format!("Set {} = {} (propagating to cluster)", key, value)
            }
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
                LogicalTypeId::Varchar.into(),
            ],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

// ---------------------------------------------------------------------------
// Table function: swarm_nodes()
// ---------------------------------------------------------------------------

struct SwarmNodesTable;

#[repr(C)]
struct SwarmNodesBindData {}

#[repr(C)]
struct SwarmNodesInitData {
    done: AtomicBool,
}

impl VTab for SwarmNodesTable {
    type InitData = SwarmNodesInitData;
    type BindData = SwarmNodesBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("node_id", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("node_name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "gossip_addr",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column("data_node", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("status", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(SwarmNodesBindData {})
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(SwarmNodesInitData {
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

        let nodes = match GossipRegistry::instance().get_node_states() {
            Ok(nodes) => nodes,
            Err(_) => {
                output.set_len(0);
                return Ok(());
            }
        };

        if nodes.is_empty() {
            output.set_len(0);
            return Ok(());
        }

        let chunk_size = nodes.len();
        let node_id_vec = output.flat_vector(0);
        let node_name_vec = output.flat_vector(1);
        let gossip_addr_vec = output.flat_vector(2);
        let data_node_vec = output.flat_vector(3);
        let status_vec = output.flat_vector(4);

        for (i, node) in nodes.iter().enumerate() {
            node_id_vec.insert(i, CString::new(node.node_id.clone())?);
            node_name_vec.insert(i, CString::new(node.node_name.clone())?);
            gossip_addr_vec.insert(i, CString::new(node.gossip_addr.clone())?);
            data_node_vec.insert(i, CString::new(node.data_node.clone())?);
            status_vec.insert(i, CString::new(node.status.clone())?);
        }

        output.set_len(chunk_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

// ---------------------------------------------------------------------------
// Table function: swarm_config()
// ---------------------------------------------------------------------------

struct SwarmConfigTable;

#[repr(C)]
struct SwarmConfigBindData {}

#[repr(C)]
struct SwarmConfigInitData {
    done: AtomicBool,
}

impl VTab for SwarmConfigTable {
    type InitData = SwarmConfigInitData;
    type BindData = SwarmConfigBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("key", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("value", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(SwarmConfigBindData {})
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(SwarmConfigInitData {
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

        let config = match GossipRegistry::instance().get_self_config() {
            Ok(config) => config,
            Err(_) => {
                output.set_len(0);
                return Ok(());
            }
        };

        if config.is_empty() {
            output.set_len(0);
            return Ok(());
        }

        let chunk_size = config.len();
        let key_vec = output.flat_vector(0);
        let value_vec = output.flat_vector(1);

        for (i, (key, value)) in config.iter().enumerate() {
            key_vec.insert(i, CString::new(key.clone())?);
            value_vec.insert(i, CString::new(value.clone())?);
        }

        output.set_len(chunk_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

// ---------------------------------------------------------------------------
// Table function: swarm_tables()
// ---------------------------------------------------------------------------

struct SwarmTablesTable;

#[repr(C)]
struct SwarmTablesBindData {}

#[repr(C)]
struct SwarmTablesInitData {
    done: AtomicBool,
}

impl VTab for SwarmTablesTable {
    type InitData = SwarmTablesInitData;
    type BindData = SwarmTablesBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("node_name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "table_name",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "approx_rows",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "schema_hash",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        Ok(SwarmTablesBindData {})
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(SwarmTablesInitData {
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

        let entries = match catalog::get_all_tables() {
            Ok(entries) => entries,
            Err(_) => {
                output.set_len(0);
                return Ok(());
            }
        };

        if entries.is_empty() {
            output.set_len(0);
            return Ok(());
        }

        let chunk_size = entries.len();
        let node_name_vec = output.flat_vector(0);
        let table_name_vec = output.flat_vector(1);
        let approx_rows_vec = output.flat_vector(2);
        let schema_hash_vec = output.flat_vector(3);

        for (i, entry) in entries.iter().enumerate() {
            node_name_vec.insert(i, CString::new(entry.node_name.clone())?);
            table_name_vec.insert(i, CString::new(entry.table_name.clone())?);
            approx_rows_vec.insert(i, CString::new(entry.approx_rows.to_string())?);
            schema_hash_vec.insert(
                i,
                CString::new(format!("0x{:X}", entry.schema_hash))?,
            );
        }

        output.set_len(chunk_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

// ---------------------------------------------------------------------------
// Table function: swarm_query(sql)
// ---------------------------------------------------------------------------

struct SwarmQueryTable;

#[repr(C)]
struct SwarmQueryBindData {
    sql: String,
    partial_results: bool,
    cached_result: Mutex<Option<coordinator::QueryResult>>,
}

#[repr(C)]
struct SwarmQueryInitData {
    done: AtomicBool,
}

impl VTab for SwarmQueryTable {
    type InitData = SwarmQueryInitData;
    type BindData = SwarmQueryBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let sql = bind.get_parameter(0).to_string();

        // Execute query at bind time to determine the schema.
        let result = coordinator::execute_distributed_query(&sql, false)
            .map_err(|e| format!("Distributed query error: {e}"))?;

        for field in result.schema.fields() {
            bind.add_result_column(
                field.name(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            );
        }

        Ok(SwarmQueryBindData {
            sql,
            partial_results: false,
            cached_result: Mutex::new(Some(result)),
        })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(SwarmQueryInitData {
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

        let bind_data = func.get_bind_data();

        let result = bind_data
            .cached_result
            .lock()
            .map_err(|e| format!("Lock error: {e}"))?
            .take()
            .ok_or("Query result already consumed")?;

        if result.batches.is_empty() {
            output.set_len(0);
            return Ok(());
        }

        let num_cols = result.schema.fields().len();
        let mut row_count = 0;

        for batch in &result.batches {
            for row in 0..batch.num_rows() {
                for col in 0..num_cols {
                    let col_array = batch.column(col);
                    let value = arrow::util::display::array_value_to_string(col_array, row)
                        .unwrap_or_default();
                    let vec = output.flat_vector(col);
                    vec.insert(row_count, CString::new(value)?);
                }
                row_count += 1;
            }
        }

        output.set_len(row_count);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeId::Varchar.into()])
    }
}

// ---------------------------------------------------------------------------
// Extension entrypoint
// ---------------------------------------------------------------------------

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    store_shared_connection(&con)?;

    // Cluster lifecycle
    con.register_scalar_function::<SwarmStartScalar>("swarm_start")
        .expect("Failed to register swarm_start function");

    con.register_scalar_function::<SwarmStartWithSeedsScalar>("swarm_start_seeds")
        .expect("Failed to register swarm_start_seeds function");

    con.register_scalar_function::<SwarmStopScalar>("swarm_stop")
        .expect("Failed to register swarm_stop function");

    // Node information
    con.register_table_function::<SwarmNodesTable>("swarm_nodes")
        .expect("Failed to register swarm_nodes function");

    con.register_table_function::<SwarmConfigTable>("swarm_config")
        .expect("Failed to register swarm_config function");

    // Node role management
    con.register_scalar_function::<SwarmSetScalar>("swarm_set")
        .expect("Failed to register swarm_set function");

    // Distributed catalog
    con.register_table_function::<SwarmTablesTable>("swarm_tables")
        .expect("Failed to register swarm_tables function");

    // Distributed queries
    con.register_table_function::<SwarmQueryTable>("swarm_query")
        .expect("Failed to register swarm_query function");

    con.register_table_function::<service_functions::SwarmServicesTable>("swarm_services")
        .expect("Failed to register swarm_services function");

    con.register_scalar_function::<service_functions::SwarmStartServiceScalar>("swarm_start_service")
        .expect("Failed to register swarm_start_service function");

    con.register_scalar_function::<service_functions::SwarmStopServiceScalar>("swarm_stop_service")
        .expect("Failed to register swarm_stop_service function");

    con.register_scalar_function::<service_functions::SwarmLoadScalar>("swarm_load")
        .expect("Failed to register swarm_load function");

    con.register_scalar_function::<service_functions::SwarmRegisterServiceScalar>("swarm_register_service")
        .expect("Failed to register swarm_register_service function");

    // Auto-bootstrap from environment config
    if let Ok(config) = config::ClusterConfig::from_env() {
        if let Some((node_name, node_cfg)) = config::get_this_node_config(&config) {
            let addr: std::net::SocketAddr = match node_cfg.gossip_addr.parse() {
                Ok(a) => a,
                Err(_) => return Ok(()),
            };

            // Derive seeds from all other nodes in the config
            let seeds: Vec<String> = config
                .nodes
                .iter()
                .filter(|(name, _)| name.as_str() != node_name)
                .map(|(_, n)| n.gossip_addr.clone())
                .collect();

            let data_node = if node_cfg.data_node { "true" } else { "false" };

            let _ = GossipRegistry::instance().start(
                &addr.ip().to_string(),
                addr.port(),
                &config.cluster_id,
                node_name,
                data_node,
                seeds,
            );

            // Orchestrate extensions from config
            if !node_cfg.extensions.is_empty() {
                let _statuses = orchestrator::orchestrate_extensions(&node_cfg.extensions);
            }

            // Start catalog advertising if data_node=true
            if node_cfg.data_node {
                let _ = catalog::advertise_local_tables();
                let _ = catalog::start_catalog_refresh();
            }
        }
    }

    Ok(())
}
