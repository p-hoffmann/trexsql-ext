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
pub mod duckdb_final_pass;
pub mod duckdb_sql_executor;
pub mod duckdb_table_provider;
pub mod distributed_scheduler;
pub mod federation_executor;
pub mod distributed_table_provider;
pub mod sharded_schema_provider;
pub mod orchestrator;
pub mod service_functions;
pub mod admission;
pub mod metrics;
pub mod shuffle_descriptor;
pub mod shuffle_partition;
pub mod shuffle_registry;
pub mod shuffle_transport;
pub mod shuffle_writer;
pub mod shuffle_reader;
pub mod shuffle_optimizer;
pub mod flight_server;
pub mod flight_functions;
pub mod server_registry;
pub mod partition;

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

static DISTRIBUTED_ENABLED: AtomicBool = AtomicBool::new(false);

pub fn is_distributed_enabled() -> bool {
    DISTRIBUTED_ENABLED.load(Ordering::Relaxed)
}

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
                        let _ = catalog::advertise_local_tables();
                        let _ = catalog::start_catalog_refresh();
                    } else if value == "false" {
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

struct SwarmSetDistributedScalar;

impl VScalar for SwarmSetDistributedScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let bool_vector = input.flat_vector(0);
        let bool_slice = bool_vector.as_slice_with_len::<bool>(input.len());
        let enabled = bool_slice[0];

        DISTRIBUTED_ENABLED.store(enabled, Ordering::Relaxed);

        if enabled && !distributed_scheduler::is_scheduler_running() {
            let is_scheduler = config::ClusterConfig::from_env()
                .ok()
                .and_then(|cfg| {
                    config::get_this_node_config(&cfg)
                        .map(|(_, node)| node.roles.contains(&"scheduler".to_string()))
                })
                .unwrap_or(true); // Default to true if no config (standalone mode)

            if !is_scheduler {
                DISTRIBUTED_ENABLED.store(false, Ordering::Relaxed);
                let msg = "Failed to enable distributed mode: this node does not have the 'scheduler' role";
                let flat_vector = output.flat_vector();
                flat_vector.insert(0, msg);
                return Ok(());
            }

            let config = distributed_scheduler::SchedulerConfig {
                bind_addr: "0.0.0.0:50050".to_string(),
            };
            if let Err(e) = distributed_scheduler::start_scheduler(config) {
                DISTRIBUTED_ENABLED.store(false, Ordering::Relaxed);
                let msg = format!("Failed to enable distributed mode: {e}");
                let flat_vector = output.flat_vector();
                flat_vector.insert(0, &msg);
                return Ok(());
            }
        } else if !enabled && distributed_scheduler::is_scheduler_running() {
            let _ = distributed_scheduler::stop_scheduler();
        }

        let response = if enabled {
            "Distributed engine enabled (queries will route through DataFusion)"
        } else {
            "Distributed engine disabled (queries will use legacy coordinator)"
        };

        let flat_vector = output.flat_vector();
        flat_vector.insert(0, response);
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Boolean.into()],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

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

        // Capture the flag once to avoid TOCTOU between check and query submission.
        let distributed = is_distributed_enabled();

        let mut admission_query_id: Option<String> = None;
        if distributed {
            let priority = admission::get_session_priority();
            let (status, qid) = admission::submit_or_check(&sql, "default", priority)
                .map_err(|e| format!("Admission error: {}", e))?;
            match status {
                admission::QueryStatus::Rejected(reason) => {
                    return Err(format!("Query rejected: {}", reason).into());
                }
                admission::QueryStatus::Queued { position } => {
                    return Err(format!("Query queued at position {}", position).into());
                }
                _ => {
                    admission_query_id = Some(qid);
                }
            }
        }

        let result = if distributed {
            let query_result = distributed_scheduler::submit_query(&sql);
            // Complete admission tracking regardless of query outcome.
            if let Some(qid) = &admission_query_id {
                let _ = admission::complete(qid);
            }
            let (schema, batches) = query_result
                .map_err(|e| format!("Distributed query error: {e}"))?;
            coordinator::QueryResult { schema, batches }
        } else {
            coordinator::execute_distributed_query(&sql, false)
                .map_err(|e| format!("Distributed query error: {e}"))?
        };

        // No final pass for the DataFusion path — DataFusion handles the
        // full SQL (ORDER BY, LIMIT, aggregation, joins, etc.) natively.
        // The legacy coordinator path already applies trexsql SQL on each node.

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

struct SwarmQueryStatusTable;

#[repr(C)]
struct SwarmQueryStatusBindData {}

#[repr(C)]
struct SwarmQueryStatusInitData {
    done: AtomicBool,
}

impl VTab for SwarmQueryStatusTable {
    type InitData = SwarmQueryStatusInitData;
    type BindData = SwarmQueryStatusBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("query_id", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("status", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("queue_position", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("submitted_at", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("user_id", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(SwarmQueryStatusBindData {})
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(SwarmQueryStatusInitData {
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

        let infos = match admission::get_all_query_info() {
            Ok(infos) => infos,
            Err(_) => {
                output.set_len(0);
                return Ok(());
            }
        };

        if infos.is_empty() {
            output.set_len(0);
            return Ok(());
        }

        let chunk_size = infos.len();
        let query_id_vec = output.flat_vector(0);
        let status_vec = output.flat_vector(1);
        let queue_pos_vec = output.flat_vector(2);
        let submitted_vec = output.flat_vector(3);
        let user_id_vec = output.flat_vector(4);

        for (i, info) in infos.iter().enumerate() {
            query_id_vec.insert(i, CString::new(info.query_id.clone())?);
            status_vec.insert(i, CString::new(info.status.clone())?);
            queue_pos_vec.insert(i, CString::new(info.queue_position.clone())?);
            submitted_vec.insert(i, CString::new(info.submitted_at.clone())?);
            user_id_vec.insert(i, CString::new(info.user_id.clone())?);
        }

        output.set_len(chunk_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

struct SwarmClusterStatusTable;

#[repr(C)]
struct SwarmClusterStatusBindData {}

#[repr(C)]
struct SwarmClusterStatusInitData {
    done: AtomicBool,
}

impl VTab for SwarmClusterStatusTable {
    type InitData = SwarmClusterStatusInitData;
    type BindData = SwarmClusterStatusBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("total_nodes", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("active_queries", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("queued_queries", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "memory_utilization_pct",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        Ok(SwarmClusterStatusBindData {})
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(SwarmClusterStatusInitData {
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

        let status = match admission::get_cluster_status() {
            Ok(s) => s,
            Err(_) => {
                output.set_len(0);
                return Ok(());
            }
        };

        let total_nodes_vec = output.flat_vector(0);
        let active_vec = output.flat_vector(1);
        let queued_vec = output.flat_vector(2);
        let mem_vec = output.flat_vector(3);

        total_nodes_vec.insert(0, CString::new(status.total_nodes.to_string())?);
        active_vec.insert(0, CString::new(status.active_queries.to_string())?);
        queued_vec.insert(0, CString::new(status.queued_queries.to_string())?);
        mem_vec.insert(
            0,
            CString::new(format!("{:.1}", status.memory_utilization_pct))?,
        );

        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

struct SwarmSetPriorityScalar;

impl VScalar for SwarmSetPriorityScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let priority_vector = input.flat_vector(0);
        let priority_slice =
            priority_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let priority_str = duckdb::types::DuckString::new(&mut { priority_slice[0] })
            .as_str()
            .to_string();

        let response = match admission::Priority::from_str(&priority_str) {
            Some(priority) => {
                admission::set_session_priority(priority);
                format!("Session priority set to '{}'", priority.as_str())
            }
            None => format!(
                "Invalid priority '{}'. Valid values: batch, interactive, system",
                priority_str
            ),
        };

        let flat_vector = output.flat_vector();
        flat_vector.insert(0, &response);
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into()],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

struct SwarmSetUserQuotaScalar;

impl VScalar for SwarmSetUserQuotaScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let user_vector = input.flat_vector(0);
        let limit_vector = input.flat_vector(1);

        let user_slice =
            user_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let limit_slice = limit_vector.as_slice_with_len::<i32>(input.len());

        let user_id = duckdb::types::DuckString::new(&mut { user_slice[0] })
            .as_str()
            .to_string();
        let max_concurrent = limit_slice[0];

        if max_concurrent < 1 {
            let flat_vector = output.flat_vector();
            flat_vector.insert(
                0,
                &format!("max_concurrent must be >= 1, got {}", max_concurrent),
            );
            return Ok(());
        }

        let response = match admission::set_user_quota(&user_id, max_concurrent as usize) {
            Ok(()) => format!(
                "User '{}' quota set to {} concurrent queries",
                user_id, max_concurrent
            ),
            Err(e) => format!("Error setting quota: {}", e),
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

struct SwarmMetricsTable;

#[repr(C)]
struct SwarmMetricsBindData {}

#[repr(C)]
struct SwarmMetricsInitData {
    done: AtomicBool,
}

impl VTab for SwarmMetricsTable {
    type InitData = SwarmMetricsInitData;
    type BindData = SwarmMetricsBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("metric_name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("metric_type", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("value", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("labels", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(SwarmMetricsBindData {})
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(SwarmMetricsInitData {
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

        let entries = metrics::instance().get_all_metrics();

        if entries.is_empty() {
            output.set_len(0);
            return Ok(());
        }

        let chunk_size = entries.len();
        let name_vec = output.flat_vector(0);
        let type_vec = output.flat_vector(1);
        let value_vec = output.flat_vector(2);
        let labels_vec = output.flat_vector(3);

        for (i, entry) in entries.iter().enumerate() {
            name_vec.insert(i, CString::new(entry.name.clone())?);
            type_vec.insert(i, CString::new(entry.metric_type.clone())?);
            value_vec.insert(i, CString::new(entry.value.clone())?);
            labels_vec.insert(i, CString::new(entry.labels.clone())?);
        }

        output.set_len(chunk_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

struct SwarmCancelQueryScalar;

impl VScalar for SwarmCancelQueryScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let id_vector = input.flat_vector(0);
        let id_slice =
            id_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let query_id = duckdb::types::DuckString::new(&mut { id_slice[0] })
            .as_str()
            .to_string();

        let response = match admission::cancel_query(&query_id) {
            Ok(_status) => format!("Query {} cancelled", query_id),
            Err(e) => format!("Query {} not found: {}", query_id, e),
        };

        let flat_vector = output.flat_vector();
        flat_vector.insert(0, &response);
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into()],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

struct SwarmPartitionTableScalar;

impl VScalar for SwarmPartitionTableScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let table_vector = input.flat_vector(0);
        let config_vector = input.flat_vector(1);

        let table_slice =
            table_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let config_slice =
            config_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());

        let table_name = duckdb::types::DuckString::new(&mut { table_slice[0] })
            .as_str()
            .to_string();
        let config_json = duckdb::types::DuckString::new(&mut { config_slice[0] })
            .as_str()
            .to_string();

        let response = match partition::swarm_partition_table_impl(&table_name, &config_json) {
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
                LogicalTypeId::Varchar.into(),
            ],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

struct SwarmCreateTableScalar;

impl VScalar for SwarmCreateTableScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let sql_vector = input.flat_vector(0);
        let config_vector = input.flat_vector(1);

        let sql_slice =
            sql_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let config_slice =
            config_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());

        let create_sql = duckdb::types::DuckString::new(&mut { sql_slice[0] })
            .as_str()
            .to_string();
        let config_json = duckdb::types::DuckString::new(&mut { config_slice[0] })
            .as_str()
            .to_string();

        let response = match partition::swarm_create_table_impl(&create_sql, &config_json) {
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
                LogicalTypeId::Varchar.into(),
            ],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

struct SwarmRepartitionTableScalar;

impl VScalar for SwarmRepartitionTableScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let table_vector = input.flat_vector(0);
        let config_vector = input.flat_vector(1);

        let table_slice =
            table_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let config_slice =
            config_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());

        let table_name = duckdb::types::DuckString::new(&mut { table_slice[0] })
            .as_str()
            .to_string();
        let config_json = duckdb::types::DuckString::new(&mut { config_slice[0] })
            .as_str()
            .to_string();

        let response = match partition::swarm_repartition_table_impl(&table_name, &config_json) {
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
                LogicalTypeId::Varchar.into(),
            ],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

struct SwarmPartitionsTable;

#[repr(C)]
struct SwarmPartitionsBindData {}

#[repr(C)]
struct SwarmPartitionsInitData {
    done: AtomicBool,
}

impl VTab for SwarmPartitionsTable {
    type InitData = SwarmPartitionsInitData;
    type BindData = SwarmPartitionsBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("table_name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("strategy", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("column", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "partition_id",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column("node_name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "flight_endpoint",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        Ok(SwarmPartitionsBindData {})
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(SwarmPartitionsInitData {
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

        let all_metadata = match partition::get_all_partition_metadata() {
            Ok(m) => m,
            Err(_) => {
                output.set_len(0);
                return Ok(());
            }
        };

        if all_metadata.is_empty() {
            output.set_len(0);
            return Ok(());
        }

        // Flatten: one row per partition assignment.
        let mut rows: Vec<(String, String, String, usize, String, String)> = Vec::new();
        for (table_name, meta) in &all_metadata {
            let (strategy_str, column_str) = match &meta.strategy {
                partition::PartitionStrategy::Hash { column, num_partitions } => {
                    (format!("hash({})", num_partitions), column.clone())
                }
                partition::PartitionStrategy::Range { column, ranges } => {
                    (format!("range({})", ranges.len()), column.clone())
                }
            };

            for assignment in &meta.assignments {
                rows.push((
                    table_name.clone(),
                    strategy_str.clone(),
                    column_str.clone(),
                    assignment.partition_id,
                    assignment.node_name.clone(),
                    assignment.flight_endpoint.clone(),
                ));
            }
        }

        if rows.is_empty() {
            output.set_len(0);
            return Ok(());
        }

        let table_name_vec = output.flat_vector(0);
        let strategy_vec = output.flat_vector(1);
        let column_vec = output.flat_vector(2);
        let partition_id_vec = output.flat_vector(3);
        let node_name_vec = output.flat_vector(4);
        let flight_endpoint_vec = output.flat_vector(5);

        for (i, (table, strategy, column, pid, node, endpoint)) in rows.iter().enumerate() {
            table_name_vec.insert(i, CString::new(table.clone())?);
            strategy_vec.insert(i, CString::new(strategy.clone())?);
            column_vec.insert(i, CString::new(column.clone())?);
            partition_id_vec.insert(i, CString::new(pid.to_string())?);
            node_name_vec.insert(i, CString::new(node.clone())?);
            flight_endpoint_vec.insert(i, CString::new(endpoint.clone())?);
        }

        output.set_len(rows.len());
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    store_shared_connection(&con)?;

    con.register_scalar_function::<SwarmStartScalar>("swarm_start")
        .expect("Failed to register swarm_start function");

    con.register_scalar_function::<SwarmStartWithSeedsScalar>("swarm_start_seeds")
        .expect("Failed to register swarm_start_seeds function");

    con.register_scalar_function::<SwarmStopScalar>("swarm_stop")
        .expect("Failed to register swarm_stop function");

    con.register_table_function::<SwarmNodesTable>("swarm_nodes")
        .expect("Failed to register swarm_nodes function");

    con.register_table_function::<SwarmConfigTable>("swarm_config")
        .expect("Failed to register swarm_config function");

    con.register_scalar_function::<SwarmSetScalar>("swarm_set")
        .expect("Failed to register swarm_set function");

    con.register_table_function::<SwarmTablesTable>("swarm_tables")
        .expect("Failed to register swarm_tables function");

    con.register_scalar_function::<SwarmSetDistributedScalar>("swarm_set_distributed")
        .expect("Failed to register swarm_set_distributed function");

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

    con.register_scalar_function::<service_functions::SwarmSetKeyScalar>("swarm_set_key")
        .expect("Failed to register swarm_set_key function");

    con.register_scalar_function::<service_functions::SwarmDeleteKeyScalar>("swarm_delete_key")
        .expect("Failed to register swarm_delete_key function");

    con.register_table_function::<SwarmQueryStatusTable>("swarm_query_status")
        .expect("Failed to register swarm_query_status function");

    con.register_table_function::<SwarmClusterStatusTable>("swarm_cluster_status")
        .expect("Failed to register swarm_cluster_status function");

    con.register_scalar_function::<SwarmSetPriorityScalar>("swarm_set_priority")
        .expect("Failed to register swarm_set_priority function");

    con.register_scalar_function::<SwarmSetUserQuotaScalar>("swarm_set_user_quota")
        .expect("Failed to register swarm_set_user_quota function");

    con.register_table_function::<SwarmMetricsTable>("swarm_metrics")
        .expect("Failed to register swarm_metrics function");

    con.register_scalar_function::<SwarmCancelQueryScalar>("swarm_cancel_query")
        .expect("Failed to register swarm_cancel_query function");

    con.register_scalar_function::<SwarmPartitionTableScalar>("swarm_partition_table")
        .expect("Failed to register swarm_partition_table function");

    con.register_scalar_function::<SwarmCreateTableScalar>("swarm_create_table")
        .expect("Failed to register swarm_create_table function");

    con.register_scalar_function::<SwarmRepartitionTableScalar>("swarm_repartition_table")
        .expect("Failed to register swarm_repartition_table function");

    con.register_table_function::<SwarmPartitionsTable>("swarm_partitions")
        .expect("Failed to register swarm_partitions function");

    // Flight server functions (merged from flight extension)
    con.register_scalar_function::<flight_functions::StartFlightServerScalar>("start_flight_server")
        .expect("Failed to register start_flight_server function");

    con.register_scalar_function::<flight_functions::StartFlightServerTlsScalar>("start_flight_server_tls")
        .expect("Failed to register start_flight_server_tls function");

    con.register_scalar_function::<flight_functions::StopFlightServerScalar>("stop_flight_server")
        .expect("Failed to register stop_flight_server function");

    con.register_scalar_function::<flight_functions::FlightVersionScalar>("flight_version")
        .expect("Failed to register flight_version scalar function");

    con.register_table_function::<flight_functions::FlightServerStatusTable>("flight_server_status")
        .expect("Failed to register flight_server_status function");

    if let Ok(config) = config::ClusterConfig::from_env() {
        if let Some((node_name, node_cfg)) = config::get_this_node_config(&config) {
            let addr: std::net::SocketAddr = match node_cfg.gossip_addr.parse() {
                Ok(a) => a,
                Err(_) => return Ok(()),
            };

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

            if !node_cfg.extensions.is_empty() {
                let _statuses = orchestrator::orchestrate_extensions(&node_cfg.extensions);
            }

            if node_cfg.data_node {
                let _ = catalog::advertise_local_tables();
                let _ = catalog::start_catalog_refresh();
            }

            if config.distributed_engine {
                DISTRIBUTED_ENABLED.store(true, Ordering::Relaxed);
                let _statuses =
                    orchestrator::start_distributed_for_roles(&node_cfg.roles, &node_cfg.gossip_addr);
            }
        }
    }

    Ok(())
}
