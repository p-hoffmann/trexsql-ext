//! Distributed table catalog reader for the swarm extension.
//!
//! Reads catalog and service information from the gossip layer to resolve
//! table names to data node endpoints.  Each data node publishes two kinds
//! of gossip keys:
//!
//! - **`catalog:{table_name}`** — JSON value `{"rows": N, "schema_hash": H}`
//!   indicating that the node holds a copy (or shard) of the named table.
//! - **`service:flight`** — JSON value
//!   `{"host": "...", "port": ..., "status": "running"}` advertising an
//!   Arrow Flight SQL endpoint.
//!
//! The functions in this module combine these two pieces of information to
//! produce [`CatalogEntry`] values that the query planner can use to decide
//! which Flight endpoints to contact for a given table.
//!
//! # Gossip integration
//!
//! The [`GossipRegistry::get_node_key_values`] method returns every key-value
//! pair published by every node known to the gossip layer.  This module is
//! the primary consumer of that method, using the returned
//! [`NodeKeyValueInfo`] values to build catalog entries.

use duckdb::arrow::array::Array as _;
use duckdb::arrow::array::RecordBatch as DuckRecordBatch;
use serde::Deserialize;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use crate::gossip::{GossipRegistry, NodeKeyValueInfo};
use crate::logging::SwarmLogger;

// ---------------------------------------------------------------------------
// Data model
// ---------------------------------------------------------------------------

/// A single catalog entry representing one table on one node.
///
/// A table that is replicated across three data nodes will produce three
/// `CatalogEntry` values — one per node.  The `flight_endpoint` field is
/// `Some` only when the node is also running a Flight SQL server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogEntry {
    /// Human-readable node name (e.g. `"node-a"`).
    pub node_name: String,
    /// UUID assigned to the node by the gossip layer.
    pub node_id: String,
    /// Fully-qualified table name as published by the node.
    pub table_name: String,
    /// Approximate row count reported by the node.
    pub approx_rows: u64,
    /// Hash of the table schema; nodes with the same hash are assumed to
    /// have compatible schemas.
    pub schema_hash: u64,
    /// Flight SQL endpoint URL (e.g. `"http://10.0.0.1:8815"`), or `None`
    /// if the node does not advertise a running Flight service.
    pub flight_endpoint: Option<String>,
}

// ---------------------------------------------------------------------------
// Gossip value schemas
// ---------------------------------------------------------------------------

/// JSON payload stored under `catalog:{table_name}` gossip keys.
#[derive(Debug, Deserialize)]
struct CatalogValue {
    rows: u64,
    schema_hash: u64,
}

/// JSON payload stored under the `service:flight` gossip key.
#[derive(Debug, Deserialize)]
struct FlightServiceValue {
    host: String,
    port: u16,
    status: String,
}

// ---------------------------------------------------------------------------
// Pure catalog resolution (no gossip dependency)
// ---------------------------------------------------------------------------

/// Parse a `service:flight` JSON value into a Flight endpoint URL.
///
/// Returns `Some("http://{host}:{port}")` when the service status is
/// `"running"`, or `None` otherwise.
fn parse_flight_endpoint(json_value: &str) -> Option<String> {
    let svc: FlightServiceValue = serde_json::from_str(json_value).ok()?;
    if svc.status == "running" {
        Some(format!("http://{}:{}", svc.host, svc.port))
    } else {
        None
    }
}

/// Parse a `catalog:{table}` JSON value into row count and schema hash.
fn parse_catalog_value(json_value: &str) -> Option<CatalogValue> {
    serde_json::from_str(json_value).ok()
}

/// Resolve a single table across all nodes.
///
/// This is a pure function: it takes pre-fetched node key-value data and
/// returns `CatalogEntry` values for every node that advertises the
/// requested table.
fn resolve_table_from_states(
    table_name: &str,
    nodes: &[NodeKeyValueInfo],
) -> Vec<CatalogEntry> {
    let catalog_key = format!("catalog:{table_name}");
    let mut entries = Vec::new();

    for node in nodes {
        // Look for the catalog key.
        let catalog_json = node
            .key_values
            .iter()
            .find(|(k, _)| k == &catalog_key)
            .map(|(_, v)| v.as_str());

        let catalog_json = match catalog_json {
            Some(v) => v,
            None => continue,
        };

        let catalog_val = match parse_catalog_value(catalog_json) {
            Some(v) => v,
            None => {
                SwarmLogger::warn(
                    "catalog",
                    &format!(
                        "Failed to parse catalog value for table '{}' on node '{}': {}",
                        table_name, node.node_name, catalog_json,
                    ),
                );
                continue;
            }
        };

        // Look for the flight service key.
        let flight_endpoint = node
            .key_values
            .iter()
            .find(|(k, _)| k == "service:flight")
            .and_then(|(_, v)| parse_flight_endpoint(v));

        entries.push(CatalogEntry {
            node_name: node.node_name.clone(),
            node_id: node.node_id.clone(),
            table_name: table_name.to_string(),
            approx_rows: catalog_val.rows,
            schema_hash: catalog_val.schema_hash,
            flight_endpoint,
        });
    }

    entries
}

/// Collect all tables advertised across all nodes.
///
/// Pure function — see [`resolve_table_from_states`] for the pattern.
fn get_all_tables_from_states(nodes: &[NodeKeyValueInfo]) -> Vec<CatalogEntry> {
    let mut entries = Vec::new();

    for node in nodes {
        // Extract the flight endpoint once per node.
        let flight_endpoint = node
            .key_values
            .iter()
            .find(|(k, _)| k == "service:flight")
            .and_then(|(_, v)| parse_flight_endpoint(v));

        // Iterate over all catalog:* keys.
        for (key, value) in &node.key_values {
            let table_name = match key.strip_prefix("catalog:") {
                Some(name) if !name.is_empty() => name,
                _ => continue,
            };

            let catalog_val = match parse_catalog_value(value) {
                Some(v) => v,
                None => {
                    SwarmLogger::warn(
                        "catalog",
                        &format!(
                            "Failed to parse catalog value for table '{}' on node '{}': {}",
                            table_name, node.node_name, value,
                        ),
                    );
                    continue;
                }
            };

            entries.push(CatalogEntry {
                node_name: node.node_name.clone(),
                node_id: node.node_id.clone(),
                table_name: table_name.to_string(),
                approx_rows: catalog_val.rows,
                schema_hash: catalog_val.schema_hash,
                flight_endpoint: flight_endpoint.clone(),
            });
        }
    }

    entries
}

/// Build a deduplicated list of table names known across the cluster.
///
/// Returns table names in sorted order.
fn list_table_names_from_states(nodes: &[NodeKeyValueInfo]) -> Vec<String> {
    let mut names: Vec<String> = nodes
        .iter()
        .flat_map(|n| n.key_values.iter())
        .filter_map(|(k, _)| k.strip_prefix("catalog:").map(String::from))
        .filter(|name| !name.is_empty())
        .collect();

    names.sort();
    names.dedup();
    names
}

// ---------------------------------------------------------------------------
// Gossip-integrated public API
// ---------------------------------------------------------------------------

/// Fetch all node key-value data from the gossip layer.
fn fetch_node_key_values() -> Result<Vec<NodeKeyValueInfo>, String> {
    GossipRegistry::instance().get_node_key_values()
}

/// Resolve a table name to the set of data nodes that hold it.
///
/// Returns one [`CatalogEntry`] per node that advertises the table via
/// a `catalog:{table_name}` gossip key.  Each entry includes the Flight
/// endpoint URL if the node is running a Flight SQL server.
///
/// # Errors
///
/// Returns `Err` if the gossip layer is not running.
pub fn resolve_table(table_name: &str) -> Result<Vec<CatalogEntry>, String> {
    let nodes = fetch_node_key_values()?;

    SwarmLogger::debug(
        "catalog",
        &format!(
            "Resolving table '{}' across {} node(s)",
            table_name,
            nodes.len(),
        ),
    );

    let entries = resolve_table_from_states(table_name, &nodes);

    SwarmLogger::debug(
        "catalog",
        &format!(
            "Table '{}' found on {} node(s)",
            table_name,
            entries.len(),
        ),
    );

    Ok(entries)
}

/// Return catalog entries for every table advertised in the cluster.
///
/// Scans all gossip keys with the `catalog:` prefix across all nodes and
/// returns a [`CatalogEntry`] for each (node, table) pair.
///
/// # Errors
///
/// Returns `Err` if the gossip layer is not running.
pub fn get_all_tables() -> Result<Vec<CatalogEntry>, String> {
    let nodes = fetch_node_key_values()?;

    SwarmLogger::debug(
        "catalog",
        &format!("Scanning catalog across {} node(s)", nodes.len()),
    );

    let entries = get_all_tables_from_states(&nodes);

    SwarmLogger::debug(
        "catalog",
        &format!(
            "Catalog scan complete: {} entry/entries across {} unique table(s)",
            entries.len(),
            {
                let mut names: Vec<&str> =
                    entries.iter().map(|e| e.table_name.as_str()).collect();
                names.sort();
                names.dedup();
                names.len()
            },
        ),
    );

    Ok(entries)
}

/// Return a sorted, deduplicated list of all table names in the cluster.
///
/// # Errors
///
/// Returns `Err` if the gossip layer is not running.
pub fn list_tables() -> Result<Vec<String>, String> {
    let nodes = fetch_node_key_values()?;
    Ok(list_table_names_from_states(&nodes))
}

/// Group catalog entries by table name, returning a map from table name
/// to the list of nodes that hold it.
///
/// This is a convenience wrapper around [`get_all_tables`] for callers
/// that need a per-table view.
pub fn tables_by_name() -> Result<HashMap<String, Vec<CatalogEntry>>, String> {
    let entries = get_all_tables()?;
    let mut map: HashMap<String, Vec<CatalogEntry>> = HashMap::new();
    for entry in entries {
        map.entry(entry.table_name.clone()).or_default().push(entry);
    }
    Ok(map)
}

// ---------------------------------------------------------------------------
// Catalog advertising (write/publish side)
// ---------------------------------------------------------------------------

/// Compute a deterministic hash over an Arrow schema (standalone arrow crate).
///
/// The hash is computed from the concatenation of each field's name and
/// data-type debug representation.  Two schemas with the same column names
/// and types (in the same order) will produce the same hash.
fn compute_schema_hash(schema: &arrow::datatypes::SchemaRef) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for field in schema.fields() {
        field.name().hash(&mut hasher);
        format!("{:?}", field.data_type()).hash(&mut hasher);
    }
    hasher.finish()
}

/// Compute a deterministic hash over a DuckDB Arrow schema.
///
/// This is the same algorithm as [`compute_schema_hash`] but accepts the
/// `SchemaRef` re-exported by the `duckdb` crate, which may be a different
/// arrow version than the standalone `arrow` dependency.
fn compute_schema_hash_duckdb(schema: &duckdb::arrow::datatypes::SchemaRef) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for field in schema.fields() {
        field.name().hash(&mut hasher);
        format!("{:?}", field.data_type()).hash(&mut hasher);
    }
    hasher.finish()
}

/// Scan local DuckDB tables and publish `catalog:{table}` gossip keys.
///
/// For each table found via `SHOW TABLES`:
/// 1. Executes `SELECT COUNT(*) FROM "{table}"` to get an approximate row
///    count.
/// 2. Computes a schema hash from the column metadata obtained via
///    `SELECT * FROM "{table}" LIMIT 0`.
/// 3. Publishes a `catalog:{table}` gossip key with a JSON payload
///    containing the row count and schema hash.
///
/// Returns the number of tables successfully advertised.
///
/// # Errors
///
/// Returns `Err` if the shared DuckDB connection is unavailable or the
/// gossip layer is not running.
pub fn advertise_local_tables() -> Result<usize, String> {
    let conn_arc = crate::get_shared_connection()
        .ok_or_else(|| "Shared DuckDB connection not available".to_string())?;

    let conn = conn_arc
        .lock()
        .map_err(|e| format!("Failed to lock shared connection: {e}"))?;

    // Retrieve the list of table names via SHOW TABLES.
    let table_names: Vec<String> = {
        let mut stmt = conn
            .prepare("SHOW TABLES")
            .map_err(|e| format!("Failed to prepare SHOW TABLES: {e}"))?;

        let batches: Vec<DuckRecordBatch> = stmt
            .query_arrow([])
            .map_err(|e| format!("Failed to execute SHOW TABLES: {e}"))?
            .collect();

        let mut names = Vec::new();
        for batch in &batches {
            if batch.num_columns() == 0 {
                continue;
            }
            let col = batch.column(0);
            let string_array = col
                .as_any()
                .downcast_ref::<duckdb::arrow::array::StringArray>()
                .ok_or_else(|| "SHOW TABLES did not return string column".to_string())?;
            for i in 0..string_array.len() {
                if !string_array.is_null(i) {
                    names.push(string_array.value(i).to_string());
                }
            }
        }
        names
    };

    if table_names.is_empty() {
        SwarmLogger::debug("catalog", "No local tables to advertise");
        return Ok(0);
    }

    let gossip = GossipRegistry::instance();
    let mut count = 0;

    for table in &table_names {
        // Get the row count.
        let row_count: u64 = {
            let count_sql = format!("SELECT COUNT(*) FROM \"{}\"", table);
            let mut stmt = conn
                .prepare(&count_sql)
                .map_err(|e| format!("Failed to prepare COUNT for table '{}': {e}", table))?;

            let batches: Vec<DuckRecordBatch> = stmt
                .query_arrow([])
                .map_err(|e| format!("Failed to execute COUNT for table '{}': {e}", table))?
                .collect();

            if let Some(batch) = batches.first() {
                if batch.num_columns() > 0 && batch.num_rows() > 0 {
                    let col = batch.column(0);
                    // COUNT(*) returns an i64 in DuckDB.
                    if let Some(arr) =
                        col.as_any().downcast_ref::<duckdb::arrow::array::Int64Array>()
                    {
                        arr.value(0) as u64
                    } else {
                        // Fall back to string parsing.
                        let s =
                            duckdb::arrow::util::display::array_value_to_string(col, 0)
                                .unwrap_or_default();
                        s.parse::<u64>().unwrap_or(0)
                    }
                } else {
                    0
                }
            } else {
                0
            }
        };

        // Get the schema hash by executing a zero-row query.
        let schema_hash: u64 = {
            let schema_sql = format!("SELECT * FROM \"{}\" LIMIT 0", table);
            let mut stmt = conn
                .prepare(&schema_sql)
                .map_err(|e| format!("Failed to prepare schema query for '{}': {e}", table))?;

            let batches: Vec<DuckRecordBatch> = stmt
                .query_arrow([])
                .map_err(|e| format!("Failed to execute schema query for '{}': {e}", table))?
                .collect();

            if let Some(batch) = batches.first() {
                compute_schema_hash_duckdb(&batch.schema())
            } else {
                0
            }
        };

        // Publish the catalog key to gossip.
        let key = format!("catalog:{}", table);
        let value = format!(r#"{{"rows": {}, "schema_hash": {}}}"#, row_count, schema_hash);

        match gossip.set_key(&key, &value) {
            Ok(()) => {
                SwarmLogger::debug(
                    "catalog",
                    &format!(
                        "Advertised table '{}': rows={}, schema_hash=0x{:X}",
                        table, row_count, schema_hash,
                    ),
                );
                count += 1;
            }
            Err(e) => {
                SwarmLogger::warn(
                    "catalog",
                    &format!("Failed to advertise table '{}': {}", table, e),
                );
            }
        }
    }

    SwarmLogger::info(
        "catalog",
        &format!("Advertised {} local table(s)", count),
    );

    Ok(count)
}

/// Remove all `catalog:*` gossip keys published by this node.
///
/// This is used when a node transitions from `data_node=true` to
/// `data_node=false` so that other nodes no longer route queries to it.
///
/// # Errors
///
/// Returns `Err` if the gossip layer is not running.
pub fn remove_catalog_keys() -> Result<usize, String> {
    let gossip = GossipRegistry::instance();

    let keys = gossip.list_keys_with_prefix("catalog:")?;
    let count = keys.len();

    for key in &keys {
        if let Err(e) = gossip.delete_key(key) {
            SwarmLogger::warn(
                "catalog",
                &format!("Failed to delete catalog key '{}': {}", key, e),
            );
        }
    }

    if count > 0 {
        SwarmLogger::info(
            "catalog",
            &format!("Removed {} catalog key(s) from gossip", count),
        );
    }

    Ok(count)
}

// ---------------------------------------------------------------------------
// Periodic catalog refresh
// ---------------------------------------------------------------------------

/// Handle for the background catalog refresh thread.
///
/// The thread periodically calls [`advertise_local_tables`] to keep gossip
/// keys up to date as local tables are created or dropped.
struct CatalogRefreshHandle {
    stop_flag: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

/// Process-wide singleton for the catalog refresh handle.
static CATALOG_REFRESH: OnceLock<std::sync::Mutex<Option<CatalogRefreshHandle>>> = OnceLock::new();

fn catalog_refresh_lock() -> &'static std::sync::Mutex<Option<CatalogRefreshHandle>> {
    CATALOG_REFRESH.get_or_init(|| std::sync::Mutex::new(None))
}

/// Spawn a background thread that periodically re-scans local tables and
/// updates gossip catalog keys.
///
/// The refresh interval defaults to 30 seconds and can be overridden via
/// the `SWARM_CATALOG_INTERVAL` environment variable (value in seconds).
///
/// Calling this function when a refresh thread is already running is a
/// no-op and returns `Ok(())`.
///
/// # Errors
///
/// Returns `Err` if the background thread could not be spawned.
pub fn start_catalog_refresh() -> Result<(), String> {
    let mut guard = catalog_refresh_lock()
        .lock()
        .map_err(|e| format!("Failed to lock catalog refresh handle: {e}"))?;

    if guard.is_some() {
        SwarmLogger::debug("catalog", "Catalog refresh is already running");
        return Ok(());
    }

    let interval_secs: u64 = std::env::var("SWARM_CATALOG_INTERVAL")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);

    let stop_flag = Arc::new(AtomicBool::new(false));
    let stop_clone = Arc::clone(&stop_flag);

    let thread = std::thread::Builder::new()
        .name("swarm-catalog-refresh".to_string())
        .spawn(move || {
            SwarmLogger::info(
                "catalog",
                &format!(
                    "Catalog refresh started (interval={}s)",
                    interval_secs,
                ),
            );

            loop {
                std::thread::sleep(std::time::Duration::from_secs(interval_secs));

                if stop_clone.load(Ordering::Acquire) {
                    break;
                }

                match advertise_local_tables() {
                    Ok(n) => {
                        SwarmLogger::debug(
                            "catalog",
                            &format!("Catalog refresh: advertised {} table(s)", n),
                        );
                    }
                    Err(e) => {
                        SwarmLogger::warn(
                            "catalog",
                            &format!("Catalog refresh failed: {}", e),
                        );
                    }
                }
            }

            SwarmLogger::info("catalog", "Catalog refresh stopped");
        })
        .map_err(|e| format!("Failed to spawn catalog refresh thread: {e}"))?;

    *guard = Some(CatalogRefreshHandle {
        stop_flag,
        thread: Some(thread),
    });

    Ok(())
}

/// Signal the background catalog refresh thread to stop.
///
/// Signal the background catalog refresh thread to stop and wait for it to
/// exit (up to one refresh interval).
pub fn stop_catalog_refresh() {
    let mut guard = match catalog_refresh_lock().lock() {
        Ok(g) => g,
        Err(e) => {
            SwarmLogger::warn(
                "catalog",
                &format!("Failed to lock catalog refresh handle for stop: {e}"),
            );
            return;
        }
    };

    if let Some(mut handle) = guard.take() {
        handle.stop_flag.store(true, Ordering::Release);
        SwarmLogger::info("catalog", "Catalog refresh stop requested");
        if let Some(thread) = handle.thread.take() {
            let _ = thread.join();
        }
    } else {
        SwarmLogger::debug("catalog", "Catalog refresh is not running");
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a `NodeKeyValueInfo` with the given keys for testing.
    fn make_node(
        node_id: &str,
        node_name: &str,
        kvs: Vec<(&str, &str)>,
    ) -> NodeKeyValueInfo {
        NodeKeyValueInfo {
            node_id: node_id.to_string(),
            node_name: node_name.to_string(),
            gossip_addr: "127.0.0.1:7100".to_string(),
            key_values: kvs
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    fn catalog_json(rows: u64, schema_hash: u64) -> String {
        format!(r#"{{"rows": {rows}, "schema_hash": {schema_hash}}}"#)
    }

    fn flight_json(host: &str, port: u16, status: &str) -> String {
        format!(r#"{{"host": "{host}", "port": {port}, "status": "{status}"}}"#)
    }

    // -- parse_flight_endpoint -----------------------------------------------

    #[test]
    fn parse_flight_endpoint_running() {
        let json = flight_json("10.0.0.1", 8815, "running");
        assert_eq!(
            parse_flight_endpoint(&json),
            Some("http://10.0.0.1:8815".to_string()),
        );
    }

    #[test]
    fn parse_flight_endpoint_stopped() {
        let json = flight_json("10.0.0.1", 8815, "stopped");
        assert_eq!(parse_flight_endpoint(&json), None);
    }

    #[test]
    fn parse_flight_endpoint_invalid_json() {
        assert_eq!(parse_flight_endpoint("not json"), None);
    }

    #[test]
    fn parse_flight_endpoint_empty() {
        assert_eq!(parse_flight_endpoint(""), None);
    }

    // -- parse_catalog_value -------------------------------------------------

    #[test]
    fn parse_catalog_value_valid() {
        let json = catalog_json(42_000, 0xDEAD);
        let val = parse_catalog_value(&json).unwrap();
        assert_eq!(val.rows, 42_000);
        assert_eq!(val.schema_hash, 0xDEAD);
    }

    #[test]
    fn parse_catalog_value_invalid() {
        assert!(parse_catalog_value("nope").is_none());
    }

    // -- resolve_table_from_states -------------------------------------------

    #[test]
    fn resolve_table_single_node() {
        let cat = catalog_json(100, 1);
        let flt = flight_json("10.0.0.1", 8815, "running");
        let nodes = vec![make_node(
            "id-a",
            "node-a",
            vec![("catalog:lineitem", &cat), ("service:flight", &flt)],
        )];

        let entries = resolve_table_from_states("lineitem", &nodes);
        assert_eq!(entries.len(), 1);

        let e = &entries[0];
        assert_eq!(e.node_name, "node-a");
        assert_eq!(e.node_id, "id-a");
        assert_eq!(e.table_name, "lineitem");
        assert_eq!(e.approx_rows, 100);
        assert_eq!(e.schema_hash, 1);
        assert_eq!(e.flight_endpoint, Some("http://10.0.0.1:8815".to_string()));
    }

    #[test]
    fn resolve_table_multi_node() {
        let cat = catalog_json(500, 42);
        let flt_a = flight_json("10.0.0.1", 8815, "running");
        let flt_b = flight_json("10.0.0.2", 8815, "running");

        let nodes = vec![
            make_node(
                "id-a",
                "node-a",
                vec![("catalog:orders", &cat), ("service:flight", &flt_a)],
            ),
            make_node(
                "id-b",
                "node-b",
                vec![("catalog:orders", &cat), ("service:flight", &flt_b)],
            ),
            make_node(
                "id-c",
                "node-c",
                vec![("catalog:customers", &catalog_json(10, 99))],
            ),
        ];

        let entries = resolve_table_from_states("orders", &nodes);
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|e| e.table_name == "orders"));
        assert!(entries.iter().all(|e| e.approx_rows == 500));
    }

    #[test]
    fn resolve_table_not_found() {
        let nodes = vec![make_node(
            "id-a",
            "node-a",
            vec![("catalog:orders", &catalog_json(10, 1))],
        )];

        let entries = resolve_table_from_states("lineitem", &nodes);
        assert!(entries.is_empty());
    }

    #[test]
    fn resolve_table_no_flight_service() {
        let cat = catalog_json(200, 7);
        let nodes = vec![make_node(
            "id-a",
            "node-a",
            vec![("catalog:orders", &cat)],
        )];

        let entries = resolve_table_from_states("orders", &nodes);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].flight_endpoint, None);
    }

    #[test]
    fn resolve_table_flight_not_running() {
        let cat = catalog_json(200, 7);
        let flt = flight_json("10.0.0.1", 8815, "stopped");
        let nodes = vec![make_node(
            "id-a",
            "node-a",
            vec![("catalog:orders", &cat), ("service:flight", &flt)],
        )];

        let entries = resolve_table_from_states("orders", &nodes);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].flight_endpoint, None);
    }

    #[test]
    fn resolve_table_skips_invalid_catalog_json() {
        let nodes = vec![make_node(
            "id-a",
            "node-a",
            vec![("catalog:broken", "not valid json")],
        )];

        let entries = resolve_table_from_states("broken", &nodes);
        assert!(entries.is_empty());
    }

    // -- get_all_tables_from_states ------------------------------------------

    #[test]
    fn get_all_tables_multiple_nodes_and_tables() {
        let flt_a = flight_json("10.0.0.1", 8815, "running");
        let flt_b = flight_json("10.0.0.2", 8815, "running");

        let nodes = vec![
            make_node(
                "id-a",
                "node-a",
                vec![
                    ("catalog:orders", &catalog_json(100, 1)),
                    ("catalog:lineitem", &catalog_json(1000, 2)),
                    ("service:flight", &flt_a),
                    ("node_name", "node-a"),
                ],
            ),
            make_node(
                "id-b",
                "node-b",
                vec![
                    ("catalog:orders", &catalog_json(200, 1)),
                    ("service:flight", &flt_b),
                ],
            ),
        ];

        let entries = get_all_tables_from_states(&nodes);
        assert_eq!(entries.len(), 3);

        // node-a has orders + lineitem, node-b has orders
        let orders: Vec<_> = entries
            .iter()
            .filter(|e| e.table_name == "orders")
            .collect();
        assert_eq!(orders.len(), 2);

        let lineitem: Vec<_> = entries
            .iter()
            .filter(|e| e.table_name == "lineitem")
            .collect();
        assert_eq!(lineitem.len(), 1);
        assert_eq!(lineitem[0].approx_rows, 1000);
        assert_eq!(
            lineitem[0].flight_endpoint,
            Some("http://10.0.0.1:8815".to_string()),
        );
    }

    #[test]
    fn get_all_tables_empty_cluster() {
        let entries = get_all_tables_from_states(&[]);
        assert!(entries.is_empty());
    }

    #[test]
    fn get_all_tables_no_catalog_keys() {
        let nodes = vec![make_node(
            "id-a",
            "node-a",
            vec![
                ("node_name", "node-a"),
                ("status", "active"),
                ("service:flight", &flight_json("10.0.0.1", 8815, "running")),
            ],
        )];

        let entries = get_all_tables_from_states(&nodes);
        assert!(entries.is_empty());
    }

    // -- list_table_names_from_states ----------------------------------------

    #[test]
    fn list_table_names_deduplicates_and_sorts() {
        let nodes = vec![
            make_node(
                "id-a",
                "node-a",
                vec![
                    ("catalog:orders", &catalog_json(10, 1)),
                    ("catalog:lineitem", &catalog_json(20, 2)),
                ],
            ),
            make_node(
                "id-b",
                "node-b",
                vec![("catalog:orders", &catalog_json(30, 1))],
            ),
        ];

        let names = list_table_names_from_states(&nodes);
        assert_eq!(names, vec!["lineitem", "orders"]);
    }

    #[test]
    fn list_table_names_empty() {
        let names = list_table_names_from_states(&[]);
        assert!(names.is_empty());
    }

    // -- CatalogEntry --------------------------------------------------------

    #[test]
    fn catalog_entry_clone_and_debug() {
        let entry = CatalogEntry {
            node_name: "node-a".to_string(),
            node_id: "id-a".to_string(),
            table_name: "orders".to_string(),
            approx_rows: 42,
            schema_hash: 0xFF,
            flight_endpoint: Some("http://localhost:8815".to_string()),
        };

        let cloned = entry.clone();
        assert_eq!(entry, cloned);

        // Debug impl should not panic.
        let _ = format!("{:?}", entry);
    }

    // -- compute_schema_hash -------------------------------------------------

    #[test]
    fn compute_schema_hash_deterministic() {
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("price", DataType::Float64, true),
        ]));

        let h1 = compute_schema_hash(&schema);
        let h2 = compute_schema_hash(&schema);
        assert_eq!(h1, h2, "Same schema must produce identical hashes");
    }

    #[test]
    fn compute_schema_hash_differs_on_column_name() {
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema_a = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        let schema_b = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
        ]));

        assert_ne!(
            compute_schema_hash(&schema_a),
            compute_schema_hash(&schema_b),
            "Different column names should produce different hashes",
        );
    }

    #[test]
    fn compute_schema_hash_differs_on_type() {
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema_a = Arc::new(Schema::new(vec![
            Field::new("val", DataType::Int64, false),
        ]));
        let schema_b = Arc::new(Schema::new(vec![
            Field::new("val", DataType::Utf8, false),
        ]));

        assert_ne!(
            compute_schema_hash(&schema_a),
            compute_schema_hash(&schema_b),
            "Different column types should produce different hashes",
        );
    }

    #[test]
    fn compute_schema_hash_empty_schema() {
        use arrow::datatypes::Schema;
        use std::sync::Arc;

        let schema = Arc::new(Schema::empty());
        // Should not panic; the exact value is not important.
        let _h = compute_schema_hash(&schema);
    }

    #[test]
    fn compute_schema_hash_column_order_matters() {
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema_a = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ]));
        let schema_b = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Utf8, true),
            Field::new("a", DataType::Int64, false),
        ]));

        assert_ne!(
            compute_schema_hash(&schema_a),
            compute_schema_hash(&schema_b),
            "Column order should affect the hash",
        );
    }

    // -- catalog_refresh_lock initialization ---------------------------------

    #[test]
    fn catalog_refresh_lock_initializes() {
        // Just verify the singleton initializes without panicking.
        let lock = catalog_refresh_lock();
        let guard = lock.lock().unwrap();
        // Initially no handle is present (unless another test started one).
        // We just verify the lock works.
        drop(guard);
    }

    // -- stop_catalog_refresh when not running -------------------------------

    #[test]
    fn stop_catalog_refresh_when_not_running_does_not_panic() {
        // Calling stop when nothing is running should be a no-op.
        stop_catalog_refresh();
    }

    // -- advertise_local_tables requires connection --------------------------

    #[test]
    fn advertise_local_tables_without_connection() {
        // When no shared connection is stored, should return an error.
        // Note: this test relies on SHARED_CONNECTION not being set in the
        // test binary.  If another test stores a connection first, this
        // will still pass because the gossip layer will not be running.
        let result = advertise_local_tables();
        assert!(result.is_err());
    }

    // -- remove_catalog_keys requires gossip --------------------------------

    #[test]
    fn remove_catalog_keys_without_gossip() {
        // When gossip is not running, should return an error.
        let result = remove_catalog_keys();
        assert!(result.is_err());
    }
}
