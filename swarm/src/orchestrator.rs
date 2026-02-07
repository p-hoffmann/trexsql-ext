use crate::config::ExtensionConfig;
use crate::gossip::GossipRegistry;
use crate::logging::SwarmLogger;

// ---------------------------------------------------------------------------
// Known service start function mapping
// ---------------------------------------------------------------------------

/// Return the SQL statement to start a service for the given extension name,
/// substituting the provided host and port.  Returns `None` for extensions
/// that have no known start function (load-only extensions).
fn start_function_sql(name: &str, host: &str, port: u16) -> Option<String> {
    match name {
        "flight" => Some(format!("SELECT start_flight_server('{host}', {port})")),
        "pgwire" => Some(format!(
            "SELECT start_pgwire_server('{host}', {port}, '', '')"
        )),
        "trexas" => Some(format!("SELECT start_trexas_server('{host}', {port})")),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Orchestrator
// ---------------------------------------------------------------------------

/// Process a list of extension configurations:
///
/// 1. `LOAD` each extension via the shared DuckDB connection.
/// 2. For extensions that declare a host **and** port, look up the known
///    start-function mapping and execute it via SQL.
/// 3. On successful start, publish the service endpoint to gossip so that
///    other nodes in the cluster can discover it.
///
/// Returns a status message for every extension in the input list.
pub fn orchestrate_extensions(extensions: &[ExtensionConfig]) -> Vec<String> {
    let conn_arc = match crate::get_shared_connection() {
        Some(c) => c,
        None => {
            SwarmLogger::error("orchestrator", "Shared DuckDB connection is not available");
            return extensions
                .iter()
                .map(|ext| format!("{}: error — no shared connection", ext.name))
                .collect();
        }
    };

    let conn = match conn_arc.lock() {
        Ok(c) => c,
        Err(_) => {
            SwarmLogger::error("orchestrator", "Shared connection lock poisoned");
            return extensions
                .iter()
                .map(|ext| format!("{}: error — connection lock poisoned", ext.name))
                .collect();
        }
    };
    let mut statuses: Vec<String> = Vec::with_capacity(extensions.len());

    for ext in extensions {
        // Step 1: LOAD the extension
        let load_sql = format!("LOAD '{}.trex'", ext.name);
        SwarmLogger::info("orchestrator", &format!("Loading extension: {}", ext.name));

        if let Err(e) = conn.execute_batch(&load_sql) {
            let msg = format!("{}: load failed — {}", ext.name, e);
            SwarmLogger::error("orchestrator", &msg);
            statuses.push(msg);
            continue;
        }

        // Step 2: Start the service if host+port are configured
        let (host, port) = match (&ext.host, ext.port) {
            (Some(h), Some(p)) => (h.as_str(), p),
            _ => {
                let msg = format!("{}: loaded", ext.name);
                SwarmLogger::info("orchestrator", &msg);
                statuses.push(msg);
                continue;
            }
        };

        let start_sql = match start_function_sql(&ext.name, host, port) {
            Some(sql) => sql,
            None => {
                SwarmLogger::warn(
                    "orchestrator",
                    &format!(
                        "No start function mapping for extension '{}'; loaded only",
                        ext.name
                    ),
                );
                statuses.push(format!("{}: loaded (no start function)", ext.name));
                continue;
            }
        };

        SwarmLogger::info(
            "orchestrator",
            &format!("Starting service: {} on {}:{}", ext.name, host, port),
        );

        if let Err(e) = conn.execute_batch(&start_sql) {
            let msg = format!("{}: start failed — {}", ext.name, e);
            SwarmLogger::error("orchestrator", &msg);
            statuses.push(msg);
            continue;
        }

        // Step 3: Publish to gossip if running
        let registry = GossipRegistry::instance();
        if registry.is_running() {
            let gossip_key = format!("service:{}", ext.name);
            let gossip_value = format!(
                r#"{{"host": "{}", "port": {}, "status": "running"}}"#,
                host, port
            );

            if let Err(e) = registry.set_key(&gossip_key, &gossip_value) {
                SwarmLogger::warn(
                    "orchestrator",
                    &format!("Failed to publish service:{} to gossip: {}", ext.name, e),
                );
            }
        }

        let msg = format!("{}: started on {}:{}", ext.name, host, port);
        SwarmLogger::info("orchestrator", &msg);
        statuses.push(msg);
    }

    statuses
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- start_function_sql lookup tests ------------------------------------

    #[test]
    fn start_sql_flight() {
        let sql = start_function_sql("flight", "0.0.0.0", 8815).unwrap();
        assert_eq!(sql, "SELECT start_flight_server('0.0.0.0', 8815)");
    }

    #[test]
    fn start_sql_pgwire() {
        let sql = start_function_sql("pgwire", "127.0.0.1", 5432).unwrap();
        assert_eq!(sql, "SELECT start_pgwire_server('127.0.0.1', 5432, '', '')");
    }

    #[test]
    fn start_sql_trexas() {
        let sql = start_function_sql("trexas", "10.0.0.1", 9090).unwrap();
        assert_eq!(sql, "SELECT start_trexas_server('10.0.0.1', 9090)");
    }

    #[test]
    fn start_sql_unknown_returns_none() {
        assert!(start_function_sql("hana", "0.0.0.0", 1234).is_none());
        assert!(start_function_sql("chdb", "0.0.0.0", 1234).is_none());
        assert!(start_function_sql("llama", "0.0.0.0", 1234).is_none());
        assert!(start_function_sql("nonexistent", "0.0.0.0", 1234).is_none());
    }

    // -- orchestrate_extensions: load-only path -----------------------------
    //
    // Without a real DuckDB connection available via the OnceLock, the
    // orchestrator returns an error status for every extension.  We verify
    // that the early-exit "no shared connection" path produces one message
    // per extension.

    #[test]
    fn orchestrate_without_connection_returns_error_per_extension() {
        let extensions = vec![
            ExtensionConfig {
                name: "hana".to_string(),
                host: None,
                port: None,
            },
            ExtensionConfig {
                name: "flight".to_string(),
                host: Some("0.0.0.0".to_string()),
                port: Some(8815),
            },
        ];

        let statuses = orchestrate_extensions(&extensions);

        // One status message per extension
        assert_eq!(statuses.len(), 2);

        // Both should indicate the missing connection
        for status in &statuses {
            assert!(
                status.contains("no shared connection"),
                "unexpected status: {status}"
            );
        }
    }

    #[test]
    fn load_only_extension_has_no_start_sql() {
        // Extensions without host/port should never match a start function,
        // regardless of name.
        assert!(start_function_sql("hana", "0.0.0.0", 0).is_none());
    }
}
