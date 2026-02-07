use crate::config::ExtensionConfig;
use crate::gossip::GossipRegistry;
use crate::logging::SwarmLogger;
use crate::service_functions::get_start_service_sql;

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

        // Step 2: Start the service if config is provided
        let config_json = match &ext.config {
            Some(cfg) => serde_json::to_string(cfg).unwrap_or_else(|_| "{}".to_string()),
            None => {
                let msg = format!("{}: loaded", ext.name);
                SwarmLogger::info("orchestrator", &msg);
                statuses.push(msg);
                continue;
            }
        };

        let start_sql = match get_start_service_sql(&ext.name, &config_json) {
            Ok(Some(sql)) => sql,
            Ok(None) => {
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
            Err(e) => {
                let msg = format!("{}: config error — {}", ext.name, e);
                SwarmLogger::error("orchestrator", &msg);
                statuses.push(msg);
                continue;
            }
        };

        // Extract host/port from config for logging and gossip
        let cfg_val: serde_json::Value =
            serde_json::from_str(&config_json).unwrap_or_default();
        let host = cfg_val["host"].as_str().unwrap_or("");
        let port = cfg_val["port"].as_u64().unwrap_or(0);

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
            let gossip_value = serde_json::json!({
                "host": host,
                "port": port,
                "status": "running",
                "config": cfg_val
            })
            .to_string();

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

    // -- get_start_service_sql lookup tests (via service_functions) ----------

    #[test]
    fn start_sql_flight() {
        let sql = get_start_service_sql("flight", r#"{"host":"0.0.0.0","port":8815}"#)
            .unwrap()
            .unwrap();
        assert_eq!(sql, "SELECT start_flight_server('0.0.0.0', 8815)");
    }

    #[test]
    fn start_sql_flight_tls() {
        let sql = get_start_service_sql(
            "flight",
            r#"{"host":"0.0.0.0","port":8815,"cert_path":"/x/cert.pem","key_path":"/x/key.pem","ca_cert_path":"/x/ca.pem"}"#,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            sql,
            "SELECT start_flight_server_tls('0.0.0.0', 8815, '/x/cert.pem', '/x/key.pem', '/x/ca.pem')"
        );
    }

    #[test]
    fn start_sql_pgwire() {
        let sql = get_start_service_sql("pgwire", r#"{"host":"127.0.0.1","port":5432}"#)
            .unwrap()
            .unwrap();
        assert_eq!(
            sql,
            "SELECT start_pgwire_server('127.0.0.1', 5432, '', '')"
        );
    }

    #[test]
    fn start_sql_pgwire_with_password() {
        let sql = get_start_service_sql(
            "pgwire",
            r#"{"host":"127.0.0.1","port":5432,"password":"secret"}"#,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            sql,
            "SELECT start_pgwire_server('127.0.0.1', 5432, 'secret', '')"
        );
    }

    #[test]
    fn start_sql_trexas() {
        let json = r#"{"host":"10.0.0.1","port":9090}"#;
        let sql = get_start_service_sql("trexas", json).unwrap().unwrap();
        let escaped = json.replace('\'', "''");
        assert_eq!(
            sql,
            format!("SELECT trex_start_server_with_config('{escaped}')")
        );
    }

    #[test]
    fn start_sql_chdb_no_path() {
        let sql = get_start_service_sql("chdb", "{}").unwrap().unwrap();
        assert_eq!(sql, "SELECT chdb_start_database()");
    }

    #[test]
    fn start_sql_chdb_with_path() {
        let sql =
            get_start_service_sql("chdb", r#"{"data_path":"/tmp/chdb"}"#)
                .unwrap()
                .unwrap();
        assert_eq!(sql, "SELECT chdb_start_database('/tmp/chdb')");
    }

    #[test]
    fn start_sql_unknown_returns_none() {
        assert!(get_start_service_sql("hana", "{}").unwrap().is_none());
        assert!(get_start_service_sql("llama", "{}").unwrap().is_none());
        assert!(get_start_service_sql("nonexistent", "{}").unwrap().is_none());
    }

    #[test]
    fn start_sql_invalid_json_returns_err() {
        assert!(get_start_service_sql("flight", "not json").is_err());
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
                config: None,
            },
            ExtensionConfig {
                name: "flight".to_string(),
                config: Some(serde_json::json!({"host": "0.0.0.0", "port": 8815})),
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
}
