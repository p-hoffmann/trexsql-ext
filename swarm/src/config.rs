use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::env;
use std::net::SocketAddr;

// ---------------------------------------------------------------------------
// Data model
// ---------------------------------------------------------------------------

/// Top-level cluster configuration parsed from the `SWARM_CONFIG` env var.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub cluster_id: String,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    pub nodes: HashMap<String, NodeConfig>,
}

/// Cluster-wide TLS configuration (shared CA certificate).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    pub ca_cert: String,
}

/// Per-node TLS configuration (certificate + private key).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeTlsConfig {
    pub cert: String,
    pub key: String,
}

/// Configuration for a single node in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub gossip_addr: String,
    #[serde(default = "default_true")]
    pub data_node: bool,
    #[serde(default)]
    pub tls: Option<NodeTlsConfig>,
    #[serde(default)]
    pub extensions: Vec<ExtensionConfig>,
}

fn default_true() -> bool {
    true
}

/// An extension hosted by a node (e.g. a Flight SQL endpoint).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtensionConfig {
    pub name: String,
    pub host: Option<String>,
    pub port: Option<u16>,
    #[serde(default)]
    pub password: Option<String>,
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

impl ClusterConfig {
    /// Read `SWARM_CONFIG` from the environment, parse it as JSON, and
    /// validate the result.  Returns `Err` with a descriptive message on
    /// any failure so the caller can fall back to manual / single-node mode.
    pub fn from_env() -> Result<Self, String> {
        let raw = env::var("SWARM_CONFIG").map_err(|_| {
            "SWARM_CONFIG environment variable is not set".to_string()
        })?;

        Self::from_json(&raw)
    }

    /// Parse a `ClusterConfig` from a raw JSON string and validate it.
    pub fn from_json(json: &str) -> Result<Self, String> {
        let config: ClusterConfig = serde_json::from_str(json).map_err(|e| {
            let msg = format!("Failed to parse SWARM_CONFIG JSON: {e}");
            eprintln!("{msg}");
            msg
        })?;

        config.validate()?;
        Ok(config)
    }

    /// Validate the parsed configuration, returning a descriptive error
    /// string on the first problem found.
    pub fn validate(&self) -> Result<(), String> {
        // 1. cluster_id must be non-empty
        if self.cluster_id.trim().is_empty() {
            return Err("cluster_id must be non-empty".to_string());
        }

        // 2 & 3. Validate gossip addresses and check uniqueness
        let mut seen_addrs: HashSet<SocketAddr> = HashSet::new();

        for (name, node) in &self.nodes {
            let addr: SocketAddr = node.gossip_addr.parse().map_err(|e| {
                format!(
                    "node '{name}': gossip_addr '{}' is not a valid SocketAddr: {e}",
                    node.gossip_addr
                )
            })?;

            if !seen_addrs.insert(addr) {
                return Err(format!(
                    "node '{name}': gossip_addr '{}' is a duplicate (already used by another node)",
                    node.gossip_addr
                ));
            }

            // 4. For each extension, if host is set then port must also be set
            for ext in &node.extensions {
                if ext.host.is_some() && ext.port.is_none() {
                    return Err(format!(
                        "node '{name}', extension '{}': host is set but port is missing",
                        ext.name
                    ));
                }
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Node identity helpers
// ---------------------------------------------------------------------------

/// Read the current node's logical name from the `SWARM_NODE` env var.
pub fn get_node_name() -> Result<String, String> {
    env::var("SWARM_NODE")
        .map_err(|_| "SWARM_NODE environment variable is not set".to_string())
}

/// Look up the current node (identified by `SWARM_NODE`) inside the given
/// cluster configuration.  Returns `Some((&node_name, &NodeConfig))` when
/// found, or `None` when `SWARM_NODE` is unset or the name does not appear
/// in the config.
pub fn get_this_node_config(config: &ClusterConfig) -> Option<(&str, &NodeConfig)> {
    let name = env::var("SWARM_NODE").ok()?;
    config
        .nodes
        .get_key_value(name.as_str())
        .map(|(k, v)| (k.as_str(), v))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal valid two-node config used by several tests.
    fn sample_json() -> &'static str {
        r#"{
            "cluster_id": "test-cluster",
            "nodes": {
                "node-a": {
                    "gossip_addr": "127.0.0.1:7100",
                    "extensions": [
                        { "name": "flight", "host": "0.0.0.0", "port": 8815 }
                    ]
                },
                "node-b": {
                    "gossip_addr": "127.0.0.1:7101",
                    "data_node": false
                }
            }
        }"#
    }

    #[test]
    fn parse_valid_config() {
        let cfg = ClusterConfig::from_json(sample_json()).unwrap();
        assert_eq!(cfg.cluster_id, "test-cluster");
        assert_eq!(cfg.nodes.len(), 2);

        let a = &cfg.nodes["node-a"];
        assert!(a.data_node); // default_true
        assert_eq!(a.extensions.len(), 1);
        assert_eq!(a.extensions[0].name, "flight");
        assert_eq!(a.extensions[0].port, Some(8815));

        let b = &cfg.nodes["node-b"];
        assert!(!b.data_node);
        assert!(b.extensions.is_empty());
    }

    #[test]
    fn parse_with_tls() {
        let json = r#"{
            "cluster_id": "secure",
            "tls": { "ca_cert": "/etc/ssl/ca.pem" },
            "nodes": {
                "n1": {
                    "gossip_addr": "10.0.0.1:7100",
                    "tls": { "cert": "/etc/ssl/n1.pem", "key": "/etc/ssl/n1.key" }
                }
            }
        }"#;
        let cfg = ClusterConfig::from_json(json).unwrap();
        assert_eq!(cfg.tls.as_ref().unwrap().ca_cert, "/etc/ssl/ca.pem");
        let n1 = &cfg.nodes["n1"];
        assert_eq!(n1.tls.as_ref().unwrap().cert, "/etc/ssl/n1.pem");
        assert_eq!(n1.tls.as_ref().unwrap().key, "/etc/ssl/n1.key");
    }

    #[test]
    fn empty_cluster_id_rejected() {
        let json = r#"{ "cluster_id": "  ", "nodes": {} }"#;
        let err = ClusterConfig::from_json(json).unwrap_err();
        assert!(err.contains("cluster_id"), "error was: {err}");
    }

    #[test]
    fn invalid_gossip_addr_rejected() {
        let json = r#"{
            "cluster_id": "c",
            "nodes": { "n": { "gossip_addr": "not-a-socket-addr" } }
        }"#;
        let err = ClusterConfig::from_json(json).unwrap_err();
        assert!(err.contains("gossip_addr"), "error was: {err}");
        assert!(err.contains("not-a-socket-addr"), "error was: {err}");
    }

    #[test]
    fn duplicate_gossip_addr_rejected() {
        let json = r#"{
            "cluster_id": "c",
            "nodes": {
                "a": { "gossip_addr": "127.0.0.1:7100" },
                "b": { "gossip_addr": "127.0.0.1:7100" }
            }
        }"#;
        let err = ClusterConfig::from_json(json).unwrap_err();
        assert!(err.contains("duplicate"), "error was: {err}");
    }

    #[test]
    fn host_without_port_rejected() {
        let json = r#"{
            "cluster_id": "c",
            "nodes": {
                "n": {
                    "gossip_addr": "127.0.0.1:7100",
                    "extensions": [{ "name": "x", "host": "0.0.0.0" }]
                }
            }
        }"#;
        let err = ClusterConfig::from_json(json).unwrap_err();
        assert!(err.contains("port"), "error was: {err}");
    }

    #[test]
    fn malformed_json_returns_err() {
        let result = ClusterConfig::from_json("{ not valid json }}}");
        assert!(result.is_err());
    }

    #[test]
    fn get_node_name_from_env() {
        env::set_var("SWARM_NODE", "node-a");
        assert_eq!(get_node_name().unwrap(), "node-a");
        env::remove_var("SWARM_NODE");
    }

    #[test]
    fn get_this_node_config_returns_matching_node() {
        let cfg = ClusterConfig::from_json(sample_json()).unwrap();

        env::set_var("SWARM_NODE", "node-b");
        let (name, node) = get_this_node_config(&cfg).unwrap();
        assert_eq!(name, "node-b");
        assert!(!node.data_node);
        env::remove_var("SWARM_NODE");
    }

    #[test]
    fn get_this_node_config_returns_none_for_unknown() {
        let cfg = ClusterConfig::from_json(sample_json()).unwrap();

        env::set_var("SWARM_NODE", "no-such-node");
        assert!(get_this_node_config(&cfg).is_none());
        env::remove_var("SWARM_NODE");
    }
}
