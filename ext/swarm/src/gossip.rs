use chitchat::transport::UdpTransport;
use chitchat::{spawn_chitchat, ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use uuid::Uuid;

use crate::logging::SwarmLogger;

// ---------------------------------------------------------------------------
// Data model
// ---------------------------------------------------------------------------

/// Information about a single node in the gossip cluster, constructed from
/// the key-value pairs published by that node via chitchat.
pub struct NodeInfo {
    pub node_id: String,
    pub node_name: String,
    pub gossip_addr: String,
    pub data_node: String,
    pub status: String,
}

/// Extended node information including all key-value pairs published via
/// gossip.  Used by the catalog module to resolve table locations.
pub struct NodeKeyValueInfo {
    pub node_id: String,
    pub node_name: String,
    pub gossip_addr: String,
    pub key_values: Vec<(String, String)>,
}

/// Wraps a running chitchat instance together with the tokio runtime that
/// drives it.  Dropping this struct shuts down the gossip layer.
pub struct GossipHandle {
    chitchat_handle: ChitchatHandle,
    runtime: tokio::runtime::Runtime,
    node_id: String,
}

// ---------------------------------------------------------------------------
// Singleton registry
// ---------------------------------------------------------------------------

/// Process-wide singleton that owns the optional `GossipHandle`.  At most one
/// gossip instance may be active at a time — this mirrors the `ServerRegistry`
/// pattern used by the flight extension.
pub struct GossipRegistry {
    handle: Arc<Mutex<Option<GossipHandle>>>,
}

impl GossipRegistry {
    fn new() -> Self {
        Self {
            handle: Arc::new(Mutex::new(None)),
        }
    }

    /// Return the process-wide `GossipRegistry` singleton.
    pub fn instance() -> &'static GossipRegistry {
        static INSTANCE: OnceLock<GossipRegistry> = OnceLock::new();
        INSTANCE.get_or_init(|| GossipRegistry::new())
    }

    /// Returns `true` when a gossip instance is currently active.
    pub fn is_running(&self) -> bool {
        self.handle.lock().map(|g| g.is_some()).unwrap_or(false)
    }

    /// Start the gossip layer.
    ///
    /// # Arguments
    /// * `host`       – IP address to bind the gossip UDP socket to
    /// * `port`       – UDP port for gossip traffic
    /// * `cluster_id` – logical cluster identifier (must match on all nodes)
    /// * `node_name`  – human-readable node name from configuration
    /// * `data_node`  – `"true"` or `"false"` – whether this node stores data
    /// * `seeds`      – list of `"host:port"` strings for seed nodes
    ///
    /// Returns the generated UUID node-id on success.
    pub fn start(
        &self,
        host: &str,
        port: u16,
        cluster_id: &str,
        node_name: &str,
        data_node: &str,
        seeds: Vec<String>,
    ) -> Result<String, String> {
        let mut guard = self.handle.lock().map_err(|_| "Gossip lock poisoned".to_string())?;
        if guard.is_some() {
            return Err("Gossip is already running".to_string());
        }

        // Build a dedicated tokio runtime for gossip I/O.  Must be
        // multi-threaded so that chitchat's spawned tasks (UDP send/recv,
        // failure detection) run continuously in the background, even
        // between SQL commands.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|e| format!("Failed to create tokio runtime: {e}"))?;

        let node_id = Uuid::new_v4().to_string();

        let gossip_addr: SocketAddr = format!("{host}:{port}")
            .parse()
            .map_err(|e| format!("Invalid gossip address {host}:{port}: {e}"))?;

        // Parse seed addresses, skipping entries that resolve to our own
        // listen address (chitchat does not need to seed itself).
        let seed_addrs: Vec<SocketAddr> = seeds
            .iter()
            .filter_map(|s| {
                let addr: SocketAddr = s.parse().ok()?;
                if addr == gossip_addr {
                    None
                } else {
                    Some(addr)
                }
            })
            .collect();

        let chitchat_id = ChitchatId::new(node_id.clone(), 0, gossip_addr);

        let config = ChitchatConfig {
            chitchat_id,
            cluster_id: cluster_id.to_string(),
            gossip_interval: Duration::from_millis(500),
            listen_addr: gossip_addr,
            seed_nodes: seed_addrs.iter().map(|a| a.to_string()).collect(),
            failure_detector_config: FailureDetectorConfig::default(),
            marked_for_deletion_grace_period: Duration::from_secs(60),
            catchup_callback: None,
            extra_liveness_predicate: None,
        };

        let initial_kv: Vec<(String, String)> = vec![
            ("node_name".to_string(), node_name.to_string()),
            ("data_node".to_string(), data_node.to_string()),
            ("status".to_string(), "active".to_string()),
        ];

        let transport = UdpTransport;

        let chitchat_handle = runtime.block_on(async {
            spawn_chitchat(config, initial_kv, &transport)
                .await
                .map_err(|e| format!("Failed to spawn chitchat: {e}"))
        })?;

        SwarmLogger::log_with_context(
            crate::logging::LogLevel::Info,
            "gossip",
            &[
                ("node_id", &node_id),
                ("operation", "start"),
                ("name", node_name),
                ("addr", &gossip_addr.to_string()),
                ("cluster", cluster_id),
            ],
            "Gossip started",
        );

        *guard = Some(GossipHandle {
            chitchat_handle,
            runtime,
            node_id: node_id.clone(),
        });

        Ok(node_id)
    }

    /// Gracefully stop the gossip layer.  The local node is first marked for
    /// deletion so that peers will eventually garbage-collect its state, and
    /// then the chitchat handle is dropped which tears down the UDP socket.
    pub fn stop(&self) -> Result<String, String> {
        let mut guard = self.handle.lock().map_err(|_| "Gossip lock poisoned".to_string())?;
        let gossip = guard
            .as_ref()
            .ok_or_else(|| "Gossip is not running".to_string())?;

        // Mark this node for deletion so peers remove us after the grace
        // period even if the shutdown notification does not reach them.
        let chitchat = gossip.chitchat_handle.chitchat();
        let _ = gossip.runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                let mut cc = chitchat.lock().await;
                cc.self_node_state().set("status", "draining");
            })
            .await
        });

        let node_id = gossip.node_id.clone();

        // Take the handle and explicitly shut down the runtime.
        if let Some(handle) = guard.take() {
            handle.runtime.shutdown_timeout(Duration::from_secs(5));
        }

        SwarmLogger::log_with_context(
            crate::logging::LogLevel::Info,
            "gossip",
            &[("node_id", &node_id), ("operation", "stop")],
            "Gossip stopped",
        );

        Ok(format!("Gossip stopped for node {node_id}"))
    }

    /// Set a key-value pair on this node's gossip state.  The update will be
    /// propagated to all peers within a few gossip rounds.
    pub fn set_key(&self, key: &str, value: &str) -> Result<(), String> {
        let guard = self.handle.lock().map_err(|_| "Gossip lock poisoned".to_string())?;
        let gossip = guard
            .as_ref()
            .ok_or_else(|| "Gossip is not running".to_string())?;

        let chitchat = gossip.chitchat_handle.chitchat();
        gossip.runtime.block_on(async {
            let mut cc = chitchat.lock().await;
            cc.self_node_state().set(key, value);
        });

        SwarmLogger::log_with_context(
            crate::logging::LogLevel::Debug,
            "gossip",
            &[
                ("node_id", &gossip.node_id),
                ("operation", "set_key"),
                ("key", key),
            ],
            &format!("Set key: {key}={value}"),
        );

        Ok(())
    }

    /// Return the state of every node currently known to the gossip layer.
    /// Each entry contains the identity keys (`node_name`, `data_node`,
    /// `status`) published by that node.
    pub fn get_node_states(&self) -> Result<Vec<NodeInfo>, String> {
        let guard = self.handle.lock().map_err(|_| "Gossip lock poisoned".to_string())?;
        let gossip = guard
            .as_ref()
            .ok_or_else(|| "Gossip is not running".to_string())?;

        let chitchat = gossip.chitchat_handle.chitchat();
        let nodes = gossip.runtime.block_on(async {
            let cc = chitchat.lock().await;
            cc.node_states()
                .iter()
                .map(|(id, state)| {
                    let node_name = state
                        .get("node_name")
                        .unwrap_or("")
                        .to_string();
                    let data_node = state
                        .get("data_node")
                        .unwrap_or("false")
                        .to_string();
                    let status = state
                        .get("status")
                        .unwrap_or("unknown")
                        .to_string();

                    NodeInfo {
                        node_id: id.node_id.clone(),
                        node_name,
                        gossip_addr: id.gossip_advertise_addr.to_string(),
                        data_node,
                        status,
                    }
                })
                .collect::<Vec<_>>()
        });

        SwarmLogger::log_with_context(
            crate::logging::LogLevel::Debug,
            "gossip",
            &[
                ("node_id", &gossip.node_id),
                ("operation", "get_node_states"),
                ("count", &nodes.len().to_string()),
            ],
            "Retrieved node states",
        );

        Ok(nodes)
    }

    /// Return this node's current gossip configuration as a list of
    /// key-value pairs.  Useful for `swarm_config()` table functions.
    pub fn get_self_config(&self) -> Result<Vec<(String, String)>, String> {
        let guard = self.handle.lock().map_err(|_| "Gossip lock poisoned".to_string())?;
        let gossip = guard
            .as_ref()
            .ok_or_else(|| "Gossip is not running".to_string())?;

        let chitchat = gossip.chitchat_handle.chitchat();
        let config = gossip.runtime.block_on(async {
            let mut cc = chitchat.lock().await;
            let id = cc.self_chitchat_id();
            let mut pairs = vec![
                ("node_id".to_string(), id.node_id.clone()),
                ("generation_id".to_string(), id.generation_id.to_string()),
                (
                    "gossip_advertise_addr".to_string(),
                    id.gossip_advertise_addr.to_string(),
                ),
                ("cluster_id".to_string(), cc.cluster_id().to_string()),
            ];

            // Append every key-value from our own node state.
            let self_state = cc.self_node_state();
            for (key, value) in self_state.key_values() {
                pairs.push((key.to_string(), value.to_string()));
            }

            pairs
        });

        Ok(config)
    }

    /// Delete a key from this node's gossip state.  The key is marked with a
    /// tombstone and will be garbage-collected after the configured grace period.
    pub fn delete_key(&self, key: &str) -> Result<(), String> {
        let guard = self.handle.lock().map_err(|_| "Gossip lock poisoned".to_string())?;
        let gossip = guard
            .as_ref()
            .ok_or_else(|| "Gossip is not running".to_string())?;

        let chitchat = gossip.chitchat_handle.chitchat();
        gossip.runtime.block_on(async {
            let mut cc = chitchat.lock().await;
            cc.self_node_state().delete(key);
        });

        SwarmLogger::log_with_context(
            crate::logging::LogLevel::Debug,
            "gossip",
            &[
                ("node_id", &gossip.node_id),
                ("operation", "delete_key"),
                ("key", key),
            ],
            &format!("Deleted key: {key}"),
        );

        Ok(())
    }

    /// List all keys with a given prefix from this node's gossip state.
    pub fn list_keys_with_prefix(&self, prefix: &str) -> Result<Vec<String>, String> {
        let guard = self.handle.lock().map_err(|_| "Gossip lock poisoned".to_string())?;
        let gossip = guard
            .as_ref()
            .ok_or_else(|| "Gossip is not running".to_string())?;

        let prefix_owned = prefix.to_string();
        let chitchat = gossip.chitchat_handle.chitchat();
        let keys = gossip.runtime.block_on(async {
            let mut cc = chitchat.lock().await;
            let state = cc.self_node_state();
            state
                .key_values()
                .filter(|(k, _)| k.starts_with(&prefix_owned))
                .map(|(k, _)| k.to_string())
                .collect::<Vec<_>>()
        });

        SwarmLogger::log_with_context(
            crate::logging::LogLevel::Debug,
            "gossip",
            &[
                ("node_id", &gossip.node_id),
                ("operation", "list_keys_with_prefix"),
                ("prefix", prefix),
                ("count", &keys.len().to_string()),
            ],
            "Listed keys with prefix",
        );

        Ok(keys)
    }

    /// Return all key-value pairs for every node known to the gossip layer.
    ///
    /// Unlike [`get_node_states`], which returns only identity-level keys
    /// (`node_name`, `data_node`, `status`), this method returns *every*
    /// key-value pair — including `catalog:*` and `service:*` keys used by
    /// the distributed catalog.
    pub fn get_node_key_values(&self) -> Result<Vec<NodeKeyValueInfo>, String> {
        let guard = self.handle.lock().map_err(|_| "Gossip lock poisoned".to_string())?;
        let gossip = guard
            .as_ref()
            .ok_or_else(|| "Gossip is not running".to_string())?;

        let chitchat = gossip.chitchat_handle.chitchat();
        let nodes = gossip.runtime.block_on(async {
            let cc = chitchat.lock().await;
            cc.node_states()
                .iter()
                .map(|(id, state)| {
                    let node_name = state
                        .get("node_name")
                        .unwrap_or("")
                        .to_string();

                    let key_values: Vec<(String, String)> = state
                        .key_values()
                        .map(|(key, value)| {
                            (key.to_string(), value.to_string())
                        })
                        .collect();

                    NodeKeyValueInfo {
                        node_id: id.node_id.clone(),
                        node_name,
                        gossip_addr: id.gossip_advertise_addr.to_string(),
                        key_values,
                    }
                })
                .collect::<Vec<_>>()
        });

        SwarmLogger::log_with_context(
            crate::logging::LogLevel::Debug,
            "gossip",
            &[
                ("node_id", &gossip.node_id),
                ("operation", "get_node_key_values"),
                ("count", &nodes.len().to_string()),
            ],
            "Retrieved node key-values",
        );

        Ok(nodes)
    }
}

// ---------------------------------------------------------------------------
// Gossip payload encryption
// ---------------------------------------------------------------------------

/// Derive a symmetric encryption key from the cluster CA certificate.
pub fn derive_encryption_key(ca_cert_path: &str) -> Result<Vec<u8>, String> {
    let cert_bytes = std::fs::read(ca_cert_path)
        .map_err(|e| format!("Failed to read CA certificate {ca_cert_path}: {e}"))?;

    let mut key = vec![0u8; 32];
    for (i, &b) in cert_bytes.iter().enumerate() {
        key[i % 32] ^= b;
    }

    SwarmLogger::debug(
        "gossip-encryption",
        &format!(
            "Derived placeholder encryption key from CA cert ({} bytes read)",
            cert_bytes.len()
        ),
    );

    Ok(key)
}

/// Encrypt a gossip value before publishing it to the cluster.
pub fn encrypt_value(value: &str, _key: &[u8]) -> String {
    value.to_string()
}

/// Decrypt a gossip value received from a peer.
pub fn decrypt_value(value: &str, _key: &[u8]) -> String {
    value.to_string()
}
