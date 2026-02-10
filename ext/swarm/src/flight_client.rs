use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use futures::TryStreamExt;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::logging::SwarmLogger;

// ---------------------------------------------------------------------------
// FlightClient
// ---------------------------------------------------------------------------

/// A lightweight wrapper around an Arrow Flight gRPC client that connects to
/// a remote DuckDB Flight server and executes SQL queries via the DoGet RPC.
///
/// Each `FlightClient` holds an open gRPC channel to a single endpoint.
/// The channel is established once during [`FlightClient::connect`] and
/// reused for all subsequent queries.
#[derive(Debug)]
pub struct FlightClient {
    endpoint: String,
    client: FlightServiceClient<Channel>,
}

impl FlightClient {
    /// Open a gRPC channel to `endpoint` and return a ready-to-use client.
    ///
    /// The endpoint should include the scheme, e.g. `"http://10.0.0.1:50051"`.
    pub async fn connect(endpoint: &str) -> Result<Self, String> {
        SwarmLogger::debug(
            "flight-client",
            &format!("Connecting to Flight server at {endpoint}"),
        );

        let channel = Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| format!("Failed to connect to {endpoint}: invalid URI: {e}"))?
            .connect()
            .await
            .map_err(|e| format!("Failed to connect to {endpoint}: {e}"))?;

        let client = FlightServiceClient::new(channel);

        SwarmLogger::debug(
            "flight-client",
            &format!("Connected to Flight server at {endpoint}"),
        );

        Ok(Self {
            endpoint: endpoint.to_string(),
            client,
        })
    }

    /// Open a gRPC channel to `endpoint` with mutual TLS (mTLS) and return a
    /// ready-to-use client.
    ///
    /// The caller supplies paths to:
    /// * `cert_path` / `key_path` -- the client's own certificate and private
    ///   key (PEM-encoded).  These are presented to the server during the TLS
    ///   handshake so that the server can authenticate the client.
    /// * `ca_cert_path` -- the CA certificate (PEM-encoded) used to verify the
    ///   server's identity.
    ///
    /// The endpoint should use the `https://` scheme.
    pub async fn connect_with_tls(
        endpoint: &str,
        cert_path: &str,
        key_path: &str,
        ca_cert_path: &str,
    ) -> Result<Self, String> {
        SwarmLogger::debug(
            "flight-client",
            &format!("Connecting to Flight server at {endpoint} with mTLS"),
        );

        let cert = std::fs::read(cert_path)
            .map_err(|e| format!("Failed to read client certificate {cert_path}: {e}"))?;
        let key = std::fs::read(key_path)
            .map_err(|e| format!("Failed to read client key {key_path}: {e}"))?;
        let ca_cert = std::fs::read(ca_cert_path)
            .map_err(|e| format!("Failed to read CA certificate {ca_cert_path}: {e}"))?;

        let identity = Identity::from_pem(cert, key);
        let ca = Certificate::from_pem(ca_cert);

        let tls_config = ClientTlsConfig::new()
            .identity(identity)
            .ca_certificate(ca);

        let channel = Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| format!("Failed to connect to {endpoint}: invalid URI: {e}"))?
            .tls_config(tls_config)
            .map_err(|e| format!("Failed to configure TLS for {endpoint}: {e}"))?
            .connect()
            .await
            .map_err(|e| format!("Failed to connect to {endpoint} with TLS: {e}"))?;

        let client = FlightServiceClient::new(channel);

        SwarmLogger::debug(
            "flight-client",
            &format!("Connected to Flight server at {endpoint} with mTLS"),
        );

        Ok(Self {
            endpoint: endpoint.to_string(),
            client,
        })
    }

    /// Execute a SQL query on the connected Flight server via DoGet.
    ///
    /// The query is encoded as a JSON ticket: `{"query": "<sql>"}`.
    /// All returned `RecordBatch`es are collected into a `Vec` along with
    /// the schema extracted from the Flight stream.
    pub async fn execute_query(
        &mut self,
        sql: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), String> {
        SwarmLogger::debug(
            "flight-client",
            &format!("Executing query on {}: {sql}", self.endpoint),
        );

        let ticket_payload = serde_json::json!({ "query": sql }).to_string();
        let ticket = Ticket::new(ticket_payload.into_bytes());

        let response = self
            .client
            .do_get(ticket)
            .await
            .map_err(|e| format!("Flight query failed on {}: {e}", self.endpoint))?;

        let flight_stream = FlightRecordBatchStream::new_from_flight_data(
            response
                .into_inner()
                .map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
        );

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut schema: Option<SchemaRef> = None;

        futures::pin_mut!(flight_stream);

        while let Some(batch) = flight_stream.try_next().await.map_err(|e| {
            format!(
                "Failed to decode Flight response from {}: {e}",
                self.endpoint
            )
        })? {
            if schema.is_none() {
                schema = Some(batch.schema());
            }
            batches.push(batch);
        }

        let schema = schema.ok_or_else(|| {
            format!(
                "Failed to decode Flight response from {}: empty response with no schema",
                self.endpoint
            )
        })?;

        SwarmLogger::debug(
            "flight-client",
            &format!(
                "Query on {} returned {} batch(es), {} total row(s)",
                self.endpoint,
                batches.len(),
                batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            ),
        );

        Ok((schema, batches))
    }
}

// ---------------------------------------------------------------------------
// Convenience helper
// ---------------------------------------------------------------------------

/// One-shot helper: connect to `endpoint`, execute `sql`, and return the
/// resulting record batches.
///
/// This is the simplest entry-point for callers that do not need to reuse a
/// connection across multiple queries.
///
/// # Async runtime
///
/// This function is `async` and must be called within a tokio context.  When
/// calling from synchronous DuckDB extension code, wrap the call with the
/// tokio runtime handle, e.g. `runtime.block_on(query_node(endpoint, sql))`.
pub async fn query_node(endpoint: &str, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let mut client = FlightClient::connect(endpoint).await?;
    let (_schema, batches) = client.execute_query(sql).await?;
    Ok(batches)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies that connecting to a non-existent server produces the
    /// expected error message format.
    #[tokio::test]
    async fn connect_to_invalid_endpoint_fails() {
        let result = FlightClient::connect("http://127.0.0.1:1").await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.starts_with("Failed to connect to http://127.0.0.1:1:"),
            "unexpected error: {err}"
        );
    }

    /// Verifies that a completely invalid URI is caught early.
    #[tokio::test]
    async fn connect_to_malformed_uri_fails() {
        let result = FlightClient::connect("not a uri").await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("Failed to connect to not a uri"),
            "unexpected error: {err}"
        );
    }

    /// Verifies query_node surfaces connection errors.
    #[tokio::test]
    async fn query_node_connection_error() {
        let result = query_node("http://127.0.0.1:1", "SELECT 1").await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("Failed to connect to http://127.0.0.1:1"),
            "unexpected error: {err}"
        );
    }
}
