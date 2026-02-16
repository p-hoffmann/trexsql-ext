use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;

use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeId};
use duckdb::vtab::arrow::WritableVector;
use duckdb::vscalar::{ScalarFunctionSignature, VScalar};
use secrecy::SecretString;
use tokio::sync::oneshot;

use crate::credential_mask;
use crate::destination::DuckDbDestination;
use crate::pipeline_registry::{self, PipelineState};
use crate::store::DuckDbStore;

struct PipelineParams {
    batch_size: usize,
    batch_timeout_ms: u64,
    retry_delay_ms: u64,
    retry_max_attempts: u32,
}

impl Default for PipelineParams {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            batch_timeout_ms: 5000,
            retry_delay_ms: 10000,
            retry_max_attempts: 5,
        }
    }
}

pub struct EtlStartScalar;

impl VScalar for EtlStartScalar {
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
        let conn_vector = input.flat_vector(1);

        let name_slice =
            name_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let conn_slice =
            conn_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());

        let pipeline_name = duckdb::types::DuckString::new(&mut { name_slice[0] })
            .as_str()
            .to_string();
        let connection_string = duckdb::types::DuckString::new(&mut { conn_slice[0] })
            .as_str()
            .to_string();

        let params = if input.num_columns() >= 6 {
            let batch_size_vector = input.flat_vector(2);
            let batch_timeout_vector = input.flat_vector(3);
            let retry_delay_vector = input.flat_vector(4);
            let retry_max_vector = input.flat_vector(5);

            let batch_size_slice = batch_size_vector.as_slice_with_len::<i32>(input.len());
            let batch_timeout_slice = batch_timeout_vector.as_slice_with_len::<i32>(input.len());
            let retry_delay_slice = retry_delay_vector.as_slice_with_len::<i32>(input.len());
            let retry_max_slice = retry_max_vector.as_slice_with_len::<i32>(input.len());

            let batch_size = batch_size_slice[0];
            let batch_timeout_ms = batch_timeout_slice[0];
            let retry_delay_ms = retry_delay_slice[0];
            let retry_max_attempts = retry_max_slice[0];

            if batch_size <= 0 {
                return Err("batch_size must be greater than 0".into());
            }
            if batch_timeout_ms <= 0 {
                return Err("batch_timeout_ms must be greater than 0".into());
            }
            if retry_delay_ms < 0 {
                return Err("retry_delay_ms must be >= 0".into());
            }
            if retry_max_attempts < 0 {
                return Err("retry_max_attempts must be >= 0".into());
            }

            PipelineParams {
                batch_size: batch_size as usize,
                batch_timeout_ms: batch_timeout_ms as u64,
                retry_delay_ms: retry_delay_ms as u64,
                retry_max_attempts: retry_max_attempts as u32,
            }
        } else {
            PipelineParams::default()
        };

        let response = start_pipeline(&pipeline_name, &connection_string, params)?;

        let flat_vector = output.flat_vector();
        flat_vector.insert(0, &response);
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![
            ScalarFunctionSignature::exact(
                vec![
                    LogicalTypeId::Varchar.into(),
                    LogicalTypeId::Varchar.into(),
                ],
                LogicalTypeId::Varchar.into(),
            ),
            ScalarFunctionSignature::exact(
                vec![
                    LogicalTypeId::Varchar.into(),
                    LogicalTypeId::Varchar.into(),
                    LogicalTypeId::Integer.into(),
                    LogicalTypeId::Integer.into(),
                    LogicalTypeId::Integer.into(),
                    LogicalTypeId::Integer.into(),
                ],
                LogicalTypeId::Varchar.into(),
            ),
        ]
    }
}

fn start_pipeline(
    pipeline_name: &str,
    connection_string: &str,
    params: PipelineParams,
) -> Result<String, Box<dyn std::error::Error>> {
    let host = credential_mask::extract_param(connection_string, "host")
        .unwrap_or("localhost")
        .to_string();
    let port: u16 = credential_mask::extract_param(connection_string, "port")
        .unwrap_or("5432")
        .parse()?;
    let dbname = credential_mask::extract_param(connection_string, "dbname")
        .unwrap_or("postgres")
        .to_string();
    let username = credential_mask::extract_param(connection_string, "user")
        .unwrap_or("postgres")
        .to_string();
    let password = credential_mask::extract_param(connection_string, "password")
        .map(|p| SecretString::from(p.to_string()));
    let publication = credential_mask::extract_param(connection_string, "publication")
        .ok_or("connection string must include 'publication=<name>'")?
        .to_string();

    let masked_conn = credential_mask::mask_password(connection_string);

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    pipeline_registry::registry()
        .reserve(
            pipeline_name,
            &masked_conn,
            &publication,
            true,
            shutdown_tx,
        )
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    let shared_conn = crate::get_shared_connection()
        .ok_or("Extension connection not initialized")?;

    let name = pipeline_name.to_string();
    let name_for_thread = pipeline_name.to_string();

    let thread_result = thread::Builder::new()
        .name(format!("etl-pipeline-{}", pipeline_name))
        .spawn(move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;

            rt.block_on(async move {
                pipeline_registry::registry().update_state(&name, PipelineState::Starting);

                let schemas = Arc::new(Mutex::new(HashMap::new()));
                let destination =
                    DuckDbDestination::new(shared_conn.clone(), name.clone(), schemas.clone());
                let store = DuckDbStore::new(shared_conn.clone(), name.clone(), schemas);

                let pg_config = etl_lib::config::PgConnectionConfig {
                    host,
                    port,
                    name: dbname,
                    username,
                    password,
                    tls: etl_lib::config::TlsConfig {
                        trusted_root_certs: String::new(),
                        enabled: false,
                    },
                    keepalive: None,
                };

                let pipeline_config = etl_lib::config::PipelineConfig {
                    id: 1,
                    publication_name: publication,
                    pg_connection: pg_config,
                    batch: etl_lib::config::BatchConfig {
                        max_size: params.batch_size,
                        max_fill_ms: params.batch_timeout_ms,
                    },
                    table_error_retry_delay_ms: params.retry_delay_ms,
                    table_error_retry_max_attempts: params.retry_max_attempts,
                    max_table_sync_workers: 4,
                    table_sync_copy: etl_lib::config::TableSyncCopyConfig::IncludeAllTables,
                    invalidated_slot_behavior: etl_lib::config::InvalidatedSlotBehavior::Error,
                };

                let mut pipeline = etl_lib::pipeline::Pipeline::new(
                    pipeline_config,
                    store,
                    destination,
                );

                pipeline_registry::registry()
                    .update_state(&name, PipelineState::Snapshotting);

                tokio::select! {
                    result = pipeline.start() => {
                        match result {
                            Ok(()) => {
                                pipeline_registry::registry()
                                    .update_state(&name, PipelineState::Stopped);
                            }
                            Err(e) => {
                                pipeline_registry::registry()
                                    .set_error(&name, &format!("{}", e));
                                return Err(format!("{}", e).into());
                            }
                        }
                    }
                    _ = shutdown_rx => {
                        pipeline_registry::registry()
                            .update_state(&name, PipelineState::Stopping);
                    }
                }

                Ok(())
            })
        });

    match thread_result {
        Ok(handle) => {
            pipeline_registry::registry().set_thread_handle(&name_for_thread, handle);
        }
        Err(e) => {
            pipeline_registry::registry().deregister(&name_for_thread);
            return Err(format!("Failed to spawn pipeline thread: {}", e).into());
        }
    }

    Ok(format!("Pipeline '{}' started", pipeline_name))
}
