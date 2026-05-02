use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::{json, Value};
use std::sync::Arc;

use crate::error::AppError;
use crate::fhir::bundle_processor;
use crate::handlers::upsert;
use crate::query_executor::{QueryResult, RequestConn};
use crate::schema::sql_builder;
use crate::sql_safety::validate_dataset_id;
use crate::state::AppState;

const MAX_BUNDLE_ENTRIES: usize = 10_000;

pub async fn process_bundle(
    State(state): State<Arc<AppState>>,
    Path(dataset_id): Path<String>,
    Json(body): Json<Value>,
) -> Result<impl IntoResponse, AppError> {
    validate_dataset_id(&dataset_id)?;

    let rt = body
        .get("resourceType")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if rt != "Bundle" {
        return Err(AppError::BadRequest(
            "Expected a FHIR Bundle resource".to_string(),
        ));
    }

    let bundle_type = body
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    match bundle_type {
        "transaction" => process_transaction(state, &dataset_id, &body).await,
        "batch" => process_batch(state, &dataset_id, &body).await,
        _ => Err(AppError::BadRequest(format!(
            "Unsupported Bundle type: '{}'. Must be 'transaction' or 'batch'",
            bundle_type
        ))),
    }
}

async fn process_transaction(
    state: Arc<AppState>,
    dataset_id: &str,
    bundle: &Value,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let entries = bundle_processor::process_bundle_entries(bundle, MAX_BUNDLE_ENTRIES)
        .map_err(|e| AppError::BadRequest(e))?;

    if entries.is_empty() {
        return Ok((
            StatusCode::OK,
            Json(json!({
                "resourceType": "Bundle",
                "type": "transaction-response",
                "entry": []
            })),
        ));
    }

    let schema_name = state.qualified_schema(dataset_id);

    // All transaction queries share one connection so BEGIN/COMMIT/ROLLBACK
    // and intermediate writes see consistent state.
    let conn = state.new_request_conn().map_err(AppError::Internal)?;

    if let QueryResult::Error(e) = conn.execute("BEGIN TRANSACTION".to_string()).await {
        eprintln!("[fhir] Failed to begin transaction: {}", e);
        return Err(AppError::Internal(
            "Failed to begin transaction".to_string(),
        ));
    }

    let mut response_entries = Vec::new();

    for entry in &entries {
        match process_single_entry(&state, &schema_name, dataset_id, entry, Some(&conn)).await {
            Ok(resp_entry) => {
                response_entries.push(resp_entry);
            }
            Err(e) => {
                let _ = conn.execute("ROLLBACK".to_string()).await;
                return Err(AppError::BadRequest(format!(
                    "Transaction failed on {}/{}: {}",
                    entry.resource_type, entry.server_id, e
                )));
            }
        }
    }

    if let QueryResult::Error(e) = conn.execute("COMMIT".to_string()).await {
        eprintln!("[fhir] Failed to commit transaction: {}", e);
        return Err(AppError::Internal(
            "Failed to commit transaction".to_string(),
        ));
    }

    Ok((
        StatusCode::OK,
        Json(json!({
            "resourceType": "Bundle",
            "type": "transaction-response",
            "entry": response_entries
        })),
    ))
}

async fn process_batch(
    state: Arc<AppState>,
    dataset_id: &str,
    bundle: &Value,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let entries = bundle_processor::process_bundle_entries(bundle, MAX_BUNDLE_ENTRIES)
        .map_err(|e| AppError::BadRequest(e))?;

    if entries.is_empty() {
        return Ok((
            StatusCode::OK,
            Json(json!({
                "resourceType": "Bundle",
                "type": "batch-response",
                "entry": []
            })),
        ));
    }

    let schema_name = state.qualified_schema(dataset_id);
    let mut response_entries = Vec::new();

    for entry in &entries {
        match process_single_entry(&state, &schema_name, dataset_id, entry, None).await {
            Ok(resp_entry) => {
                response_entries.push(resp_entry);
            }
            Err(e) => {
                response_entries.push(json!({
                    "response": {
                        "status": "400 Bad Request",
                        "outcome": {
                            "resourceType": "OperationOutcome",
                            "issue": [{
                                "severity": "error",
                                "code": "processing",
                                "diagnostics": e.to_string()
                            }]
                        }
                    }
                }));
            }
        }
    }

    Ok((
        StatusCode::OK,
        Json(json!({
            "resourceType": "Bundle",
            "type": "batch-response",
            "entry": response_entries
        })),
    ))
}

async fn process_single_entry(
    state: &AppState,
    schema_name: &str,
    dataset_id: &str,
    entry: &bundle_processor::ProcessedEntry,
    outer_conn: Option<&RequestConn>,
) -> Result<Value, String> {
    let table_name = entry.resource_type.to_lowercase();

    match entry.method.as_str() {
        "POST" => {
            let now = chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%SZ")
                .to_string();
            let mut resource = entry.resource.clone();
            if let Some(obj) = resource.as_object_mut() {
                obj.insert("id".to_string(), Value::String(entry.server_id.clone()));
                obj.insert(
                    "meta".to_string(),
                    json!({
                        "versionId": "1",
                        "lastUpdated": now
                    }),
                );
            }
            let raw_json = serde_json::to_string(&resource)
                .map_err(|e| format!("JSON serialize: {}", e))?;

            let transform_spec = state.registry.get_json_transform(&entry.resource_type)
                .map_err(|e| format!("Transform spec: {}", e))?;
            let column_names = state.registry.get_column_names(&entry.resource_type)
                .map_err(|e| format!("Column names: {}", e))?;
            let insert_sql = sql_builder::build_insert_sql(
                schema_name, &table_name, 1, &transform_spec, &column_names,
            );

            let result = match outer_conn {
                Some(conn) => conn.execute_params(insert_sql, vec![entry.server_id.clone(), raw_json]).await,
                None => {
                    let conn = state.new_request_conn().map_err(|e| format!("conn: {e}"))?;
                    conn.execute_params(insert_sql, vec![entry.server_id.clone(), raw_json]).await
                }
            };

            match result {
                QueryResult::Error(e) => Err(format!("Insert failed: {}", e)),
                _ => Ok(json!({
                    "response": {
                        "status": "201 Created",
                        "location": format!("/{}/{}/{}", dataset_id, entry.resource_type, entry.server_id),
                        "etag": "W/\"1\""
                    }
                })),
            }
        }
        "PUT" => {
            let transform_spec = state.registry.get_json_transform(&entry.resource_type)
                .map_err(|e| format!("Transform spec: {}", e))?;
            let column_names = state.registry.get_column_names(&entry.resource_type)
                .map_err(|e| format!("Column names: {}", e))?;
            let mut resource = entry.resource.clone();

            let result = upsert::upsert_resource(
                state,
                schema_name,
                &entry.resource_type,
                &entry.server_id,
                &mut resource,
                &transform_spec,
                &column_names,
                outer_conn,
            )
            .await?;

            let status = if result.is_new { "201 Created" } else { "200 OK" };
            Ok(json!({
                "response": {
                    "status": status,
                    "location": format!("/{}/{}/{}", dataset_id, entry.resource_type, entry.server_id),
                    "etag": format!("W/\"{}\"", result.version)
                }
            }))
        }
        "DELETE" => {
            if entry.server_id.is_empty() {
                return Err("DELETE entry missing resource id".to_string());
            }

            let check_sql = format!(
                "SELECT _version_id::VARCHAR, _raw FROM {schema}.\"{table}\" WHERE _id = $1 AND NOT _is_deleted",
                schema = schema_name,
                table = table_name,
            );

            // Reuse the outer transaction conn when present so all delete-related
            // statements run on the same connection; otherwise use a fresh per-op conn.
            let owned_conn;
            let conn_ref: &RequestConn = match outer_conn {
                Some(c) => c,
                None => {
                    owned_conn = state.new_request_conn().map_err(|e| format!("conn: {e}"))?;
                    &owned_conn
                }
            };

            let check_result = conn_ref
                .execute_params(check_sql, vec![entry.server_id.clone()])
                .await;

            let (current_version, current_raw) = match check_result {
                QueryResult::Select { rows, .. } => {
                    if rows.is_empty() {
                        return Err(format!("Resource {}/{} not found", entry.resource_type, entry.server_id));
                    }
                    let v = rows[0]
                        .get(0)
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(1);
                    let raw = rows[0]
                        .get(1)
                        .and_then(|v| v.as_str())
                        .unwrap_or("{}")
                        .to_string();
                    (v, raw)
                }
                QueryResult::Error(e) => return Err(format!("Delete check failed: {}", e)),
                _ => return Err(format!("Resource {}/{} not found", entry.resource_type, entry.server_id)),
            };

            let new_version = current_version + 1;

            let history_sql = format!(
                "INSERT INTO {schema}._history (_id, _resource_type, _version_id, _last_updated, _raw, _is_deleted) \
                 VALUES ($1, $2, {version}, CURRENT_TIMESTAMP, $3, false)",
                schema = schema_name,
                version = current_version,
            );
            let history_params = vec![entry.server_id.clone(), entry.resource_type.clone(), current_raw];
            if let QueryResult::Error(e) = conn_ref.execute_params(history_sql, history_params).await {
                eprintln!("[fhir] WARNING: history write failed for {}/{}: {}", entry.resource_type, entry.server_id, e);
            }

            let delete_sql = format!(
                "UPDATE {schema}.\"{table}\" SET _is_deleted = true, \
                 _version_id = {version}, _last_updated = CURRENT_TIMESTAMP \
                 WHERE _id = $1",
                schema = schema_name,
                table = table_name,
                version = new_version,
            );

            let result = conn_ref
                .execute_params(delete_sql, vec![entry.server_id.clone()])
                .await;

            match result {
                QueryResult::Error(e) => Err(format!("Delete failed: {}", e)),
                _ => Ok(json!({
                    "response": {
                        "status": "204 No Content"
                    }
                })),
            }
        }
        _ => Err(format!("Unsupported method: {}", entry.method)),
    }
}
