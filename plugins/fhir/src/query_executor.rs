use trex_pool_client::arrow_array::{Array, RecordBatch};
use trex_pool_client::arrow_schema::{DataType, Schema, TimeUnit};
use std::sync::Arc;

#[derive(Debug)]
pub enum QueryResult {
    Select {
        rows: Vec<Vec<serde_json::Value>>,
        columns: Vec<String>,
    },
    Execute {
        rows_affected: usize,
    },
    Json(String),
    Error(String),
}

/// A leased session against the shared `trex_pool`. The session pins one
/// Connection for the lifetime of this `RequestConn` so transactions
/// (BEGIN/COMMIT/ROLLBACK) cannot interleave between concurrent FHIR requests.
/// On drop, the session is destroyed; the pool runs cleanup (ROLLBACK + RESET)
/// before returning the Connection to the shared pool.
pub struct RequestConn {
    session_id: u64,
}

impl RequestConn {
    pub fn new() -> Result<Self, String> {
        let session_id = trex_pool_client::create_session()?;
        Ok(Self { session_id })
    }

    pub async fn execute(&self, query: String) -> QueryResult {
        let session_id = self.session_id;
        tokio::task::spawn_blocking(move || {
            match trex_pool_client::session_execute(session_id, &query) {
                Ok((schema, batches)) => arrow_batches_to_query_result(&schema, &batches),
                Err(e) => QueryResult::Error(e),
            }
        })
        .await
        .unwrap_or_else(|_| QueryResult::Error("spawn_blocking failed".into()))
    }

    pub async fn execute_params(&self, query: String, params: Vec<String>) -> QueryResult {
        let session_id = self.session_id;
        tokio::task::spawn_blocking(move || {
            match trex_pool_client::session_execute_params(session_id, &query, &params) {
                Ok((schema, batches)) => arrow_batches_to_query_result(&schema, &batches),
                Err(e) => QueryResult::Error(e),
            }
        })
        .await
        .unwrap_or_else(|_| QueryResult::Error("spawn_blocking failed".into()))
    }
}

impl Drop for RequestConn {
    fn drop(&mut self) {
        let _ = trex_pool_client::destroy_session(self.session_id);
    }
}

fn arrow_batches_to_query_result(schema: &Arc<Schema>, batches: &[RecordBatch]) -> QueryResult {
    if schema.fields().is_empty() && batches.is_empty() {
        return QueryResult::Execute { rows_affected: 0 };
    }

    let columns: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();
    for batch in batches {
        let batch_schema = batch.schema();
        for r in 0..batch.num_rows() {
            let mut row_values = Vec::with_capacity(batch.num_columns());
            for (i, field) in batch_schema.fields().iter().enumerate() {
                let col = batch.column(i);
                row_values.push(column_value_to_json(col.as_ref(), r, field.data_type()));
            }
            rows.push(row_values);
        }
    }
    QueryResult::Select { rows, columns }
}

/// Convert an Arrow value to a JSON string. FHIR handlers consume rows via
/// `serde_json::Value::as_str()`, so every cell is rendered as a JSON string
/// (or `Null`) regardless of the underlying Arrow type.
fn column_value_to_json(
    array: &dyn Array,
    row: usize,
    dt: &DataType,
) -> serde_json::Value {
    use trex_pool_client::arrow_array::*;
    use serde_json::Value as JV;

    if array.is_null(row) {
        return JV::Null;
    }
    match dt {
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            JV::String(a.value(row).to_string())
        }
        DataType::LargeUtf8 => {
            let a = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            JV::String(a.value(row).to_string())
        }
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            JV::String(a.value(row).to_string())
        }
        DataType::Int8 => {
            let a = array.as_any().downcast_ref::<Int8Array>().unwrap();
            JV::String(a.value(row).to_string())
        }
        DataType::Int16 => {
            let a = array.as_any().downcast_ref::<Int16Array>().unwrap();
            JV::String(a.value(row).to_string())
        }
        DataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
            JV::String(a.value(row).to_string())
        }
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
            JV::String(a.value(row).to_string())
        }
        DataType::UInt8 => {
            let a = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            JV::String(a.value(row).to_string())
        }
        DataType::UInt16 => {
            let a = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            JV::String(a.value(row).to_string())
        }
        DataType::UInt32 => {
            let a = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            JV::String(a.value(row).to_string())
        }
        DataType::UInt64 => {
            let a = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            JV::String(a.value(row).to_string())
        }
        DataType::Float32 => {
            let a = array.as_any().downcast_ref::<Float32Array>().unwrap();
            JV::String(a.value(row).to_string())
        }
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
            JV::String(a.value(row).to_string())
        }
        DataType::Decimal128(_, scale) => {
            let a = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let value = a.value(row) as f64 / 10_f64.powi(*scale as i32);
            JV::String(value.to_string())
        }
        DataType::Date32 => {
            let a = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = a.value(row);
            let ts = days as i64 * 86400;
            let dt = chrono::DateTime::from_timestamp(ts, 0)
                .unwrap_or(chrono::DateTime::UNIX_EPOCH);
            JV::String(dt.format("%Y-%m-%d").to_string())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let a = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let micros = a.value(row);
            let dt = chrono::DateTime::from_timestamp_micros(micros)
                .unwrap_or(chrono::DateTime::UNIX_EPOCH);
            JV::String(dt.to_rfc3339())
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let a = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let millis = a.value(row);
            let dt = chrono::DateTime::from_timestamp_millis(millis)
                .unwrap_or(chrono::DateTime::UNIX_EPOCH);
            JV::String(dt.to_rfc3339())
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let a = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            let secs = a.value(row);
            let dt = chrono::DateTime::from_timestamp(secs, 0)
                .unwrap_or(chrono::DateTime::UNIX_EPOCH);
            JV::String(dt.to_rfc3339())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let a = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let nanos = a.value(row);
            let dt = chrono::DateTime::from_timestamp_nanos(nanos);
            JV::String(dt.to_rfc3339())
        }
        _ => JV::Null,
    }
}
