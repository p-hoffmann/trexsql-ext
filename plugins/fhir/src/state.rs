use crate::fhir::resource_registry::ResourceRegistry;
use crate::fhir::search_parameter::SearchParamRegistry;
use crate::query_executor::RequestConn;
use crate::sql_safety::{to_qualified_meta_schema, to_qualified_schema};
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<ResourceRegistry>,
    pub search_params: Arc<SearchParamRegistry>,
    pub db_name: String,
}

impl AppState {
    pub fn new(
        registry: Arc<ResourceRegistry>,
        search_params: Arc<SearchParamRegistry>,
        db_name: String,
    ) -> Self {
        Self {
            registry,
            search_params,
            db_name,
        }
    }

    pub fn new_request_conn(&self) -> Result<RequestConn, String> {
        RequestConn::new()
    }

    pub fn qualified_schema(&self, dataset_id: &str) -> String {
        to_qualified_schema(&self.db_name, dataset_id)
    }

    pub fn meta_schema(&self) -> String {
        to_qualified_meta_schema(&self.db_name)
    }
}
