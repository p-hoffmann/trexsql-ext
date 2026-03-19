use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug)]
pub struct ProcessedEntry {
    pub resource: Value,
    pub resource_type: String,
    pub server_id: String,
    pub method: String,
    pub request_url: Option<String>,
}

/// Parse a FHIR request URL like "Patient/123" into (ResourceType, id).
fn parse_request_url(url: &str) -> Option<(&str, &str)> {
    let parts: Vec<&str> = url.splitn(2, '/').collect();
    if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() {
        Some((parts[0], parts[1]))
    } else {
        None
    }
}

pub fn process_bundle_entries(
    bundle: &Value,
    max_entries: usize,
) -> Result<Vec<ProcessedEntry>, String> {
    let entries = bundle
        .get("entry")
        .and_then(|v| v.as_array())
        .ok_or("Bundle missing 'entry' array")?;

    if entries.is_empty() {
        return Ok(Vec::new());
    }

    if entries.len() > max_entries {
        return Err(format!(
            "Bundle exceeds maximum entry count: {} > {}",
            entries.len(),
            max_entries
        ));
    }

    let mut processed = Vec::with_capacity(entries.len());
    let mut ref_map: HashMap<String, String> = HashMap::new();

    for entry in entries {
        let resource = entry
            .get("resource")
            .ok_or("Bundle entry missing 'resource'")?
            .clone();

        let resource_type = resource
            .get("resourceType")
            .and_then(|v| v.as_str())
            .ok_or("Bundle entry resource missing 'resourceType'")?
            .to_string();

        let method = entry
            .get("request")
            .and_then(|r| r.get("method"))
            .and_then(|v| v.as_str())
            .unwrap_or("POST")
            .to_uppercase();

        let request_url = entry
            .get("request")
            .and_then(|r| r.get("url"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let server_id = match method.as_str() {
            "PUT" | "DELETE" => {
                // For PUT/DELETE, extract ID from request.url (e.g. "Patient/123" -> "123")
                request_url
                    .as_deref()
                    .and_then(parse_request_url)
                    .map(|(_, id)| id.to_string())
                    .or_else(|| {
                        resource.get("id").and_then(|v| v.as_str()).map(|s| s.to_string())
                    })
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
            }
            _ => uuid::Uuid::new_v4().to_string(),
        };

        if let Some(full_url) = entry.get("fullUrl").and_then(|v| v.as_str()) {
            if full_url.starts_with("urn:uuid:") {
                let server_ref = format!("{}/{}", resource_type, server_id);
                ref_map.insert(full_url.to_string(), server_ref);
            }
        }

        processed.push(ProcessedEntry {
            resource,
            resource_type,
            server_id,
            method,
            request_url,
        });
    }

    for entry in &mut processed {
        resolve_references(&mut entry.resource, &ref_map);
    }

    Ok(processed)
}

fn resolve_references(value: &mut Value, ref_map: &HashMap<String, String>) {
    match value {
        Value::Object(obj) => {
            if let Some(ref_val) = obj.get_mut("reference") {
                if let Some(ref_str) = ref_val.as_str() {
                    if let Some(resolved) = ref_map.get(ref_str) {
                        *ref_val = Value::String(resolved.clone());
                    }
                }
            }
            for (_, v) in obj.iter_mut() {
                resolve_references(v, ref_map);
            }
        }
        Value::Array(arr) => {
            for v in arr.iter_mut() {
                resolve_references(v, ref_map);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_process_bundle_empty() {
        let bundle = json!({
            "resourceType": "Bundle",
            "type": "transaction",
            "entry": []
        });
        let result = process_bundle_entries(&bundle, 1000).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_process_bundle_basic() {
        let bundle = json!({
            "resourceType": "Bundle",
            "type": "transaction",
            "entry": [{
                "fullUrl": "urn:uuid:abc-123",
                "resource": {
                    "resourceType": "Patient",
                    "name": [{"family": "Smith"}]
                },
                "request": {
                    "method": "POST",
                    "url": "Patient"
                }
            }]
        });
        let result = process_bundle_entries(&bundle, 1000).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].resource_type, "Patient");
        assert_eq!(result[0].method, "POST");
        assert!(!result[0].server_id.is_empty());
    }

    #[test]
    fn test_temp_reference_resolution() {
        let bundle = json!({
            "resourceType": "Bundle",
            "type": "transaction",
            "entry": [
                {
                    "fullUrl": "urn:uuid:patient-1",
                    "resource": {
                        "resourceType": "Patient",
                        "name": [{"family": "Smith"}]
                    },
                    "request": {"method": "POST", "url": "Patient"}
                },
                {
                    "fullUrl": "urn:uuid:obs-1",
                    "resource": {
                        "resourceType": "Observation",
                        "subject": {"reference": "urn:uuid:patient-1"},
                        "status": "final"
                    },
                    "request": {"method": "POST", "url": "Observation"}
                }
            ]
        });

        let result = process_bundle_entries(&bundle, 1000).unwrap();
        assert_eq!(result.len(), 2);

        let obs = &result[1];
        let subject_ref = obs
            .resource
            .get("subject")
            .and_then(|s| s.get("reference"))
            .and_then(|r| r.as_str())
            .unwrap();
        assert!(subject_ref.starts_with("Patient/"));
        assert!(!subject_ref.contains("urn:uuid:"));
    }

    #[test]
    fn test_put_extracts_id_from_request_url() {
        let bundle = json!({
            "resourceType": "Bundle",
            "type": "transaction",
            "entry": [{
                "resource": {
                    "resourceType": "Patient",
                    "name": [{"family": "Smith"}]
                },
                "request": {
                    "method": "PUT",
                    "url": "Patient/123"
                }
            }]
        });
        let result = process_bundle_entries(&bundle, 1000).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].server_id, "123");
        assert_eq!(result[0].method, "PUT");
    }

    #[test]
    fn test_delete_extracts_id_from_request_url() {
        let bundle = json!({
            "resourceType": "Bundle",
            "type": "transaction",
            "entry": [{
                "resource": {
                    "resourceType": "Patient"
                },
                "request": {
                    "method": "DELETE",
                    "url": "Patient/456"
                }
            }]
        });
        let result = process_bundle_entries(&bundle, 1000).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].server_id, "456");
        assert_eq!(result[0].method, "DELETE");
    }

    #[test]
    fn test_post_generates_uuid() {
        let bundle = json!({
            "resourceType": "Bundle",
            "type": "transaction",
            "entry": [{
                "resource": {
                    "resourceType": "Patient",
                    "name": [{"family": "Smith"}]
                },
                "request": {
                    "method": "POST",
                    "url": "Patient"
                }
            }]
        });
        let result = process_bundle_entries(&bundle, 1000).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].method, "POST");
        // POST should generate a UUID, not "Patient"
        assert_ne!(result[0].server_id, "Patient");
        assert!(result[0].server_id.contains('-')); // UUID format
    }

    #[test]
    fn test_put_falls_back_to_resource_id() {
        let bundle = json!({
            "resourceType": "Bundle",
            "type": "transaction",
            "entry": [{
                "resource": {
                    "resourceType": "Patient",
                    "id": "from-resource"
                },
                "request": {
                    "method": "PUT",
                    "url": "Patient"
                }
            }]
        });
        let result = process_bundle_entries(&bundle, 1000).unwrap();
        assert_eq!(result[0].server_id, "from-resource");
    }

    #[test]
    fn test_max_entries_exceeded() {
        let bundle = json!({
            "resourceType": "Bundle",
            "type": "transaction",
            "entry": [
                {"resource": {"resourceType": "Patient"}, "request": {"method": "POST", "url": "Patient"}},
                {"resource": {"resourceType": "Patient"}, "request": {"method": "POST", "url": "Patient"}},
                {"resource": {"resourceType": "Patient"}, "request": {"method": "POST", "url": "Patient"}}
            ]
        });
        let result = process_bundle_entries(&bundle, 2);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exceeds maximum"));
    }
}
