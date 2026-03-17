use crate::fhir::structure_definition::{DefinitionRegistry, ElementInfo};
use crate::schema::type_mapping::{fhir_to_duckdb_type, is_primitive_type};

const MAX_RECURSION_DEPTH: usize = 4;

pub fn generate_column_names(
    registry: &DefinitionRegistry,
    resource_type: &str,
) -> Result<Vec<String>, String> {
    let sd = registry
        .get_resource(resource_type)
        .ok_or_else(|| format!("Unknown resource type: {}", resource_type))?;

    Ok(sd
        .elements
        .iter()
        .filter(|e| e.name != "resourceType")
        .filter(|e| element_to_transform_field(registry, e, 0).is_some())
        .map(|e| e.name.clone())
        .collect())
}

pub fn generate_json_transform(
    registry: &DefinitionRegistry,
    resource_type: &str,
) -> Result<String, String> {
    let sd = registry
        .get_resource(resource_type)
        .ok_or_else(|| format!("Unknown resource type: {}", resource_type))?;

    let fields: Vec<String> = sd
        .elements
        .iter()
        .filter(|e| e.name != "resourceType")
        .filter_map(|element| element_to_transform_field(registry, element, 0))
        .collect();

    Ok(format!("{{{}}}", fields.join(", ")))
}

fn element_to_transform_field(
    registry: &DefinitionRegistry,
    element: &ElementInfo,
    depth: usize,
) -> Option<String> {
    let name = &element.name;
    let type_str = element_to_transform_type(registry, element, depth)?;

    Some(format!("\"{}\": {}", name, type_str))
}

fn element_to_transform_type(
    registry: &DefinitionRegistry,
    element: &ElementInfo,
    depth: usize,
) -> Option<String> {
    if depth >= MAX_RECURSION_DEPTH {
        return Some("\"VARCHAR\"".to_string());
    }

    if element.content_reference.is_some() {
        if depth >= MAX_RECURSION_DEPTH - 1 {
            let t = if element.is_array {
                "[\"VARCHAR\"]"
            } else {
                "\"VARCHAR\""
            };
            return Some(t.to_string());
        }
    }

    // Choice types not supported by json_transform; stored in _raw.
    if element.is_choice {
        return None;
    }

    if element.type_codes.is_empty() {
        if !element.children.is_empty() {
            let struct_str = children_to_transform(registry, &element.children, depth);
            return Some(if element.is_array {
                format!("[{}]", struct_str)
            } else {
                struct_str
            });
        }
        return None;
    }

    let type_code = &element.type_codes[0];

    if !element.children.is_empty() {
        let struct_str = children_to_transform(registry, &element.children, depth);
        return Some(if element.is_array {
            format!("[{}]", struct_str)
        } else {
            struct_str
        });
    }

    let resolved = resolve_transform_type(registry, type_code, depth);
    Some(if element.is_array {
        format!("[{}]", resolved)
    } else {
        resolved
    })
}

fn resolve_transform_type(registry: &DefinitionRegistry, type_code: &str, depth: usize) -> String {
    if is_primitive_type(type_code) {
        return format!("\"{}\"", fhir_to_duckdb_type(type_code));
    }

    if let Some(type_def) = registry.get_type(type_code) {
        if type_def.elements.is_empty() {
            return "\"VARCHAR\"".to_string();
        }
        return children_to_transform(registry, &type_def.elements, depth + 1);
    }

    match type_code {
        "Extension" | "BackboneElement" | "Element" => "\"VARCHAR\"".to_string(),
        "Resource" => "\"JSON\"".to_string(),
        "Narrative" => {
            "{\"status\": \"VARCHAR\", \"div\": \"VARCHAR\"}".to_string()
        }
        "Reference" => {
            "{\"reference\": \"VARCHAR\", \"type\": \"VARCHAR\", \"display\": \"VARCHAR\"}"
                .to_string()
        }
        _ => "\"VARCHAR\"".to_string(),
    }
}

fn children_to_transform(
    registry: &DefinitionRegistry,
    children: &[ElementInfo],
    depth: usize,
) -> String {
    if depth >= MAX_RECURSION_DEPTH {
        return "\"VARCHAR\"".to_string();
    }

    let fields: Vec<String> = children
        .iter()
        .filter_map(|child| element_to_transform_field(registry, child, depth + 1))
        .collect();

    if fields.is_empty() {
        return "\"VARCHAR\"".to_string();
    }

    format!("{{{}}}", fields.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fhir_server::load_default_definitions;

    #[test]
    fn test_column_names_patient() {
        let defs = load_default_definitions().expect("load defs");
        let cols = generate_column_names(&defs, "Patient").expect("column names");
        assert!(!cols.is_empty(), "Patient should have columns");
        assert!(cols.contains(&"id".to_string()), "should contain id");
        assert!(cols.contains(&"gender".to_string()), "should contain gender");
        assert!(cols.contains(&"birthDate".to_string()), "should contain birthDate");
        assert!(cols.contains(&"name".to_string()), "should contain name");
        assert!(!cols.contains(&"resourceType".to_string()), "should not contain resourceType");
    }

    #[test]
    fn test_column_names_excludes_choice_types() {
        let defs = load_default_definitions().expect("load defs");
        let cols = generate_column_names(&defs, "Observation").expect("column names");
        // value[x] is a choice type and should be excluded
        assert!(!cols.iter().any(|c| c.contains("[x]")), "should not contain [x] columns");
        // but non-choice columns should be present
        assert!(cols.contains(&"status".to_string()), "should contain status");
        assert!(cols.contains(&"code".to_string()), "should contain code");
    }

    #[test]
    fn test_column_names_matches_transform_keys() {
        let defs = load_default_definitions().expect("load defs");
        let cols = generate_column_names(&defs, "Patient").expect("column names");
        let transform = generate_json_transform(&defs, "Patient").expect("transform");
        // Every column name should appear as a key in the transform spec
        for col in &cols {
            assert!(
                transform.contains(&format!("\"{}\":", col)),
                "transform should contain key for column '{}'. Transform: {}",
                col,
                &transform[..transform.len().min(500)]
            );
        }
    }

    #[test]
    fn test_column_names_unknown_type() {
        let defs = load_default_definitions().expect("load defs");
        let result = generate_column_names(&defs, "FakeResource");
        assert!(result.is_err());
    }
}
