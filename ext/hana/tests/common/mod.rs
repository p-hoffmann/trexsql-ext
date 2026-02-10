// Common utilities for HANA integration tests

use std::env;

/// HANA connection configuration for tests
pub struct HanaTestConfig {
    pub connection_url: String,
    pub should_skip: bool,
    pub skip_reason: String,
}

impl HanaTestConfig {
    pub fn new() -> Self {
        match env::var("HANA_TEST_URL") {
            Ok(url) if !url.is_empty() => {
                let should_skip = env::var("SKIP_HANA_TESTS")
                    .unwrap_or_else(|_| "false".to_string()) == "true";
                Self {
                    connection_url: url,
                    should_skip,
                    skip_reason: "SKIP_HANA_TESTS=true".to_string(),
                }
            }
            _ => Self {
                connection_url: String::new(),
                should_skip: true,
                skip_reason: "HANA_TEST_URL not set".to_string(),
            },
        }
    }
}

/// Setup function for HANA integration tests
pub fn setup() {
    // Initialize logging for tests only once
    let _ = env_logger::try_init();
    println!("Setting up HANA integration tests");
}

/// Check if HANA database is available at the given URL.
/// This is used to conditionally run integration tests.
#[allow(dead_code)]
pub fn is_hana_available(url: &str) -> bool {
    // Try to parse the URL and check basic connectivity
    // This is a simplified check - in a real scenario you might want to
    // actually attempt a connection
    !url.is_empty() && (url.starts_with("hdbsql://") || url.starts_with("hdbsqls://"))
}
