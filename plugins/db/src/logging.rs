use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
    Trace = 5,
}

impl LogLevel {
    pub fn from_str(s: &str) -> LogLevel {
        match s.to_uppercase().as_str() {
            "ERROR" => LogLevel::Error,
            "WARN" | "WARNING" => LogLevel::Warn,
            "INFO" => LogLevel::Info,
            "DEBUG" => LogLevel::Debug,
            "TRACE" => LogLevel::Trace,
            _ => LogLevel::Info,
        }
    }

    pub fn current() -> LogLevel {
        env::var("SWARM_LOG_LEVEL")
            .map(|s| LogLevel::from_str(&s))
            .unwrap_or(LogLevel::Info)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Error => "ERROR",
            LogLevel::Warn => "WARN",
            LogLevel::Info => "INFO",
            LogLevel::Debug => "DEBUG",
            LogLevel::Trace => "TRACE",
        }
    }
}

pub struct SwarmLogger;

impl SwarmLogger {
    fn timestamp() -> String {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let secs = duration.as_secs();
        let millis = duration.subsec_millis();
        format!("{}.{:03}", secs, millis)
    }

    /// Redact known sensitive patterns (passwords, tokens, credentials) from log messages.
    fn sanitize(message: &str) -> String {
        let mut result = message.to_string();
        // Redact password=... or password: ... patterns (case-insensitive)
        let patterns = [
            "password", "passwd", "secret", "token", "credential", "authorization",
        ];
        for pat in &patterns {
            // Match pattern followed by = or : or space, then a value (up to whitespace or quote)
            let lower = result.to_lowercase();
            let mut start = 0;
            while let Some(idx) = lower[start..].find(pat) {
                let abs_idx = start + idx;
                let after_key = abs_idx + pat.len();
                if after_key < result.len() {
                    let rest = &result[after_key..];
                    // Check for separator: =, :, or whitespace followed by value
                    if let Some(first_char) = rest.chars().next() {
                        if first_char == '=' || first_char == ':' {
                            let value_start = after_key + 1;
                            // Skip optional quotes/spaces
                            let value_bytes = result[value_start..].as_bytes();
                            let mut vs = 0;
                            while vs < value_bytes.len() && (value_bytes[vs] == b' ' || value_bytes[vs] == b'\'' || value_bytes[vs] == b'"') {
                                vs += 1;
                            }
                            let actual_start = value_start + vs;
                            // Find end of value
                            let mut ve = actual_start;
                            while ve < result.len() {
                                let c = result.as_bytes()[ve];
                                if c == b' ' || c == b'\'' || c == b'"' || c == b',' || c == b';' || c == b'\n' || c == b')' {
                                    break;
                                }
                                ve += 1;
                            }
                            if ve > actual_start {
                                result.replace_range(actual_start..ve, "[REDACTED]");
                            }
                        }
                    }
                }
                start = abs_idx + 1;
            }
        }
        result
    }

    pub fn log(level: LogLevel, category: &str, message: &str) {
        if level > LogLevel::current() {
            return;
        }
        let timestamp = Self::timestamp();
        let sanitized = Self::sanitize(message);
        eprintln!(
            "[{}] [{}] [{}] {}",
            timestamp,
            level.as_str(),
            category,
            sanitized
        );
    }

    pub fn log_with_context(
        level: LogLevel,
        category: &str,
        context: &[(&str, &str)],
        message: &str,
    ) {
        if level > LogLevel::current() {
            return;
        }
        let timestamp = Self::timestamp();
        let ctx_str = context
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(" ");
        let sanitized_ctx = Self::sanitize(&ctx_str);
        let sanitized_msg = Self::sanitize(message);
        eprintln!(
            "[{}] [{}] [{}] [{}] {}",
            timestamp,
            level.as_str(),
            category,
            sanitized_ctx,
            sanitized_msg
        );
    }

    pub fn error(category: &str, message: &str) {
        Self::log(LogLevel::Error, category, message);
    }

    pub fn warn(category: &str, message: &str) {
        Self::log(LogLevel::Warn, category, message);
    }

    pub fn info(category: &str, message: &str) {
        Self::log(LogLevel::Info, category, message);
    }

    pub fn debug(category: &str, message: &str) {
        Self::log(LogLevel::Debug, category, message);
    }

    pub fn trace(category: &str, message: &str) {
        Self::log(LogLevel::Trace, category, message);
    }
}

#[macro_export]
macro_rules! swarm_error {
    ($category:expr, $($arg:tt)*) => {
        $crate::logging::SwarmLogger::error($category, &format!($($arg)*))
    };
}

#[macro_export]
macro_rules! swarm_warn {
    ($category:expr, $($arg:tt)*) => {
        $crate::logging::SwarmLogger::warn($category, &format!($($arg)*))
    };
}

#[macro_export]
macro_rules! swarm_info {
    ($category:expr, $($arg:tt)*) => {
        $crate::logging::SwarmLogger::info($category, &format!($($arg)*))
    };
}

#[macro_export]
macro_rules! swarm_debug {
    ($category:expr, $($arg:tt)*) => {
        $crate::logging::SwarmLogger::debug($category, &format!($($arg)*))
    };
}

#[macro_export]
macro_rules! swarm_trace {
    ($category:expr, $($arg:tt)*) => {
        $crate::logging::SwarmLogger::trace($category, &format!($($arg)*))
    };
}
