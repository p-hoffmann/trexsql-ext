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
        env::var("FLIGHT_LOG_LEVEL")
            .map(|s| {
                if let Ok(n) = s.parse::<u8>() {
                    match n {
                        1 => LogLevel::Error,
                        2 => LogLevel::Warn,
                        3 => LogLevel::Info,
                        4 => LogLevel::Debug,
                        5 => LogLevel::Trace,
                        _ => LogLevel::Info,
                    }
                } else {
                    LogLevel::from_str(&s)
                }
            })
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

pub struct FlightLogger;

impl FlightLogger {
    fn timestamp() -> String {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let secs = duration.as_secs();
        let millis = duration.subsec_millis();
        format!("{}.{:03}", secs, millis)
    }

    pub fn log(level: LogLevel, category: &str, message: &str) {
        if level > LogLevel::current() {
            return;
        }
        let timestamp = Self::timestamp();
        eprintln!(
            "[{}] [flight] [{}] [{}] {}",
            timestamp,
            level.as_str(),
            category,
            message
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
        eprintln!(
            "[{}] [flight] [{}] [{}] [{}] {}",
            timestamp,
            level.as_str(),
            category,
            ctx_str,
            message
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
