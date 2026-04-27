use crate::validation;
use serde_json::json;
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Clone, Copy, PartialEq)]
enum Status {
    Starting,
    Running,
    Stopped,
    Error,
}

impl Status {
    fn as_str(&self) -> &'static str {
        match self {
            Status::Starting => "starting",
            Status::Running => "running",
            Status::Stopped => "stopped",
            Status::Error => "error",
        }
    }
}

struct OutputLine {
    id: u64,
    text: String,
    stream: &'static str, // "stdout" or "stderr"
    timestamp_ms: u64,
}

struct ManagedProcess {
    child: Child,
    stdin: Option<std::process::ChildStdin>,
    stdout_buf: Arc<Mutex<VecDeque<OutputLine>>>,
    stderr_buf: Arc<Mutex<VecDeque<OutputLine>>>,
    status: Arc<Mutex<Status>>,
    port: u16,
    detected_url: Arc<Mutex<Option<String>>>,
    next_line_id: Arc<Mutex<u64>>,
}

const MAX_BUFFER_LINES: usize = 1000;

type Registry = Mutex<HashMap<String, ManagedProcess>>;

fn registry() -> &'static Registry {
    static REG: OnceLock<Registry> = OnceLock::new();
    REG.get_or_init(|| Mutex::new(HashMap::new()))
}

fn next_id(counter: &Arc<Mutex<u64>>) -> u64 {
    let mut n = counter.lock().unwrap();
    *n += 1;
    *n
}

fn push_line(buf: &Arc<Mutex<VecDeque<OutputLine>>>, line: OutputLine) {
    let mut b = buf.lock().unwrap();
    b.push_back(line);
    while b.len() > MAX_BUFFER_LINES {
        b.pop_front();
    }
}

/// Start a long-lived process.
/// config_json: {"path": "/abs/path", "command": "npm run dev", "port": 3001}
pub fn process_start(process_id: &str, config_json: &str) -> Result<String, Box<dyn Error>> {
    let config: serde_json::Value =
        serde_json::from_str(config_json).map_err(|e| format!("Invalid config JSON: {e}"))?;

    let path = config["path"]
        .as_str()
        .ok_or("config.path required")?;
    let command = config["command"]
        .as_str()
        .ok_or("config.command required")?;
    let port = config["port"]
        .as_u64()
        .ok_or("config.port required")? as u16;

    validation::validate_workspace_path(path)?;
    let (cmd, args) = validation::validate_command(command)?;

    // Stop existing process with this ID if any
    {
        let mut reg = registry().lock().unwrap();
        if let Some(mut old) = reg.remove(process_id) {
            let _ = old.child.kill();
            let _ = old.child.wait();
        }
    }

    let arg_refs: Vec<&str> = args.iter().copied().collect();
    let mut child = Command::new(cmd)
        .args(&arg_refs)
        .current_dir(path)
        .env("PORT", port.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("{cmd} spawn failed: {e}"))?;

    let child_stdin = child.stdin.take();

    let pid = child.id();

    let stdout_buf: Arc<Mutex<VecDeque<OutputLine>>> = Arc::new(Mutex::new(VecDeque::new()));
    let stderr_buf: Arc<Mutex<VecDeque<OutputLine>>> = Arc::new(Mutex::new(VecDeque::new()));
    let status = Arc::new(Mutex::new(Status::Starting));
    let detected_url: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let next_line_id = Arc::new(Mutex::new(0u64));

    // Spawn stdout reader thread
    if let Some(stdout) = child.stdout.take() {
        let buf = Arc::clone(&stdout_buf);
        let st = Arc::clone(&status);
        let url = Arc::clone(&detected_url);
        let nid = Arc::clone(&next_line_id);
        std::thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                let Ok(text) = line else { break };
                // Detect URL or "listening on port" pattern
                if let Some(m) = text.find("http://localhost:") {
                    let rest = &text[m..];
                    let end = rest.find(|c: char| c.is_whitespace()).unwrap_or(rest.len());
                    let found_url = &rest[..end];
                    let mut u = url.lock().unwrap();
                    if u.is_none() {
                        *u = Some(found_url.to_string());
                        let mut s = st.lock().unwrap();
                        *s = Status::Running;
                    }
                } else if text.contains("listening on port") {
                    let mut s = st.lock().unwrap();
                    if *s == Status::Starting {
                        *s = Status::Running;
                    }
                }
                let id = next_id(&nid);
                push_line(&buf, OutputLine {
                    id,
                    text,
                    stream: "stdout",
                    timestamp_ms: now_ms(),
                });
            }
        });
    }

    // Spawn stderr reader thread
    if let Some(stderr) = child.stderr.take() {
        let buf = Arc::clone(&stderr_buf);
        let nid = Arc::clone(&next_line_id);
        std::thread::spawn(move || {
            let reader = BufReader::new(stderr);
            for line in reader.lines() {
                let Ok(text) = line else { break };
                let id = next_id(&nid);
                push_line(&buf, OutputLine {
                    id,
                    text,
                    stream: "stderr",
                    timestamp_ms: now_ms(),
                });
            }
        });
    }

    let managed = ManagedProcess {
        child,
        stdin: child_stdin,
        stdout_buf,
        stderr_buf,
        status,
        port,
        detected_url,
        next_line_id,
    };

    registry().lock().unwrap().insert(process_id.to_string(), managed);

    Ok(json!({"ok": true, "port": port, "pid": pid}).to_string())
}

/// Stop a managed process.
pub fn process_stop(process_id: &str, _unused: &str) -> Result<String, Box<dyn Error>> {
    let mut reg = registry().lock().unwrap();
    if let Some(mut proc) = reg.remove(process_id) {
        let _ = proc.child.kill();
        let _ = proc.child.wait();
        Ok(json!({"ok": true}).to_string())
    } else {
        Ok(json!({"ok": true, "message": "no such process"}).to_string())
    }
}

/// Get status of a managed process.
pub fn process_status(process_id: &str, _unused: &str) -> Result<String, Box<dyn Error>> {
    let mut reg = registry().lock().unwrap();
    if let Some(proc) = reg.get_mut(process_id) {
        // Check if process has exited
        let exited = match proc.child.try_wait() {
            Ok(Some(_)) => true,
            _ => false,
        };
        if exited {
            let mut s = proc.status.lock().unwrap();
            if *s != Status::Stopped {
                *s = Status::Stopped;
            }
        }
        let status = proc.status.lock().unwrap();
        let url = proc.detected_url.lock().unwrap();
        let pid = proc.child.id();
        Ok(json!({
            "status": status.as_str(),
            "port": proc.port,
            "url": *url,
            "pid": pid,
        })
        .to_string())
    } else {
        Ok(json!({"status": "stopped", "port": null, "url": null, "pid": null}).to_string())
    }
}

/// Get output lines since a given line ID. Returns lines from both stdout and stderr merged by ID.
pub fn process_output(process_id: &str, since_line_id: &str) -> Result<String, Box<dyn Error>> {
    let since: u64 = since_line_id.parse().unwrap_or(0);

    let reg = registry().lock().unwrap();
    if let Some(proc) = reg.get(process_id) {
        let mut lines = Vec::new();

        // Collect from stdout
        {
            let buf = proc.stdout_buf.lock().unwrap();
            for ol in buf.iter() {
                if ol.id > since {
                    lines.push(json!({
                        "id": ol.id,
                        "type": ol.stream,
                        "text": ol.text,
                        "ts": ol.timestamp_ms,
                    }));
                }
            }
        }

        // Collect from stderr
        {
            let buf = proc.stderr_buf.lock().unwrap();
            for ol in buf.iter() {
                if ol.id > since {
                    lines.push(json!({
                        "id": ol.id,
                        "type": ol.stream,
                        "text": ol.text,
                        "ts": ol.timestamp_ms,
                    }));
                }
            }
        }

        // Sort by ID
        lines.sort_by_key(|l| l["id"].as_u64().unwrap_or(0));

        let last_id = lines.last().and_then(|l| l["id"].as_u64()).unwrap_or(since);

        Ok(json!({"lines": lines, "last_id": last_id}).to_string())
    } else {
        Ok(json!({"lines": [], "last_id": since}).to_string())
    }
}

/// Write input to a managed process's stdin.
pub fn process_input(process_id: &str, input_text: &str) -> Result<String, Box<dyn Error>> {
    use std::io::Write;
    let mut reg = registry().lock().unwrap();
    if let Some(proc) = reg.get_mut(process_id) {
        if let Some(ref mut stdin) = proc.stdin {
            stdin
                .write_all(input_text.as_bytes())
                .map_err(|e| format!("Failed to write to stdin: {e}"))?;
            stdin
                .write_all(b"\n")
                .map_err(|e| format!("Failed to write newline: {e}"))?;
            stdin.flush().map_err(|e| format!("Failed to flush stdin: {e}"))?;
            Ok(json!({"ok": true}).to_string())
        } else {
            Err("Process stdin not available".into())
        }
    } else {
        Err(format!("Process not found: {process_id}").into())
    }
}
