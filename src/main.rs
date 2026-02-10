use std::env;
use std::fs;
use std::path::Path;
use std::process;

use duckdb::Connection;

fn main() {
    let args: Vec<String> = env::args().collect();
    let check_mode = args.iter().any(|a| a == "--check");

    let db_path = env::var("DATABASE_PATH").unwrap_or_else(|_| ":memory:".to_string());
    let ext_dir = env::var("EXTENSION_DIR")
        .unwrap_or_else(|_| "/usr/lib/trexsql/extensions".to_string());

    eprintln!("Opening database: {db_path}");
    let conn = Connection::open(&db_path).expect("Failed to open database");

    let mut loaded = 0u32;
    let mut failures = 0u32;
    let ext_path = Path::new(&ext_dir);
    if ext_path.is_dir() {
        match fs::read_dir(ext_path) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().and_then(|e| e.to_str()) == Some("trex") {
                        let path_str = path.display().to_string();
                        let safe_path = path_str.replace("'", "''");
                        eprint!("Loading extension: {path_str} ... ");
                        match conn.execute(&format!("LOAD '{safe_path}'"), []) {
                            Ok(_) => {
                                eprintln!("ok");
                                loaded += 1;
                            }
                            Err(e) => {
                                eprintln!("failed: {e}");
                                failures += 1;
                            }
                        }
                    }
                }
            }
            Err(e) => eprintln!("Warning: could not read extension dir {ext_dir}: {e}"),
        }
    } else {
        eprintln!("Warning: extension dir {ext_dir} does not exist");
    }

    if check_mode {
        if failures > 0 {
            eprintln!("{failures} extension(s) failed to load");
            process::exit(1);
        }
        if loaded == 0 {
            eprintln!("No extensions found in {ext_dir}");
            process::exit(1);
        }
        eprintln!("All {loaded} extension(s) loaded successfully");
        process::exit(0);
    }

    eprintln!("TrexSQL ready. Waiting for shutdown signal...");

    #[cfg(unix)]
    {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let shutdown = Arc::new(AtomicBool::new(false));

        signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&shutdown))
            .expect("Failed to register SIGTERM handler");
        signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&shutdown))
            .expect("Failed to register SIGINT handler");

        while !shutdown.load(Ordering::Relaxed) {
            std::thread::park_timeout(std::time::Duration::from_secs(1));
        }
    }

    #[cfg(not(unix))]
    {
        std::thread::park();
    }

    eprintln!("Shutting down.");
    drop(conn);
}
