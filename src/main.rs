use std::env;
use std::fs;
use std::path::Path;
use std::process;

use duckdb::{Config, Connection};

fn main() {
    let args: Vec<String> = env::args().collect();
    let check_mode = args.iter().any(|a| a == "--check");

    let db_path = env::var("DATABASE_PATH").unwrap_or_else(|_| ":memory:".to_string());
    let ext_dir = env::var("EXTENSION_DIR")
        .unwrap_or_else(|_| "/usr/lib/trexsql/extensions".to_string());

    println!("Opening database: {db_path}");
    let config = Config::default()
        .allow_unsigned_extensions()
        .expect("Failed to set config");
    let conn = Connection::open_with_flags(&db_path, config)
        .expect("Failed to open database");

    let mut loaded = 0u32;
    let mut failures = 0u32;
    let ext_path = Path::new(&ext_dir);
    if ext_path.is_dir() {
        match fs::read_dir(ext_path) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    let path = entry.path();
                    let ext = path.extension().and_then(|e| e.to_str());
                    if ext == Some("trex") || ext == Some("duckdb_extension") {
                        let path_str = path.display().to_string();
                        let safe_path = path_str.replace("'", "''");
                        print!("Loading extension: {path_str} ... ");
                        match conn.execute(&format!("LOAD '{safe_path}'"), []) {
                            Ok(_) => {
                                println!("ok");
                                loaded += 1;
                            }
                            Err(e) => {
                                println!("FAILED: {e}");
                                failures += 1;
                            }
                        }
                    }
                }
            }
            Err(e) => println!("Warning: could not read extension dir {ext_dir}: {e}"),
        }
    } else {
        println!("Warning: extension dir {ext_dir} does not exist");
    }

    // Attach PostgreSQL as _config so extensions can access the configuration database
    if let Ok(database_url) = env::var("DATABASE_URL") {
        let safe_url = database_url.replace('\'', "''");
        print!("Attaching config database ... ");
        match conn.execute("INSTALL postgres", []) {
            Ok(_) => {}
            Err(e) => {
                println!("FAILED to install postgres scanner: {e}");
            }
        }
        let attach_sql = format!("ATTACH '{safe_url}' AS _config (TYPE postgres)");
        match conn.execute(&attach_sql, []) {
            Ok(_) => println!("ok"),
            Err(e) => println!("FAILED: {e}"),
        }
    }

    // Run core schema migrations via the migration extension
    if let Ok(schema_dir) = env::var("SCHEMA_DIR") {
        let safe_dir = schema_dir.replace('\'', "''");
        let migration_sql = format!(
            "SELECT * FROM trex_migration_run_schema('{safe_dir}', 'trex', '_config')"
        );
        print!("Running core schema migrations ... ");
        match conn.execute(&migration_sql, []) {
            Ok(_) => println!("ok"),
            Err(e) => eprintln!("FAILED: {e}"),
        }
    }

    if check_mode {
        if failures > 0 {
            println!("{failures} extension(s) failed to load");
            process::exit(1);
        }
        if loaded == 0 {
            println!("No extensions found in {ext_dir}");
            process::exit(1);
        }
        println!("All {loaded} extension(s) loaded successfully");
        return;
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
