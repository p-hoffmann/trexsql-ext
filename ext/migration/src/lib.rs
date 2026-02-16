extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

use chrono::Utc;
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection,
};
use libduckdb_sys as ffi;
use siphasher::sip::SipHasher13;
use std::{
    collections::HashMap,
    error::Error,
    ffi::CString,
    fs,
    hash::{Hash, Hasher},
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex, OnceLock,
    },
};

// ── Shared Connection (raw FFI, following hana pattern) ──────────────────────

struct SharedConn(ffi::duckdb_connection);

unsafe impl Send for SharedConn {}
unsafe impl Sync for SharedConn {}

impl Drop for SharedConn {
    fn drop(&mut self) {
        unsafe {
            if !self.0.is_null() {
                ffi::duckdb_disconnect(&mut self.0);
            }
        }
    }
}

static SHARED_CONNECTION: OnceLock<Mutex<SharedConn>> = OnceLock::new();

fn execute_sql(sql: &str) -> Result<(), Box<dyn Error>> {
    let mutex = SHARED_CONNECTION
        .get()
        .ok_or("Migration extension not initialized")?;
    let guard = mutex.lock().map_err(|_| "Connection mutex poisoned")?;
    let conn = guard.0;

    unsafe {
        let c_sql = CString::new(sql)?;
        let mut result: ffi::duckdb_result = std::mem::zeroed();
        let state = ffi::duckdb_query(conn, c_sql.as_ptr(), &mut result);

        let ok = if state != ffi::duckdb_state_DuckDBSuccess {
            let err_ptr = ffi::duckdb_result_error(&mut result);
            let err_msg = if err_ptr.is_null() {
                format!("SQL execution failed: {}", sql)
            } else {
                let c_str = std::ffi::CStr::from_ptr(err_ptr);
                format!("{}", c_str.to_string_lossy())
            };
            Err(err_msg)
        } else {
            Ok(())
        };

        ffi::duckdb_destroy_result(&mut result);
        ok?;
    }

    Ok(())
}

struct QueryRow {
    columns: Vec<String>,
}

fn query_sql(sql: &str) -> Result<Vec<QueryRow>, Box<dyn Error>> {
    let mutex = SHARED_CONNECTION
        .get()
        .ok_or("Migration extension not initialized")?;
    let guard = mutex.lock().map_err(|_| "Connection mutex poisoned")?;
    let conn = guard.0;

    unsafe {
        let c_sql = CString::new(sql)?;
        let mut result: ffi::duckdb_result = std::mem::zeroed();
        let state = ffi::duckdb_query(conn, c_sql.as_ptr(), &mut result);

        if state != ffi::duckdb_state_DuckDBSuccess {
            let err_ptr = ffi::duckdb_result_error(&mut result);
            let err_msg = if err_ptr.is_null() {
                format!("Query failed: {}", sql)
            } else {
                let c_str = std::ffi::CStr::from_ptr(err_ptr);
                format!("{}", c_str.to_string_lossy())
            };
            ffi::duckdb_destroy_result(&mut result);
            return Err(err_msg.into());
        }

        let row_count = ffi::duckdb_row_count(&mut result);
        let col_count = ffi::duckdb_column_count(&mut result);
        let mut rows = Vec::new();

        for row_idx in 0..row_count {
            let mut columns = Vec::new();
            for col_idx in 0..col_count {
                let val = ffi::duckdb_value_varchar(&mut result, col_idx, row_idx);
                let s = if val.is_null() {
                    String::new()
                } else {
                    let c_str = std::ffi::CStr::from_ptr(val);
                    let s = c_str.to_string_lossy().to_string();
                    ffi::duckdb_free(val as *mut _);
                    s
                };
                columns.push(s);
            }
            rows.push(QueryRow { columns });
        }

        ffi::duckdb_destroy_result(&mut result);
        Ok(rows)
    }
}

fn init_shared_connection(db: ffi::duckdb_database) -> Result<(), Box<dyn Error>> {
    // OnceLock::get_or_init is atomic — if two threads race, only one closure runs
    // and the other blocks until the value is set. This avoids the TOCTOU race of
    // a manual get() + set() pattern.
    SHARED_CONNECTION.get_or_init(|| unsafe {
        let mut conn: ffi::duckdb_connection = std::ptr::null_mut();
        let state = ffi::duckdb_connect(db, &mut conn);
        if state != ffi::duckdb_state_DuckDBSuccess {
            // Return a poisoned mutex with null — callers will fail on use
            return Mutex::new(SharedConn(std::ptr::null_mut()));
        }
        Mutex::new(SharedConn(conn))
    });

    // Verify the connection is valid
    let mutex = SHARED_CONNECTION.get().unwrap();
    let guard = mutex.lock().map_err(|_| "Connection mutex poisoned")?;
    if guard.0.is_null() {
        return Err("Failed to create shared connection".into());
    }
    Ok(())
}

// ── Migration File ───────────────────────────────────────────────────────────

struct MigrationFile {
    version: i32,
    name: String,
    sql: String,
    checksum: u64,
}

impl MigrationFile {
    fn from_path(path: &Path) -> Option<Self> {
        let filename = path.file_name()?.to_str()?;

        if !filename.starts_with('V') || !filename.ends_with(".sql") {
            return None;
        }

        let without_prefix = &filename[1..];
        let without_suffix = &without_prefix[..without_prefix.len() - 4];

        let sep_pos = without_suffix.find("__")?;
        let version_str = &without_suffix[..sep_pos];
        let name = &without_suffix[sep_pos + 2..];

        let version: i32 = version_str.parse().ok()?;
        if version <= 0 {
            return None;
        }

        if name.is_empty() || !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return None;
        }

        let sql = fs::read_to_string(path).ok()?;
        let checksum = compute_checksum(name, version, &sql);

        Some(MigrationFile {
            version,
            name: name.to_string(),
            sql,
            checksum,
        })
    }
}

fn compute_checksum(name: &str, version: i32, sql: &str) -> u64 {
    let mut hasher = SipHasher13::new();
    name.hash(&mut hasher);
    version.hash(&mut hasher);
    sql.hash(&mut hasher);
    hasher.finish()
}

// ── File Discovery ───────────────────────────────────────────────────────────

fn discover_migrations(dir_path: &str) -> Result<Vec<MigrationFile>, Box<dyn Error>> {
    let path = Path::new(dir_path);
    if !path.exists() {
        return Err(format!("Directory not found: {}", dir_path).into());
    }
    if !path.is_dir() {
        return Err(format!("Not a directory: {}", dir_path).into());
    }

    let mut migrations = Vec::new();

    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let file_path = entry.path();
        if file_path.is_file() {
            if let Some(migration) = MigrationFile::from_path(&file_path) {
                migrations.push(migration);
            }
        }
    }

    if migrations.is_empty() {
        return Err(format!("No migration files found in: {}", dir_path).into());
    }

    let mut seen_versions: HashMap<i32, String> = HashMap::new();
    for m in &migrations {
        if let Some(existing) = seen_versions.get(&m.version) {
            return Err(format!(
                "Duplicate version {}: found in both '{}' and 'V{}__{}.sql'",
                m.version, existing, m.version, m.name
            )
            .into());
        }
        seen_versions.insert(m.version, format!("V{}__{}.sql", m.version, m.name));
    }

    migrations.sort_by_key(|m| m.version);
    Ok(migrations)
}

// ── Schema History ───────────────────────────────────────────────────────────

#[allow(dead_code)]
struct AppliedMigration {
    version: i32,
    name: String,
    applied_on: String,
    checksum: u64,
}

fn ensure_history_table() -> Result<(), Box<dyn Error>> {
    execute_sql(
        "CREATE TABLE IF NOT EXISTS refinery_schema_history(\
            version INT4 PRIMARY KEY,\
            name VARCHAR(255),\
            applied_on VARCHAR(255),\
            checksum VARCHAR(255)\
        );",
    )
}

fn query_applied_migrations() -> Result<Vec<AppliedMigration>, Box<dyn Error>> {
    let rows = query_sql(
        "SELECT version, name, applied_on, checksum \
         FROM refinery_schema_history ORDER BY version",
    )?;

    let mut result = Vec::new();
    for row in rows {
        if row.columns.len() < 4 {
            continue;
        }
        let version: i32 = row.columns[0]
            .parse()
            .map_err(|_| format!("Invalid version in schema history: {}", row.columns[0]))?;
        let checksum: u64 = row.columns[3]
            .parse()
            .map_err(|_| format!("Invalid checksum in schema history: {}", row.columns[3]))?;
        result.push(AppliedMigration {
            version,
            name: row.columns[1].clone(),
            applied_on: row.columns[2].clone(),
            checksum,
        });
    }
    Ok(result)
}

fn insert_migration_record(migration: &MigrationFile) -> Result<(), Box<dyn Error>> {
    let mutex = SHARED_CONNECTION
        .get()
        .ok_or("Migration extension not initialized")?;
    let guard = mutex.lock().map_err(|_| "Connection mutex poisoned")?;
    let conn = guard.0;

    unsafe {
        let sql = CString::new(
            "INSERT INTO refinery_schema_history (version, name, applied_on, checksum) \
             VALUES ($1, $2, $3, $4);",
        )?;
        let mut stmt: ffi::duckdb_prepared_statement = std::ptr::null_mut();
        let state = ffi::duckdb_prepare(conn, sql.as_ptr(), &mut stmt);
        if state != ffi::duckdb_state_DuckDBSuccess {
            let err = ffi::duckdb_prepare_error(stmt);
            let msg = if err.is_null() {
                "Failed to prepare insert statement".to_string()
            } else {
                std::ffi::CStr::from_ptr(err).to_string_lossy().to_string()
            };
            ffi::duckdb_destroy_prepare(&mut stmt);
            return Err(msg.into());
        }

        ffi::duckdb_bind_int32(stmt, 1, migration.version);
        let c_name = CString::new(migration.name.as_str())?;
        ffi::duckdb_bind_varchar(stmt, 2, c_name.as_ptr());
        let applied_on = Utc::now().to_rfc3339();
        let c_applied_on = CString::new(applied_on.as_str())?;
        ffi::duckdb_bind_varchar(stmt, 3, c_applied_on.as_ptr());
        let checksum_str = migration.checksum.to_string();
        let c_checksum = CString::new(checksum_str.as_str())?;
        ffi::duckdb_bind_varchar(stmt, 4, c_checksum.as_ptr());

        let mut result: ffi::duckdb_result = std::mem::zeroed();
        let exec_state = ffi::duckdb_execute_prepared(stmt, &mut result);

        let ok = if exec_state != ffi::duckdb_state_DuckDBSuccess {
            let err_ptr = ffi::duckdb_result_error(&mut result);
            let msg = if err_ptr.is_null() {
                "Failed to insert migration record".to_string()
            } else {
                std::ffi::CStr::from_ptr(err_ptr).to_string_lossy().to_string()
            };
            Err(msg)
        } else {
            Ok(())
        };

        ffi::duckdb_destroy_result(&mut result);
        ffi::duckdb_destroy_prepare(&mut stmt);
        ok?;
    }
    Ok(())
}

// ── Verification ─────────────────────────────────────────────────────────────

fn verify_migrations(
    discovered: &[MigrationFile],
    applied: &[AppliedMigration],
) -> Result<Vec<usize>, Box<dyn Error>> {
    let applied_map: HashMap<i32, &AppliedMigration> =
        applied.iter().map(|a| (a.version, a)).collect();

    let mut pending_indices = Vec::new();

    for (idx, migration) in discovered.iter().enumerate() {
        if let Some(applied_migration) = applied_map.get(&migration.version) {
            if applied_migration.checksum != migration.checksum {
                return Err(format!(
                    "Checksum mismatch for migration V{}__{}: \
                     file has been modified since it was applied",
                    migration.version, migration.name
                )
                .into());
            }
        } else {
            pending_indices.push(idx);
        }
    }

    Ok(pending_indices)
}

// ── Migration Execution ──────────────────────────────────────────────────────

struct MigrationResult {
    version: i32,
    name: String,
    status: String,
}

fn execute_migrations(
    discovered: &[MigrationFile],
    pending_indices: &[usize],
) -> Result<Vec<MigrationResult>, Box<dyn Error>> {
    let mut results = Vec::new();
    let pending_set: std::collections::HashSet<usize> =
        pending_indices.iter().copied().collect();

    for (idx, migration) in discovered.iter().enumerate() {
        if !pending_set.contains(&idx) {
            results.push(MigrationResult {
                version: migration.version,
                name: migration.name.clone(),
                status: "skipped".to_string(),
            });
        }
    }

    for &idx in pending_indices {
        let migration = &discovered[idx];

        execute_sql("BEGIN TRANSACTION;")?;
        match execute_sql(&migration.sql) {
            Ok(_) => match insert_migration_record(migration) {
                Ok(_) => {
                    execute_sql("COMMIT;")?;
                    results.push(MigrationResult {
                        version: migration.version,
                        name: migration.name.clone(),
                        status: "applied".to_string(),
                    });
                }
                Err(e) => {
                    let rollback_err = execute_sql("ROLLBACK;").err();
                    let mut msg = format!(
                        "Migration V{}__{} failed to record: {}",
                        migration.version, migration.name, e
                    );
                    if let Some(re) = rollback_err {
                        msg.push_str(&format!("; rollback also failed: {}", re));
                    }
                    return Err(msg.into());
                }
            },
            Err(e) => {
                let rollback_err = execute_sql("ROLLBACK;").err();
                let mut msg = format!(
                    "Migration V{}__{} failed: {}",
                    migration.version, migration.name, e
                );
                if let Some(re) = rollback_err {
                    msg.push_str(&format!("; rollback also failed: {}", re));
                }
                return Err(msg.into());
            }
        }
    }

    results.sort_by_key(|r| r.version);
    Ok(results)
}

// ── MigrateVTab ──────────────────────────────────────────────────────────────

#[repr(C)]
struct MigrateBindData {
    path: String,
}

#[repr(C)]
struct MigrateInitData {
    results: Vec<MigrationResult>,
    index: AtomicUsize,
}

struct MigrateVTab;

impl VTab for MigrateVTab {
    type InitData = MigrateInitData;
    type BindData = MigrateBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("version", LogicalTypeHandle::from(LogicalTypeId::Integer));
        bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("status", LogicalTypeHandle::from(LogicalTypeId::Varchar));

        let path = bind.get_parameter(0).to_string();
        Ok(MigrateBindData { path })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = init.get_bind_data::<Self::BindData>();
        if bind_data.is_null() {
            return Err("Bind data is null".into());
        }
        let path = unsafe { (*bind_data).path.clone() };

        let discovered = discover_migrations(&path)?;
        ensure_history_table()?;
        let applied = query_applied_migrations()?;
        let pending_indices = verify_migrations(&discovered, &applied)?;
        let results = execute_migrations(&discovered, &pending_indices)?;

        Ok(MigrateInitData {
            results,
            index: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();
        let current_index = init_data.index.fetch_add(1, Ordering::Relaxed);

        if current_index >= init_data.results.len() {
            output.set_len(0);
            return Ok(());
        }

        let result = &init_data.results[current_index];

        let mut version_vector = output.flat_vector(0);
        version_vector.as_mut_slice::<i32>()[0] = result.version;

        let name_vector = output.flat_vector(1);
        name_vector.insert(0, result.name.as_str());

        let status_vector = output.flat_vector(2);
        status_vector.insert(0, result.status.as_str());

        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

// ── MigrationStatusVTab ──────────────────────────────────────────────────────

struct MigrationStatusResult {
    version: i32,
    name: String,
    status: String,
    applied_on: String,
    checksum: String,
}

#[repr(C)]
struct MigrationStatusBindData {
    path: String,
}

#[repr(C)]
struct MigrationStatusInitData {
    results: Vec<MigrationStatusResult>,
    index: AtomicUsize,
}

struct MigrationStatusVTab;

impl VTab for MigrationStatusVTab {
    type InitData = MigrationStatusInitData;
    type BindData = MigrationStatusBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("version", LogicalTypeHandle::from(LogicalTypeId::Integer));
        bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("status", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "applied_on",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column("checksum", LogicalTypeHandle::from(LogicalTypeId::Varchar));

        let path = bind.get_parameter(0).to_string();
        Ok(MigrationStatusBindData { path })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = init.get_bind_data::<Self::BindData>();
        if bind_data.is_null() {
            return Err("Bind data is null".into());
        }
        let path = unsafe { (*bind_data).path.clone() };

        let discovered = discover_migrations(&path)?;
        ensure_history_table()?;
        let applied = query_applied_migrations()?;

        let applied_map: HashMap<i32, &AppliedMigration> =
            applied.iter().map(|a| (a.version, a)).collect();

        let mut results = Vec::new();
        for migration in &discovered {
            let (status, applied_on) = match applied_map.get(&migration.version) {
                Some(am) => {
                    if am.checksum == migration.checksum {
                        ("applied".to_string(), am.applied_on.clone())
                    } else {
                        ("checksum_mismatch".to_string(), am.applied_on.clone())
                    }
                }
                None => ("pending".to_string(), String::new()),
            };

            results.push(MigrationStatusResult {
                version: migration.version,
                name: migration.name.clone(),
                status,
                applied_on,
                checksum: migration.checksum.to_string(),
            });
        }

        Ok(MigrationStatusInitData {
            results,
            index: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();
        let current_index = init_data.index.fetch_add(1, Ordering::Relaxed);

        if current_index >= init_data.results.len() {
            output.set_len(0);
            return Ok(());
        }

        let result = &init_data.results[current_index];

        let mut version_vector = output.flat_vector(0);
        version_vector.as_mut_slice::<i32>()[0] = result.version;

        let name_vector = output.flat_vector(1);
        name_vector.insert(0, result.name.as_str());

        let status_vector = output.flat_vector(2);
        status_vector.insert(0, result.status.as_str());

        let applied_on_vector = output.flat_vector(3);
        applied_on_vector.insert(0, result.applied_on.as_str());

        let checksum_vector = output.flat_vector(4);
        checksum_vector.insert(0, result.checksum.as_str());

        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

// ── Extension Entrypoint (manual C API, following hana pattern) ──────────────

unsafe fn extension_entrypoint(connection: Connection) -> Result<(), Box<dyn Error>> {
    connection.register_table_function::<MigrateVTab>("migrate")?;
    connection.register_table_function::<MigrationStatusVTab>("migration_status")?;
    Ok(())
}

unsafe fn migration_init_c_api_internal(
    info: ffi::duckdb_extension_info,
    access: *const ffi::duckdb_extension_access,
) -> Result<bool, Box<dyn Error>> {
    let have_api_struct =
        ffi::duckdb_rs_extension_api_init(info, access, "v1.3.2").unwrap();

    if !have_api_struct {
        return Ok(false);
    }

    let db: ffi::duckdb_database = *(*access).get_database.unwrap()(info);

    init_shared_connection(db)?;

    let connection = Connection::open_from_raw(db.cast())?;
    extension_entrypoint(connection)?;

    Ok(true)
}

#[no_mangle]
pub unsafe extern "C" fn migration_init_c_api(
    info: ffi::duckdb_extension_info,
    access: *const ffi::duckdb_extension_access,
) -> bool {
    let init_result = migration_init_c_api_internal(info, access);

    if let Err(x) = init_result {
        let error_c_string = std::ffi::CString::new(x.to_string());
        match error_c_string {
            Ok(e) => {
                (*access).set_error.unwrap()(info, e.as_ptr());
            }
            Err(_) => {
                let error_msg =
                    c"An error occurred but the extension failed to allocate an error string";
                (*access).set_error.unwrap()(info, error_msg.as_ptr());
            }
        }
        return false;
    }

    init_result.unwrap()
}
