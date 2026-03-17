# TrexSQL

The monorepo of TrexSQL

## `trex` Binary

The root crate builds a `trex` server binary that manages the TrexSQL database, auto-loads all `.trex` extensions from a directory, and waits for a shutdown signal.

```bash
cargo build --release
DATABASE_PATH=my.db EXTENSION_DIR=./extensions ./target/release/trex
```

Pass `--check` to verify extensions load and exit.

## Extensions

| Extension | Language | Description |
|-----------|----------|-------------|
| `plugins/db` | Rust | Distributed cluster coordination + Arrow Flight SQL |
| `plugins/atlas` | C/C++ | OHDSI Atlas Cohort definition to SQL translation |
| `plugins/ai` | C/C++ | LLM inference via llama.cpp (CUDA/Vulkan/Metal) |
| `plugins/tpm` | Rust | Package manager functions (resolve, install, tree) |
| `plugins/hana` | Rust | SAP HANA database scanner |
| `plugins/pgwire` | Rust | PostgreSQL wire protocol |
| `plugins/chdb` | Rust | ClickHouse integration |
| `plugins/etl` | Rust | Supabase ETL (PostgreSQL CDC replication) |
| `plugins/fhir` | Rust | FHIR server |
| `plugins/cql2elm` | C | CQL to ELM translation (GraalVM native) |
| `plugins/migration` | Rust | Database schema migration |
| `plugins/pgt` | Rust | PostgreSQL to SQL transformer (library) |

## Build

```bash
# Clone with submodules
git clone --recurse-submodules <repo-url>
```

All extensions follow the same Makefile pattern:

```bash
make configure      # One-time setup (Python venv, platform detection)
make debug          # Debug build
make release        # Release build
make test_debug     # Run tests
make clean          # Remove build/
make clean_all      # Remove build/ and configure/
```
