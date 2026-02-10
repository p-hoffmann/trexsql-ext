# TrexSQL

A monorepo of TrexSQL

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
| `ext/tpm` | Rust | Package manager functions (resolve, install, tree) |
| `ext/circe` | C/C++ | OHDSI Atlas Cohort defintion to SQL translation |
| `ext/llama` | C/C++ | LLM inference via llama.cpp (CUDA/Vulkan/Metal) |
| `ext/hana` | Rust | SAP HANA database scanner |
| `ext/pgwire` | Rust | PostgreSQL wire protocol |
| `ext/chdb` | Rust | ClickHouse integration |
| `ext/flight` | Rust | Arrow Flight SQL |
| `ext/swarm` | Rust | Distributed cluster coordination |
| `ext/pgt` | Rust | PostgreSQL to SQL transformer (library) |

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
