import duckdb
import multiprocessing as mp
import pytest
import time
import os

# Must use 'spawn' so each child gets a fresh process (no inherited static state).
try:
    mp.set_start_method("spawn")
except RuntimeError:
    pass  # already set

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FLIGHT_EXT_TREX = f"{REPO_ROOT}/ext/flight/build/debug/extension/flight/flight.trex"
SWARM_EXT_TREX = f"{REPO_ROOT}/ext/swarm/build/debug/extension/swarm/swarm.trex"
PGWIRE_EXT_TREX = f"{REPO_ROOT}/ext/pgwire/build/debug/extension/pgwire/pgwire.trex"
CIRCE_EXT_TREX = f"{REPO_ROOT}/ext/circe/build/release/extension/circe/circe.trex"
LLAMA_EXT_TREX = f"{REPO_ROOT}/ext/llama/build/debug/extension/llama/llama.trex"
CHDB_EXT_TREX = f"{REPO_ROOT}/ext/chdb/build/debug/extension/chdb/chdb.trex"
HANA_EXT_TREX = f"{REPO_ROOT}/ext/hana/build/debug/extension/hana_scan/hana_scan.trex"
TPM_EXT_TREX = f"{REPO_ROOT}/ext/tpm/build/debug/extension/tpm/tpm.trex"
ETL_EXT_TREX = f"{REPO_ROOT}/ext/etl/build/debug/extension/etl/etl.trex"

# DuckDB Python API requires .duckdb_extension suffix for LOAD.
FLIGHT_EXT = f"{REPO_ROOT}/ext/flight/build/debug/extension/flight/flight.duckdb_extension"
SWARM_EXT = f"{REPO_ROOT}/ext/swarm/build/debug/extension/swarm/swarm.duckdb_extension"
PGWIRE_EXT = f"{REPO_ROOT}/ext/pgwire/build/debug/extension/pgwire/pgwire.duckdb_extension"
CIRCE_EXT = f"{REPO_ROOT}/ext/circe/build/release/extension/circe/circe.duckdb_extension"
LLAMA_EXT = f"{REPO_ROOT}/ext/llama/build/debug/extension/llama/llama.duckdb_extension"
CHDB_EXT = f"{REPO_ROOT}/ext/chdb/build/debug/extension/chdb/chdb.duckdb_extension"
HANA_EXT = f"{REPO_ROOT}/ext/hana/build/debug/extension/hana_scan/hana_scan.duckdb_extension"
TPM_EXT = f"{REPO_ROOT}/ext/tpm/build/debug/extension/tpm/tpm.duckdb_extension"
ETL_EXT = f"{REPO_ROOT}/ext/etl/build/debug/extension/etl/etl.duckdb_extension"

for src, dst in [
    (FLIGHT_EXT_TREX, FLIGHT_EXT),
    (SWARM_EXT_TREX, SWARM_EXT),
    (PGWIRE_EXT_TREX, PGWIRE_EXT),
    (CIRCE_EXT_TREX, CIRCE_EXT),
    (LLAMA_EXT_TREX, LLAMA_EXT),
    (CHDB_EXT_TREX, CHDB_EXT),
    (HANA_EXT_TREX, HANA_EXT),
    (TPM_EXT_TREX, TPM_EXT),
    (ETL_EXT_TREX, ETL_EXT),
]:
    if os.path.exists(src) and not os.path.exists(dst):
        os.symlink(src, dst)

_next_gossip_port = 19000
_next_flight_port = 19100
_next_pgwire_port = 19200


def alloc_ports():
    """Allocate a unique gossip+flight+pgwire port tuple per call."""
    global _next_gossip_port, _next_flight_port, _next_pgwire_port
    gp, fp, pp = _next_gossip_port, _next_flight_port, _next_pgwire_port
    _next_gossip_port += 1
    _next_flight_port += 1
    _next_pgwire_port += 1
    return gp, fp, pp


# ---------------------------------------------------------------------------
# Node: each node is a separate OS process (extensions use static globals)
# ---------------------------------------------------------------------------

def _node_worker(ext_paths, cmd_queue, result_queue):
    """Child process: create DuckDB connection, load extensions, run commands."""
    try:
        conn = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
        for path in ext_paths:
            conn.execute(f"LOAD '{path}'")
        result_queue.put(("ready", None))
    except Exception as e:
        result_queue.put(("init_error", str(e)))
        return

    while True:
        cmd = cmd_queue.get()
        if cmd is None:  # shutdown
            break
        try:
            result = conn.execute(cmd)
            try:
                rows = result.fetchall()
            except Exception:
                rows = []
            result_queue.put(("ok", rows))
        except Exception as e:
            result_queue.put(("error", str(e)))

    conn.close()


class Node:
    """A DuckDB node running in a separate process with extensions loaded."""

    def __init__(self, ext_paths, gossip_port, flight_port, pgwire_port):
        self.gossip_port = gossip_port
        self.flight_port = flight_port
        self.pgwire_port = pgwire_port
        self._cmd_queue = mp.Queue()
        self._result_queue = mp.Queue()
        self._process = mp.Process(
            target=_node_worker,
            args=(ext_paths, self._cmd_queue, self._result_queue),
        )
        self._process.start()
        status, data = self._result_queue.get(timeout=15)
        if status != "ready":
            self._process.join(timeout=5)
            raise RuntimeError(f"Node init failed: {data}")

    def execute(self, sql):
        """Execute SQL and return fetchall() result (list of tuples)."""
        self._cmd_queue.put(sql)
        status, data = self._result_queue.get(timeout=30)
        if status == "error":
            raise RuntimeError(data)
        return data

    def close(self):
        try:
            self._cmd_queue.put(None)
            self._process.join(timeout=5)
        except Exception:
            pass
        if self._process.is_alive():
            self._process.kill()
            self._process.join(timeout=2)


def wait_for(node, sql, check, timeout=10, interval=0.5):
    """Retry sql on node until check(result) returns True."""
    deadline = time.time() + timeout
    last_result = None
    last_err = None
    while time.time() < deadline:
        try:
            last_result = node.execute(sql)
            if check(last_result):
                return last_result
        except Exception as e:
            last_err = e
        time.sleep(interval)
    msg = f"Condition not met within {timeout}s for: {sql}"
    if last_result is not None:
        msg += f"\nLast result: {last_result}"
    if last_err is not None:
        msg += f"\nLast error: {last_err}"
    raise TimeoutError(msg)


@pytest.fixture
def node_factory():
    """Factory that creates DuckDB nodes (each in a separate process)."""
    nodes = []

    def create_node(load_flight=True, load_swarm=True, load_pgwire=False,
                     load_circe=False, load_llama=False, load_chdb=False,
                     load_hana=False, load_tpm=False, load_etl=False):
        ext_paths = []
        if load_flight:
            ext_paths.append(FLIGHT_EXT)
        if load_swarm:
            ext_paths.append(SWARM_EXT)
        if load_pgwire:
            ext_paths.append(PGWIRE_EXT)
        if load_circe:
            ext_paths.append(CIRCE_EXT)
        if load_llama:
            ext_paths.append(LLAMA_EXT)
        if load_chdb:
            ext_paths.append(CHDB_EXT)
        if load_hana:
            ext_paths.append(HANA_EXT)
        if load_tpm:
            ext_paths.append(TPM_EXT)
        if load_etl:
            ext_paths.append(ETL_EXT)
        gossip_port, flight_port, pgwire_port = alloc_ports()
        node = Node(ext_paths, gossip_port, flight_port, pgwire_port)
        nodes.append(node)
        return node

    yield create_node

    for node in nodes:
        try:
            node.close()
        except Exception:
            pass
