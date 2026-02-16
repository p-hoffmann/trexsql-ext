"""Trexas standalone tests.

Verifies that the trexas extension can load, start/stop an HTTP server,
and serve health check endpoints via the embedded Deno runtime.
"""

import json
import time
import urllib.request

from conftest import REPO_ROOT, wait_for


# Paths to trexas service entry points inside the trex submodule.
MAIN_SERVICE_PATH = f"{REPO_ROOT}/ext/runtime/main/index.ts"
EVENT_WORKER_PATH = f"{REPO_ROOT}/ext/runtime/event-worker/index.ts"


def test_load_trexas(node_factory):
    """Extension loads and trex_version() returns a version string."""
    node = node_factory(load_trexas=True, load_flight=False, load_swarm=False)
    result = node.execute("SELECT trex_version()")
    assert len(result) == 1
    assert result[0][0] is not None
    assert len(result[0][0]) > 0


def test_trexas_server_lifecycle(node_factory):
    """Start server, verify it appears in list, stop it, verify it's gone."""
    node = node_factory(load_trexas=True, load_flight=False, load_swarm=False)

    # Start server
    result = node.execute(
        f"SELECT trex_start_server('127.0.0.1', {node.trexas_port}, "
        f"'{MAIN_SERVICE_PATH}', '{EVENT_WORKER_PATH}')"
    )
    assert len(result) == 1

    # Verify server appears in list
    servers = wait_for(
        node,
        "SELECT * FROM trex_list_servers()",
        lambda rows: len(rows) >= 1,
        timeout=10,
    )
    assert len(servers) >= 1

    # Stop server
    server_id = servers[0][0]
    result = node.execute(f"SELECT trex_stop_server({server_id})")
    assert len(result) == 1

    # Verify server is gone
    time.sleep(1)
    servers_after = node.execute("SELECT * FROM trex_list_servers()")
    assert len(servers_after) == 0


def test_trexas_health_endpoint(node_factory):
    """Start server and verify HTTP health endpoint responds."""
    node = node_factory(load_trexas=True, load_flight=False, load_swarm=False)

    # Start server
    node.execute(
        f"SELECT trex_start_server('127.0.0.1', {node.trexas_port}, "
        f"'{MAIN_SERVICE_PATH}', '{EVENT_WORKER_PATH}')"
    )

    # Wait for server to be ready, then hit health endpoint
    deadline = time.time() + 10
    resp_data = None
    while time.time() < deadline:
        try:
            url = f"http://127.0.0.1:{node.trexas_port}/_internal/health"
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=2) as resp:
                resp_data = json.loads(resp.read().decode())
                break
        except Exception:
            time.sleep(0.5)

    assert resp_data is not None, "Health endpoint did not respond within 10s"
    assert resp_data.get("message") == "ok"
