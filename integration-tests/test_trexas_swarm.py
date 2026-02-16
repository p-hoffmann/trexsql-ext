"""Trexas + Swarm integration tests.

Verifies that trexas can be registered as a swarm service and discovered
via gossip across nodes, following the same pattern as chdb swarm tests.
"""

import json
import time
import urllib.request

from conftest import REPO_ROOT, wait_for


MAIN_SERVICE_PATH = f"{REPO_ROOT}/ext/runtime/ext/trexas/main/index.ts"
EVENT_WORKER_PATH = f"{REPO_ROOT}/ext/runtime/ext/trexas/event-worker/index.ts"


def test_swarm_register_trexas(node_factory):
    """Start trexas + swarm, register trexas as service, verify it appears in swarm_services()."""
    node = node_factory(load_trexas=True, load_flight=True, load_swarm=True)

    # Start trex server
    node.execute(
        f"SELECT trex_start_server('127.0.0.1', {node.trexas_port}, "
        f"'{MAIN_SERVICE_PATH}', '{EVENT_WORKER_PATH}')"
    )

    # Start swarm
    node.execute(
        f"SELECT swarm_start('0.0.0.0', {node.gossip_port}, 'test-cluster')"
    )

    # Register trexas as service
    node.execute(
        f"SELECT swarm_register_service('trexas', '127.0.0.1', {node.trexas_port})"
    )

    # Verify trexas shows up in swarm_services()
    services = wait_for(
        node,
        "SELECT * FROM swarm_services()",
        lambda rows: any(r[1] == "trexas" for r in rows),
        timeout=10,
    )
    trexas_rows = [r for r in services if r[1] == "trexas"]
    assert len(trexas_rows) >= 1
    assert trexas_rows[0][4] == "running"  # status column


def test_trexas_queries_with_swarm(node_factory):
    """Trex server still serves HTTP while swarm is active."""
    node = node_factory(load_trexas=True, load_flight=True, load_swarm=True)

    # Start trex server + swarm + register
    node.execute(
        f"SELECT trex_start_server('127.0.0.1', {node.trexas_port}, "
        f"'{MAIN_SERVICE_PATH}', '{EVENT_WORKER_PATH}')"
    )
    node.execute(
        f"SELECT swarm_start('0.0.0.0', {node.gossip_port}, 'test-cluster')"
    )
    node.execute(
        f"SELECT swarm_register_service('trexas', '127.0.0.1', {node.trexas_port})"
    )

    # Verify HTTP health endpoint still works alongside swarm
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


def test_two_node_trexas_discovery(node_factory):
    """Two nodes both register trexas; both visible in swarm_services() via gossip."""
    node_a = node_factory(load_trexas=True, load_flight=True, load_swarm=True)
    node_b = node_factory(load_trexas=True, load_flight=True, load_swarm=True)

    # Node A: start trex server + swarm, register trexas
    node_a.execute(
        f"SELECT trex_start_server('127.0.0.1', {node_a.trexas_port}, "
        f"'{MAIN_SERVICE_PATH}', '{EVENT_WORKER_PATH}')"
    )
    node_a.execute(
        f"SELECT swarm_start('0.0.0.0', {node_a.gossip_port}, 'test-cluster')"
    )
    node_a.execute(
        f"SELECT swarm_register_service('trexas', '127.0.0.1', {node_a.trexas_port})"
    )

    # Node B: start trex server + swarm (join Node A via seeds), register trexas
    node_b.execute(
        f"SELECT trex_start_server('127.0.0.1', {node_b.trexas_port}, "
        f"'{MAIN_SERVICE_PATH}', '{EVENT_WORKER_PATH}')"
    )
    node_b.execute(
        f"SELECT swarm_start_seeds('0.0.0.0', {node_b.gossip_port}, 'test-cluster', "
        f"'127.0.0.1:{node_a.gossip_port}')"
    )
    node_b.execute(
        f"SELECT swarm_register_service('trexas', '127.0.0.1', {node_b.trexas_port})"
    )

    # Wait for gossip convergence - both nodes see 2 trexas services
    wait_for(
        node_a,
        "SELECT * FROM swarm_services()",
        lambda rows: len([r for r in rows if r[1] == "trexas"]) >= 2,
        timeout=15,
    )
    services_b = wait_for(
        node_b,
        "SELECT * FROM swarm_services()",
        lambda rows: len([r for r in rows if r[1] == "trexas"]) >= 2,
        timeout=15,
    )
    trexas_rows = [r for r in services_b if r[1] == "trexas"]
    assert len(trexas_rows) >= 2


def test_trexas_service_coexistence(node_factory):
    """Single node runs flight + swarm + trexas; all services registered and visible."""
    node = node_factory(load_trexas=True, load_flight=True, load_swarm=True)

    # Start all services
    node.execute(
        f"SELECT trex_start_server('127.0.0.1', {node.trexas_port}, "
        f"'{MAIN_SERVICE_PATH}', '{EVENT_WORKER_PATH}')"
    )
    node.execute(f"SELECT start_flight_server('0.0.0.0', {node.flight_port})")
    node.execute(
        f"SELECT swarm_start('0.0.0.0', {node.gossip_port}, 'test-cluster')"
    )

    # Register both services
    node.execute(
        f"SELECT swarm_register_service('flight', '127.0.0.1', {node.flight_port})"
    )
    node.execute(
        f"SELECT swarm_register_service('trexas', '127.0.0.1', {node.trexas_port})"
    )

    # Verify both service types appear in swarm_services()
    services = wait_for(
        node,
        "SELECT * FROM swarm_services()",
        lambda rows: (
            any(r[1] == "trexas" for r in rows)
            and any(r[1] == "flight" for r in rows)
        ),
        timeout=10,
    )
    trexas_rows = [r for r in services if r[1] == "trexas"]
    flight_rows = [r for r in services if r[1] == "flight"]
    assert len(trexas_rows) >= 1
    assert len(flight_rows) >= 1
