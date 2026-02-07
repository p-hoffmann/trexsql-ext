"""ChDB + Swarm integration tests.

Verifies that chdb can be registered as a metadata-only swarm service
(port=0, no real TCP listener) and discovered via gossip across nodes.
"""

from conftest import wait_for


def test_swarm_register_chdb(node_factory):
    """Start chdb + swarm, register chdb as service, verify it appears in swarm_services()."""
    node = node_factory(load_chdb=True, load_flight=True, load_swarm=True)

    # Start chdb engine
    node.execute("SELECT chdb_start_database('')")

    # Start swarm
    node.execute(
        f"SELECT swarm_start('0.0.0.0', {node.gossip_port}, 'test-cluster')"
    )

    # Register chdb as metadata-only service (port=0, no real listener)
    node.execute("SELECT swarm_register_service('chdb', '127.0.0.1', 0)")

    # Verify chdb shows up in swarm_services()
    services = wait_for(
        node,
        "SELECT * FROM swarm_services()",
        lambda rows: any(r[1] == "chdb" for r in rows),
        timeout=10,
    )
    chdb_rows = [r for r in services if r[1] == "chdb"]
    assert len(chdb_rows) >= 1
    assert chdb_rows[0][4] == "running"  # status column


def test_chdb_queries_with_swarm(node_factory):
    """chdb queries still work while swarm is active."""
    node = node_factory(load_chdb=True, load_flight=True, load_swarm=True)

    # Start chdb + swarm + register
    node.execute("SELECT chdb_start_database('')")
    node.execute(
        f"SELECT swarm_start('0.0.0.0', {node.gossip_port}, 'test-cluster')"
    )
    node.execute("SELECT swarm_register_service('chdb', '127.0.0.1', 0)")

    # Run a chdb query — should work fine alongside swarm
    result = node.execute("SELECT * FROM chdb_scan('SELECT 1 as a')")
    assert len(result) == 1
    assert result[0][0] == "1"


def test_two_node_chdb_discovery(node_factory):
    """Two nodes both register chdb; both visible in swarm_services() via gossip."""
    node_a = node_factory(load_chdb=True, load_flight=True, load_swarm=True)
    node_b = node_factory(load_chdb=True, load_flight=True, load_swarm=True)

    # Node A: start chdb + swarm, register chdb
    node_a.execute("SELECT chdb_start_database('')")
    node_a.execute(
        f"SELECT swarm_start('0.0.0.0', {node_a.gossip_port}, 'test-cluster')"
    )
    node_a.execute("SELECT swarm_register_service('chdb', '127.0.0.1', 0)")

    # Node B: start chdb + swarm (join Node A via seeds), register chdb
    node_b.execute("SELECT chdb_start_database('')")
    node_b.execute(
        f"SELECT swarm_start_seeds('0.0.0.0', {node_b.gossip_port}, 'test-cluster', "
        f"'127.0.0.1:{node_a.gossip_port}')"
    )
    node_b.execute("SELECT swarm_register_service('chdb', '127.0.0.1', 0)")

    # Wait for gossip convergence — both nodes see 2 chdb services
    wait_for(
        node_a,
        "SELECT * FROM swarm_services()",
        lambda rows: len([r for r in rows if r[1] == "chdb"]) >= 2,
        timeout=15,
    )
    services_b = wait_for(
        node_b,
        "SELECT * FROM swarm_services()",
        lambda rows: len([r for r in rows if r[1] == "chdb"]) >= 2,
        timeout=15,
    )
    chdb_rows = [r for r in services_b if r[1] == "chdb"]
    assert len(chdb_rows) >= 2


def test_chdb_service_coexistence(node_factory):
    """Single node runs flight + swarm + chdb; both services registered and visible."""
    node = node_factory(load_chdb=True, load_flight=True, load_swarm=True)

    # Start all services
    node.execute("SELECT chdb_start_database('')")
    node.execute(f"SELECT start_flight_server('0.0.0.0', {node.flight_port})")
    node.execute(
        f"SELECT swarm_start('0.0.0.0', {node.gossip_port}, 'test-cluster')"
    )

    # Register both services
    node.execute(
        f"SELECT swarm_register_service('flight', '127.0.0.1', {node.flight_port})"
    )
    node.execute("SELECT swarm_register_service('chdb', '127.0.0.1', 0)")

    # Verify both service types appear in swarm_services()
    services = wait_for(
        node,
        "SELECT * FROM swarm_services()",
        lambda rows: (
            any(r[1] == "chdb" for r in rows)
            and any(r[1] == "flight" for r in rows)
        ),
        timeout=10,
    )
    chdb_rows = [r for r in services if r[1] == "chdb"]
    flight_rows = [r for r in services if r[1] == "flight"]
    assert len(chdb_rows) >= 1
    assert len(flight_rows) >= 1
