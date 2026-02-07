"""Tier 1: Single-node smoke tests.

Verifies that a single DuckDB instance can load both extensions,
start a Flight server, start swarm, and register itself.
"""

from conftest import wait_for


def test_load_extensions(node_factory):
    """Both extensions load without error."""
    node = node_factory(load_flight=True, load_swarm=True)
    result = node.execute("SELECT 1")
    assert result == [(1,)]


def test_flight_server_lifecycle(node_factory):
    """Start flight server, check status, stop it."""
    node = node_factory(load_flight=True, load_swarm=False)

    # Create test data
    node.execute(
        "CREATE TABLE orders AS "
        "SELECT i as id, 'US' as region, random() * 100 as price "
        "FROM range(100) t(i)"
    )

    # Start flight server
    node.execute(f"SELECT start_flight_server('0.0.0.0', {node.flight_port})")

    # Verify server is running
    status = node.execute("SELECT * FROM flight_server_status()")
    assert len(status) > 0, "flight_server_status() returned no rows"

    # Stop flight server
    node.execute(f"SELECT stop_flight_server('0.0.0.0', {node.flight_port})")


def test_swarm_self_discovery(node_factory):
    """Start swarm, register flight service, see self in swarm_nodes()."""
    node = node_factory(load_flight=True, load_swarm=True)

    # Create test data
    node.execute(
        "CREATE TABLE orders AS "
        "SELECT i as id, 'US' as region, random() * 100 as price "
        "FROM range(100) t(i)"
    )

    # Start flight + swarm
    node.execute(f"SELECT start_flight_server('0.0.0.0', {node.flight_port})")
    node.execute(f"SELECT swarm_start('0.0.0.0', {node.gossip_port}, 'test-cluster')")
    node.execute(
        f"SELECT swarm_register_service('flight', '127.0.0.1', {node.flight_port})"
    )

    # Verify self-discovery via swarm_nodes
    nodes = wait_for(
        node,
        "SELECT * FROM swarm_nodes()",
        lambda rows: len(rows) >= 1,
        timeout=5,
    )
    assert len(nodes) >= 1, f"Expected at least 1 node, got {len(nodes)}"


def test_swarm_tables_single_node(node_factory):
    """Single-node swarm_tables() shows local table."""
    node = node_factory(load_flight=True, load_swarm=True)

    node.execute(
        "CREATE TABLE orders AS "
        "SELECT i as id, 'US' as region, random() * 100 as price "
        "FROM range(100) t(i)"
    )

    node.execute(f"SELECT start_flight_server('0.0.0.0', {node.flight_port})")
    node.execute(f"SELECT swarm_start('0.0.0.0', {node.gossip_port}, 'test-cluster')")
    node.execute(
        f"SELECT swarm_register_service('flight', '127.0.0.1', {node.flight_port})"
    )

    tables = wait_for(
        node,
        "SELECT * FROM swarm_tables()",
        lambda rows: len(rows) >= 1,
        timeout=15,
    )
    assert len(tables) >= 1, f"Expected at least 1 table entry, got {len(tables)}"
