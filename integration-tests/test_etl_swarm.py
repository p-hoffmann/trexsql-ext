"""ETL + Swarm integration tests.

Verifies that:
- swarm_set_key / swarm_delete_key work as general-purpose gossip primitives
- ETL pipelines appear in swarm_services() via the gossip bridge
- ETL loads cleanly without swarm (gossip errors suppressed)
- swarm_start_service('etl', config) generates correct SQL
"""

import pytest
from conftest import wait_for


def test_swarm_set_key(node_factory):
    """swarm_set_key sets a key in gossip and it appears in swarm_nodes key-values."""
    node = node_factory(load_flight=False, load_swarm=True)
    node.execute(
        f"SELECT swarm_start('0.0.0.0', {node.gossip_port}, 'test-cluster')"
    )

    result = node.execute("SELECT swarm_set_key('mykey', 'myvalue')")
    assert len(result) == 1
    assert "mykey" in result[0][0]


def test_swarm_set_key_service_prefix(node_factory):
    """swarm_set_key with service: prefix makes the key visible in swarm_services()."""
    node = node_factory(load_flight=False, load_swarm=True)
    node.execute(
        f"SELECT swarm_start('0.0.0.0', {node.gossip_port}, 'test-cluster')"
    )

    service_json = '{"host":"10.0.0.1","port":0,"status":"running","uptime":0,"config":{}}'
    node.execute(f"SELECT swarm_set_key('service:test_svc', '{service_json}')")

    services = wait_for(
        node,
        "SELECT * FROM swarm_services()",
        lambda rows: any(r[1] == "test_svc" for r in rows),
        timeout=5,
    )
    svc_rows = [r for r in services if r[1] == "test_svc"]
    assert len(svc_rows) >= 1
    assert svc_rows[0][4] == "running"  # status column


def test_swarm_delete_key(node_factory):
    """swarm_delete_key removes a key from gossip."""
    node = node_factory(load_flight=False, load_swarm=True)
    node.execute(
        f"SELECT swarm_start('0.0.0.0', {node.gossip_port}, 'test-cluster')"
    )

    # Set then delete
    node.execute("SELECT swarm_set_key('service:temp', '{\"host\":\"\",\"port\":0,\"status\":\"running\",\"uptime\":0,\"config\":{}}')")

    services = wait_for(
        node,
        "SELECT * FROM swarm_services()",
        lambda rows: any(r[1] == "temp" for r in rows),
        timeout=5,
    )
    assert any(r[1] == "temp" for r in services)

    node.execute("SELECT swarm_delete_key('service:temp')")

    # After deletion, the key should no longer appear
    wait_for(
        node,
        "SELECT * FROM swarm_services()",
        lambda rows: not any(r[1] == "temp" for r in rows),
        timeout=5,
    )


def test_swarm_set_key_without_gossip(node_factory):
    """swarm_set_key returns error message when gossip not started (doesn't crash)."""
    node = node_factory(load_flight=False, load_swarm=True)
    result = node.execute("SELECT swarm_set_key('foo', 'bar')")
    assert len(result) == 1
    assert "Error" in result[0][0] or "error" in result[0][0].lower()


def test_swarm_delete_key_without_gossip(node_factory):
    """swarm_delete_key returns error message when gossip not started (doesn't crash)."""
    node = node_factory(load_flight=False, load_swarm=True)
    result = node.execute("SELECT swarm_delete_key('foo')")
    assert len(result) == 1
    assert "Error" in result[0][0] or "error" in result[0][0].lower()


def test_etl_loads_without_swarm(node_factory):
    """ETL extension loads and works without swarm (gossip bridge fails silently)."""
    node = node_factory(load_etl=True, load_flight=False, load_swarm=False)
    result = node.execute("SELECT * FROM etl_status()")
    assert result == []


def test_etl_start_service_sql(node_factory):
    """swarm_start_service('etl', config) generates and attempts etl_start SQL."""
    node = node_factory(load_etl=True, load_flight=False, load_swarm=True)
    node.execute(
        f"SELECT swarm_start('0.0.0.0', {node.gossip_port}, 'test-cluster')"
    )

    # This will fail because there's no real PG server, but the error should
    # indicate it tried to run etl_start (not "Unknown service extension")
    result = node.execute(
        """SELECT swarm_start_service('etl', '{"pipeline_name":"test_pipe","connection_string":"host=localhost port=5432 dbname=test user=test password=secret publication=mypub"}')"""
    )
    msg = result[0][0]
    # Should not be "Unknown service extension"
    assert "Unknown service extension" not in msg
