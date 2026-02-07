"""HANA standalone integration tests.

Verifies that the hana extension can load, scan queries via hana_scan/hana_query,
and execute DDL via hana_execute against an SAP HANA Express instance.

Start HANA before running:  make hana-up
"""

import os
import socket
import time

import pytest

HANA_TEST_URL = os.environ.get(
    "HANA_TEST_URL", "hdbsql://SYSTEM:Toor1234@localhost:39041/HDB"
)


def _hana_reachable(host="localhost", port=39041, timeout=2):
    """Return True if the HANA SQL port accepts TCP connections."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _hana_reachable(), reason="HANA not reachable on localhost:39041"
)


def test_hana_load(node_factory):
    """Extension loads and basic DuckDB SQL works."""
    node = node_factory(load_hana=True, load_flight=False, load_swarm=False)
    result = node.execute("SELECT 1")
    assert result == [(1,)]


def test_hana_scan_basic(node_factory):
    """hana_scan() returns data from a simple HANA query."""
    node = node_factory(load_hana=True, load_flight=False, load_swarm=False)
    result = node.execute(
        f"SELECT * FROM hana_scan('SELECT 1 AS a FROM DUMMY', '{HANA_TEST_URL}')"
    )
    assert len(result) == 1
    assert result[0][0] == 1


def test_hana_query_alias(node_factory):
    """hana_query() is an alias for hana_scan() and returns the same result."""
    node = node_factory(load_hana=True, load_flight=False, load_swarm=False)
    scan_result = node.execute(
        f"SELECT * FROM hana_scan('SELECT 1 AS a FROM DUMMY', '{HANA_TEST_URL}')"
    )
    query_result = node.execute(
        f"SELECT * FROM hana_query('SELECT 1 AS a FROM DUMMY', '{HANA_TEST_URL}')"
    )
    assert scan_result == query_result


def test_hana_scan_system_table(node_factory):
    """hana_scan() can query HANA system tables."""
    node = node_factory(load_hana=True, load_flight=False, load_swarm=False)
    result = node.execute(
        f"SELECT * FROM hana_scan("
        f"'SELECT SCHEMA_NAME FROM SYS.TABLES WHERE TABLE_NAME = ''DUMMY''', "
        f"'{HANA_TEST_URL}')"
    )
    assert len(result) >= 1
    schemas = [row[0] for row in result]
    assert "SYS" in schemas


def test_hana_execute_ddl(node_factory):
    """hana_execute() can run DDL (CREATE/DROP TABLE)."""
    node = node_factory(load_hana=True, load_flight=False, load_swarm=False)
    table_name = f"TREX_TEST_{int(time.time())}"
    try:
        node.execute(
            f"SELECT hana_execute('{HANA_TEST_URL}', "
            f"'CREATE TABLE {table_name} (ID INTEGER, NAME NVARCHAR(100))')"
        )
        # Verify the table exists
        result = node.execute(
            f"SELECT * FROM hana_scan("
            f"'SELECT TABLE_NAME FROM SYS.TABLES WHERE TABLE_NAME = ''{table_name}''', "
            f"'{HANA_TEST_URL}')"
        )
        tables = [row[0] for row in result]
        assert table_name in tables
    finally:
        try:
            node.execute(
                f"SELECT hana_execute('{HANA_TEST_URL}', 'DROP TABLE {table_name}')"
            )
        except Exception:
            pass


def test_hana_scan_error_handling(node_factory):
    """hana_scan() raises RuntimeError on invalid SQL."""
    node = node_factory(load_hana=True, load_flight=False, load_swarm=False)
    with pytest.raises(RuntimeError):
        node.execute(
            f"SELECT * FROM hana_scan('SELECT * FROM NONEXISTENT_TABLE_XYZ', "
            f"'{HANA_TEST_URL}')"
        )
