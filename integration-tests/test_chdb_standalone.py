"""ChDB standalone tests.

Verifies that the chdb extension can load, start/stop a ClickHouse database
session, and scan query results via chdb_scan/chdb_query.

Note: chdb_execute_dml has a known crash (slice::from_raw_parts panic) so
DML tests are skipped until that is fixed.
"""


def test_chdb_load(node_factory):
    """Extension loads and basic SQL works."""
    node = node_factory(load_chdb=True, load_flight=False, load_swarm=False)
    result = node.execute("SELECT 1")
    assert result == [(1,)]


def test_chdb_start_stop_database(node_factory):
    """chdb_start_database() and chdb_stop_database() lifecycle."""
    node = node_factory(load_chdb=True, load_flight=False, load_swarm=False)

    # Start database
    result = node.execute("SELECT chdb_start_database('')")
    assert len(result) == 1
    assert result[0][0] == "Database started"

    # Stop database
    result = node.execute("SELECT chdb_stop_database()")
    assert len(result) == 1
    assert result[0][0] == "Database stopped"


def test_chdb_scan_query(node_factory):
    """chdb_scan() returns rows from ClickHouse SELECT queries."""
    node = node_factory(load_chdb=True, load_flight=False, load_swarm=False)

    node.execute("SELECT chdb_start_database('')")

    # Simple query
    result = node.execute("SELECT * FROM chdb_scan('SELECT 1 as a, 2 as b')")
    assert len(result) == 1
    assert result[0][0] == "1"
    assert result[0][1] == "2"

    node.execute("SELECT chdb_stop_database()")


def test_chdb_scan_version(node_factory):
    """chdb_scan() can query ClickHouse system functions."""
    node = node_factory(load_chdb=True, load_flight=False, load_swarm=False)

    node.execute("SELECT chdb_start_database('')")

    result = node.execute("SELECT * FROM chdb_scan('SELECT version()')")
    assert len(result) == 1
    version = result[0][0]
    assert version is not None
    # ClickHouse version format: major.minor.patch.build
    assert "." in version

    node.execute("SELECT chdb_stop_database()")


def test_chdb_query_alias(node_factory):
    """chdb_query() is an alias for chdb_scan() and returns same results."""
    node = node_factory(load_chdb=True, load_flight=False, load_swarm=False)

    node.execute("SELECT chdb_start_database('')")

    scan_result = node.execute(
        "SELECT * FROM chdb_scan('SELECT 1 as a, 2 as b')"
    )
    query_result = node.execute(
        "SELECT * FROM chdb_query('SELECT 1 as a, 2 as b')"
    )
    assert scan_result == query_result

    node.execute("SELECT chdb_stop_database()")
