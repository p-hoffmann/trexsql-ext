"""PgWire DECIMAL/NUMERIC encoding tests.

Regression coverage for a bug where any DECIMAL/NUMERIC value crossing
the pgwire wire encoder crashed the trex worker process. The trigger
was as small as `SELECT 1.5` (DuckDB infers DECIMAL) or any explicit
`::numeric` cast. INSERTs into NUMERIC columns were unaffected; only
SELECTs that returned a NUMERIC-typed column to the client crashed.

These tests:
  * exercise every shape of NUMERIC return value over the wire
  * assert the connection stays alive after a decimal query
    (the previous bug killed the entire trex process, not just the
    connection)
"""

from decimal import Decimal

import psycopg2


def _start_server(node):
    node.execute(
        f"SELECT trex_pgwire_start('127.0.0.1', {node.pgwire_port}, 'test', '')"
    )


def _stop_server(node):
    node.execute(
        f"SELECT trex_pgwire_stop('127.0.0.1', {node.pgwire_port})"
    )


def _connect(node):
    return psycopg2.connect(
        host="127.0.0.1",
        port=node.pgwire_port,
        user="any",
        password="test",
        dbname="memory",
    )


def test_pgwire_decimal_literal_select(node_factory):
    """SELECT 1.5 — DuckDB infers DECIMAL; result must round-trip."""
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1.5")
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == Decimal("1.5")
        cur.close()
    finally:
        conn.close()
        _stop_server(node)


def test_pgwire_explicit_numeric_cast(node_factory):
    """SELECT 1::numeric — explicit cast on integer literal."""
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1::numeric")
        assert cur.fetchall() == [(Decimal(1),)]
        cur.close()
    finally:
        conn.close()
        _stop_server(node)


def test_pgwire_cast_syntax_variants(node_factory):
    """All four equivalent cast spellings must work without crashing."""
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        cur = conn.cursor()
        for sql in (
            "SELECT 1::numeric",
            "SELECT 1::DECIMAL",
            "SELECT CAST(1 AS NUMERIC)",
            "SELECT CAST(1 AS DECIMAL(10,2))",
        ):
            cur.execute(sql)
            rows = cur.fetchall()
            assert len(rows) == 1, f"{sql} returned no rows"
            assert rows[0][0] == Decimal(1), f"{sql} returned {rows[0][0]!r}"
        cur.close()
    finally:
        conn.close()
        _stop_server(node)


def test_pgwire_numeric_column_roundtrip(node_factory):
    """CREATE TABLE with NUMERIC column, INSERT, SELECT — round-trip."""
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("CREATE TABLE prices (id BIGINT, n NUMERIC(10,2))")
        cur.execute("INSERT INTO prices VALUES (1, 3.14), (2, 0.05), (3, 9999.99)")
        cur.execute("SELECT id, n FROM prices ORDER BY id")
        rows = cur.fetchall()
        assert rows == [
            (1, Decimal("3.14")),
            (2, Decimal("0.05")),
            (3, Decimal("9999.99")),
        ]
        cur.close()
    finally:
        conn.close()
        _stop_server(node)


def test_pgwire_numeric_null(node_factory):
    """NULL in a NUMERIC column must encode as Python None."""
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("CREATE TABLE n_nulls (id BIGINT, n NUMERIC(10,2))")
        cur.execute("INSERT INTO n_nulls VALUES (1, NULL), (2, 7.50)")
        cur.execute("SELECT id, n FROM n_nulls ORDER BY id")
        assert cur.fetchall() == [(1, None), (2, Decimal("7.50"))]
        cur.close()
    finally:
        conn.close()
        _stop_server(node)


def test_pgwire_aggregate_with_numeric_cast(node_factory):
    """ROUND(AVG(int_col)::numeric, 1) — the original real-world repro."""
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE people AS "
            "SELECT i AS id, 1900 + (i % 80) AS yob FROM range(100) t(i)"
        )
        cur.execute("SELECT ROUND(AVG(yob)::numeric, 1) FROM people")
        rows = cur.fetchall()
        assert len(rows) == 1
        assert isinstance(rows[0][0], Decimal)
        cur.close()
    finally:
        conn.close()
        _stop_server(node)


def test_pgwire_mixed_columns_with_numeric(node_factory):
    """Ensure column ordering/encoding still works when some cols are NUMERIC."""
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE mixed (id BIGINT, label VARCHAR, "
            "price NUMERIC(10,2), qty INTEGER)"
        )
        cur.execute(
            "INSERT INTO mixed VALUES "
            "(1, 'apple', 0.99, 10), (2, 'pear', 1.25, 4)"
        )
        cur.execute("SELECT id, label, price, qty FROM mixed ORDER BY id")
        assert cur.fetchall() == [
            (1, "apple", Decimal("0.99"), 10),
            (2, "pear", Decimal("1.25"), 4),
        ]
        cur.close()
    finally:
        conn.close()
        _stop_server(node)


def test_pgwire_server_survives_after_decimal_query(node_factory):
    """Regression: a decimal query must not crash the trex worker.

    Previously, any NUMERIC value crossing the wire killed the entire
    trex process. After the fix, the server must still answer subsequent
    queries on the same connection.
    """
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        cur = conn.cursor()
        # If this crashes, the next execute() will raise InterfaceError.
        cur.execute("SELECT 1.5")
        cur.fetchall()
        # Same connection, follow-up query — proves the worker is alive.
        cur.execute("SELECT 42")
        assert cur.fetchall() == [(42,)]
        # And one more decimal, to rule out a one-shot survivor.
        cur.execute("SELECT 2.71828::numeric")
        rows = cur.fetchall()
        assert isinstance(rows[0][0], Decimal)
        cur.close()
    finally:
        conn.close()
        _stop_server(node)


def test_pgwire_many_decimal_rows(node_factory):
    """Multi-row decimal result — exercises per-row encoder loop."""
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        cur = conn.cursor()
        cur.execute("SELECT i::DECIMAL(10,2) FROM range(50) t(i)")
        rows = cur.fetchall()
        assert len(rows) == 50
        assert rows[0][0] == Decimal("0.00")
        assert rows[49][0] == Decimal("49.00")
        for r in rows:
            assert isinstance(r[0], Decimal)
        cur.close()
    finally:
        conn.close()
        _stop_server(node)
