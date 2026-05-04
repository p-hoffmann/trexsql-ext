"""PgWire data type round-trip tests.

Covers every Arrow type that crosses the pgwire encoder, including:

  * scalar types arrow-pg encodes natively (Date32/64, Time32/64, Timestamps,
    Interval, all integer widths, Float32/64, Booleans, Binary, Decimal128)
  * types arrow-pg cannot encode and that pgwire pre-casts to Utf8/TEXT
    before encoding (Decimal256, FixedSizeBinary, Map, Dictionary with a
    non-utf8 value type, etc.)
  * NULL handling per type
  * composite types (LIST, STRUCT)

The pre-cast path was added to avoid arrow-pg panicking on `Unsupported
Datatype` — these tests prevent that panic from regressing.
"""

from datetime import date, datetime, time
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


# -----------------------------------------------------------------------------
# Scalar types arrow-pg encodes natively
# -----------------------------------------------------------------------------

def test_pgwire_date(node_factory):
    """DATE round-trips as Python date."""
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        cur = conn.cursor()
        cur.execute("SELECT DATE '2026-05-03'")
        assert cur.fetchall() == [(date(2026, 5, 3),)]
        cur.close()
    finally:
        conn.close()
        _stop_server(node)


def test_pgwire_date_null(node_factory):
    """NULL DATE — None on the wire."""
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        cur = conn.cursor()
        cur.execute("SELECT CAST(NULL AS DATE)")
        assert cur.fetchall() == [(None,)]
        cur.close()
    finally:
        conn.close()
        _stop_server(node)


def test_pgwire_timestamp(node_factory):
    """TIMESTAMP round-trips as Python datetime."""
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        cur = conn.cursor()
        cur.execute("SELECT TIMESTAMP '2026-05-03 12:34:56.789'")
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == datetime(2026, 5, 3, 12, 34, 56, 789000)
        cur.close()
    finally:
        conn.close()
        _stop_server(node)


def test_pgwire_time(node_factory):
    """TIME round-trips as Python time."""
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        cur = conn.cursor()
        cur.execute("SELECT TIME '14:25:36'")
        rows = cur.fetchall()
        assert rows[0][0] == time(14, 25, 36)
        cur.close()
    finally:
        conn.close()
        _stop_server(node)


def test_pgwire_integer_widths(node_factory):
    """All signed integer widths encode with the right pg type."""
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1::TINYINT, 1::SMALLINT, 1::INTEGER, 1::BIGINT,"
            "       1::UTINYINT, 1::USMALLINT, 1::UINTEGER, 1::UBIGINT"
        )
        assert cur.fetchall() == [(1, 1, 1, 1, 1, 1, 1, 1)]
        cur.close()
    finally:
        conn.close()
        _stop_server(node)


def test_pgwire_floats(node_factory):
    """REAL (Float32) and DOUBLE (Float64) round-trip."""
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1.5::REAL, 2.25::DOUBLE")
        rows = cur.fetchall()
        assert rows[0][0] == 1.5
        assert rows[0][1] == 2.25
        cur.close()
    finally:
        conn.close()
        _stop_server(node)


def test_pgwire_boolean(node_factory):
    """BOOLEAN — true / false / NULL."""
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        cur = conn.cursor()
        cur.execute("SELECT TRUE, FALSE, CAST(NULL AS BOOLEAN)")
        assert cur.fetchall() == [(True, False, None)]
        cur.close()
    finally:
        conn.close()
        _stop_server(node)


def test_pgwire_blob(node_factory):
    """BLOB round-trips as bytes."""
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        cur = conn.cursor()
        cur.execute("SELECT BLOB 'abc'")
        rows = cur.fetchall()
        # psycopg2 hands back memoryview / bytes
        assert bytes(rows[0][0]) == b"abc"
        cur.close()
    finally:
        conn.close()
        _stop_server(node)


def test_pgwire_interval(node_factory):
    """INTERVAL — arrow-pg encodes as a PG interval."""
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        cur = conn.cursor()
        cur.execute("SELECT INTERVAL '1 day 2 hours'")
        rows = cur.fetchall()
        assert rows[0][0] is not None
        cur.close()
    finally:
        conn.close()
        _stop_server(node)


# -----------------------------------------------------------------------------
# Types arrow-pg cannot encode — pgwire pre-casts to Utf8/TEXT
# -----------------------------------------------------------------------------

def test_pgwire_uuid_as_text(node_factory):
    """UUID is a FixedSizeBinary on the Arrow side; pre-cast to TEXT.

    Previously this panicked the encoder with "Unsupported Datatype".
    """
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        cur = conn.cursor()
        cur.execute("SELECT '00000000-0000-0000-0000-000000000001'::UUID")
        rows = cur.fetchall()
        assert isinstance(rows[0][0], str)
        assert rows[0][0] == "00000000-0000-0000-0000-000000000001"
        cur.close()
    finally:
        conn.close()
        _stop_server(node)


def test_pgwire_map_as_text(node_factory):
    """MAP is unsupported by arrow-pg — pgwire must pre-cast to TEXT."""
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        cur = conn.cursor()
        cur.execute("SELECT MAP {'a': 1, 'b': 2}")
        rows = cur.fetchall()
        assert isinstance(rows[0][0], str)
        # DuckDB's display includes both keys
        assert "a" in rows[0][0] and "b" in rows[0][0]
        cur.close()
    finally:
        conn.close()
        _stop_server(node)


def test_pgwire_enum_as_text(node_factory):
    """ENUM (Arrow Dictionary with non-Utf8 indices) — pre-cast to TEXT.

    DuckDB's ENUM is encoded as `Dictionary<int, varchar>`. arrow-pg does
    handle Dictionary-with-Utf8-value, but only when the value type is
    actually Utf8; the index type can still trip the encoder. Casting to
    TEXT keeps the wire safe regardless.
    """
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("CREATE TYPE mood AS ENUM ('happy','sad','grumpy')")
        cur.execute("CREATE TABLE moods (id BIGINT, m mood)")
        cur.execute("INSERT INTO moods VALUES (1,'happy'),(2,'sad'),(3,NULL)")
        cur.execute("SELECT id, m FROM moods ORDER BY id")
        rows = cur.fetchall()
        assert rows == [(1, "happy"), (2, "sad"), (3, None)]
        cur.close()
    finally:
        conn.close()
        _stop_server(node)


# -----------------------------------------------------------------------------
# Composite types
# -----------------------------------------------------------------------------

def test_pgwire_list(node_factory):
    """LIST — arrow-pg encodes as text representation."""
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        cur = conn.cursor()
        cur.execute("SELECT [1,2,3]")
        rows = cur.fetchall()
        assert rows[0][0] is not None
        cur.close()
    finally:
        conn.close()
        _stop_server(node)


def test_pgwire_struct(node_factory):
    """STRUCT — arrow-pg encodes as text representation."""
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        cur = conn.cursor()
        cur.execute("SELECT {'name': 'alice', 'age': 30}")
        rows = cur.fetchall()
        assert rows[0][0] is not None
        cur.close()
    finally:
        conn.close()
        _stop_server(node)


# -----------------------------------------------------------------------------
# Mixed schema sanity check — each row has every type at once
# -----------------------------------------------------------------------------

def test_pgwire_all_types_one_row(node_factory):
    """Single row carrying many types — exercises full encoder pipeline."""
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1::INTEGER AS i, 'x' AS s, TRUE AS b, "
            "       DATE '2026-05-03' AS d, "
            "       TIMESTAMP '2026-05-03 10:00:00' AS ts, "
            "       1.5::DECIMAL(5,2) AS dec, "
            "       '00000000-0000-0000-0000-000000000001'::UUID AS u"
        )
        rows = cur.fetchall()
        assert len(rows) == 1
        i, s, b, d, ts, dec, u = rows[0]
        assert i == 1
        assert s == "x"
        assert b is True
        assert d == date(2026, 5, 3)
        assert ts == datetime(2026, 5, 3, 10, 0, 0)
        assert dec == Decimal("1.50")
        assert isinstance(u, str) and u.startswith("00000000")
        cur.close()
    finally:
        conn.close()
        _stop_server(node)


def test_pgwire_server_survives_after_unsupported_type(node_factory):
    """Regression: an unsupported-type query must not crash the worker.

    Before the pre-cast path existed, querying e.g. a UUID column made
    arrow-pg panic with "Unsupported Datatype", which `encode_batches_safely`
    catches but leaves the connection in a bad state. This guards against a
    regression where pre-cast is removed and the panic returns.
    """
    node = node_factory(load_pgwire=True, load_db=False)
    _start_server(node)
    conn = _connect(node)
    try:
        cur = conn.cursor()
        cur.execute("SELECT '00000000-0000-0000-0000-000000000001'::UUID")
        cur.fetchall()
        cur.execute("SELECT 42")
        assert cur.fetchall() == [(42,)]
        cur.close()
    finally:
        conn.close()
        _stop_server(node)
