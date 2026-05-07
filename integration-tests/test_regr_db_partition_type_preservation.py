"""Regression: trex_db_partition_table must preserve column-type metadata in shards.

Bug fixed: plugins/db/src/partition.rs::arrow_type_to_sql() previously stripped
type metadata from Arrow types when generating shard DDL, causing:

  * Timestamp(_, Some(tz))   -> "TIMESTAMP"        (TIMESTAMPTZ downgraded)
  * Decimal128(p, s)         -> "DECIMAL"          (precision/scale lost)
  * Time32(_) / Time64(_)    -> "TIME"             (acceptable in DuckDB)

After the fix:

  * Timestamp(_, Some(_))    -> "TIMESTAMPTZ"
  * Timestamp(_, None)       -> "TIMESTAMP"
  * Decimal128(p, s)         -> "DECIMAL(p, s)"

This test partitions a table that exercises all three cases and asserts that
the shard's column types in information_schema.columns match the original.
"""

import json

from conftest import wait_for


def _setup_two_node_cluster(node_factory):
    node_a = node_factory()
    node_b = node_factory()

    node_a.execute(f"SELECT trex_db_flight_start('0.0.0.0', {node_a.flight_port})")
    node_b.execute(f"SELECT trex_db_flight_start('0.0.0.0', {node_b.flight_port})")

    node_a.execute(
        f"SELECT trex_db_start('0.0.0.0', {node_a.gossip_port}, 'type-preservation')"
    )
    node_b.execute(
        f"SELECT trex_db_start_seeds('0.0.0.0', {node_b.gossip_port}, "
        f"'type-preservation', '127.0.0.1:{node_a.gossip_port}')"
    )

    node_a.execute(
        f"SELECT trex_db_register_service('flight', '127.0.0.1', {node_a.flight_port})"
    )
    node_b.execute(
        f"SELECT trex_db_register_service('flight', '127.0.0.1', {node_b.flight_port})"
    )

    node_a.execute("SELECT trex_db_set('data_node', 'true')")
    node_b.execute("SELECT trex_db_set('data_node', 'true')")

    wait_for(
        node_a,
        "SELECT * FROM trex_db_services()",
        lambda rows: sum(1 for r in rows if r[1] == 'flight' and r[4] == 'running') >= 2,
        timeout=15,
    )

    return node_a, node_b


def _column_type(node, table_name, column_name):
    """Return the SQL data_type string for a column from information_schema.columns.

    Returns None if the table or column does not exist on the node.
    """
    try:
        rows = node.execute(
            "SELECT data_type FROM information_schema.columns "
            f"WHERE table_name = '{table_name}' AND column_name = '{column_name}'"
        )
        if not rows:
            return None
        return rows[0][0]
    except RuntimeError:
        return None


def test_partition_preserves_timestamptz_decimal_and_timestamp(node_factory):
    """Shard DDL must preserve TIMESTAMPTZ, DECIMAL(p,s), TIMESTAMP fidelity.

    Regression for arrow_type_to_sql collapsing type metadata.
    """
    node_a, node_b = _setup_two_node_cluster(node_factory)

    # Build a table that exercises the three previously-broken cases.
    # id: BIGINT (not affected by the bug, but needed as hash key)
    # ts_tz:    TIMESTAMPTZ      <- previously collapsed to TIMESTAMP
    # ts_plain: TIMESTAMP        <- baseline (no tz)
    # price:    DECIMAL(38, 10)  <- previously collapsed to DECIMAL(18, 3)
    node_a.execute(
        "CREATE TABLE typed_orders ("
        "    id BIGINT, "
        "    ts_tz TIMESTAMPTZ, "
        "    ts_plain TIMESTAMP, "
        "    price DECIMAL(38, 10)"
        ")"
    )
    node_a.execute(
        "INSERT INTO typed_orders "
        "SELECT "
        "    i AS id, "
        "    CAST('2025-01-01 00:00:00+00' AS TIMESTAMPTZ) "
        "        + INTERVAL (i) SECOND AS ts_tz, "
        "    CAST('2025-01-01 00:00:00' AS TIMESTAMP) "
        "        + INTERVAL (i) SECOND AS ts_plain, "
        "    CAST(i * 1234567890 AS DECIMAL(38, 10)) AS price "
        "FROM range(20) t(i)"
    )

    # Sanity: the source table itself reports the expected types.
    src_ts_tz = _column_type(node_a, "typed_orders", "ts_tz")
    src_ts_plain = _column_type(node_a, "typed_orders", "ts_plain")
    src_price = _column_type(node_a, "typed_orders", "price")
    # DuckDB reports TIMESTAMPTZ as either "TIMESTAMP WITH TIME ZONE" or
    # "TIMESTAMP_TZ" depending on version — match either.
    assert src_ts_tz is not None
    assert "TIME ZONE" in src_ts_tz.upper() or "_TZ" in src_ts_tz.upper(), (
        f"source ts_tz unexpectedly typed as {src_ts_tz!r}"
    )
    assert src_ts_plain is not None
    assert src_ts_plain.upper().startswith("TIMESTAMP"), src_ts_plain
    assert "TIME ZONE" not in src_ts_plain.upper(), src_ts_plain
    assert src_price is not None
    assert "DECIMAL(38,10)" in src_price.replace(" ", "").upper(), src_price

    # Hash-partition across the two nodes.
    config = json.dumps({
        "strategy": "hash",
        "column": "id",
        "partitions": 2,
    })
    result = node_a.execute(
        f"SELECT trex_db_partition_table('typed_orders', '{config}')"
    )
    result_str = result[0][0]
    assert "Error" not in result_str, f"Partition failed: {result_str}"

    # Wait until both shards are catalogued.
    wait_for(
        node_a,
        "SELECT * FROM trex_db_tables()",
        lambda rows: sum(1 for r in rows if r[1] == "typed_orders") >= 2,
        timeout=20,
    )

    # The shards live as locally-named DuckDB tables on each node. After
    # partitioning, exactly one of {node_a, node_b} should hold the rows for
    # each hash bucket. We don't care which — we just need to find the actual
    # shard table on each node and assert its column types.
    #
    # The shard table is named "typed_orders" locally on whichever node
    # received the partition assignment, since the create_sql uses the original
    # name. So inspect both nodes; at least one (and likely both) carry the
    # shard.
    shards_inspected = 0
    for node, label in [(node_a, "a"), (node_b, "b")]:
        ts_tz_type = _column_type(node, "typed_orders", "ts_tz")
        ts_plain_type = _column_type(node, "typed_orders", "ts_plain")
        price_type = _column_type(node, "typed_orders", "price")

        if ts_tz_type is None:
            # No shard on this node — skip.
            continue
        shards_inspected += 1

        # ts_tz must NOT have been downgraded to plain TIMESTAMP.
        assert "TIME ZONE" in ts_tz_type.upper() or "_TZ" in ts_tz_type.upper(), (
            f"node_{label} shard typed_orders.ts_tz lost timezone metadata: "
            f"got {ts_tz_type!r} (was {src_ts_tz!r} on source)"
        )

        # ts_plain stays plain TIMESTAMP.
        assert ts_plain_type is not None
        assert ts_plain_type.upper().startswith("TIMESTAMP"), ts_plain_type
        assert "TIME ZONE" not in ts_plain_type.upper(), (
            f"node_{label} shard typed_orders.ts_plain unexpectedly tz-aware: "
            f"{ts_plain_type!r}"
        )

        # price must keep DECIMAL(38, 10) — not collapse to DECIMAL(18, 3).
        assert price_type is not None
        normalized = price_type.replace(" ", "").upper()
        assert "DECIMAL(38,10)" in normalized, (
            f"node_{label} shard typed_orders.price lost precision/scale: "
            f"got {price_type!r} (was {src_price!r} on source)"
        )

    assert shards_inspected >= 1, (
        "Expected at least one node to hold a shard of typed_orders"
    )


def test_partition_round_trip_preserves_timestamptz_value(node_factory):
    """Round-tripping a TIMESTAMPTZ value through partitioning must not drift.

    If the shard DDL collapses TIMESTAMPTZ to TIMESTAMP, the wall-clock value
    in UTC will silently shift by the local-tz offset on read. This test
    inserts a fixed UTC instant and asserts the same instant comes back.
    """
    node_a, node_b = _setup_two_node_cluster(node_factory)

    # A single-row table with a known UTC instant. Hash partition by id.
    node_a.execute(
        "CREATE TABLE tz_probe AS "
        "SELECT 1 AS id, "
        "       CAST('2025-06-15 12:34:56+00' AS TIMESTAMPTZ) AS ts_utc"
    )

    # Capture the source value as a UTC ISO string.
    src_iso = node_a.execute(
        "SELECT strftime(ts_utc AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%S') "
        "FROM tz_probe"
    )[0][0]
    assert src_iso == "2025-06-15T12:34:56", src_iso

    config = json.dumps({
        "strategy": "hash",
        "column": "id",
        "partitions": 2,
    })
    result = node_a.execute(
        f"SELECT trex_db_partition_table('tz_probe', '{config}')"
    )
    assert "Error" not in result[0][0], result[0][0]

    wait_for(
        node_a,
        "SELECT * FROM trex_db_tables()",
        lambda rows: any(r[1] == "tz_probe" for r in rows),
        timeout=20,
    )

    # Read the value back from whichever shard holds it.
    found = False
    for node in (node_a, node_b):
        try:
            rows = node.execute(
                "SELECT strftime(ts_utc AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%S') "
                "FROM tz_probe WHERE id = 1"
            )
        except RuntimeError:
            continue
        if not rows:
            continue
        found = True
        assert rows[0][0] == "2025-06-15T12:34:56", (
            f"TIMESTAMPTZ value drifted through partitioning: "
            f"source={src_iso!r}, shard={rows[0][0]!r}"
        )
    assert found, "tz_probe row not found on any shard after partitioning"
