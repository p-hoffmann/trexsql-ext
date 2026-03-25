"""Microbenchmark: normal DuckDB query vs query via db extension (single node).

Compares direct SQL execution against trex_db_query() on the same node.
Run:  python perf_normal_vs_extension.py
"""

import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from conftest import Node, DB_EXT, alloc_ports, wait_for


def bench(node, sql, iterations=50, warmup=5):
    """Run sql repeatedly, return timing stats in ms."""
    for _ in range(warmup):
        node.execute(sql)

    times = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        node.execute(sql)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        times.append(elapsed_ms)

    return {
        "min": round(min(times), 2),
        "median": round(statistics.median(times), 2),
        "mean": round(statistics.mean(times), 2),
        "p95": round(sorted(times)[int(len(times) * 0.95)], 2),
        "max": round(max(times), 2),
    }


def print_row(label, stats):
    print(f"  {label:<30} {stats['min']:>8.2f} {stats['median']:>8.2f} "
          f"{stats['mean']:>8.2f} {stats['p95']:>8.2f} {stats['max']:>8.2f}")


def main():
    iterations = 50
    warmup = 5
    table_sizes = [100, 1_000, 10_000, 100_000]

    print("=" * 80)
    print("Microbenchmark: Normal Query vs trex_db_query() — Single Node")
    print("=" * 80)

    gp, fp, pp = alloc_ports()
    node = Node([DB_EXT], gp, fp, pp)

    # Start flight + gossip so trex_db_query works
    node.execute(f"SELECT trex_db_flight_start('0.0.0.0', {fp})")
    node.execute(f"SELECT trex_db_start('0.0.0.0', {gp}, 'bench-cluster')")
    node.execute(f"SELECT trex_db_register_service('flight', '127.0.0.1', {fp})")

    # Wait for self-discovery (gossip can take a while on a single node)
    wait_for(node, "SELECT * FROM trex_db_nodes()", lambda r: len(r) >= 1, timeout=30)

    # Wait for table catalog to propagate
    wait_for(node, "SELECT * FROM trex_db_tables()",
             lambda r: len(r) >= 0, timeout=10)

    queries = [
        ("SELECT COUNT(*)",           "SELECT COUNT(*) FROM orders"),
        ("SELECT * (full scan)",      "SELECT * FROM orders"),
        ("WHERE filter",              "SELECT * FROM orders WHERE region = 'US'"),
        ("GROUP BY + SUM",            "SELECT region, SUM(price) FROM orders GROUP BY region"),
        ("ORDER BY LIMIT",            "SELECT * FROM orders ORDER BY price DESC LIMIT 10"),
    ]

    for size in table_sizes:
        # Recreate table
        node.execute("DROP TABLE IF EXISTS orders")
        node.execute(
            f"CREATE TABLE orders AS "
            f"SELECT i AS id, "
            f"  CASE WHEN i % 3 = 0 THEN 'US' WHEN i % 3 = 1 THEN 'EU' ELSE 'APAC' END AS region, "
            f"  (i % 1000) * 0.99 AS price "
            f"FROM range({size}) t(i)"
        )

        # Wait for table to appear in catalog
        wait_for(node, "SELECT * FROM trex_db_tables()",
                 lambda r: any('orders' in str(row) for row in r), timeout=15)

        print(f"\n--- {size:,} rows ---")
        print(f"  {'Query':<30} {'Min':>8} {'Median':>8} {'Mean':>8} {'P95':>8} {'Max':>8}  (ms, n={iterations})")
        print(f"  {'-'*30} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*8}")

        for label, sql in queries:
            # Normal direct query
            normal = bench(node, sql, iterations, warmup)

            # Same query via db extension
            escaped = sql.replace("'", "''")
            ext_sql = f"SELECT * FROM trex_db_query('{escaped}')"
            extension = bench(node, ext_sql, iterations, warmup)

            overhead = extension['median'] - normal['median']
            ratio = extension['median'] / normal['median'] if normal['median'] > 0 else float('inf')

            print_row(f"{label} [normal]", normal)
            print_row(f"{label} [extension]", extension)
            print(f"  {'  → overhead':<30} {overhead:>+8.2f} ms ({ratio:.2f}x)")
            print()

    node.close()
    print("Done.")


if __name__ == "__main__":
    main()
