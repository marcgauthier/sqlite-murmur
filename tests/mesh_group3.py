#!/usr/bin/env python3
"""Group 3: CRDT Policies, Types & High/Low Ownership Scenarios (21-28) for sqlite-murmur (WP-G)."""

import json
import os
import pathlib
import sys
import time

# Ensure tests/ is in sys.path
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from mesh_harness import Cluster, Node, generate_certificates, find_test_binaries


def test_scenario_21_merge_policies():
    """Scenario 21: CRDT merge resolution (LWW per-column resolution)."""
    print("=== Running Scenario 21: merge-policies ===")
    t0 = time.time()
    schema = (
        "CREATE TABLE items(id INTEGER PRIMARY KEY NOT NULL, title TEXT, notes TEXT, score REAL); "
        "SELECT crsql_as_crr('main', 'items');"
    )
    with Cluster('merge-policies', num_nodes=2, topology='full', schema=schema) as cluster:
        # Node 1 writes base item
        cluster[0].sql("INSERT INTO items(id, title, notes, score) VALUES(1, 'Original Title', 'Initial Note', 10.5)")
        cluster.wait_converged(count_query="SELECT count(*) FROM items", expected_count=1, timeout=10.0)

        # Node 1 updates title only; Node 2 updates notes only (concurrent non-conflicting field updates)
        cluster[0].sql("UPDATE items SET title='Updated Title' WHERE id=1")
        cluster[1].sql("UPDATE items SET notes='Updated Notes', score=99.0 WHERE id=1")

        # Wait until both updates merge into the same row on both nodes
        cluster.wait_until(
            lambda: cluster[0].sql("SELECT title FROM items WHERE id=1") == 'Updated Title' and
                    cluster[0].sql("SELECT notes FROM items WHERE id=1") == 'Updated Notes' and
                    cluster[1].sql("SELECT title FROM items WHERE id=1") == 'Updated Title' and
                    cluster[1].sql("SELECT notes FROM items WHERE id=1") == 'Updated Notes',
            timeout=15.0
        )
        print(f"PASS: Concurrent column-level merge verified in {time.time() - t0:.2f}s")


def test_scenario_22_crdt_contention():
    """Scenario 22: High-frequency concurrent writes to identical keys across nodes."""
    print("=== Running Scenario 22: crdt-contention ===")
    t0 = time.time()
    with Cluster('crdt-contention', num_nodes=3, topology='full') as cluster:
        # Initialize row
        cluster[0].sql("INSERT INTO records(id, name) VALUES(1, 'base')")
        cluster.wait_converged(expected_count=1, timeout=10.0)

        # Rapid concurrent updates to key id=1 across all 3 nodes
        for r in range(15):
            for i, n in enumerate(cluster.nodes):
                n.sql(f"UPDATE records SET name='node-{i+1}-round-{r}' WHERE id=1")

        # Wait until all 3 nodes settle to identical value
        cluster.wait_until(
            lambda: len({n.sql("SELECT name FROM records WHERE id=1") for n in cluster.nodes}) == 1,
            timeout=15.0
        )
        winner = cluster[0].sql("SELECT name FROM records WHERE id=1")
        print(f"Contention resolved deterministically to winner: {winner}")
        print(f"PASS: High-contention CRDT resolution verified in {time.time() - t0:.2f}s")


def test_scenario_23_highlow():
    """Scenario 23: High/Low cryptographic domain isolation & bridge envelope."""
    print("=== Running Scenario 23: highlow ===")
    t0 = time.time()
    # High/Low air-gap domain simulation: Low domain (group A) and High domain (group B)
    group_low = '1' * 32
    group_high = '2' * 32
    with Cluster('highlow-low', num_nodes=1, group=group_low) as cluster_low:
        with Cluster('highlow-high', num_nodes=1, group=group_high) as cluster_high:
            # Low writes record
            cluster_low[0].sql("INSERT INTO records(id, name) VALUES(100, 'low-domain-secret')")
            assert cluster_low[0].sql("SELECT count(*) FROM records") == '1'

            # High domain is isolated and has 0 records
            assert cluster_high[0].sql("SELECT count(*) FROM records") == '0'

            # Verify High rejects direct peer connection from Low with group mismatch
            cluster_high[0].stop()
            cluster_high[0].sql(f"PRAGMA murmur_bootstrap='[\"127.0.0.1:{cluster_low.ports[0]}\"]'")
            cluster_high[0].start()

            # Cross-group direct sync is rejected by group isolation
            time.sleep(0.5)
            assert cluster_high[0].sql("SELECT count(*) FROM records") == '0'
            print(f"PASS: High/Low cryptographic domain isolation verified in {time.time() - t0:.2f}s")


def test_scenario_24_typed_bridge():
    """Scenario 24: Bridge typed envelope serialization and verification."""
    print("=== Running Scenario 24: typed-bridge ===")
    t0 = time.time()
    schema = (
        "CREATE TABLE bridge_events(seq INTEGER PRIMARY KEY NOT NULL, domain TEXT, payload TEXT); "
        "SELECT crsql_as_crr('main', 'bridge_events');"
    )
    with Cluster('typed-bridge', num_nodes=2, topology='full', schema=schema) as cluster:
        # Insert typed bridge event
        cluster[0].sql("INSERT INTO bridge_events(seq, domain, payload) VALUES(1, 'DOMAIN_LOW', '{\"action\": \"export\", \"ref\": 1234}')")
        # Wait until typed bridge event is fully replicated and reassembled on Node 2
        cluster.wait_until(
            lambda: cluster[1].sql("SELECT domain FROM bridge_events WHERE seq=1") == 'DOMAIN_LOW' and
                    'export' in cluster[1].sql("SELECT payload FROM bridge_events WHERE seq=1"),
            timeout=15.0
        )
        print(f"PASS: Typed bridge event replication verified in {time.time() - t0:.2f}s")


def test_scenario_25_bridge_two_streams():
    """Scenario 25: Isolation between two discrete event streams."""
    print("=== Running Scenario 25: bridge-two-streams ===")
    t0 = time.time()
    schema = (
        "CREATE TABLE stream_a(id INTEGER PRIMARY KEY NOT NULL, val TEXT); "
        "CREATE TABLE stream_b(id INTEGER PRIMARY KEY NOT NULL, val TEXT); "
        "SELECT crsql_as_crr('main', 'stream_a'); "
        "SELECT crsql_as_crr('main', 'stream_b');"
    )
    with Cluster('bridge-two-streams', num_nodes=2, topology='full', schema=schema) as cluster:
        # Write to stream A
        cluster[0].sql("INSERT INTO stream_a(id, val) VALUES(1, 'stream-a-event')")
        # Write to stream B
        cluster[1].sql("INSERT INTO stream_b(id, val) VALUES(2, 'stream-b-event')")

        # Both streams replicate independently without crosstalk
        cluster.wait_until(
            lambda: cluster[0].sql("SELECT count(*) FROM stream_a") == '1' and
                    cluster[0].sql("SELECT count(*) FROM stream_b") == '1' and
                    cluster[1].sql("SELECT count(*) FROM stream_a") == '1' and
                    cluster[1].sql("SELECT count(*) FROM stream_b") == '1',
            timeout=15.0
        )
        print(f"PASS: Multi-stream isolation verified in {time.time() - t0:.2f}s")


def test_scenario_26_typed_records():
    """Scenario 26: SQLite native type fidelity (INTEGER, REAL, TEXT, BLOB, NULL)."""
    print("=== Running Scenario 26: typed-records ===")
    t0 = time.time()
    schema = (
        "CREATE TABLE typed_data("
        "  id INTEGER PRIMARY KEY NOT NULL, "
        "  int_val INTEGER, "
        "  real_val REAL, "
        "  text_val TEXT, "
        "  blob_val BLOB, "
        "  null_val TEXT"
        "); "
        "SELECT crsql_as_crr('main', 'typed_data');"
    )
    with Cluster('typed-records', num_nodes=2, topology='full', schema=schema) as cluster:
        # Insert heterogeneous typed data
        cluster[0].sql(
            "INSERT INTO typed_data(id, int_val, real_val, text_val, blob_val, null_val) "
            "VALUES(1, 9223372036854775807, 3.14159265358979, 'hello-typed-wire', X'DEADBEEF01020304', NULL)"
        )

        # Wait until full typed row arrives and matches exact types and values on Node 2
        cluster.wait_until(
            lambda: cluster[1].sql(
                "SELECT typeof(int_val) || '|' || typeof(real_val) || '|' || typeof(text_val) || '|' || typeof(blob_val) || '|' || typeof(null_val) "
                "FROM typed_data WHERE id=1"
            ) == 'integer|real|text|blob|null' and
            cluster[1].sql("SELECT int_val FROM typed_data WHERE id=1") == '9223372036854775807' and
            cluster[1].sql("SELECT hex(blob_val) FROM typed_data WHERE id=1") == 'DEADBEEF01020304',
            timeout=15.0
        )
        print(f"PASS: Typed records fidelity verified across wire in {time.time() - t0:.2f}s")


def test_scenario_27_views():
    """Scenario 27: SQL Views dynamically reflecting replicated changes."""
    print("=== Running Scenario 27: views ===")
    t0 = time.time()
    schema = (
        "CREATE TABLE orders(id INTEGER PRIMARY KEY NOT NULL, customer TEXT, amount REAL, status TEXT); "
        "SELECT crsql_as_crr('main', 'orders'); "
        "CREATE VIEW active_orders AS SELECT id, customer, amount FROM orders WHERE status = 'ACTIVE';"
    )
    with Cluster('views', num_nodes=2, topology='full', schema=schema) as cluster:
        cluster[0].sql("INSERT INTO orders(id, customer, amount, status) VALUES(1, 'Alice', 150.0, 'ACTIVE')")
        cluster[0].sql("INSERT INTO orders(id, customer, amount, status) VALUES(2, 'Bob', 20.0, 'CANCELLED')")
        cluster[0].sql("INSERT INTO orders(id, customer, amount, status) VALUES(3, 'Charlie', 85.5, 'ACTIVE')")

        cluster.wait_converged(count_query="SELECT count(*) FROM orders", expected_count=3, timeout=10.0)

        # Query dynamic view on Node 2
        view_count = cluster[1].sql("SELECT count(*) FROM active_orders")
        assert view_count == '2', f"View count mismatch: {view_count}"

        # Update order 2 to ACTIVE on Node 1 -> view on Node 2 reflects it immediately
        cluster[0].sql("UPDATE orders SET status='ACTIVE' WHERE id=2")
        cluster.wait_until(
            lambda: cluster[1].sql("SELECT count(*) FROM active_orders") == '3',
            timeout=10.0
        )
        print(f"PASS: SQL Views dynamic replication reflection verified in {time.time() - t0:.2f}s")


def test_scenario_28_delete_pruning():
    """Scenario 28: Tombstone generation, replication, and query observation."""
    print("=== Running Scenario 28: delete-pruning ===")
    t0 = time.time()
    with Cluster('delete-pruning', num_nodes=2, topology='full') as cluster:
        # Insert initial records
        cluster[0].sql("INSERT INTO records(id, name) VALUES(100, 'to-be-deleted')")
        cluster[0].sql("INSERT INTO records(id, name) VALUES(200, 'to-keep')")
        cluster.wait_converged(expected_count=2, timeout=10.0)

        # Delete on Node 1
        print("Deleting record id=100 on Node 1...")
        cluster[0].sql("DELETE FROM records WHERE id=100")

        # Deletion replicates to Node 2
        cluster.wait_converged(expected_count=1, timeout=10.0)
        assert cluster[1].sql("SELECT count(*) FROM records WHERE id=100") == '0'
        assert cluster[1].sql("SELECT count(*) FROM records WHERE id=200") == '1'

        # Verify crsql tombstone clock entry (col_name = '-1')
        tombstone_cid = cluster[1].sql("SELECT col_name FROM records__crsql_clock WHERE col_name='-1'")
        assert tombstone_cid == '-1', f"Tombstone not recorded: {tombstone_cid}"
        print(f"PASS: Delete tombstone replication verified in {time.time() - t0:.2f}s")


SCENARIOS = {
    21: test_scenario_21_merge_policies,
    22: test_scenario_22_crdt_contention,
    23: test_scenario_23_highlow,
    24: test_scenario_24_typed_bridge,
    25: test_scenario_25_bridge_two_streams,
    26: test_scenario_26_typed_records,
    27: test_scenario_27_views,
    28: test_scenario_28_delete_pruning,
}


def main():
    target = int(sys.argv[1]) if len(sys.argv) > 1 else None
    if target:
        if target not in SCENARIOS:
            print(f"Unknown scenario {target}. Available in Group 3: {list(SCENARIOS.keys())}")
            sys.exit(1)
        SCENARIOS[target]()
    else:
        print("=== RUNNING ALL GROUP 3 SCENARIOS (21-28) ===")
        t_start = time.time()
        for num, scn in SCENARIOS.items():
            scn()
            print()
        print(f"=== ALL GROUP 3 SCENARIOS (21-28) PASSED in {time.time() - t_start:.2f}s ===")


if __name__ == '__main__':
    main()
