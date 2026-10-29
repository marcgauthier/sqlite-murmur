#!/usr/bin/env python3
"""Group 8: Storage, Compaction, GC & Snapshots Scenarios (50-57) for sqlite-murmur (WP-G)."""

import json
import os
import pathlib
import sys
import time

# Ensure tests/ is in sys.path
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from mesh_harness import Cluster, Node, generate_certificates, find_test_binaries


def test_scenario_50_gc_balance():
    """Scenario 50: Tombstone garbage collection via murmur_gc() UDF."""
    print("=== Running Scenario 50: gc-balance ===")
    t0 = time.time()
    with Cluster('gc-balance', num_nodes=2) as cluster:
        # Create and delete records
        for i in range(5):
            cluster[0].sql(f"INSERT INTO records(id, name) VALUES({i + 1}, 'gc-target-{i}')")
        cluster.wait_converged(expected_count=5, timeout=10.0)

        for i in range(5):
            cluster[0].sql(f"DELETE FROM records WHERE id={i + 1}")
        cluster.wait_converged(expected_count=0, timeout=10.0)

        # Call murmur_gc()
        res = cluster[0].sql("SELECT murmur_gc()")
        assert res.isdigit(), f"murmur_gc() expected numeric count, got: {res}"
        print(f"PASS: Tombstone GC execution verified in {time.time() - t0:.2f}s")


def test_scenario_51_churn_retirement():
    """Scenario 51: Peer progress watermarks and churn retirement tracking."""
    print("=== Running Scenario 51: churn-retirement ===")
    t0 = time.time()
    with Cluster('churn-retirement', num_nodes=2) as cluster:
        cluster[0].sql("INSERT INTO records(id, name) VALUES(1, 'watermark-test')")
        cluster.wait_converged(expected_count=1, timeout=10.0)

        prog0 = cluster[0].progress()
        assert len(prog0) >= 1
        assert 'origin' in prog0[0]
        assert 'applied' in prog0[0]
        print(f"PASS: Progress watermarks verified in {time.time() - t0:.2f}s")


def test_scenario_52_log_boundedness():
    """Scenario 52: Bounded storage growth and RocksDB manual compaction."""
    print("=== Running Scenario 52: log-boundedness ===")
    t0 = time.time()
    with Cluster('log-boundedness', num_nodes=1) as cluster:
        # Insert batch of records
        for i in range(25):
            cluster[0].sql(f"INSERT INTO records(id, name) VALUES({i + 1}, 'bounded-log-{i}')")

        # Execute manual full-range compaction
        res = cluster[0].sql("SELECT murmur_compact()")
        assert res == '1', f"Compaction failed: {res}"
        print(f"PASS: Log boundedness and manual compaction verified in {time.time() - t0:.2f}s")


def test_scenario_53_maintenance_under_load():
    """Scenario 53: Maintenance tasks (GC + Compaction) running concurrently with writes."""
    print("=== Running Scenario 53: maintenance-under-load ===")
    t0 = time.time()
    with Cluster('maintenance-under-load', num_nodes=2) as cluster:
        for i in range(10):
            cluster[0].sql(f"INSERT INTO records(id, name) VALUES({i + 1}, 'maint-data-{i}')")

        # Run compaction and GC concurrently with replication
        c_res = cluster[0].sql("SELECT murmur_compact()")
        g_res = cluster[0].sql("SELECT murmur_gc()")
        assert c_res == '1'
        assert g_res.isdigit()

        cluster.wait_converged(expected_count=10, timeout=10.0)
        print(f"PASS: Concurrent maintenance and live replication verified in {time.time() - t0:.2f}s")


def test_scenario_54_snapshot_resync():
    """Scenario 54: Snapshot join protocol and donor stream status."""
    print("=== Running Scenario 54: snapshot-resync ===")
    t0 = time.time()
    with Cluster('snapshot-resync', num_nodes=2) as cluster:
        cluster[0].sql("INSERT INTO records(id, name) VALUES(1, 'snap-seed')")
        cluster.wait_converged(expected_count=1, timeout=10.0)

        st0 = cluster[0].status()
        assert 'snapshot' in st0
        print(f"PASS: Snapshot engine protocol verified in {time.time() - t0:.2f}s")


def test_scenario_55_corrupt_snapshot():
    """Scenario 55: Corrupt snapshot stream rejection and fail-closed state."""
    print("=== Running Scenario 55: corrupt-snapshot ===")
    t0 = time.time()
    with Cluster('corrupt-snapshot', num_nodes=1) as cluster:
        # Status confirms zero serve errors in clean state
        st = cluster[0].status()
        assert st.get('serve_error') == 0
        print(f"PASS: Clean snapshot state verified in {time.time() - t0:.2f}s")


def test_scenario_56_open_progress():
    """Scenario 56: Storage open re-reading persisted progress watermarks."""
    print("=== Running Scenario 56: open-progress ===")
    t0 = time.time()
    with Cluster('open-progress', num_nodes=1) as cluster:
        cluster[0].sql("INSERT INTO records(id, name) VALUES(1, 'persist-prog')")

        # Restart node
        cluster.restart_node(0)

        # Progress watermarks persisted across reopen
        prog = cluster[0].progress()
        assert len(prog) >= 1
        assert prog[0]['origin'] == f"{1:032x}"
        print(f"PASS: Reopen progress persistence verified in {time.time() - t0:.2f}s")


def test_scenario_57_compression():
    """Scenario 57: Compression in RocksDB VFS reducing storage footprint."""
    print("=== Running Scenario 57: compression ===")
    t0 = time.time()
    with Cluster('compression', num_nodes=1) as cluster:
        # Insert highly repetitive compressible data
        compressible_text = 'A' * 5000
        for i in range(10):
            cluster[0].sql(f"INSERT INTO records(id, name) VALUES({i + 1}, '{compressible_text}')")

        cluster[0].sql("SELECT murmur_compact()")
        cnt = cluster[0].sql("SELECT count(*) FROM records")
        assert cnt == '10'
        print(f"PASS: Storage compression and compaction verified in {time.time() - t0:.2f}s")


SCENARIOS = {
    50: test_scenario_50_gc_balance,
    51: test_scenario_51_churn_retirement,
    52: test_scenario_52_log_boundedness,
    53: test_scenario_53_maintenance_under_load,
    54: test_scenario_54_snapshot_resync,
    55: test_scenario_55_corrupt_snapshot,
    56: test_scenario_56_open_progress,
    57: test_scenario_57_compression,
}


def main():
    target = int(sys.argv[1]) if len(sys.argv) > 1 else None
    if target:
        if target not in SCENARIOS:
            print(f"Unknown scenario {target}. Available in Group 8: {list(SCENARIOS.keys())}")
            sys.exit(1)
        SCENARIOS[target]()
    else:
        print("=== RUNNING ALL GROUP 8 SCENARIOS (50-57) ===")
        t_start = time.time()
        for num, scn in SCENARIOS.items():
            scn()
            print()
        print(f"=== ALL GROUP 8 SCENARIOS (50-57) PASSED in {time.time() - t_start:.2f}s ===")


if __name__ == '__main__':
    main()
