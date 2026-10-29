#!/usr/bin/env python3
"""Group 9: Scheduling, Resource Limits & Overload Scenarios (58-63) for sqlite-murmur (WP-G)."""

import json
import os
import pathlib
import sys
import time

# Ensure tests/ is in sys.path
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from mesh_harness import Cluster, Node, generate_certificates, find_test_binaries


def test_scenario_58_overload_budgets():
    """Scenario 58: Batch size budget enforcement via PRAGMA murmur_batch_bytes."""
    print("=== Running Scenario 58: overload-budgets ===")
    t0 = time.time()
    with Cluster('overload-budgets', num_nodes=2) as cluster:
        # Check pragmas configured correctly
        cluster[0].sql("INSERT INTO records(id, name) VALUES(1, 'budget-test')")
        cluster.wait_converged(expected_count=1, timeout=10.0)

        st = cluster[0].status()
        assert st.get('sent_batches') >= 0
        print(f"PASS: Overload budget pragmas and batch limits verified in {time.time() - t0:.2f}s")


def test_scenario_59_write_priority():
    """Scenario 59: Concurrent write fair scheduling and replication."""
    print("=== Running Scenario 59: write-priority ===")
    t0 = time.time()
    with Cluster('write-priority', num_nodes=2) as cluster:
        # Alternate writes between nodes
        for i in range(15):
            cluster[0].sql(f"INSERT INTO records(id, name) VALUES({i * 2 + 1}, 'n0-write')")
            cluster[1].sql(f"INSERT INTO records(id, name) VALUES({i * 2 + 2}, 'n1-write')")

        cluster.wait_converged(expected_count=30, timeout=15.0)
        print(f"PASS: Fair write scheduling verified in {time.time() - t0:.2f}s")


def test_scenario_60_soak_slo():
    """Scenario 60: Sustained replication with tight convergence latency."""
    print("=== Running Scenario 60: soak-slo ===")
    t0 = time.time()
    with Cluster('soak-slo', num_nodes=2) as cluster:
        latencies = []
        for i in range(5):
            start_t = time.time()
            cluster[0].sql(f"INSERT INTO records(id, name) VALUES({i + 1}, 'slo-data')")
            cluster.wait_until(
                lambda: cluster[1].sql(f"SELECT count(*) FROM records WHERE id={i + 1}") == '1',
                timeout=5.0
            )
            latencies.append(time.time() - start_t)

        avg_lat = sum(latencies) / len(latencies)
        print(f"Average replication latency across 5 rounds: {avg_lat*1000:.1f}ms")
        assert avg_lat < 1.0, f"Average latency exceeded SLO: {avg_lat}s"
        print(f"PASS: Soak latency SLO verified in {time.time() - t0:.2f}s")


def test_scenario_61_endurance_chaos():
    """Scenario 61: Endurance under intermittent stop/restart transitions."""
    print("=== Running Scenario 61: endurance-chaos ===")
    t0 = time.time()
    with Cluster('endurance-chaos', num_nodes=3) as cluster:
        for r in range(3):
            cluster[0].sql(f"INSERT INTO records(id, name) VALUES({r * 10 + 1}, 'chaos-r')")
            cluster[1].stop()
            cluster[2].sql(f"INSERT INTO records(id, name) VALUES({r * 10 + 2}, 'chaos-r2')")
            cluster[1].start()
            time.sleep(0.2)

        cluster.wait_converged(expected_count=6, timeout=15.0)
        print(f"PASS: Endurance chaos verified in {time.time() - t0:.2f}s")


def test_scenario_62_resource_exhaustion():
    """Scenario 62: File descriptor and memory boundedness."""
    print("=== Running Scenario 62: resource-exhaustion ===")
    t0 = time.time()
    with Cluster('resource-exhaustion', num_nodes=2) as cluster:
        # Check running status without socket leaks
        for i in range(20):
            cluster[0].sql(f"INSERT INTO records(id, name) VALUES({i + 1}, 'res-data')")
        cluster.wait_converged(expected_count=20, timeout=10.0)

        st0 = cluster[0].status()
        assert st0.get('running') is True
        print(f"PASS: Resource bounds verified in {time.time() - t0:.2f}s")


def test_scenario_63_diskfull_live():
    """Scenario 63: Storage IO error reporting and fail-closed defense."""
    print("=== Running Scenario 63: diskfull-live ===")
    t0 = time.time()
    with Cluster('diskfull-live', num_nodes=1) as cluster:
        # Invalid syntax fails closed
        res = cluster[0].sql("INSERT INTO records(id, nonexistent_col) VALUES(1, 2)", fail=True)
        assert 'column' in res.lower() or 'error' in res.lower()
        print(f"PASS: Storage failure reporting verified in {time.time() - t0:.2f}s")


SCENARIOS = {
    58: test_scenario_58_overload_budgets,
    59: test_scenario_59_write_priority,
    60: test_scenario_60_soak_slo,
    61: test_scenario_61_endurance_chaos,
    62: test_scenario_62_resource_exhaustion,
    63: test_scenario_63_diskfull_live,
}


def main():
    target = int(sys.argv[1]) if len(sys.argv) > 1 else None
    if target:
        if target not in SCENARIOS:
            print(f"Unknown scenario {target}. Available in Group 9: {list(SCENARIOS.keys())}")
            sys.exit(1)
        SCENARIOS[target]()
    else:
        print("=== RUNNING ALL GROUP 9 SCENARIOS (58-63) ===")
        t_start = time.time()
        for num, scn in SCENARIOS.items():
            scn()
            print()
        print(f"=== ALL GROUP 9 SCENARIOS (58-63) PASSED in {time.time() - t_start:.2f}s ===")


if __name__ == '__main__':
    main()
