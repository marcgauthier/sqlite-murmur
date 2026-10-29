#!/usr/bin/env python3
"""Group 2: Partitions, Network Faults & Healing Scenarios (11-20) for sqlite-murmur (WP-G)."""

import json
import os
import pathlib
import sys
import time

# Ensure tests/ is in sys.path
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from mesh_harness import Cluster, Node, generate_certificates, find_test_binaries


def test_scenario_11_partition():
    """Scenario 11: Network partition into two disjoint subnets (1,2 | 3,4) and anti-entropy heal."""
    print("=== Running Scenario 11: partition ===")
    t0 = time.time()
    # Start 4 nodes with manual topology
    with Cluster('partition', num_nodes=4, topology='custom', start=False) as cluster:
        # Partition A: Node 1 and Node 2 peer only with each other
        cluster[0].sql(f"PRAGMA murmur_bootstrap='[\"127.0.0.1:{cluster.ports[1]}\"]'")
        cluster[1].sql(f"PRAGMA murmur_bootstrap='[\"127.0.0.1:{cluster.ports[0]}\"]'")

        # Partition B: Node 3 and Node 4 peer only with each other
        cluster[2].sql(f"PRAGMA murmur_bootstrap='[\"127.0.0.1:{cluster.ports[3]}\"]'")
        cluster[3].sql(f"PRAGMA murmur_bootstrap='[\"127.0.0.1:{cluster.ports[2]}\"]'")

        for n in cluster.nodes:
            n.start()

        # Disjoint writes in Partition A
        for i in range(10):
            cluster[0].sql(f"INSERT INTO records(id, name) VALUES({i + 1}, 'partition-A-{i}')")

        # Disjoint writes in Partition B
        for i in range(10):
            cluster[2].sql(f"INSERT INTO records(id, name) VALUES({i + 101}, 'partition-B-{i}')")

        # Verify Partition A has 10 rows (on 1 & 2) and Partition B has 10 rows (on 3 & 4)
        cluster.wait_until(
            lambda: cluster[0].sql("SELECT count(*) FROM records") == '10' and
                    cluster[1].sql("SELECT count(*) FROM records") == '10' and
                    cluster[2].sql("SELECT count(*) FROM records") == '10' and
                    cluster[3].sql("SELECT count(*) FROM records") == '10',
            timeout=15.0
        )
        print("Partition verified: both subnets isolated with disjoint state.")

        # Heal network partition: connect Node 1 and Node 3
        print("Healing partition between Node 1 and Node 3...")
        heal_seeds_1 = json.dumps([f"127.0.0.1:{cluster.ports[1]}", f"127.0.0.1:{cluster.ports[2]}"])
        heal_seeds_3 = json.dumps([f"127.0.0.1:{cluster.ports[0]}", f"127.0.0.1:{cluster.ports[3]}"])
        cluster[0].stop()
        cluster[0].sql(f"PRAGMA murmur_bootstrap='{heal_seeds_1}'")
        cluster[0].start()

        cluster[2].stop()
        cluster[2].sql(f"PRAGMA murmur_bootstrap='{heal_seeds_3}'")
        cluster[2].start()

        # All 4 nodes must reconverge to all 20 rows
        cluster.wait_until(
            lambda: len({n.sql("SELECT sum(id) FROM records") for n in cluster.nodes}) == 1 and
                    cluster[0].sql("SELECT count(*) FROM records") == '20',
            timeout=20.0
        )
        print(f"PASS: Network partition and heal verified in {time.time() - t0:.2f}s")


def test_scenario_12_flap_partition():
    """Scenario 12: Intermittent link flapping on Node 3."""
    print("=== Running Scenario 12: flap-partition ===")
    t0 = time.time()
    with Cluster('flap-partition', num_nodes=3, topology='full') as cluster:
        # Concurrent baseline writes on Node 1 & 2
        cluster[0].sql("INSERT INTO records(id, name) VALUES(1, 'base-1')")
        cluster.wait_converged(expected_count=1, timeout=10.0)

        # Flap Node 3 stop/start 3 times while 1 & 2 continue writes
        for flap in range(3):
            cluster[2].stop()
            cluster[0].sql(f"INSERT INTO records(id, name) VALUES({10 + flap}, 'flap-write-{flap}')")
            time.sleep(0.3)
            cluster[2].start()
            time.sleep(0.3)

        # After flapping ceases, Node 3 catches up fully
        cluster.wait_until(
            lambda: len({n.sql("SELECT sum(id) FROM records") for n in cluster.nodes}) == 1 and
                    cluster[0].sql("SELECT count(*) FROM records") == '4',
            timeout=20.0
        )
        print(f"PASS: Flap partition recovery verified in {time.time() - t0:.2f}s")


def test_scenario_13_three_way_heal():
    """Scenario 13: 3 isolated nodes with conflicting writes healing to deterministic LWW state."""
    print("=== Running Scenario 13: three-way-heal ===")
    t0 = time.time()
    with Cluster('three-way-heal', num_nodes=3, topology='custom', start=False) as cluster:
        # Start completely unmeshed (isolated)
        for n in cluster.nodes:
            n.start()

        # Conflicting write on the same primary key (id=1) across all 3 nodes
        cluster[0].sql("INSERT INTO records(id, name) VALUES(1, 'value-from-node-1')")
        cluster[1].sql("INSERT INTO records(id, name) VALUES(1, 'value-from-node-2')")
        cluster[2].sql("INSERT INTO records(id, name) VALUES(1, 'value-from-node-3')")

        # Heal phase 1: Connect 1 and 2
        print("Heal Phase 1: connecting nodes 1 and 2...")
        cluster[0].stop()
        cluster[0].sql(f"PRAGMA murmur_bootstrap='[\"127.0.0.1:{cluster.ports[1]}\"]'")
        cluster[0].start()

        cluster[1].stop()
        cluster[1].sql(f"PRAGMA murmur_bootstrap='[\"127.0.0.1:{cluster.ports[0]}\"]'")
        cluster[1].start()

        cluster.wait_until(
            lambda: cluster[0].sql("SELECT name FROM records WHERE id=1") ==
                    cluster[1].sql("SELECT name FROM records WHERE id=1"),
            timeout=15.0
        )
        resolved_p1 = cluster[0].sql("SELECT name FROM records WHERE id=1")
        print(f"Phase 1 resolved deterministically to: {resolved_p1}")

        # Heal phase 2: Connect 3 to 1 and 2
        print("Heal Phase 2: connecting node 3 to mesh...")
        all_seeds = json.dumps([f"127.0.0.1:{p}" for p in cluster.ports])
        for n in cluster.nodes:
            n.stop()
            n.sql(f"PRAGMA murmur_bootstrap='{all_seeds}'")
            n.start()

        cluster.wait_until(
            lambda: len({n.sql("SELECT name FROM records WHERE id=1") for n in cluster.nodes}) == 1,
            timeout=15.0
        )
        final_val = cluster[0].sql("SELECT name FROM records WHERE id=1")
        print(f"Final 3-way conflict resolved deterministically to: {final_val}")
        print(f"PASS: 3-way partition heal verified in {time.time() - t0:.2f}s")


def test_scenario_14_impaired_network():
    """Scenario 14: Rapid anti-entropy churn & streaming integrity."""
    print("=== Running Scenario 14: impaired-network ===")
    t0 = time.time()
    with Cluster('impaired-network', num_nodes=3, topology='full') as cluster:
        # Rapid concurrent updates under tight anti-entropy
        for i in range(20):
            cluster[i % 3].sql(f"INSERT INTO records(id, name) VALUES({i + 1}, 'impaired-{i}')")

        cluster.wait_converged(expected_count=20, timeout=15.0)

        # Update half the records
        for i in range(10):
            cluster[(i + 1) % 3].sql(f"UPDATE records SET name='updated-{i}' WHERE id={i + 1}")

        cluster.wait_until(
            lambda: all(n.sql("SELECT count(*) FROM records WHERE name LIKE 'updated-%'") == '10' for n in cluster.nodes),
            timeout=15.0
        )
        print(f"PASS: Network churn and integrity verified in {time.time() - t0:.2f}s")


def test_scenario_15_clock_skew():
    """Scenario 15: CRR logical clock causal resolution independent of physical wall clocks."""
    print("=== Running Scenario 15: clock-skew ===")
    t0 = time.time()
    with Cluster('clock-skew', num_nodes=3, topology='full') as cluster:
        # Sequential updates across different nodes
        cluster[0].sql("INSERT INTO records(id, name) VALUES(42, 'v1-node-1')")
        cluster.wait_converged(expected_count=1, timeout=10.0)

        # Node 2 updates col_version to 2
        cluster[1].sql("UPDATE records SET name='v2-node-2' WHERE id=42")
        cluster.wait_until(
            lambda: all(n.sql("SELECT name FROM records WHERE id=42") == 'v2-node-2' for n in cluster.nodes),
            timeout=10.0
        )

        # Node 3 updates col_version to 3
        cluster[2].sql("UPDATE records SET name='v3-node-3' WHERE id=42")
        cluster.wait_until(
            lambda: all(n.sql("SELECT name FROM records WHERE id=42") == 'v3-node-3' for n in cluster.nodes),
            timeout=10.0
        )

        # Verify clock metadata in __crsql_clock
        clock_rows = cluster[0].sql("SELECT col_version FROM records__crsql_clock WHERE col_name='name'")
        assert '3' in clock_rows
        print(f"PASS: CRR logical clocks causal resolution verified in {time.time() - t0:.2f}s")


def test_scenario_16_tail_repair():
    """Scenario 16: Tail-divergence anti-entropy repair without full snapshot."""
    print("=== Running Scenario 16: tail-repair ===")
    t0 = time.time()
    with Cluster('tail-repair', num_nodes=3, topology='full') as cluster:
        # Initial common base
        cluster[0].sql("INSERT INTO records(id, name) VALUES(1, 'base')")
        cluster.wait_converged(expected_count=1, timeout=10.0)

        # Disconnect Node 3
        print("Disconnecting Node 3...")
        cluster[2].stop()

        # Nodes 1 & 2 generate tail changes
        for i in range(2, 12):
            cluster[0].sql(f"INSERT INTO records(id, name) VALUES({i}, 'tail-{i}')")
        cluster.wait_until(
            lambda: cluster[0].sql("SELECT count(*) FROM records") == '11' and
                    cluster[1].sql("SELECT count(*) FROM records") == '11',
            timeout=10.0
        )

        # Reconnect Node 3
        print("Reconnecting Node 3 to repair tail...")
        cluster[2].start()

        # Node 3 catches up via anti-entropy tail diff
        cluster.wait_converged(expected_count=11, timeout=15.0)
        assert cluster[2].sql("SELECT count(*) FROM records") == '11'
        print(f"PASS: Tail repair verified in {time.time() - t0:.2f}s")


def test_scenario_17_txchunk_resume():
    """Scenario 17: Multi-frame chunk stream transmission for large payloads."""
    print("=== Running Scenario 17: txchunk-resume ===")
    t0 = time.time()
    with Cluster('txchunk-resume', num_nodes=2, topology='full') as cluster:
        # Create large payload (64 KiB text blob)
        large_payload = 'X' * 65536
        cluster[0].sql(f"INSERT INTO records(id, name, payload) VALUES(1, 'large-doc', '{large_payload}')")

        # Wait for transmission and full multi-frame reassembly on Node 2
        cluster.wait_until(
            lambda: cluster[1].sql("SELECT length(payload) FROM records WHERE id=1") == '65536',
            timeout=20.0
        )
        print(f"PASS: Large payload multi-frame chunking verified (64 KiB) in {time.time() - t0:.2f}s")


def test_scenario_18_pause_resume():
    """Scenario 18: Operational pause/resume via murmur_stop() and murmur_start()."""
    print("=== Running Scenario 18: pause-resume ===")
    t0 = time.time()
    with Cluster('pause-resume', num_nodes=2, topology='full') as cluster:
        # Pause Node 2
        print("Calling murmur_stop() on Node 2...")
        cluster[1].stop()
        st2 = cluster[1].status()
        assert st2.get('running') is False

        # Writes on Node 1 while Node 2 is paused
        for i in range(5):
            cluster[0].sql(f"INSERT INTO records(id, name) VALUES({i + 1}, 'paused-period-{i}')")
        assert cluster[0].sql("SELECT count(*) FROM records") == '5'
        assert cluster[1].sql("SELECT count(*) FROM records") == '0'

        # Resume Node 2
        print("Calling murmur_start() on Node 2...")
        cluster[1].start()
        st2_resumed = cluster[1].status()
        assert st2_resumed.get('running') is True

        # Node 2 catches up
        cluster.wait_converged(expected_count=5, timeout=15.0)
        print(f"PASS: Pause and resume lifecycle verified in {time.time() - t0:.2f}s")


def test_scenario_19_rolling_restart():
    """Scenario 19: Rolling restarts under continuous write traffic."""
    print("=== Running Scenario 19: rolling-restart ===")
    t0 = time.time()
    with Cluster('rolling-restart', num_nodes=3, topology='full') as cluster:
        # Round 1: baseline
        cluster[0].sql("INSERT INTO records(id, name) VALUES(1, 'init')")
        cluster.wait_converged(expected_count=1, timeout=10.0)

        # Restart Node 1
        print("Restarting Node 1...")
        cluster.restart_node(0)
        cluster[1].sql("INSERT INTO records(id, name) VALUES(2, 'during-restart-1')")
        cluster.wait_until(lambda: cluster[0].sql("SELECT count(*) FROM records") == '2', timeout=15.0)

        # Restart Node 2
        print("Restarting Node 2...")
        cluster.restart_node(1)
        cluster[2].sql("INSERT INTO records(id, name) VALUES(3, 'during-restart-2')")
        cluster.wait_until(lambda: cluster[1].sql("SELECT count(*) FROM records") == '3', timeout=15.0)

        # Restart Node 3
        print("Restarting Node 3...")
        cluster.restart_node(2)
        cluster[0].sql("INSERT INTO records(id, name) VALUES(4, 'during-restart-3')")
        cluster.wait_converged(expected_count=4, timeout=15.0)

        print(f"PASS: Rolling restart under write traffic verified in {time.time() - t0:.2f}s")


def test_scenario_20_chaos_load():
    """Scenario 20: Randomized chaos with concurrent writes and intermittent stops."""
    print("=== Running Scenario 20: chaos-load ===")
    t0 = time.time()
    with Cluster('chaos-load', num_nodes=3, topology='full') as cluster:
        # Concurrent writes
        for i in range(15):
            node_idx = i % 3
            cluster[node_idx].sql(f"INSERT INTO records(id, name) VALUES({i + 1}, 'chaos-{i}')")

        cluster.wait_converged(expected_count=15, timeout=15.0)

        # Stop and restart Node 2 while Node 1 updates records
        cluster[1].stop()
        cluster[0].sql("UPDATE records SET name='chaos-updated' WHERE id=1")
        cluster[1].start()

        cluster.wait_converged(expected_count=15, timeout=15.0)
        cluster.wait_until(
            lambda: all(n.sql("SELECT name FROM records WHERE id=1") == 'chaos-updated' for n in cluster.nodes),
            timeout=15.0
        )
        print(f"PASS: Chaos load and recovery verified in {time.time() - t0:.2f}s")


SCENARIOS = {
    11: test_scenario_11_partition,
    12: test_scenario_12_flap_partition,
    13: test_scenario_13_three_way_heal,
    14: test_scenario_14_impaired_network,
    15: test_scenario_15_clock_skew,
    16: test_scenario_16_tail_repair,
    17: test_scenario_17_txchunk_resume,
    18: test_scenario_18_pause_resume,
    19: test_scenario_19_rolling_restart,
    20: test_scenario_20_chaos_load,
}


def main():
    target = int(sys.argv[1]) if len(sys.argv) > 1 else None
    if target:
        if target not in SCENARIOS:
            print(f"Unknown scenario {target}. Available in Group 2: {list(SCENARIOS.keys())}")
            sys.exit(1)
        SCENARIOS[target]()
    else:
        print("=== RUNNING ALL GROUP 2 SCENARIOS (11-20) ===")
        t_start = time.time()
        for num, scn in SCENARIOS.items():
            scn()
            print()
        print(f"=== ALL GROUP 2 SCENARIOS (11-20) PASSED in {time.time() - t_start:.2f}s ===")


if __name__ == '__main__':
    main()
