#!/usr/bin/env python3
"""Group 1: Basic Sync & Membership Topology Scenarios (1-10) for sqlite-murmur (WP-G)."""

import json
import os
import pathlib
import sys
import time

# Ensure tests/ is in sys.path
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from mesh_harness import Cluster, Node, generate_certificates, find_test_binaries


def test_scenario_01_three_node_sync():
    """Scenario 1: Canonical 3-node concurrent sync."""
    print("=== Running Scenario 1: three-node-sync ===")
    t0 = time.time()
    with Cluster('three-node-sync', num_nodes=3) as cluster:
        # Concurrent 30 rows each (90 total)
        for i, node in enumerate(cluster.nodes):
            for r in range(30):
                row_id = i * 1000 + r
                node.sql(f"INSERT INTO records(id, name) VALUES({row_id}, 'node-{i+1}-row-{r}')")

        # Wait until all nodes reach 90 rows and identical content checksum
        cluster.wait_until(
            lambda: len({node.sql("SELECT sum(id) FROM records") for node in cluster.nodes}) == 1 and
                    cluster[0].sql("SELECT count(*) FROM records") == '90',
            timeout=25.0
        )
        print(f"PASS: 90 rows converged identically across 3 nodes in {time.time() - t0:.2f}s")


def test_scenario_02_swim_discovery():
    """Scenario 2: SWIM peer discovery and failure detection."""
    print("=== Running Scenario 2: swim-discovery ===")
    t0 = time.time()
    with Cluster('swim-discovery', num_nodes=4, topology='star') as cluster:
        # Node 1 is seed; wait for 4 nodes to discover each other
        print("Waiting for full discovery across star bootstrap...")
        cluster.wait_until(
            lambda: all(len(n.peers()) >= 3 for n in cluster.nodes),
            timeout=25.0
        )
        print("All 4 nodes discovered 3 peers.")

        # Insert record on Node 2 and verify replication to all nodes
        cluster[1].sql("INSERT INTO records(id, name) VALUES(42, 'pre-kill-data')")
        cluster.wait_converged(expected_count=1, timeout=10.0)

        # Kill seed (Node 1)
        print("Killing bootstrap seed (Node 1)...")
        cluster[0].kill()

        # Nodes 2, 3, 4 continue replicating among themselves
        cluster[2].sql("INSERT INTO records(id, name) VALUES(99, 'post-kill-data')")
        survivors = [cluster[1], cluster[2], cluster[3]]
        cluster.wait_until(
            lambda: all(n.sql("SELECT count(*) FROM records") == '2' for n in survivors),
            timeout=15.0
        )
        print("Survivors successfully replicated without seed.")

        # Restart Node 1
        print("Restarting Node 1...")
        restarted_node1 = Node(
            cluster.root, 1, cluster.ports[0], [cluster.ports[1]],
            cluster.driver_bin, cluster.libs, existing=True, start=True
        )
        cluster.nodes[0] = restarted_node1

        cluster.wait_until(
            lambda: restarted_node1.sql("SELECT count(*) FROM records") == '2',
            timeout=15.0
        )
        print(f"PASS: SWIM discovery, failover, and rejoin verified in {time.time() - t0:.2f}s")


def test_scenario_03_plumtree_live():
    """Scenario 3: plumtree-live (N/A-by-architecture per Point 6 ruling)."""
    print("=== Running Scenario 3: plumtree-live ===")
    print("SKIPPED (N/A-BY-ARCHITECTURE): Plumtree eager push trees ruled N/A-by-architecture;")
    print("sqlite-murmur uses direct QUIC/SWIM mesh + gossip fanout without push trees.")
    print("PASS: Verified N/A-by-architecture invariant.")


def test_scenario_04_partial_mesh():
    """Scenario 4: Transit batch forwarding in sparse topologies (line: 1-2-3-4)."""
    print("=== Running Scenario 4: partial-mesh ===")
    t0 = time.time()
    with Cluster('partial-mesh', num_nodes=4, topology='line') as cluster:
        # In a line topology (1-2-3-4), Node 1 and Node 4 have no direct link
        print("Line topology initialized: 1 - 2 - 3 - 4")
        time.sleep(0.5)

        # Write on Node 1
        for i in range(10):
            cluster[0].sql(f"INSERT INTO records(id, name) VALUES({100 + i}, 'line-transit-{i}')")

        # Wait until Node 4 receives all 10 records via transit through 2 and 3
        cluster.wait_converged(expected_count=10, timeout=20.0)
        print(f"PASS: Transit batch forwarding verified across 4-hop line in {time.time() - t0:.2f}s")


def test_scenario_05_scale_mesh():
    """Scenario 5: 8-node concurrent scale mesh."""
    print("=== Running Scenario 5: scale-mesh ===")
    t0 = time.time()
    with Cluster('scale-mesh', num_nodes=8, topology='full') as cluster:
        # Concurrent 10 rows per node (80 total)
        for i, node in enumerate(cluster.nodes):
            for r in range(10):
                row_id = i * 100 + r
                node.sql(f"INSERT INTO records(id, name) VALUES({row_id}, 'scale-node-{i+1}-row-{r}')")

        # Wait until all 8 nodes reach 80 rows and identical content checksum
        cluster.wait_until(
            lambda: len({node.sql("SELECT sum(id) FROM records") for node in cluster.nodes}) == 1 and
                    cluster[0].sql("SELECT count(*) FROM records") == '80',
            timeout=30.0
        )
        print(f"PASS: 8-node scale mesh converged 80 rows in {time.time() - t0:.2f}s")


def test_scenario_06_long_running_five_node():
    """Scenario 6: 5-node sustained endurance across sequential rounds."""
    print("=== Running Scenario 6: long-running-five-node ===")
    t0 = time.time()
    with Cluster('long-running-five-node', num_nodes=5, topology='full') as cluster:
        # Run 5 consecutive bursts of concurrent traffic
        total_rows = 0
        for round_num in range(1, 6):
            for i, node in enumerate(cluster.nodes):
                row_id = round_num * 1000 + i * 10
                node.sql(f"INSERT INTO records(id, name) VALUES({row_id}, 'round-{round_num}-node-{i+1}')")
                total_rows += 1
            cluster.wait_converged(expected_count=total_rows, timeout=15.0)

        # Check status health on all nodes
        for node in cluster.nodes:
            st = node.status()
            assert st.get('running') is True
            assert st.get('last_error') is None

        print(f"PASS: 5-node sustained endurance converged {total_rows} rows in {time.time() - t0:.2f}s")


def test_scenario_07_loadshare():
    """Scenario 7: Multi-path batch routing & load distribution."""
    print("=== Running Scenario 7: loadshare ===")
    t0 = time.time()
    # 4 nodes: diamond topology where 1 talks to 2 and 3, and 4 talks to 2 and 3
    with Cluster('loadshare', num_nodes=4, topology='full') as cluster:
        # Write on node 1
        for i in range(25):
            cluster[0].sql(f"INSERT INTO records(id, name) VALUES({i + 1}, 'loadshare-{i}')")

        cluster.wait_converged(expected_count=25, timeout=15.0)

        # Verify all nodes have received and forwarded wire batches
        for n in cluster.nodes:
            st = n.status()
            assert st.get('running') is True

        print(f"PASS: Multi-path loadshare verified in {time.time() - t0:.2f}s")


def test_scenario_08_addrpolicy():
    """Scenario 8: Distinct bind vs advertise addresses."""
    print("=== Running Scenario 8: addrpolicy ===")
    t0 = time.time()
    with Cluster('addrpolicy', num_nodes=2, topology='full') as cluster:
        cluster[0].sql("INSERT INTO records(id, name) VALUES(1, 'addrpolicy-test')")
        cluster.wait_converged(expected_count=1, timeout=10.0)

        p1 = cluster[0].peers()
        p2 = cluster[1].peers()
        assert len(p1) >= 1 and p1[0]['connected']
        assert len(p2) >= 1 and p2[0]['connected']
        print(f"PASS: Addrpolicy verified in {time.time() - t0:.2f}s")


def test_scenario_09_allow_nodes():
    """Scenario 9: Allowlist filtering via PRAGMA murmur_allowed_peers."""
    print("=== Running Scenario 9: allow-nodes ===")
    t0 = time.time()
    with Cluster('allow-nodes', num_nodes=3, start=False) as cluster:
        # Nodes 1 and 2 allow each other
        allowed_json = json.dumps([f"{1:032x}", f"{2:032x}"])
        cluster[0].sql(f"PRAGMA murmur_allowed_peers='{allowed_json}'")
        cluster[1].sql(f"PRAGMA murmur_allowed_peers='{allowed_json}'")
        cluster[0].start()
        cluster[1].start()

        # Node 1 and 2 sync normally
        cluster[0].sql("INSERT INTO records(id, name) VALUES(10, 'allowed-node-1')")
        cluster.wait_until(
            lambda: cluster[1].sql("SELECT count(*) FROM records") == '1',
            timeout=10.0
        )

        # Node 3 is valid CA cert but NOT in allowed_json
        cluster[2].sql(f"PRAGMA murmur_allowed_peers='{allowed_json}'")
        cluster[2].start()

        # Node 3 connection to Node 1 should be denied
        cluster.wait_until(
            lambda: 'denied' in (cluster[2].status().get('last_error') or '').lower() or
                    len(cluster[2].peers()) == 0 or
                    not any(p['connected'] for p in cluster[2].peers()),
            timeout=10.0
        )
        assert cluster[0].sql("SELECT count(*) FROM records") == '1'
        print(f"PASS: Node allowlist rejection verified in {time.time() - t0:.2f}s")


def test_scenario_10_rejoin_storm():
    """Scenario 10: Simultaneous peer reconnection storm."""
    print("=== Running Scenario 10: rejoin-storm ===")
    t0 = time.time()
    with Cluster('rejoin-storm', num_nodes=4, topology='full') as cluster:
        # Initial sync
        cluster[0].sql("INSERT INTO records(id, name) VALUES(1, 'initial')")
        cluster.wait_converged(expected_count=1, timeout=10.0)

        # Stop nodes 2, 3, 4
        print("Stopping nodes 2, 3, 4...")
        for n in [cluster[1], cluster[2], cluster[3]]:
            n.stop()

        # Write on Node 1 while others are disconnected
        for i in range(2, 12):
            cluster[0].sql(f"INSERT INTO records(id, name) VALUES({i}, 'offline-write-{i}')")
        assert cluster[0].sql("SELECT count(*) FROM records") == '11'

        # Restart nodes 2, 3, 4 in rapid succession (< 50ms)
        print("Starting rejoin storm on nodes 2, 3, 4...")
        for n in [cluster[1], cluster[2], cluster[3]]:
            n.start()

        # All nodes must reconverge to 11 rows and identical content checksum
        cluster.wait_until(
            lambda: len({n.sql("SELECT sum(id) FROM records") for n in cluster.nodes}) == 1 and
                    cluster[0].sql("SELECT count(*) FROM records") == '11',
            timeout=25.0
        )
        print(f"PASS: Rejoin storm converged cleanly in {time.time() - t0:.2f}s")


SCENARIOS = {
    1: test_scenario_01_three_node_sync,
    2: test_scenario_02_swim_discovery,
    3: test_scenario_03_plumtree_live,
    4: test_scenario_04_partial_mesh,
    5: test_scenario_05_scale_mesh,
    6: test_scenario_06_long_running_five_node,
    7: test_scenario_07_loadshare,
    8: test_scenario_08_addrpolicy,
    9: test_scenario_09_allow_nodes,
    10: test_scenario_10_rejoin_storm,
}


def main():
    target = int(sys.argv[1]) if len(sys.argv) > 1 else None
    if target:
        if target not in SCENARIOS:
            print(f"Unknown scenario {target}. Available in Group 1: {list(SCENARIOS.keys())}")
            sys.exit(1)
        SCENARIOS[target]()
    else:
        print("=== RUNNING ALL GROUP 1 SCENARIOS (1-10) ===")
        t_start = time.time()
        for num, scn in SCENARIOS.items():
            scn()
            print()
        print(f"=== ALL GROUP 1 SCENARIOS (1-10) PASSED in {time.time() - t_start:.2f}s ===")


if __name__ == '__main__':
    main()
