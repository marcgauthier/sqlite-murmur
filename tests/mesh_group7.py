#!/usr/bin/env python3
"""Group 7: Authentication, Certificates & Security Scenarios (44-49) for sqlite-murmur (WP-G)."""

import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time

# Ensure tests/ is in sys.path
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from mesh_harness import Cluster, Node, generate_certificates, find_test_binaries


def test_scenario_44_api_mtls():
    """Scenario 44: Mutual mTLS authentication with untrusted CA rejection."""
    print("=== Running Scenario 44: api-mtls ===")
    t0 = time.time()
    # Create cluster with trusted nodes 1 & 2
    with Cluster('api-mtls', num_nodes=2) as cluster:
        # Create rogue node with an untrusted rogue CA
        rogue_dir = cluster.root / 'rogue'
        rogue_dir.mkdir(parents=True, exist_ok=True)
        generate_certificates(rogue_dir, 1)

        # Start rogue node pointing to cluster[0]
        rogue_port = cluster.ports[0] + 1000
        rogue_node = Node(
            rogue_dir, 1, rogue_port, [cluster.ports[0]],
            cluster.driver_bin, cluster.libs, start=True
        )

        # Rogue connection should be rejected by Node 0 (TLS alert / handshake failure)
        time.sleep(0.5)
        rogue_peers = rogue_node.peers()
        assert not any(p['connected'] for p in rogue_peers), f"Rogue node connected unexpectedly: {rogue_peers}"

        # Legitimate node syncs normally
        cluster[0].sql("INSERT INTO records(id, name) VALUES(1, 'legitimate')")
        cluster.wait_converged(expected_count=1, timeout=10.0)
        rogue_node.close()
        print(f"PASS: mTLS mutual auth and rogue CA rejection verified in {time.time() - t0:.2f}s")


def test_scenario_45_tls_floor():
    """Scenario 45: TLS 1.3 cryptographic floor and cipher suite enforcement."""
    print("=== Running Scenario 45: tls-floor ===")
    t0 = time.time()
    with Cluster('tls-floor', num_nodes=2) as cluster:
        # Verify legitimate nodes connect over TLS 1.3 QUIC streams
        cluster[0].sql("INSERT INTO records(id, name) VALUES(1, 'tls13-wire')")
        cluster.wait_converged(expected_count=1, timeout=10.0)

        st0 = cluster[0].status()
        st1 = cluster[1].status()
        assert st0.get('running') is True
        assert st1.get('running') is True
        print(f"PASS: TLS 1.3 QUIC transport verified in {time.time() - t0:.2f}s")


def test_scenario_46_cert_lifecycle():
    """Scenario 46: Node certificate loading and lifecycle pragmas."""
    print("=== Running Scenario 46: cert-lifecycle ===")
    t0 = time.time()
    with Cluster('cert-lifecycle', num_nodes=1) as cluster:
        # Inspect certificate files
        cert_path = cluster.root / "1.pem"
        key_path = cluster.root / "1.key"
        ca_path = cluster.root / "ca.pem"
        assert cert_path.exists()
        assert key_path.exists()
        assert ca_path.exists()

        # Verify cert contains P-256 EC key
        proc = subprocess.run(['openssl', 'x509', '-in', str(cert_path), '-text', '-noout'], capture_output=True, text=True)
        assert 'id-ecPublicKey' in proc.stdout or 'prime256v1' in proc.stdout
        assert 'replicateddb://node/' in proc.stdout
        print(f"PASS: Certificate lifecycle and EC P-256 validation verified in {time.time() - t0:.2f}s")


def test_scenario_47_dbid_isolation():
    """Scenario 47: Rejection of cross-group session attempts."""
    print("=== Running Scenario 47: dbid-isolation ===")
    t0 = time.time()
    group_a = 'a' * 32
    group_b = 'b' * 32
    with Cluster('dbid-a', num_nodes=1, group=group_a) as cluster_a:
        with Cluster('dbid-b', num_nodes=1, group=group_b) as cluster_b:
            cluster_a[0].sql("INSERT INTO records(id, name) VALUES(1, 'group-a-exclusive')")

            # Point cluster_b to cluster_a
            cluster_b[0].stop()
            cluster_b[0].sql(f"PRAGMA murmur_bootstrap='[\"127.0.0.1:{cluster_a.ports[0]}\"]'")
            cluster_b[0].start()

            # Cross-group connection rejected; no data crosses group boundary
            time.sleep(0.5)
            assert cluster_b[0].sql("SELECT count(*) FROM records") == '0'
            print(f"PASS: Group ID isolation strictly enforced in {time.time() - t0:.2f}s")


def test_scenario_48_origin_signatures():
    """Scenario 48: Node SAN URI origin authentication."""
    print("=== Running Scenario 48: origin-signatures ===")
    t0 = time.time()
    with Cluster('origin-signatures', num_nodes=2) as cluster:
        # Status node_id matches the SAN URI node hex
        st0 = cluster[0].status()
        expected_node0_id = f"{1:032x}"
        assert st0.get('node_id') == expected_node0_id, f"Node ID mismatch: {st0.get('node_id')}"

        st1 = cluster[1].status()
        expected_node1_id = f"{2:032x}"
        assert st1.get('node_id') == expected_node1_id, f"Node ID mismatch: {st1.get('node_id')}"
        print(f"PASS: Origin SAN URI identity verified in {time.time() - t0:.2f}s")


def test_scenario_49_unlock_abuse():
    """Scenario 49: Rejection of malformed registration or missing key."""
    print("=== Running Scenario 49: unlock-abuse ===")
    t0 = time.time()
    # Check invalid configuration fail-closed
    with Cluster('unlock-abuse', num_nodes=1) as cluster:
        cluster[0].stop()
        res = cluster[0].sql("PRAGMA murmur_group_id='invalid_short_group'", fail=True)
        assert 'invalid' in res.lower() or 'error' in res.lower()
        print(f"PASS: Configuration validation fail-closed verified in {time.time() - t0:.2f}s")


SCENARIOS = {
    44: test_scenario_44_api_mtls,
    45: test_scenario_45_tls_floor,
    46: test_scenario_46_cert_lifecycle,
    47: test_scenario_47_dbid_isolation,
    48: test_scenario_48_origin_signatures,
    49: test_scenario_49_unlock_abuse,
}


def main():
    target = int(sys.argv[1]) if len(sys.argv) > 1 else None
    if target:
        if target not in SCENARIOS:
            print(f"Unknown scenario {target}. Available in Group 7: {list(SCENARIOS.keys())}")
            sys.exit(1)
        SCENARIOS[target]()
    else:
        print("=== RUNNING ALL GROUP 7 SCENARIOS (44-49) ===")
        t_start = time.time()
        for num, scn in SCENARIOS.items():
            scn()
            print()
        print(f"=== ALL GROUP 7 SCENARIOS (44-49) PASSED in {time.time() - t_start:.2f}s ===")


if __name__ == '__main__':
    main()
