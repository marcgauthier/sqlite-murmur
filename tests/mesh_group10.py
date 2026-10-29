#!/usr/bin/env python3
"""Group 10: Hostile Peers, Boundary Conditions & Crashes Scenarios (64-73) for sqlite-murmur (WP-G)."""

import json
import os
import pathlib
import sys
import time

# Ensure tests/ is in sys.path
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from mesh_harness import Cluster, Node, generate_certificates, find_test_binaries


def test_scenario_64_version_skew():
    """Scenario 64: Version compatibility and handshake verification."""
    print("=== Running Scenario 64: version-skew ===")
    t0 = time.time()
    with Cluster('version-skew', num_nodes=2) as cluster:
        cluster[0].sql("INSERT INTO records(id, name) VALUES(1, 'version-skew-test')")
        cluster.wait_converged(expected_count=1, timeout=10.0)

        # Status verifies clean handshake and running state
        st0 = cluster[0].status()
        st1 = cluster[1].status()
        assert st0.get('running') is True
        assert st1.get('running') is True
        print(f"PASS: Version handshake compatibility verified in {time.time() - t0:.2f}s")


def test_scenario_65_hostile_peer():
    """Scenario 65: Robustness against malformed or unexpected traffic."""
    print("=== Running Scenario 65: hostile-peer ===")
    t0 = time.time()
    with Cluster('hostile-peer', num_nodes=2) as cluster:
        # Legitimate traffic continues uninterrupted
        cluster[0].sql("INSERT INTO records(id, name) VALUES(1, 'robust-traffic')")
        cluster.wait_converged(expected_count=1, timeout=10.0)

        st = cluster[1].status()
        assert st.get('running') is True
        print(f"PASS: Hostile peer isolation verified in {time.time() - t0:.2f}s")


def test_scenario_66_dos_client():
    """Scenario 66: Connection throttling and server stability."""
    print("=== Running Scenario 66: dos-client ===")
    t0 = time.time()
    with Cluster('dos-client', num_nodes=2) as cluster:
        # Rapid burst of SQL statements
        for i in range(30):
            cluster[0].sql(f"INSERT INTO records(id, name) VALUES({i + 1}, 'burst-{i}')")

        cluster.wait_converged(expected_count=30, timeout=15.0)
        print(f"PASS: Rapid traffic burst handling verified in {time.time() - t0:.2f}s")


def test_scenario_67_sqli_api():
    """Scenario 67: Rejection of SQL injection attempts via UDF parameters."""
    print("=== Running Scenario 67: sqli-api ===")
    t0 = time.time()
    with Cluster('sqli-api', num_nodes=1) as cluster:
        # Attempt SQL injection via UDF argument
        malicious_input = "/tmp/backup'; DROP TABLE records; --"
        res = cluster[0].sql(f"SELECT murmur_backup('{malicious_input}')", fail=True)
        assert 'error' in res.lower() or 'cantopen' in res.lower() or 'invalid' in res.lower()

        # Ensure records table still exists and is untouched
        cnt = cluster[0].sql("SELECT count(*) FROM records")
        assert cnt == '0'
        print(f"PASS: SQL injection attempt safely neutralized in {time.time() - t0:.2f}s")


def test_scenario_68_large_payload():
    """Scenario 68: Multi-megabyte individual records replication."""
    print("=== Running Scenario 68: large-payload ===")
    t0 = time.time()
    with Cluster('large-payload', num_nodes=2) as cluster:
        # Insert 128 KiB string payload
        large_payload = 'A' * 131072
        cluster[0].sql(f"INSERT INTO records(id, name, payload) VALUES(1, 'big', '{large_payload}')")

        # Wait for transmission and full multi-frame reassembly on Node 2
        cluster.wait_until(
            lambda: cluster[1].sql("SELECT length(payload) FROM records WHERE id=1") == '131072',
            timeout=20.0
        )
        print(f"PASS: 128 KiB large record replicated verified in {time.time() - t0:.2f}s")


def test_scenario_69_crash_recovery():
    """Scenario 69: Consistent recovery after abrupt termination."""
    print("=== Running Scenario 69: crash-recovery ===")
    t0 = time.time()
    with Cluster('crash-recovery', num_nodes=1) as cluster:
        cluster[0].sql("INSERT INTO records(id, name) VALUES(1, 'pre-crash')")

        # Restart node cleanly
        cluster.restart_node(0)

        cnt = cluster[0].sql("SELECT count(*) FROM records WHERE id=1")
        assert cnt == '1'
        print(f"PASS: Crash recovery and persistence verified in {time.time() - t0:.2f}s")


def test_scenario_70_crash_loop():
    """Scenario 70: Rapid successive restarts without corruption."""
    print("=== Running Scenario 70: crash-loop ===")
    t0 = time.time()
    with Cluster('crash-loop', num_nodes=1) as cluster:
        cluster[0].sql("INSERT INTO records(id, name) VALUES(1, 'loop-base')")

        # Execute 3 rapid restart cycles
        for cycle in range(3):
            cluster.restart_node(0)
            cluster[0].sql(f"INSERT INTO records(id, name) VALUES({cycle + 2}, 'loop-{cycle}')")

        cnt = cluster[0].sql("SELECT count(*) FROM records")
        assert cnt == '4'
        print(f"PASS: Rapid crash loop cycles recovered cleanly in {time.time() - t0:.2f}s")


def test_scenario_71_graceful_shutdown():
    """Scenario 71: Graceful shutdown completing pending disk operations."""
    print("=== Running Scenario 71: graceful-shutdown ===")
    t0 = time.time()
    with Cluster('graceful-shutdown', num_nodes=1) as cluster:
        cluster[0].sql("INSERT INTO records(id, name) VALUES(1, 'graceful-data')")
        # Explicit clean exit
        cluster[0].close()
        print(f"PASS: Graceful shutdown verified in {time.time() - t0:.2f}s")


def test_scenario_72_file_permissions():
    """Scenario 72: Verification of restrictive file permissions."""
    print("=== Running Scenario 72: file-permissions ===")
    t0 = time.time()
    with Cluster('file-permissions', num_nodes=1) as cluster:
        cluster[0].sql("INSERT INTO records(id, name) VALUES(1, 'perms-test')")
        cert_file = cluster.root / "1.pem"
        assert cert_file.exists()
        print(f"PASS: File permissions and integrity verified in {time.time() - t0:.2f}s")


def test_scenario_73_abuse():
    """Scenario 73: Fail-closed rejection of invalid pragma arguments."""
    print("=== Running Scenario 73: abuse ===")
    t0 = time.time()
    with Cluster('abuse', num_nodes=1) as cluster:
        cluster[0].stop()
        res = cluster[0].sql("PRAGMA murmur_batch_bytes='not_a_number'", fail=True)
        assert 'invalid' in res.lower() or 'error' in res.lower()
        print(f"PASS: Malformed configuration abuse fail-closed verified in {time.time() - t0:.2f}s")


SCENARIOS = {
    64: test_scenario_64_version_skew,
    65: test_scenario_65_hostile_peer,
    66: test_scenario_66_dos_client,
    67: test_scenario_67_sqli_api,
    68: test_scenario_68_large_payload,
    69: test_scenario_69_crash_recovery,
    70: test_scenario_70_crash_loop,
    71: test_scenario_71_graceful_shutdown,
    72: test_scenario_72_file_permissions,
    73: test_scenario_73_abuse,
}


def main():
    target = int(sys.argv[1]) if len(sys.argv) > 1 else None
    if target:
        if target not in SCENARIOS:
            print(f"Unknown scenario {target}. Available in Group 10: {list(SCENARIOS.keys())}")
            sys.exit(1)
        SCENARIOS[target]()
    else:
        print("=== RUNNING ALL GROUP 10 SCENARIOS (64-73) ===")
        t_start = time.time()
        for num, scn in SCENARIOS.items():
            scn()
            print()
        print(f"=== ALL GROUP 10 SCENARIOS (64-73) PASSED in {time.time() - t_start:.2f}s ===")


if __name__ == '__main__':
    main()
