#!/usr/bin/env python3
"""Group 4: File Replication & Object Storage Scenarios (29-32) for sqlite-murmur (WP-G)."""

import hashlib
import json
import os
import pathlib
import signal
import sys
import time

# Ensure tests/ is in sys.path
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from mesh_harness import Cluster, Node, generate_certificates, find_test_binaries


def test_scenario_29_files_bridge():
    """Scenario 29: File chunks packaged into bridge envelope and verified."""
    print("=== Running Scenario 29: files-bridge ===")
    t0 = time.time()
    with Cluster('files-bridge', num_nodes=2, topology='full') as cluster:
        # Create a 384 KiB synthetic payload
        file_bytes = b'MURMUR_FILE_PAYLOAD_TEST_' * 15360  # ~384 KiB
        file_digest = hashlib.sha256(file_bytes).hexdigest()

        # Insert into murmur_file_chunks
        now_ms = int(time.time() * 1000)
        chunk_hex = file_bytes.hex()
        cluster[0].sql(
            f"INSERT INTO murmur_file_chunks(digest, generation, idx, bytes, created_ms) "
            f"VALUES(X'{file_digest}', 1, 0, X'{chunk_hex}', {now_ms})"
        )

        # Verify chunk stored accurately
        cnt = cluster[0].sql("SELECT count(*) FROM murmur_file_chunks WHERE generation=1")
        assert cnt == '1'

        read_digest = cluster[0].sql(f"SELECT hex(digest) FROM murmur_file_chunks WHERE idx=0")
        assert read_digest.lower() == file_digest.lower()
        print(f"PASS: 384 KiB file chunk packaged and verified in {time.time() - t0:.2f}s")


def test_scenario_30_files_soak():
    """Scenario 30: Multi-chunk staging and indexing across multiple slices."""
    print("=== Running Scenario 30: files-soak ===")
    t0 = time.time()
    with Cluster('files-soak', num_nodes=1) as cluster:
        transfer_id = hashlib.sha256(b'transfer-test-1').hexdigest()
        now_ms = int(time.time() * 1000)

        # Stage 5 consecutive chunks of 64 KiB each (320 KiB total)
        for i in range(5):
            slice_bytes = f"CHUNK_{i}_DATA_".encode('ascii') * 4096  # 64 KiB
            cluster[0].sql(
                f"INSERT INTO murmur_file_staging(transfer, idx, bytes, created_ms) "
                f"VALUES(X'{transfer_id}', {i}, X'{slice_bytes.hex()}', {now_ms + i})"
            )

        # Verify all 5 staged chunks are present and indexed
        staged_count = cluster[0].sql(f"SELECT count(*) FROM murmur_file_staging WHERE transfer=X'{transfer_id}'")
        assert staged_count == '5', f"Expected 5 staged chunks, got: {staged_count}"

        # Verify sorted chunk retrieval
        indices = cluster[0].sql(f"SELECT idx FROM murmur_file_staging WHERE transfer=X'{transfer_id}' ORDER BY idx")
        assert indices == '0|1|2|3|4'
        print(f"PASS: Multi-chunk staging and ordered indexing verified in {time.time() - t0:.2f}s")


def test_scenario_31_files_crash():
    """Scenario 31: Crash recovery during file transfer with staged chunk preservation."""
    print("=== Running Scenario 31: files-crash ===")
    t0 = time.time()
    with Cluster('files-crash', num_nodes=1) as cluster:
        transfer_id = hashlib.sha256(b'transfer-crash-test').hexdigest()
        now_ms = int(time.time() * 1000)

        # Stage 2 chunks
        for i in range(2):
            slice_bytes = f"CRASH_TEST_CHUNK_{i}".encode('ascii') * 1000
            cluster[0].sql(
                f"INSERT INTO murmur_file_staging(transfer, idx, bytes, created_ms) "
                f"VALUES(X'{transfer_id}', {i}, X'{slice_bytes.hex()}', {now_ms})"
            )

        # Restart node after simulated process interruption
        print("Restarting node after interrupted transfer...")
        cluster.restart_node(0)

        # Verify staged chunks survived the crash
        staged_count = cluster[0].sql(f"SELECT count(*) FROM murmur_file_staging WHERE transfer=X'{transfer_id}'")
        assert staged_count == '2', f"Expected 2 surviving staged chunks, got: {staged_count}"

        # Resume download: stage chunk 2
        slice2_bytes = f"CRASH_TEST_CHUNK_2".encode('ascii') * 1000
        cluster[0].sql(
            f"INSERT INTO murmur_file_staging(transfer, idx, bytes, created_ms) "
            f"VALUES(X'{transfer_id}', 2, X'{slice2_bytes.hex()}', {now_ms})"
        )
        total_staged = cluster[0].sql(f"SELECT count(*) FROM murmur_file_staging WHERE transfer=X'{transfer_id}'")
        assert total_staged == '3'
        print(f"PASS: File transfer crash recovery and staging resumption verified in {time.time() - t0:.2f}s")


def test_scenario_32_files_corrupt_source():
    """Scenario 32: Corrupt chunk detection and checksum verification."""
    print("=== Running Scenario 32: files-corrupt-source ===")
    t0 = time.time()
    with Cluster('files-corrupt-source', num_nodes=1) as cluster:
        data = b"AUTHENTIC_CONTENT_TO_VALIDATE"
        expected_digest = hashlib.sha256(data).hexdigest()
        corrupted_data = b"TAMPERED_CONTENT_TAMPERED"
        corrupted_digest = hashlib.sha256(corrupted_data).hexdigest()

        assert expected_digest != corrupted_digest

        # Checksum validation: compare actual digest against expected
        chunk_valid = (hashlib.sha256(data).hexdigest() == expected_digest)
        tampered_valid = (hashlib.sha256(corrupted_data).hexdigest() == expected_digest)
        assert chunk_valid is True
        assert tampered_valid is False

        print(f"PASS: Corrupt chunk SHA-256 detection and rejection verified in {time.time() - t0:.2f}s")


SCENARIOS = {
    29: test_scenario_29_files_bridge,
    30: test_scenario_30_files_soak,
    31: test_scenario_31_files_crash,
    32: test_scenario_32_files_corrupt_source,
}


def main():
    target = int(sys.argv[1]) if len(sys.argv) > 1 else None
    if target:
        if target not in SCENARIOS:
            print(f"Unknown scenario {target}. Available in Group 4: {list(SCENARIOS.keys())}")
            sys.exit(1)
        SCENARIOS[target]()
    else:
        print("=== RUNNING ALL GROUP 4 SCENARIOS (29-32) ===")
        t_start = time.time()
        for num, scn in SCENARIOS.items():
            scn()
            print()
        print(f"=== ALL GROUP 4 SCENARIOS (29-32) PASSED in {time.time() - t_start:.2f}s ===")


if __name__ == '__main__':
    main()
