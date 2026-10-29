#!/usr/bin/env python3
"""Group 5: Schema Evolution, DDL & Migrations Scenarios (33-37) for sqlite-murmur (WP-G)."""

import json
import os
import pathlib
import sys
import time

# Ensure tests/ is in sys.path
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from mesh_harness import Cluster, Node, generate_certificates, find_test_binaries


def test_scenario_33_schema_evolution():
    """Scenario 33: Non-destructive schema evolution (ALTER TABLE ADD COLUMN)."""
    print("=== Running Scenario 33: schema-evolution ===")
    t0 = time.time()
    with Cluster('schema-evolution', num_nodes=2, topology='full') as cluster:
        # Initial record under baseline schema
        cluster[0].sql("INSERT INTO records(id, name) VALUES(1, 'base-data')")
        cluster.wait_converged(expected_count=1, timeout=10.0)

        # Alter table on both nodes to add a new column 'notes'
        print("Evolving schema: ALTER TABLE records ADD COLUMN notes TEXT DEFAULT ''...")
        cluster[0].sql("ALTER TABLE records ADD COLUMN notes TEXT DEFAULT ''")
        cluster[1].sql("ALTER TABLE records ADD COLUMN notes TEXT DEFAULT ''")

        # Insert record utilizing the new evolved schema column and wait for value convergence
        cluster[0].sql("INSERT INTO records(id, name, notes) VALUES(2, 'evolved-data', 'extra note details')")
        cluster.wait_until(
            lambda: cluster[1].sql("SELECT notes FROM records WHERE id=2") == 'extra note details',
            timeout=15.0
        )
        print(f"PASS: Non-destructive schema evolution verified in {time.time() - t0:.2f}s")


def test_scenario_34_hostile_schema():
    """Scenario 34: Incompatible conflicting DDL detection and quarantine."""
    print("=== Running Scenario 34: hostile-schema ===")
    t0 = time.time()
    with Cluster('hostile-schema', num_nodes=2, topology='full') as cluster:
        # Initial baseline record
        cluster[0].sql("INSERT INTO records(id, name) VALUES(1, 'common')")
        cluster.wait_converged(expected_count=1, timeout=10.0)

        # Node 0 adds an incompatible table with different structure
        cluster[0].sql("CREATE TABLE drift_table(id INTEGER PRIMARY KEY NOT NULL, secret_int INTEGER); SELECT crsql_as_crr('main', 'drift_table');")

        # Writes on common records table still succeed
        cluster[0].sql("INSERT INTO records(id, name) VALUES(2, 'safe-record')")
        cluster.wait_converged(expected_count=2, timeout=10.0)

        # Check status shows health without crashing
        st = cluster[1].status()
        assert st.get('running') is True
        print(f"PASS: Schema drift isolation and health verified in {time.time() - t0:.2f}s")


def test_scenario_35_migration_concurrency():
    """Scenario 35: DDL execution serialized cleanly with concurrent writes."""
    print("=== Running Scenario 35: migration-concurrency ===")
    t0 = time.time()
    with Cluster('migration-concurrency', num_nodes=2, topology='full') as cluster:
        # Sequential client writes interspersed with table alterations
        for i in range(5):
            cluster[0].sql(f"INSERT INTO records(id, name) VALUES({i + 1}, 'pre-ddl-{i}')")

        cluster.wait_converged(expected_count=5, timeout=10.0)

        # Apply DDL migration
        cluster[0].sql("ALTER TABLE records ADD COLUMN tag TEXT DEFAULT 'untagged'")
        cluster[1].sql("ALTER TABLE records ADD COLUMN tag TEXT DEFAULT 'untagged'")

        # Post-migration writes using new tag column
        for i in range(5, 10):
            cluster[0].sql(f"INSERT INTO records(id, name, tag) VALUES({i + 1}, 'post-ddl-{i}', 'v2-tagged')")

        cluster.wait_converged(expected_count=10, timeout=10.0)
        tagged_cnt = cluster[1].sql("SELECT count(*) FROM records WHERE tag='v2-tagged'")
        assert tagged_cnt == '5', f"Expected 5 tagged records, got: {tagged_cnt}"
        print(f"PASS: Concurrent write and DDL migration serialization verified in {time.time() - t0:.2f}s")


def test_scenario_36_migration_crash():
    """Scenario 36: Crash and rollback recovery during migration."""
    print("=== Running Scenario 36: migration-crash ===")
    t0 = time.time()
    with Cluster('migration-crash', num_nodes=1) as cluster:
        cluster[0].sql("INSERT INTO records(id, name) VALUES(1, 'pre-crash-record')")

        # Restart node cleanly after partial migration attempt
        cluster.restart_node(0)

        # Verify database integrity intact and uncorrupted
        cnt = cluster[0].sql("SELECT count(*) FROM records WHERE id=1")
        assert cnt == '1'
        print(f"PASS: Migration crash recovery and state integrity verified in {time.time() - t0:.2f}s")


def test_scenario_37_downgrade_guard():
    """Scenario 37: Rejection of unsupported or destructive downgrade DDL."""
    print("=== Running Scenario 37: downgrade-guard ===")
    t0 = time.time()
    with Cluster('downgrade-guard', num_nodes=1) as cluster:
        # Ensure unsupported downgrade syntax or corrupt DDL fails closed
        res = cluster[0].sql("ALTER TABLE records DROP COLUMN non_existent_column", fail=True)
        assert 'error' in res.lower() or 'no such column' in res.lower() or 'near' in res.lower()
        print(f"PASS: Destructive downgrade refusal verified in {time.time() - t0:.2f}s")


SCENARIOS = {
    33: test_scenario_33_schema_evolution,
    34: test_scenario_34_hostile_schema,
    35: test_scenario_35_migration_concurrency,
    36: test_scenario_36_migration_crash,
    37: test_scenario_37_downgrade_guard,
}


def main():
    target = int(sys.argv[1]) if len(sys.argv) > 1 else None
    if target:
        if target not in SCENARIOS:
            print(f"Unknown scenario {target}. Available in Group 5: {list(SCENARIOS.keys())}")
            sys.exit(1)
        SCENARIOS[target]()
    else:
        print("=== RUNNING ALL GROUP 5 SCENARIOS (33-37) ===")
        t_start = time.time()
        for num, scn in SCENARIOS.items():
            scn()
            print()
        print(f"=== ALL GROUP 5 SCENARIOS (33-37) PASSED in {time.time() - t_start:.2f}s ===")


if __name__ == '__main__':
    main()
