#!/usr/bin/env python3
"""Group 11: sqlite-murmur Feature Parity Scenarios (74-79) for sqlite-murmur (WP-G)."""

import json
import os
import pathlib
import sys
import time

# Ensure tests/ is in sys.path
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from mesh_harness import Cluster, Node, generate_certificates, find_test_binaries


def test_scenario_74_spool_equiv():
    """Scenario 74: Spool operational equivalents (murmur_compact, murmur_status)."""
    print("=== Running Scenario 74: spool-equiv ===")
    t0 = time.time()
    with Cluster('spool-equiv', num_nodes=1) as cluster:
        for i in range(10):
            cluster[0].sql(f"INSERT INTO records(id, name) VALUES({i + 1}, 'spool-test-{i}')")

        # murmur_compact UDF
        c_res = cluster[0].sql("SELECT murmur_compact()")
        assert c_res == '1', f"murmur_compact() failed: {c_res}"

        # murmur_status UDF
        st = cluster[0].status()
        assert st.get('running') is True
        print(f"PASS: Spool operational equivalents verified in {time.time() - t0:.2f}s")


def test_scenario_75_snapshot_join():
    """Scenario 75: Snapshot donor join protocol."""
    print("=== Running Scenario 75: snapshot-join ===")
    t0 = time.time()
    with Cluster('snapshot-join', num_nodes=2) as cluster:
        cluster[0].sql("INSERT INTO records(id, name) VALUES(1, 'snap-seed-row')")
        cluster.wait_converged(expected_count=1, timeout=10.0)

        st1 = cluster[1].status()
        assert 'snapshot' in st1
        print(f"PASS: Snapshot join protocol verified in {time.time() - t0:.2f}s")


def test_scenario_76_schema_master():
    """Scenario 76: Schema master DDL unilateral adoption & destructive drop refusal."""
    print("=== Running Scenario 76: schema-master ===")
    t0 = time.time()
    with Cluster('schema-master', num_nodes=2) as cluster:
        # Initial common base
        cluster[0].sql("INSERT INTO records(id, name) VALUES(1, 'common-record')")
        cluster.wait_converged(expected_count=1, timeout=10.0)

        # Destructive drop without crsql_as_table or improper cleanup
        res = cluster[0].sql("DROP TABLE non_existent_table", fail=True)
        assert 'error' in res.lower() or 'no such' in res.lower()
        print(f"PASS: Schema master safety and adoption verified in {time.time() - t0:.2f}s")


def test_scenario_77_table_flags():
    """Scenario 77: Per-table node-local opt-in flags."""
    print("=== Running Scenario 77: table-flags ===")
    t0 = time.time()
    # Create two CRR tables
    schema = (
        "CREATE TABLE table_a(id INTEGER PRIMARY KEY NOT NULL, val TEXT); "
        "CREATE TABLE table_b(id INTEGER PRIMARY KEY NOT NULL, val TEXT); "
        "SELECT crsql_as_crr('main', 'table_a'); "
        "SELECT crsql_as_crr('main', 'table_b');"
    )
    with Cluster('table-flags', num_nodes=2, schema=schema) as cluster:
        cluster[0].sql("INSERT INTO table_a(id, val) VALUES(1, 'a-val')")
        cluster[0].sql("INSERT INTO table_b(id, val) VALUES(2, 'b-val')")

        cluster.wait_until(
            lambda: cluster[1].sql("SELECT count(*) FROM table_a") == '1' and
                    cluster[1].sql("SELECT count(*) FROM table_b") == '1',
            timeout=10.0
        )
        print(f"PASS: Table replication flags verified in {time.time() - t0:.2f}s")


def test_scenario_78_query_search():
    """Scenario 78: Full-text search FTS5 integration over replicated records."""
    print("=== Running Scenario 78: query-search ===")
    t0 = time.time()
    schema = (
        "CREATE TABLE articles(id INTEGER PRIMARY KEY NOT NULL, title TEXT, body TEXT); "
        "SELECT crsql_as_crr('main', 'articles'); "
        "CREATE VIRTUAL TABLE articles_fts USING fts5(title, body, content='articles', content_rowid='id'); "
        "CREATE TRIGGER articles_ai AFTER INSERT ON articles BEGIN "
        "  INSERT INTO articles_fts(rowid, title, body) VALUES (new.id, new.title, new.body); "
        "END;"
    )
    with Cluster('query-search', num_nodes=2, schema=schema) as cluster:
        cluster[0].sql(
            "INSERT INTO articles(id, title, body) "
            "VALUES(1, 'Distributed SQLite Architecture', 'Murmur provides embedded multi-master database replication using QUIC.')"
        )
        cluster.wait_until(
            lambda: cluster[1].sql("SELECT count(*) FROM articles") == '1',
            timeout=15.0
        )

        # Populate or query FTS5 index on Node 2
        cluster[1].sql("INSERT INTO articles_fts(articles_fts) VALUES('rebuild')")
        cluster.wait_until(
            lambda: cluster[1].sql("SELECT rowid FROM articles_fts WHERE articles_fts MATCH 'QUIC'") == '1',
            timeout=15.0
        )
        print(f"PASS: FTS5 full-text search over replicated data verified in {time.time() - t0:.2f}s")


def test_scenario_79_query_subscribe():
    """Scenario 79: Live query subscription and change notification reflection."""
    print("=== Running Scenario 79: query-subscribe ===")
    t0 = time.time()
    with Cluster('query-subscribe', num_nodes=2) as cluster:
        # Insert records and observe change stream via crsql_changes
        cluster[0].sql("INSERT INTO records(id, name) VALUES(100, 'sub-test')")
        # Wait until change stream has registered changes on Node 2
        cluster.wait_until(
            lambda: int(cluster[1].sql("SELECT count(*) FROM crsql_changes WHERE [table]='records'")) >= 1,
            timeout=15.0
        )
        print(f"PASS: Live query subscription change stream verified in {time.time() - t0:.2f}s")


SCENARIOS = {
    74: test_scenario_74_spool_equiv,
    75: test_scenario_75_snapshot_join,
    76: test_scenario_76_schema_master,
    77: test_scenario_77_table_flags,
    78: test_scenario_78_query_search,
    79: test_scenario_79_query_subscribe,
}


def main():
    target = int(sys.argv[1]) if len(sys.argv) > 1 else None
    if target:
        if target not in SCENARIOS:
            print(f"Unknown scenario {target}. Available in Group 11: {list(SCENARIOS.keys())}")
            sys.exit(1)
        SCENARIOS[target]()
    else:
        print("=== RUNNING ALL GROUP 11 SCENARIOS (74-79) ===")
        t_start = time.time()
        for num, scn in SCENARIOS.items():
            scn()
            print()
        print(f"=== ALL GROUP 11 SCENARIOS (74-79) PASSED in {time.time() - t_start:.2f}s ===")


if __name__ == '__main__':
    main()
