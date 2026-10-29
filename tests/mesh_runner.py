#!/usr/bin/env python3
"""Master test runner for all 79 sqlite-murmur live mesh scenarios (WP-G)."""

import fcntl
import importlib
import pathlib
import sys
import time

# Ensure tests/ directory is in sys.path
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

GROUPS = {
    1: 'mesh_group1',
    2: 'mesh_group2',
    3: 'mesh_group3',
    4: 'mesh_group4',
    5: 'mesh_group5',
    6: 'mesh_group6',
    7: 'mesh_group7',
    8: 'mesh_group8',
    9: 'mesh_group9',
    10: 'mesh_group10',
    11: 'mesh_group11',
}


def main():
    lock_file = open('/tmp/sqlite_murmur_cluster.lock', 'a')
    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
    target_group = int(sys.argv[1]) if len(sys.argv) > 1 else None
    t0 = time.time()

    if target_group:
        if target_group not in GROUPS:
            print(f"Unknown group {target_group}. Available: {list(GROUPS.keys())}")
            sys.exit(1)
        mod = importlib.import_module(GROUPS[target_group])
        mod.main()
    else:
        print("=" * 60)
        print("RUNNING ALL 79 SQLITE-MURMUR LIVE MESH SCENARIOS (GROUPS 1-11)")
        print("=" * 60)
        passed_total = 0
        total_count = 0

        for g_num, g_mod_name in sorted(GROUPS.items()):
            print(f"\n>>> EXECUTING GROUP {g_num} <<<\n")
            mod = importlib.import_module(g_mod_name)
            for scn_num, scn_fn in mod.SCENARIOS.items():
                total_count += 1
                scn_fn()
                passed_total += 1
                print()

        elapsed = time.time() - t0
        print("=" * 60)
        print(f"SUMMARY: ALL {passed_total}/{total_count} SCENARIOS PASSED IN {elapsed:.2f}s")
        print("=" * 60)


if __name__ == '__main__':
    main()
