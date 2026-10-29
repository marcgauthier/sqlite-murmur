#!/usr/bin/env python3
"""Check comparison matrices, backend policies, overrides and reported measurements."""
import csv
import math
import subprocess
import sys
from pathlib import Path


def validate(output, duration, expected=None):
    lines = output.splitlines()
    config_line = next(line for line in lines if line.startswith("BENCH_CONFIG,"))
    config = dict(field.split("=", 1) for field in config_line.split(",")[1:])
    if expected:
        for key, value in expected.items():
            assert config[key] == str(value), (key, config[key], value)
    murmur = config["backend"] == "murmur"
    memory = config["backend"] == "sqlite-memory"
    assert config["encryption"] == ("on" if murmur else "off")
    assert config["journal_mode"] == ("delete" if murmur else "memory" if memory else "wal")
    assert config["synchronous"] == ("2" if murmur else "0" if memory else "1")
    assert config["persistence"] == ("periodic_flush" if murmur else "volatile" if memory else "wal_checkpoint")
    if murmur:
        assert 1 <= int(config["background_threads"]) <= 256
        assert int(config["cache_bytes"]) == int(config["cache_pages"]) * 4096
        assert config["cipher"] in ("aegis256", "aes256gcm")
        assert config["compression"].startswith("zstd") and int(config["zstd_level"]) > 0
    else:
        assert config["cipher"] == config["compression"] == "none"
        assert config["zstd_level"] == "0"
        for field in ("background_threads", "cache_pages", "cache_bytes", "rocksdb_cache_bytes", "rocksdb_sync", "vfs_block_size", "flush_interval_ms"):
            assert config[field] == "NA", (field, config[field])
    assert int(config["sqlite_cache_size"]) <= 0
    assert int(config["sqlite_cache_bytes"]) == -int(config["sqlite_cache_size"]) * 1024
    header = next(next(csv.reader([line]))[1:] for line in lines if line.startswith("BENCH_HEADER,"))
    data = next(next(csv.reader([line]))[1:] for line in lines if line.startswith("BENCH_CSV,"))
    row = dict(zip(header, data, strict=True))
    reads, writes, operations = (int(row[key]) for key in ("reads", "writes", "operations"))
    seconds = float(row["seconds"])
    assert seconds >= duration and writes > 0
    assert reads == writes * 4 and operations == reads + writes
    assert writes == int(row["transactions"]) * int(config["batch_size"])
    assert math.isfinite(float(row["mean_write_transaction_ms"])) and float(row["mean_write_transaction_ms"]) >= 0
    assert math.isclose(float(row["ops_per_second"]), operations / seconds, rel_tol=0.001)
    assert float(row["ops_per_second_including_persistence"]) <= float(row["ops_per_second"])
    assert int(row["logical_database_bytes"]) > 0
    assert int(row["logical_database_bytes"]) % int(config["page_size"]) == 0
    assert (int(row["disk_bytes"]) == 0) if memory else (int(row["disk_bytes"]) > 0)
    assert row["zstd_level"] == config["zstd_level"]
    if murmur:
        assert int(row["rocksdb_files_bytes"]) == int(row["disk_bytes"])
        assert 0 <= float(row["compression_savings_pct"]) <= 100
    else:
        assert row["rocksdb_files_bytes"] == row["compression_savings_pct"] == "NA"
    if memory:
        assert float(row["persistence_seconds"]) == 0
    for field in ("backend", "suite", "replication", "batch_size"):
        assert row[field] == config[field]
    if config["replication"] == "on":
        revision = (Path(__file__).resolve().parents[1] / "third-party/cr-sqlite/sqlite-murmur-revision.txt").read_text().strip()
        assert config["crsqlite_version"] == revision
    else:
        assert config["crsqlite_version"] == "none"
    assert int(row["tracked_changes"]) == (writes if config["replication"] == "on" else 0)
    assert math.isclose(float(row["wall_ops_per_second_including_persistence"]), operations / (float(row["wall_seconds"]) + float(row["persistence_seconds"])), rel_tol=0.001)
    assert float(row["wall_seconds"]) >= seconds - 0.000002
    if config["ingest"] != "local":
        assert int(row["applied_changes"]) == writes
        assert int(row["packed_bytes"]) > writes
        assert float(row["source_prepare_seconds"]) > 0
        assert config["timing"] == "destination_active"
    assert "BENCH_DONE" in lines
    return row


def reports(output):
    chunks = output.split("BENCH_CONFIG,")[1:]
    assert output.splitlines()[-1] == "BENCH_SUITE_DONE"
    assert output.splitlines().count("BENCH_DONE") == len(chunks)
    return ["BENCH_CONFIG," + chunk for chunk in chunks]


def validate_suite(output, duration, groups=("off", "on")):
    seen = set()
    for report in reports(output):
        row = validate(report, duration, {"sqlite_cache_size": -1048576, "ingest": "local"})
        key = (row["replication"], row["backend"], int(row["batch_size"]))
        assert key not in seen, key
        seen.add(key)
        assert row["suite"] == ("crsqlite" if row["replication"] == "on" else "no-replication")
    assert seen == {(replication, backend, batch) for replication in groups
                    for backend in ("murmur", "sqlite-disk", "sqlite-memory") for batch in (1, 100)}, seen
    for replication in groups:
        assert ("Test 2: cr-sqlite local change tracking" if replication == "on" else "Test 1: no replication") in output
    assert output.count("| Backend | Inserts/tx |") == len(groups)
    assert output.count("| Inserts/tx | Murmur / SQLite disk ops/s |") == len(groups)


def main():
    binary, aegis, crsqlite = sys.argv[1:]
    cipher = "aegis256" if aegis == "ON" else "aes256gcm"
    defaults = {"cipher": cipher, "flush_interval_ms": 100, "cache_pages": 16384, "sqlite_cache_size": -1048576,
                "replication": "off", "crsqlite_version": "none", "compression": "zstd3", "zstd_level": 3, "page_size": 4096, "connections": 1, "batch_size": 1, "rocksdb_cache_bytes": 33554432}
    cases = [([], defaults),
             (["--flush-interval-ms=250", "--cache-pages=32", "--sqlite-cache-kib=0", "--compression=zstd1", "--cipher=aes256gcm", "--page-size=8192"],
              {"cipher": "aes256gcm", "flush_interval_ms": 250, "cache_pages": 32, "sqlite_cache_size": 0,
               "compression": "zstd1", "zstd_level": 1, "page_size": 8192}),
             (["--compression=zstd6", "--flush-interval-ms=10"], {"compression": "zstd6", "zstd_level": 6, "flush_interval_ms": 10}),
             (["--batch-size=100"], {"batch_size": 100})]
    cases += [([f"--background-threads={threads}"], {"background_threads": threads}) for threads in (1, 4, 8, 16)]
    if crsqlite == "ON":
        revision = (Path(__file__).resolve().parents[1] / "third-party/cr-sqlite/sqlite-murmur-revision.txt").read_text().strip()
        cases += [(["--replication=on", f"--batch-size={batch}"], {"replication": "on", "crsqlite_version": revision, "batch_size": batch}) for batch in (1, 100)]
        cases += [([f"--ingest={mode}", f"--batch-size={batch}"], {"ingest": mode, "replication": "on", "connections": 2}) for mode in ("row", "blob") for batch in (1, 100)]
    else:
        run = subprocess.run([binary, "--suite=single", "--replication=on"], capture_output=True, text=True, timeout=5)
        assert run.returncode == 2 and "SQLITE_MURMUR_ENABLE_CRSQLITE=ON" in run.stderr
        for mode in ("row", "blob"):
            run = subprocess.run([binary, "--suite=single", f"--ingest={mode}"], capture_output=True, text=True, timeout=5)
            assert run.returncode == 2 and "SQLITE_MURMUR_ENABLE_CRSQLITE=ON" in run.stderr
    for args, expected in cases:
        run = subprocess.run([binary, "--suite=single", "--seconds=0.01", f"--cipher={cipher}", *args],
                             capture_output=True, text=True, timeout=30)
        assert run.returncode == 0, run.stdout + run.stderr
        assert len(reports(run.stdout)) == 1
        validate(run.stdout, 0.01, expected)
    # Exercise the default matrix, each group selector, and native single-run opens.
    for suite_args, groups in [([], ("off", "on")), (["--suite=no-replication"], ("off",)), (["--suite=crsqlite"], ("on",))]:
        if crsqlite != "ON" and "on" in groups:
            run = subprocess.run([binary, *suite_args, f"--cipher={cipher}"], capture_output=True, text=True, timeout=5)
            assert run.returncode == 2 and "SQLITE_MURMUR_ENABLE_CRSQLITE=ON" in run.stderr
            continue
        run = subprocess.run([binary, "--seconds=0.01", f"--cipher={cipher}", *suite_args], capture_output=True, text=True, timeout=60)
        assert run.returncode == 0, run.stdout + run.stderr
        validate_suite(run.stdout, 0.01, groups)
    for backend in ("sqlite-disk", "sqlite-memory"):
        run = subprocess.run([binary, "--suite=single", f"--backend={backend}", "--seconds=0.01", "--page-size=8192"], capture_output=True, text=True, timeout=30)
        assert run.returncode == 0, run.stdout + run.stderr
        validate(run.stdout, 0.01, {"backend": backend, "page_size": 8192})
    if aegis != "ON":
        run = subprocess.run([binary, "--suite=single", "--seconds=0.01"], capture_output=True, text=True, timeout=5)
        assert run.returncode == 2 and "SQLITE_MURMUR_ENABLE_AEGIS256=ON" in run.stderr
    invalid = ["--suite=invalid", "--backend=invalid", "--compression=none", "--ingest=invalid","--background-threads=0", "--background-threads=257", "--background-threads=-1", "--background-threads=invalid","--replication=invalid","--seconds=0", "--seconds=-1", "--seconds=nan", "--seconds=inf", "--seconds=1junk",
               "--seconds=3601", "--unknown", "--flush-interval-ms=0", "--flush-interval-ms=4294967296",
               "--cache-pages=0", "--cache-pages=-1", "--cache-pages=18446744073709551616",
               "--page-size=4097", "--page-size=256", "--cipher=unknown", "--compression=zstd0",
               "--compression=zstd23", "--compression=zstd3junk", "--compression=unknown",
               "--sqlite-cache-kib=-1", "--sqlite-cache-kib=2147483648", "--sqlite-cache-kib=1junk",
               "--batch-size=0", "--batch-size=-1", "--batch-size=1000001", "--batch-size=100junk"]
    for arg in invalid:
        bad = subprocess.run([binary, "--suite=single", arg], capture_output=True, text=True, timeout=5)
        assert bad.returncode == 2 and "BENCH_ERROR," in bad.stderr, arg
    for args in (["--batch-size=1"], ["--replication=off"], ["--ingest=local"], ["--backend=murmur"],
                 ["--sqlite-cache-kib=0"], ["--suite=single", "--backend=sqlite-disk", "--ingest=row"]):
        run = subprocess.run([binary, *args], capture_output=True, text=True, timeout=5)
        assert run.returncode == 2 and "BENCH_ERROR," in run.stderr, args
    help_run = subprocess.run([binary, "--help"], capture_output=True, text=True, timeout=5)
    assert help_run.returncode == 0
    for setting in ("--seconds=60", "--flush-interval-ms=100", "--cache-pages=16384", "--sqlite-cache-kib=1048576", "--compression=zstd3", "--cipher=aegis256", "--suite=all", "--backend=murmur"):
        assert setting in help_run.stdout
    print("Benchmark matrices, backend policies, overrides and reporting checks passed.")


if __name__ == "__main__":
    main()
