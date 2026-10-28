#!/usr/bin/env python3
"""Check the one-run benchmark defaults, overrides and reported measurements."""
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
    assert 1 <= int(config["background_threads"]) <= 256
    assert int(config["cache_bytes"]) == int(config["cache_pages"]) * 4096
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
    assert int(row["rocksdb_files_bytes"]) > 0
    assert row["zstd_level"] == config["zstd_level"]
    assert 0 <= float(row["compression_savings_pct"]) <= 100
    if config["compression"] == "none":
        assert float(row["compression_savings_pct"]) == 0
    assert int(row["tracked_changes"]) == (writes if config["replication"] == "on" else 0)
    if config["replication"] == "on":
        assert config["murmur_version"] != "none"
        assert int(row["spool_bytes"]) > 0
    else:
        assert config["murmur_version"] == "none"
        assert int(row["spool_bytes"]) == 0
    assert math.isclose(float(row["wall_ops_per_second_including_persistence"]), operations / (float(row["wall_seconds"]) + float(row["persistence_seconds"])), rel_tol=0.001)
    assert float(row["wall_seconds"]) >= seconds - 0.000002
    if config["ingest"] != "local":
        assert int(row["applied_changes"]) == writes
        assert int(row["packed_bytes"]) > writes
        assert float(row["source_prepare_seconds"]) > 0
        assert config["timing"] == "destination_active"
    assert lines[-1] == "BENCH_DONE"
    return row


def murmur_version():
    cargo = (Path(__file__).resolve().parents[1]
             / "third-party/murmur-extension/crates/murmur-sqlite-ext/Cargo.toml").read_text()
    line = next(line for line in cargo.splitlines() if line.startswith("version = "))
    return line.split('"')[1]


def main():
    binary, aegis, murmurext = sys.argv[1:]
    cipher = "aegis256" if aegis == "ON" else "aes256gcm"
    defaults = {"cipher": cipher, "flush_interval_ms": 100, "cache_pages": 16384, "sqlite_cache_size": -1048576,
                "replication": "off", "murmur_version": "none", "compression": "zstd3", "zstd_level": 3, "page_size": 4096, "connections": 1, "batch_size": 1, "rocksdb_cache_bytes": 33554432}
    cases = [([], defaults),
             (["--flush-interval-ms=250", "--cache-pages=32", "--sqlite-cache-kib=0", "--compression=none", "--cipher=aes256gcm", "--page-size=8192"],
              {"cipher": "aes256gcm", "flush_interval_ms": 250, "cache_pages": 32, "sqlite_cache_size": 0,
               "compression": "none", "zstd_level": 0, "page_size": 8192}),
             (["--compression=zstd6", "--flush-interval-ms=10"], {"compression": "zstd6", "zstd_level": 6, "flush_interval_ms": 10}),
             (["--batch-size=100"], {"batch_size": 100})]
    cases += [([f"--background-threads={threads}"], {"background_threads": threads}) for threads in (1, 4, 8, 16)]
    if murmurext == "ON":
        version = murmur_version()
        cases += [(["--replication=on", f"--batch-size={batch}"], {"replication": "on", "murmur_version": version, "batch_size": batch}) for batch in (1, 100)]
        cases += [(["--ingest=log", f"--batch-size={batch}"], {"ingest": "log", "replication": "on", "connections": 2}) for batch in (1, 100)]
    else:
        run = subprocess.run([binary, "--replication=on"], capture_output=True, text=True, timeout=5)
        assert run.returncode == 2 and "SQLITE_EXPRESS_ENABLE_MURMUR_EXT=ON" in run.stderr
        run = subprocess.run([binary, "--ingest=log"], capture_output=True, text=True, timeout=5)
        assert run.returncode == 2 and "SQLITE_EXPRESS_ENABLE_MURMUR_EXT=ON" in run.stderr
    for args, expected in cases:
        run = subprocess.run([binary, "--seconds=0.01", f"--cipher={cipher}", *args],
                             capture_output=True, text=True, timeout=30)
        assert run.returncode == 0, run.stdout + run.stderr
        validate(run.stdout, 0.01, expected)
    if aegis != "ON":
        run = subprocess.run([binary, "--seconds=0.01"], capture_output=True, text=True, timeout=5)
        assert run.returncode == 2 and "SQLITE_EXPRESS_ENABLE_AEGIS256=ON" in run.stderr
    invalid = ["--ingest=invalid", "--ingest=row", "--ingest=blob", "--background-threads=0", "--background-threads=257", "--background-threads=-1", "--background-threads=invalid","--replication=invalid","--seconds=0", "--seconds=-1", "--seconds=nan", "--seconds=inf", "--seconds=1junk",
               "--seconds=3601", "--unknown", "--flush-interval-ms=0", "--flush-interval-ms=4294967296",
               "--cache-pages=0", "--cache-pages=-1", "--cache-pages=18446744073709551616",
               "--page-size=4097", "--page-size=256", "--cipher=unknown", "--compression=zstd0",
               "--compression=zstd23", "--compression=zstd3junk", "--compression=unknown",
               "--sqlite-cache-kib=-1", "--sqlite-cache-kib=2147483648", "--sqlite-cache-kib=1junk",
               "--batch-size=0", "--batch-size=-1", "--batch-size=1000001", "--batch-size=100junk"]
    for arg in invalid:
        bad = subprocess.run([binary, arg], capture_output=True, text=True, timeout=5)
        assert bad.returncode == 2 and "BENCH_ERROR," in bad.stderr, arg
    help_run = subprocess.run([binary, "--help"], capture_output=True, text=True, timeout=5)
    assert help_run.returncode == 0
    for setting in ("--seconds=60", "--flush-interval-ms=100", "--cache-pages=16384", "--sqlite-cache-kib=1048576", "--compression=zstd3", "--cipher=aegis256"):
        assert setting in help_run.stdout
    print("Benchmark defaults, overrides, compression and reporting checks passed.")


if __name__ == "__main__":
    main()
