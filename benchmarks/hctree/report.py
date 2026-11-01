"""Validate HC-tree/Murmur benchmark artifacts before publishing throughput.

replication=on in this benchmark is local cr-sqlite change tracking,
not network/QUIC replication. Do not accept partial logs or stale files.
"""
import csv
import math
from pathlib import Path
import sys


def one_line(lines, prefix, label):
    found = [line for line in lines if line.startswith(prefix)]
    if len(found) != 1:
        raise ValueError(f"{label}: expected exactly one {prefix!r}, found {len(found)}")
    return found[0][len(prefix):]


def number(result, key, label, integer=False):
    try:
        value = int(result[key]) if integer else float(result[key])
    except (ValueError, KeyError) as error:
        raise ValueError(f"{label}: invalid {key}") from error
    if not integer and not math.isfinite(value):
        raise ValueError(f"{label}: nonfinite {key}")
    return value


def require(condition, label, reason):
    if not condition:
        raise ValueError(f"{label}: {reason}")


def completed_lines(path):
    label = path.name
    data = path.read_text()
    require(data.endswith("\n"), label, "truncated log (missing newline)")
    lines = data.splitlines()
    require((len(lines) >= 2 and lines[-2:] == ["BENCH_DONE", "BENCH_SUITE_DONE"])
            or (bool(lines) and lines[-1] == "BENCH_DONE"), label,
            "missing terminal BENCH_DONE (incomplete run)")
    require(lines.count("BENCH_DONE") == 1, label, "duplicate completion marker")
    require(not any(line.startswith("BENCH_ERROR,") for line in lines), label,
            "BENCH_ERROR present despite completion marker")
    return lines


def validate_counts(result, label, batch):
    writes = number(result, "writes", label, True)
    reads = number(result, "reads", label, True)
    ops = number(result, "operations", label, True)
    tx = number(result, "transactions", label, True)
    seconds = number(result, "seconds", label)
    throughput = number(result, "ops_per_second", label)
    require(writes > 0 and reads == 4 * writes and ops == 5 * writes,
            label, "mixed workload counts inconsistent")
    require(tx > 0 and tx * batch == writes, label,
            "transaction/batch count inconsistent")
    require(seconds > 0 and throughput > 0 and
            math.isclose(throughput, ops / seconds, rel_tol=1e-4),
            label, "throughput does not match operations / seconds")
    require(number(result, "mean_write_transaction_ms", label) >= 0,
            label, "negative transaction time")
    require(number(result, "disk_bytes", label, True) > 0,
            label, "no persisted disk data")


def murmur_result(out, replication, batch, prefix="murmur"):
    label = f"{prefix}-{replication}-{batch}.txt"
    lines = completed_lines(out / label)
    engine = "hctree" if prefix == "integrated-hctree" else "standard"
    require(one_line(lines, "BENCH_ENGINE,", label) == engine,
            label, f"engine is not {engine}")
    header = next(csv.reader([one_line(lines, "BENCH_HEADER,", label)]))
    values = next(csv.reader([one_line(lines, "BENCH_CSV,", label)]))
    require(len(header) == len(values) and len(set(header)) == len(header),
            label, "CSV columns invalid")
    result = dict(zip(header, values))
    require(result.get("suite") == "single" and result.get("backend") == "murmur"
            and result.get("replication") == replication
            and number(result, "batch_size", label, True) == batch,
            label, "case configuration mismatch")
    config = one_line(lines, "BENCH_CONFIG,", label)
    settings = dict(part.split("=", 1) for part in config.split(",") if "=" in part)
    require(settings.get("replication") == replication and
            settings.get("batch_size") == str(batch) and
            settings.get("encryption") == "on" and
            settings.get("vfs_block_size") == "4096" and
            settings.get("ingest") == "local" and
            settings.get("rocksdb_sync") == "1",
            label, "expected Murmur VFS/benchmark settings missing")
    validate_counts(result, label, batch)
    tracked = number(result, "tracked_changes", label, True)
    require(tracked == (number(result, "writes", label, True)
                        if replication == "on" else 0),
            label, "local cr-sqlite change count mismatch")
    require(number(result, "logical_database_bytes", label, True) > 0,
            label, "empty logical database")
    require(number(result, "rocksdb_files_bytes", label, True) > 0,
            label, "missing RocksDB files")
    return result


def hctree_result(out, batch):
    label = f"hctree-off-{batch}.txt"
    lines = completed_lines(out / label)
    result = {}
    for line in lines[:-1]:
        if line == "BENCH_DONE":
            continue
        if "," not in line:
            raise ValueError(f"{label}: invalid output line {line!r}")
        key, value = line.split(",", 1)
        if key in result:
            raise ValueError(f"{label}: duplicate key {key}")
        result[key] = value
    require(result.get("backend") == "hctree" and
            result.get("replication") == "off" and
            number(result, "batch_size", label, True) == batch,
            label, "standalone case configuration mismatch")
    require(number(result, "journal_rows", label, True) == 0,
            label, "native HC-tree replication unexpectedly enabled")
    require(bool(result.get("source_id")), label, "missing pinned engine source id")
    validate_counts(result, label, batch)
    return result


def report(out):
    # Validate every case before publishing any summary; reject partial runs.
    rows = []
    pairs = []
    for batch in (1, 100):
        hctree = hctree_result(out, batch)
        hctree_ops = float(hctree["ops_per_second"])
        rows.append(("off", batch, "hctree", hctree_ops, hctree["writes"],
                     hctree["mean_write_transaction_ms"], 0, "storage-only"))
        for repl in ("off", "on"):
            murmur = murmur_result(out, repl, batch)
            murmur_ops = float(murmur["ops_per_second"])
            rows.append((repl, batch, "murmur", murmur_ops, murmur["writes"],
                         murmur["mean_write_transaction_ms"], murmur["tracked_changes"],
                         "local cr-sqlite change tracking" if repl == "on" else "storage-only"))
            pairs.append(f"BENCH_REFERENCE,{repl},{batch},murmur={murmur_ops:.2f},standalone_hctree={hctree_ops:.2f}")
    for batch in (1, 100):
        for repl in ("off", "on"):
            baseline = murmur_result(out, repl, batch)
            integrated = murmur_result(out, repl, batch, "integrated-hctree")
            a, b = float(baseline["ops_per_second"]), float(integrated["ops_per_second"])
            rows.append((repl, batch, "murmur+hctree", b, integrated["writes"],
                         integrated["mean_write_transaction_ms"], integrated["tracked_changes"],
                         "HC-tree in Murmur; local CRR tracking " + repl))
            pairs.append(f"BENCH_INTEGRATED_PAIR,{repl},{batch},{a:.2f},{b:.2f},{b/a:.3f}")
    import io
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["replication", "batch_size", "backend", "ops_per_second", "writes",
                     "mean_transaction_ms", "tracked_changes", "measurement_scope"])
    writer.writerows(rows)
    (out / "summary.csv").write_text(buffer.getvalue())
    for line in pairs:
        print(line)
    print("BENCHMARK_COMPLETE,10_cases,HCTree_native_replication_disabled,QUIC_not_measured")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: report.py RESULTS_DIR")
    report(Path(sys.argv[1]))
