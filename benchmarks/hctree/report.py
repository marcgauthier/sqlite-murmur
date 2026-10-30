"""Summarize six storage/replication cases without implying HCTree has replication enabled."""
import csv
import pathlib
import sys

out = pathlib.Path(sys.argv[1])
rows = []


def murmur_result(replication, batch):
    text = (out / f"murmur-{replication}-{batch}.txt").read_text()
    header = next(csv.reader([next(line for line in text.splitlines()
                                   if line.startswith("BENCH_HEADER,"))]))[1:]
    values = next(csv.reader([next(line for line in text.splitlines()
                                   if line.startswith("BENCH_CSV,"))]))[1:]
    result = dict(zip(header, values))
    if result["backend"] != "murmur" or result["replication"] != replication or int(result["batch_size"]) != batch:
        raise ValueError(f"Murmur case mismatch: {replication}/{batch}")
    if replication == "on" and int(result["tracked_changes"]) < int(result["writes"]):
        raise ValueError("Murmur replication metadata missing")
    if replication == "off" and int(result["tracked_changes"]) != 0:
        raise ValueError("Unexpected Murmur replication metadata")
    return result


def hctree_result(batch):
    result = dict(line.split(",", 1) for line in
                  (out / f"hctree-off-{batch}.txt").read_text().splitlines() if "," in line)
    if result["backend"] != "hctree" or result["replication"] != "off" or int(result["batch_size"]) != batch:
        raise ValueError(f"HCTree storage-only case mismatch: {batch}")
    if int(result["journal_rows"]) != 0:
        raise ValueError("HCTree native journal unexpectedly enabled")
    return result


for batch in (1, 100):
    hctree = hctree_result(batch)
    hctree_ops = float(hctree["ops_per_second"])
    rows.append(("off", batch, "hctree", hctree_ops, hctree["writes"],
                 hctree["mean_write_transaction_ms"], 0, "storage-only"))
    for repl in ("off", "on"):
        murmur = murmur_result(repl, batch)
        murmur_ops = float(murmur["ops_per_second"])
        rows.append((repl, batch, "murmur", murmur_ops, murmur["writes"],
                     murmur["mean_write_transaction_ms"], murmur["tracked_changes"],
                     "cr-sqlite change tracking" if repl == "on" else "storage-only"))
        if repl == "off":
            print(f"BENCH_PAIR,storage-only,{batch},{murmur_ops:.2f},{hctree_ops:.2f},{murmur_ops/hctree_ops:.3f}")
        else:
            print(f"BENCH_REFERENCE,murmur-with-crsqlite-vs-hctree-storage-only,{batch},"
                  f"{murmur_ops:.2f},{hctree_ops:.2f},{murmur_ops/hctree_ops:.3f}")

with (out / "summary.csv").open("w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["replication", "batch_size", "backend", "ops_per_second", "writes",
                "mean_transaction_ms", "tracked_changes", "measurement_scope"])
    w.writerows(rows)
print("BENCHMARK_COMPLETE,6_cases,HCTree_storage_only")
