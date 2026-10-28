#!/usr/bin/env python3
"""Headless ingestion (merge) benchmark for cr-sqlite.

Repeatable extraction of py/perf/perf.ipynb cell 20 (merge benchmark) plus
its dependencies (cell 2 connect/random_str, cell 4 create_crr_tables,
cell 8 time_x). No jupyter, no matplotlib, no manual cell runs.

Workload per trial: read one tx batch of changes from a source CRR db and
ingest them row-by-row into a destination CRR db via
``INSERT INTO crsql_changes VALUES (?, ...)`` inside one transaction --
the same ingest pattern as perf.ipynb cell 20 and
py/correctness/tests/test_sync.py::sync_left_to_right.

Phases (each timed per trial with time.perf_counter_ns):
  1. clean merge  -- ingest inserts into a fresh peer (no row conflicts)
  2. clean control -- equivalent row count via INSERT OR REPLACE into a
     plain (non-CRR) merge_control table, for the CRR-vs-vanilla ratio
  3. dirty merge  -- after modify_rows() updates every source row, ingest
     again (exercises the update/tie path on the destination)
  4. dirty control -- control re-run for the dirty phase

This completes the truncated tail of notebook cell 20, which ends at the
bare assignment ``perf_insert = time_x`` without running the dirty half.

Prerequisite (same as the notebook):
  cd core && make loadable   # builds core/dist/crsqlite.{so,dll,dylib}

Usage:
  cd py/perf && python3 ingest_bench.py [--trials N] [--batch-size M]
      [--extension PATH] [--seed S] [--range-scan] [--json]
      [--ingest rowby|bulk|blob|both|all] [--apply-only]

  --range-scan switches the merge read from the notebook-faithful
  per-batch ``WHERE db_version = ?`` to the canonical sync scan
  ``WHERE db_version > ? ORDER BY db_version, seq`` (one full ingest
  per trial over a fresh destination). Default keeps notebook behavior.

  --ingest selects the apply path(s): row-by-row INSERT (legacy),
  bulk stage+commit, packed blob, both, or all three (with
  rowby-vs-bulk/blob ratios). Default both.

  --apply-only prefetches every batch untimed and times dest-side
  apply (+commit) alone: the ingestion number, excluding extraction
  (a separate operation, usually on another node) and blob packing
  (sender-side; reported separately as pack_ms_per_batch).

Exit status: 0 on success; 2 if the extension cannot be loaded.
"""

import argparse
import json
import os
import random
import sqlite3
import string
import sys
import time

DEFAULT_TRIALS = 10
DEFAULT_BATCH_SIZE = 100
# Notebook defaults (trials=100, batch_size=1000) take a long time headless;
# pass --trials 100 --batch-size 1000 to reproduce notebook scale.


def random_str(rng):
    return "".join(rng.choices(string.ascii_lowercase, k=5))


def find_extension(explicit):
    if explicit:
        return explicit
    here = os.path.dirname(os.path.abspath(__file__))
    for cand in (
        os.path.join(here, "..", "..", "core", "dist", "crsqlite"),
        os.path.join(here, "..", "..", "core", "dist", "crsqlite.so"),
    ):
        if os.path.exists(cand) or os.path.exists(cand + ".so"):
            return cand if cand.endswith("crsqlite") else cand[: -len(".so")] + "crsqlite"
    return os.path.join(here, "..", "..", "core", "dist", "crsqlite")


def connect_memory(extension):
    c = sqlite3.connect(":memory:")
    c.execute("PRAGMA journal_mode = WAL")
    c.execute("PRAGMA synchronous = NORMAL")
    c.enable_load_extension(True)
    c.execute("SELECT load_extension(?)", (extension,))
    return c


def create_crr_tables(c):
    c.execute("CREATE TABLE user (id primary key not null, name)")
    c.execute("CREATE TABLE deck (id primary key not null, owner_id, title)")
    c.execute('CREATE TABLE slide (id primary key not null, deck_id, "order")')
    c.execute("CREATE TABLE component (id primary key not null, type, slide_id, content)")
    c.execute("SELECT crsql_as_crr('user')")
    c.execute("SELECT crsql_as_crr('deck')")
    c.execute("SELECT crsql_as_crr('slide')")
    c.execute("SELECT crsql_as_crr('component')")


def create_merge_control(c):
    c.execute("CREATE TABLE merge_control (id PRIMARY KEY, tx INTEGER, content TEXT)")
    c.execute("CREATE INDEX merge_control_tx ON merge_control (tx)")
    c.commit()


def setup_merge_test_db(extension, trials, batch_size, rng):
    """Build a source db with `trials` committed tx batches of `batch_size` rows.

    Each batch is 1 tx so changes can be pulled per batch via db_version,
    exactly like perf.ipynb cell 20 setup_merge_test_db().
    """
    c = connect_memory(extension)
    create_crr_tables(c)
    create_merge_control(c)

    for t in range(trials):
        offset = t * batch_size
        for i in range(batch_size):
            c.execute("INSERT INTO user VALUES (?, ?)", (i + offset, random_str(rng)))
            c.execute(
                "INSERT INTO deck VALUES (?, ?, ?)",
                (i + offset, i + offset, random_str(rng)),
            )
            c.execute(
                "INSERT INTO slide VALUES (?, ?, ?)", (i + offset, i + offset, i)
            )
            c.execute(
                "INSERT INTO component VALUES (?, ?, ?, ?)",
                (i + offset, "text", i + offset, random_str(rng)),
            )

            j = i * 4
            c.execute(
                "INSERT INTO merge_control VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?)",
                (
                    j + offset * 4, t, random_str(rng),
                    j + 1 + offset * 4, t, random_str(rng),
                    j + 2 + offset * 4, t, random_str(rng),
                    j + 3 + offset * 4, t, random_str(rng),
                ),
            )
        c.commit()  # each batch is 1 tx so we can get changes per batch for merge

    return c


def modify_rows(c, trials, batch_size, rng):
    """UPDATE every row (one tx per batch), like perf.ipynb cell 20."""
    for t in range(trials):
        offset = t * batch_size
        for i in range(batch_size):
            c.execute(
                "UPDATE user SET name = ? WHERE id = ?",
                (random_str(rng), i + offset),
            )
            c.execute(
                "UPDATE deck SET title = ? WHERE id = ?",
                (random_str(rng), i + offset),
            )
            c.execute(
                "UPDATE component SET content = ? WHERE id = ?",
                (random_str(rng), i + offset),
            )
        c.commit()


def fetch_batch(merge_from, db_version):
    """Untimed-in-apply-only-mode extraction of one batch."""
    return merge_from.execute(
        "SELECT * FROM crsql_changes WHERE db_version = ?", (db_version,)
    ).fetchall()


def apply_batch_rowby(merge_to, changes):
    """Dest-side only: apply prefetched changes one INSERT at a time."""
    for change in changes:
        merge_to.execute(
            "INSERT INTO crsql_changes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", change
        )
    merge_to.commit()


def apply_batch_bulk(merge_to, changes):
    """Dest-side only: stage prefetched changes, then grouped-apply."""
    merge_to.execute("SELECT crsql_ingest_begin()")
    merge_to.executemany(
        "INSERT INTO crsql_staging VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", changes
    )
    merge_to.execute("SELECT crsql_ingest_commit()").fetchone()
    merge_to.commit()


def apply_batch_blob(merge_to, blob):
    """Dest-side only: apply one pre-packed blob in a single call."""
    merge_to.execute("SELECT crsql_ingest_blob(?)", (blob,)).fetchone()
    merge_to.commit()


def merge_batch(merge_from, merge_to, trial, batch_size):
    # Notebook-faithful per-batch equality read: db_version = trial + 1.
    changes = merge_from.execute(
        "SELECT * FROM crsql_changes WHERE db_version = ?", (trial + 1,)
    )
    for change in changes:
        merge_to.execute(
            "INSERT INTO crsql_changes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", change
        )
    merge_to.commit()


def merge_batch_bulk(merge_from, merge_to, trial, batch_size):
    """Same batch via bulk ingest: stage then grouped-apply in one call."""
    changes = fetch_batch(merge_from, trial + 1)
    apply_batch_bulk(merge_to, changes)


def _bulk_packer():
    """Import the committed blob packer (lives in the correctness pkg)."""
    import os
    here = os.path.dirname(os.path.abspath(__file__))
    pkg = os.path.normpath(os.path.join(here, "..", "correctness", "src"))
    if pkg not in sys.path:
        sys.path.insert(0, pkg)
    from crsql_correctness.bulk import pack_changes
    return pack_changes


def merge_batch_blob(merge_from, merge_to, trial, batch_size):
    """Same batch via packed-blob ingest: pack then one grouped-apply call."""
    pack_changes = _bulk_packer()
    changes = fetch_batch(merge_from, trial + 1)
    blob = pack_changes(changes)
    apply_batch_blob(merge_to, blob)


def merge_range_scan(merge_from, extension, trials, batch_size, rng):
    """Canonical sync-scan ingest: fresh dest, one full WHERE db_version>? merge.

    Returns elapsed ms. Destination is rebuilt per call so every trial
    ingests the same full changeset into a clean peer.
    """
    merge_to = connect_memory(extension)
    create_crr_tables(merge_to)
    start = time.perf_counter_ns()
    changes = merge_from.execute(
        "SELECT * FROM crsql_changes WHERE db_version > ? ORDER BY db_version, seq",
        (0,),
    )
    for change in changes:
        merge_to.execute(
            "INSERT INTO crsql_changes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", change
        )
    merge_to.commit()
    end = time.perf_counter_ns()
    merge_to.close()
    return (end - start) / 1000000


def control_batch(merge_from, merge_to, trial):
    rows = merge_from.execute(
        "SELECT * FROM merge_control WHERE tx = ?", (trial,)
    )
    apply_control(merge_to, rows)


def apply_control(merge_to, rows):
    """Dest-side only: plain INSERT OR REPLACE loop + commit."""
    for row in rows:
        merge_to.execute("INSERT OR REPLACE INTO merge_control VALUES (?, ?, ?)", row)
    merge_to.commit()


def time_clean_merge(merge_from, merge_to_factory, trials, batch_size,
                     merge_fn=merge_batch):
    timings = []
    merge_to = merge_to_factory()
    for trial in range(trials):
        start = time.perf_counter_ns()
        merge_fn(merge_from, merge_to, trial, batch_size)
        end = time.perf_counter_ns()
        timings.append((end - start) / 1000000)
    return merge_to, timings


def time_control(merge_from, merge_to, trials):
    timings = []
    for trial in range(trials):
        start = time.perf_counter_ns()
        control_batch(merge_from, merge_to, trial)
        end = time.perf_counter_ns()
        timings.append((end - start) / 1000000)
    return timings


def summarize(timings):
    s = sorted(timings)
    n = len(s)
    med = s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2 if n else 0.0
    return {
        "trials": n,
        "total_ms": sum(s),
        "mean_ms": (sum(s) / n) if n else 0.0,
        "median_ms": med,
        "min_ms": s[0] if n else 0.0,
        "max_ms": s[-1] if n else 0.0,
    }


def main(argv=None):
    ap = argparse.ArgumentParser(description="Headless cr-sqlite ingestion (merge) benchmark")
    ap.add_argument("--trials", type=int, default=DEFAULT_TRIALS)
    ap.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    ap.add_argument("--extension", default=None,
                    help="path to crsqlite extension (default: ../../core/dist/crsqlite)")
    ap.add_argument("--seed", type=int, default=42,
                    help="RNG seed for repeatable payloads (default: 42)")
    ap.add_argument("--range-scan", action="store_true",
                    help="use canonical WHERE db_version>? ORDER BY scan instead of per-batch equality")
    ap.add_argument("--ingest", choices=("rowby", "bulk", "blob", "both", "all"), default="both",
                    help="ingest path(s) to time: row-by-row INSERT, bulk stage+commit, "
                         "packed blob, both rowby+bulk (default), or all three")
    ap.add_argument("--json", action="store_true", help="emit machine-readable JSON only")
    ap.add_argument("--apply-only", action="store_true",
                    help="ingestion-only: prefetch all batches untimed, then time "
                         "dest-side apply (excludes extraction + blob packing)")
    args = ap.parse_args(argv)

    extension = find_extension(args.extension)
    try:
        probe = connect_memory(extension)
        probe.close()
    except Exception as e:
        print(
            "error: cannot load crsqlite extension at %r (%s)\n"
            "build it first: cd core && make loadable" % (extension, e),
            file=sys.stderr,
        )
        return 2

    rng = random.Random(args.seed)
    merge_from = setup_merge_test_db(extension, args.trials, args.batch_size, rng)

    n_changes = merge_from.execute(
        "SELECT COUNT(*) FROM crsql_changes"
    ).fetchone()[0]

    pack_ms_per_batch = None
    if args.range_scan:
        merge_timings = [
            merge_range_scan(merge_from, extension, args.trials, args.batch_size, rng)
            for _ in range(args.trials)
        ]
        # Control under range-scan: full-table copy per trial.
        control_timings = []
        for _ in range(args.trials):
            ctl_to = connect_memory(extension)
            create_merge_control(ctl_to)
            start = time.perf_counter_ns()
            for row in merge_from.execute("SELECT * FROM merge_control"):
                ctl_to.execute(
                    "INSERT OR REPLACE INTO merge_control VALUES (?, ?, ?)", row
                )
            ctl_to.commit()
            end = time.perf_counter_ns()
            control_timings.append((end - start) / 1000000)
            ctl_to.close()
        phases = {
            "merge": merge_timings,
            "control": control_timings,
        }
    else:
        merge_to = setup_merge_test_db(extension, args.trials, args.batch_size,
                                       random.Random(args.seed + 1))

        def fresh_dest():
            # Fresh destination peer per phase so clean and dirty merges
            # each ingest into a peer holding the same base rows.
            d = connect_memory(extension)
            create_crr_tables(d)
            create_merge_control(d)
            # Copy base rows (all batches) so the dest starts identical to
            # the notebook's setup_merge_test_db() destination.
            for t in range(args.trials):
                off = t * args.batch_size
                for i in range(args.batch_size):
                    d.execute("INSERT INTO user VALUES (?, ?)", (i + off, "base"))
                    d.execute("INSERT INTO deck VALUES (?, ?, ?)", (i + off, i + off, "base"))
                    d.execute("INSERT INTO slide VALUES (?, ?, ?)", (i + off, i + off, i))
                    d.execute("INSERT INTO component VALUES (?, ?, ?, ?)",
                              (i + off, "text", i + off, "base"))
                d.commit()
            return d

        want_rowby = args.ingest in ("rowby", "both", "all")
        want_bulk = args.ingest in ("bulk", "both", "all")
        want_blob = args.ingest in ("blob", "all")
        phases = {}

        def time_apply(dest, items, apply_fn):
            timings = []
            for it in items:
                start = time.perf_counter_ns()
                apply_fn(dest, it)
                end = time.perf_counter_ns()
                timings.append((end - start) / 1000000)
            return timings

        if args.apply_only:
            # Ingestion-only: extraction + packing happen untimed up front;
            # only dest-side apply (+commit) is measured.
            clean_batches = [fetch_batch(merge_from, t + 1)
                             for t in range(args.trials)]
            clean_control_rows = [merge_from.execute(
                "SELECT * FROM merge_control WHERE tx = ?", (t,)).fetchall()
                for t in range(args.trials)]
            clean_blobs = None
            if want_blob:
                pack_changes = _bulk_packer()
                t0 = time.perf_counter_ns()
                clean_blobs = [pack_changes(b) for b in clean_batches]
                pack_ms_per_batch = (
                    (time.perf_counter_ns() - t0) / 1000000 / args.trials)

            if want_rowby:
                dest_clean = fresh_dest()
                phases["clean_merge"] = time_apply(
                    dest_clean, clean_batches, apply_batch_rowby)
                phases["clean_control"] = time_apply(
                    dest_clean, clean_control_rows, apply_control)
                dest_clean.close()

            if want_bulk:
                dest_bulk = fresh_dest()
                phases["clean_bulk"] = time_apply(
                    dest_bulk, clean_batches, apply_batch_bulk)
                dest_bulk.close()

            if want_blob:
                dest_blob = fresh_dest()
                phases["clean_blob"] = time_apply(
                    dest_blob, clean_blobs, apply_batch_blob)
                dest_blob.close()
        else:
            if want_rowby:
                dest_clean = fresh_dest()
                _, clean_merge = time_clean_merge(merge_from, lambda: dest_clean,
                                                  args.trials, args.batch_size)
                clean_control = time_control(merge_from, dest_clean, args.trials)
                phases["clean_merge"] = clean_merge
                phases["clean_control"] = clean_control
                dest_clean.close()

            if want_bulk:
                dest_bulk = fresh_dest()
                _, clean_bulk = time_clean_merge(merge_from, lambda: dest_bulk,
                                                 args.trials, args.batch_size,
                                                 merge_fn=merge_batch_bulk)
                phases["clean_bulk"] = clean_bulk
                dest_bulk.close()

            if want_blob:
                dest_blob = fresh_dest()
                _, clean_blob = time_clean_merge(merge_from, lambda: dest_blob,
                                                 args.trials, args.batch_size,
                                                 merge_fn=merge_batch_blob)
                phases["clean_blob"] = clean_blob
                dest_blob.close()

        # Dirty half (truncated in the notebook): update all source rows,
        # then merge the update batches. db_version continues after the
        # insert batches, so trial t reads db_version = trials + t + 1.
        modify_rows(merge_from, args.trials, args.batch_size, rng)

        def time_dirty(merge_fn):
            dest = fresh_dest()
            # Bring the dest up to date with the insert batches first so
            # the timed ingest measures update/tie application, not inserts.
            for t in range(args.trials):
                merge_fn(merge_from, dest, t, args.batch_size)
            pack_changes = _bulk_packer() if merge_fn is merge_batch_blob else None
            timings = []
            for t in range(args.trials):
                start = time.perf_counter_ns()
                if merge_fn is merge_batch_bulk:
                    changes = merge_from.execute(
                        "SELECT * FROM crsql_changes WHERE db_version = ?",
                        (args.trials + t + 1,),
                    ).fetchall()
                    dest.execute("SELECT crsql_ingest_begin()")
                    dest.executemany(
                        "INSERT INTO crsql_staging VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        changes,
                    )
                    dest.execute("SELECT crsql_ingest_commit()").fetchone()
                    dest.commit()
                elif merge_fn is merge_batch_blob:
                    changes = merge_from.execute(
                        "SELECT * FROM crsql_changes WHERE db_version = ?",
                        (args.trials + t + 1,),
                    ).fetchall()
                    blob = pack_changes(changes)
                    dest.execute("SELECT crsql_ingest_blob(?)", (blob,)).fetchone()
                    dest.commit()
                else:
                    changes = merge_from.execute(
                        "SELECT * FROM crsql_changes WHERE db_version = ?",
                        (args.trials + t + 1,),
                    )
                    for change in changes:
                        dest.execute(
                            "INSERT INTO crsql_changes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            change,
                        )
                    dest.commit()
                end = time.perf_counter_ns()
                timings.append((end - start) / 1000000)
            return dest, timings

        if args.apply_only:
            dirty_batches = [fetch_batch(merge_from, args.trials + t + 1)
                             for t in range(args.trials)]
            dirty_blobs = None
            if want_blob:
                pack_changes = _bulk_packer()
                t0 = time.perf_counter_ns()
                dirty_blobs = [pack_changes(b) for b in dirty_batches]
                pack_ms_per_batch = (
                    (pack_ms_per_batch or 0)
                    + (time.perf_counter_ns() - t0) / 1000000 / args.trials) / 2

            def time_dirty_apply(items, apply_fn, warm_items, warm_fn):
                dest = fresh_dest()
                for it in warm_items:
                    warm_fn(dest, it)
                return dest, time_apply(dest, items, apply_fn)

            if want_rowby:
                dest_dirty, dirty_merge = time_dirty_apply(
                    dirty_batches, apply_batch_rowby,
                    clean_batches, apply_batch_rowby)
                phases["dirty_merge"] = dirty_merge
                dirty_control = time_apply(
                    dest_dirty, clean_control_rows, apply_control)
                phases["dirty_control"] = dirty_control
                dest_dirty.close()

            if want_bulk:
                dest_dirty_bulk, dirty_bulk = time_dirty_apply(
                    dirty_batches, apply_batch_bulk,
                    clean_batches, apply_batch_bulk)
                phases["dirty_bulk"] = dirty_bulk
                dest_dirty_bulk.close()

            if want_blob:
                dest_dirty_blob, dirty_blob = time_dirty_apply(
                    dirty_blobs, apply_batch_blob,
                    clean_blobs, apply_batch_blob)
                phases["dirty_blob"] = dirty_blob
                dest_dirty_blob.close()
        else:
            if want_rowby:
                dest_dirty, dirty_merge = time_dirty(merge_batch)
                dirty_control = time_control(merge_from, dest_dirty, args.trials)
                phases["dirty_merge"] = dirty_merge
                phases["dirty_control"] = dirty_control
                dest_dirty.close()

            if want_bulk:
                dest_dirty_bulk, dirty_bulk = time_dirty(merge_batch_bulk)
                phases["dirty_bulk"] = dirty_bulk
                dest_dirty_bulk.close()

            if want_blob:
                dest_dirty_blob, dirty_blob = time_dirty(merge_batch_blob)
                phases["dirty_blob"] = dirty_blob
                dest_dirty_blob.close()

        merge_to.close()

    merge_from.close()

    if args.range_scan:
        mode = "range_scan"
    elif args.apply_only:
        mode = "apply_only"
    else:
        mode = "per_batch_notebook"
    result = {
        "trials": args.trials,
        "batch_size": args.batch_size,
        "seed": args.seed,
        "mode": mode,
        "total_changes": n_changes,
        "pack_ms_per_batch": pack_ms_per_batch,
        "phases": {k: {"timings_ms": v, "summary": summarize(v)}
                   for k, v in phases.items()},
    }
    # CRR-vs-control ratios on means.
    if args.range_scan:
        m = result["phases"]["merge"]["summary"]["mean_ms"]
        c = result["phases"]["control"]["summary"]["mean_ms"]
        result["ratios"] = {"merge_vs_control_mean": (m / c) if c else None}
    else:
        result["ratios"] = {}
        ph = result["phases"]
        if "clean_merge" in ph and "clean_control" in ph:
            cm = ph["clean_merge"]["summary"]["mean_ms"]
            cc = ph["clean_control"]["summary"]["mean_ms"]
            result["ratios"]["clean_merge_vs_control_mean"] = (cm / cc) if cc else None
        if "dirty_merge" in ph and "dirty_control" in ph:
            dm = ph["dirty_merge"]["summary"]["mean_ms"]
            dc = ph["dirty_control"]["summary"]["mean_ms"]
            result["ratios"]["dirty_merge_vs_control_mean"] = (dm / dc) if dc else None
        if "clean_merge" in ph and "clean_bulk" in ph:
            cm = ph["clean_merge"]["summary"]["mean_ms"]
            cb = ph["clean_bulk"]["summary"]["mean_ms"]
            result["ratios"]["clean_rowby_vs_bulk_mean"] = (cm / cb) if cb else None
        if "dirty_merge" in ph and "dirty_bulk" in ph:
            dm = ph["dirty_merge"]["summary"]["mean_ms"]
            db = ph["dirty_bulk"]["summary"]["mean_ms"]
            result["ratios"]["dirty_rowby_vs_bulk_mean"] = (dm / db) if db else None
        if "clean_merge" in ph and "clean_blob" in ph:
            cm = ph["clean_merge"]["summary"]["mean_ms"]
            cb = ph["clean_blob"]["summary"]["mean_ms"]
            result["ratios"]["clean_rowby_vs_blob_mean"] = (cm / cb) if cb else None
        if "dirty_merge" in ph and "dirty_blob" in ph:
            dm = ph["dirty_merge"]["summary"]["mean_ms"]
            db = ph["dirty_blob"]["summary"]["mean_ms"]
            result["ratios"]["dirty_rowby_vs_blob_mean"] = (dm / db) if db else None

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print("ingest_bench: trials=%d batch_size=%d seed=%d mode=%s total_changes=%d"
              % (args.trials, args.batch_size, args.seed, result["mode"], n_changes))
        if result["pack_ms_per_batch"] is not None:
            print("blob pack (sender-side, excluded): %.2fms/batch"
                  % result["pack_ms_per_batch"])
        for name, phase in result["phases"].items():
            s = phase["summary"]
            print("%-13s mean=%8.2fms median=%8.2fms min=%8.2fms max=%8.2fms total=%10.2fms"
                  % (name, s["mean_ms"], s["median_ms"], s["min_ms"], s["max_ms"], s["total_ms"]))
        for k, v in result["ratios"].items():
            print("%s: %.2fx" % (k, v) if v is not None else "%s: n/a" % k)
    return 0


if __name__ == "__main__":
    sys.exit(main())
