jupyter-lab from this dir

Headless ingestion benchmark (no jupyter/matplotlib):
  cd core && make loadable   # builds core/dist/crsqlite
  cd py/perf && python3 ingest_bench.py [--trials N] [--batch-size M] [--seed S] [--range-scan] [--ingest rowby|bulk|blob|both|all] [--apply-only] [--json]
See ingest_bench.py docstring. Re-extracted from perf.ipynb cell 20 (+ cells 2,4,8).

Modes:
- default per-batch end-to-end (extract + apply, notebook-faithful).
- `--apply-only`: prefetch all batches untimed, then time dest-side apply
  only. This is the ingestion number: extraction is a separate operation
  (usually on another node) and neither path changes it.
- `--ingest blob|bulk|all`: compare the grouped bulk/blob apply paths
  against row-by-row `INSERT INTO crsql_changes`.

Bulk ingest API (additive; row-by-row contract unchanged):
- `SELECT crsql_ingest_begin()` then `INSERT INTO crsql_staging` (same
  9-column order as crsql_changes) then `SELECT crsql_ingest_commit()`
  returns the applied count. Staging is cleared on success only.
- `SELECT crsql_ingest_blob(?)` applies a packed v1 blob (`CRSB` magic)
  in one call, no staging I/O. Pack with
  `py/correctness/src/crsql_correctness/bulk.py::pack_changes`
  (accepts rows verbatim from `SELECT * FROM crsql_changes`).
- Both group by (table, pk): one key resolve + one clock scan + one
  multi-column base upsert per row-run. Version numbering, sentinel /
  resurrect / delete handling, tie-breaks, rows_impacted, and atomicity
  (whole-commit savepoint) match row-by-row exactly; NULL sites/versions
  coerce as xUpdate does (site 0, versions 0).
- Equivalence is covered by py/correctness/tests/test_bulk_ingest.py
  (arrival/reverse/shuffle/dupe orders, composite PKs, defaults, 12-col
  wide table, pk-only table, NULL sites, error paths).

Measured results (this machine, 4 cores, trials=10 batch=100):
- Narrow bench schema (1-3 cols/table, ~2 changes/row), apply-only:
  blob 1.38x clean (+38%), 1.28x dirty (+28%) vs row-by-row.
  Staging bulk is ~1.0x (TEMP round-trip eats the grouping win); it
  remains as the SQL-composable path, blob is the fast path.
- Wide rows (10 cols/row, 1000 changes): blob 1.85x (+85%).
- Transparent row-by-row micro-opts (direct sync-bit flag, site-ordinal
  last-value cache, once-per-tx db version, winner-clock upsert with
  explicit NULL->0 site mapping, narrow-table clock-scan fold, no-op
  update short-circuit): ~6%, kept in all paths.
- End-to-end (extract+apply) shows less than apply-only because the
  ~5.5ms/800-change extraction is unchanged by design.

Optimization disposition (from the investigate list):
- Implemented + measured: site-ordinal cache, pk/key grouping per row-run,
  clock/base write consolidation per row-run, sync-bit SQL removal
  (direct flag; grouped scope in bulk), once-per-tx data_version for
  writes, no-op update short-circuit, winner-clock upsert, native
  batch-apply API (staging + blob).
- Investigated, not pursued (profile said no): change-query plan cache
  (prepare is ~0.2us/change amortized), vtab planner estimates (no
  evidence of misplanning on sync filters), composite (db_version,seq)
  index (write-cost risk, unmeasured benefit), join-based value fetch
  (same probe count, more complexity).
- Read-path (extraction) union-query work intentionally out of scope:
  it is not ingestion, and the goal was met on the apply side.
