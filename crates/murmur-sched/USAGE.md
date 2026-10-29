# Murmur scheduler import

This rlib reuses the archived Murmur fair writer scheduler. It grants one
exclusive writer ticket at a time across Local, Remote and Maintenance work.
Waiting Local/Remote classes share measured service time with bounded debt;
an idle class lends its capacity, and waiting Maintenance work receives a
reserved grant after the configured interactive grant count. FIFO order is
preserved within each class. A ticket releases on drop or explicit release.

This crate is not yet connected to sqlite-murmur's production writer path.
Importing it alone does not impose writer fairness or memory budgets.
See [INTEGRATION-PROPOSAL.md](INTEGRATION-PROPOSAL.md) for the proposed bridge.

The consuming SQLite integration configures options exclusively with PRAGMAs.
It must range-check persisted values, build a valid `SchedulerConfig`, call
`validate()`, then construct `WriterScheduler`. `new()` does not validate.
No configuration file, environment knob or public option-setting C API is
introduced by this import.

| Proposed PRAGMA | Default | Validation |
| --- | --- | --- |
| `murmur_writer_local_share` | 90 | 1 through 99 |
| `murmur_writer_remote_share` | 10 | Read-only, derived as 100 minus local |
| `murmur_writer_max_debt_ms` | 1000 | 1 through 60000 |
| `murmur_writer_maintenance_reserve_every` | 100 | 1 through 1000000 |

These PRAGMAs await downstream wiring. Proposed example after integration:

```sql
PRAGMA murmur_writer_local_share = 80;
PRAGMA murmur_writer_remote_share; -- 20
PRAGMA murmur_writer_max_debt_ms = 1000;
PRAGMA murmur_writer_maintenance_reserve_every = 100;
```

Writers must obtain admission before entering the serialized SQLite worker
or acquiring a SQLite lock. Never wait for a ticket inside that worker.
Move the granted ticket into the queued job so canceling a caller cannot
release admission while its job is still running. Release admission before
network I/O. `admit_timeout()` returns `Busy` for an expired queued request;
a concurrent grant wins the timeout race. `close()` wakes queued callers
with `Closed`, rejects future admissions, and lets an active ticket finish.

The imported scheduler uses blocking channels. Async callers must use
`spawn_blocking`; blocking admission must not run on a Tokio worker thread.
Snapshots expose grant counts, queued callers, waiting/service nanoseconds,
contended grants/service, normalized debt, and shares. Counts reset for a
new scheduler. This scheduler does not preempt transactions or enforce byte
budgets, and arbitrary native SQLite writes are not yet admitted through it.

After the lead merges the path dependency into murmur-replication and updates
its lock, run all four archived unit tests and downstream regression tests
from the repository root using the existing vendor tree without network access:

```sh
cargo test -p murmur-sched --offline --frozen \
  --manifest-path crates/murmur-replication/Cargo.toml \
  --target-dir /tmp/sqlite-murmur-sched-target \
  --config 'source.crates-io.replace-with="vendored-sources"' \
  --config "source.vendored-sources.directory=\"$PWD/crates/murmur-replication/vendor\""
```

There is no standalone Cargo.lock: the consuming crate owns dependency
resolution. Neither SQLite nor a system SQLite development package is
required for the scheduler. The
archive and shared vendor tree remain unchanged.
