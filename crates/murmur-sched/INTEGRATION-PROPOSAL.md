# Proposed sqlite-murmur integration

This proposal is for lead review. Only the isolated scheduler crate is
implemented here. Shared Rust, C++, CMake, vendor and documentation changes
below are intentionally not applied by this worker.

## Dependency and build changes

Add `murmur-sched = { path = "../murmur-sched" }` to murmur-replication.
Its two direct dependencies are exact archive pins: serde `=1.0.229` with
derive and thiserror `=2.0.21`. All nine registry dependencies already exist
at these versions/checksums in murmur-replication/vendor. `cargo vendor
--offline` therefore needs no additional crate sources. The parent lock
only needs a murmur-sched package entry and its dependency edge from
murmur-replication; reconcile the standalone lock with the parent lock.

Extend the CMake Rust source dependency list with murmur-sched/src/*.rs and
its Cargo.toml, and register its tests. Add this directory and provenance
to sources.json, regenerate the source checksum inventory using the existing
verifier workflow, and run verify.py. This crate creates no listener or
SQLite dependency, and requires no RUSTFLAGS SQLite shim.

## Worker admission bridge

Re-export the existing Rust types; do not introduce a second scheduler:

```rust,ignore
use murmur_sched::{SchedulerConfig, SchedulerSnapshot, Ticket, WriterClass,
                   WriterScheduler};

// On the existing Worker, shared across every connection to the same Store:
fn call_admitted<T: Send + 'static>(
    &self,
    class: WriterClass,
    timeout: Duration,
    f: impl FnOnce(&mut Store) -> Result<T> + Send + 'static,
) -> Result<T>;

async fn run_admitted<T: Send + 'static>(
    self: &Arc<Self>,
    class: WriterClass,
    timeout: Duration,
    f: impl FnOnce(&mut Store) -> Result<T> + Send + 'static,
) -> Result<T>;

fn scheduler_snapshot(&self) -> SchedulerSnapshot;
```

`call_admitted` obtains `scheduler.admit_timeout(class, timeout)` on the
calling blocking thread, then moves the ticket into the closure passed to
the existing `Worker::call`. The closure owns admission until its transaction
and persistence barrier complete, including error unwinding. Do not hold
the ticket only on the caller's stack: cancellation could otherwise release
it while a queued job still executes. Map Invalid/Busy/Closed structurally
to the approved typed-error equivalents. `run_admitted` invokes the entire
blocking admission-and-call operation in `spawn_blocking`.

Never acquire admission inside the serialized worker, hold its mutex while
waiting for a job reply, or hold an engine peers/view/control mutex during
admission. A worker waiting on a ticket whose owner is queued behind it is
a deadlock. Existing read-only `Worker::call/run` remains unscheduled.
Dispatch delay before the closure starts is included in ticket service time;
report that fact rather than labeling it pure SQLite execution time.

Call sites in the current engine/store:

| Site | Admission |
| --- | --- |
| pull: `stage_begin`, each `stage`, `reset`, final `apply` | Remote, one ticket per worker job |
| serve: schema and `export` reads | None; keep byte/session admission |
| serve: ACK progress persistence and `flush` | Remote for that short worker job |
| backend `commit`, `observe`, peer-progress mutation | Remote when entered from peer processing |
| backend retention checkpoint/prune/reclaim | Maintenance per bounded worker unit |
| local migrations, managed-key rebuild, search-index maintenance | Maintenance per bounded unit, coordinated with WP-E |
| configuration and ordinary application writes | Local only through the safe SQLite bridge described below |

Backend operations need an explicit caller class; a blanket Remote label on
every backend call misclassifies maintenance. Do not recursively admit when
a scheduled operation calls backend/store helpers. Persist-before-ACK ordering
stays intact. Never retain a writer ticket across QUIC reads/writes or while
waiting for the rest of an inbound batch. Shutdown closes pending admission,
lets active jobs finish, drains accepted worker jobs, then joins the worker.
Network stop alone must not close a scheduler shared by live local handles.

## Local SQLite writer integration requires a lead decision

The archive admits before `Database::write` queues its transaction. Here,
applications call native sqlite3_prepare/step/exec directly; scheduling just
Worker jobs cannot enforce the approved 90/10 local/remote policy. Expose
`local_admission_enforced: false` in health until this boundary is wired.

An internal C/Rust bridge could acquire/release opaque Local tickets around
native SQLite write transactions. It must admit before any SQLite lock that
the competing writer needs, release on commit, rollback, failure, interrupt
and close, and avoid reacquiring during CRR triggers or recursive statements.
Explicit multi-statement transactions keep one ticket until transaction end.
It is an internal execution bridge, not an option-setting C API.

Do not implement blocking acquisition naively in VFS xLock(RESERVED): the
connection may already hold SHARED, while the ticket owner needs EXCLUSIVE
to commit. Likewise, trace/authorizer callbacks are not sufficient transaction
admission hooks: prepare can occur without execute and trace cannot return
an admission failure. A correct SQLite-core hook must handle read-to-write
upgrades without waiting while holding a blocking read lock (return BUSY and
let the application retry the transaction where necessary). Until that hook
or an explicitly scoped managed execution path is approved and tested, the
honest deliverable is remote/maintenance scheduling, not full writer fairness.

## PRAGMA configuration

Follow murmur_config persistence, defaults and read-back in config.rs:

| Name | Default | Validate |
| --- | --- | --- |
| `writer_local_share` | 90 | Integer 1..99 |
| `writer_remote_share` | Derived 10 | Read-only 100 minus local; reject assignment |
| `writer_max_debt_ms` | 1000 | Integer 1..60000 |
| `writer_maintenance_reserve_every` | 100 | Integer 1..1000000 |

The SQL names have the `murmur_` prefix. Persist the three writable values;
derive the remote read-back so two independently persisted shares cannot
become inconsistent. Range-check before converting to u32 or constructing
Duration, then call SchedulerConfig::validate. The archived validator adds
two u32 shares without checked addition, so do not feed it unchecked values.
The archive constructor itself does not call validate.

Follow existing config lifecycle restrictions: configuration outside a SQL
transaction, no changes to active replication settings. Replacing a scheduler
while any Local ticket or queued job exists would break mutual exclusion,
even if networking is stopped. Until a coordinated quiescent reconfiguration
gate is implemented, treat scheduler changes as persisted values effective
on next database-service creation; show effective and persisted config
separately in health. Do not silently replace live scheduler generations.
This lifecycle detail needs lead approval before exposing the PRAGMAs.

Use the existing operation deadline for each call, or thread an explicit
action deadline down from its caller. Do not repurpose max_debt_ms as an
admission timeout: it bounds normalized service debt, not waiting time.

## Byte budgets and network admission

Reuse existing queue_bytes, staging_bytes, batch_bytes and max_batch_bytes
PRAGMAs; preserve their defaults and config validation. No new byte knob is
needed. The scheduler itself counts time/grants, not bytes.

- Serve already reserves max_batch_bytes from its serving semaphore before
  export allocates data. Retain that reservation through send/ACK and drop
  it on success, error, cancellation or shutdown. Never obtain a writer
  ticket before this budget wait. Reserve-before-export prevents overshoot.
- Pull validates the declared length and reserves receiving bytes before
  staging. Hold that reservation through full blob reassembly/apply, because
  `Store::apply` currently allocates a Vec with the entire declared size.
  Each chunk receives a short Remote writer ticket after its bytes exist.
- Staging_bytes is a persistent-stage bound, not an RSS bound. Enforce it
  transactionally against the current stage total before inserting a chunk.
  Failed transfers need stage cleanup under a short bounded job.
- Serving and receiving are currently two separate queue_bytes semaphores;
  their combined allocations can reach twice queue_bytes, plus framed
  decoding buffers, CRR parsing, SQLite/RocksDB caches and worker closures.
  Report both budgets; do not label queue_bytes a process-memory cap.
- `Worker::sender` is currently an unbounded mpsc queue. Network batches have
  semaphores, but generic captured closure payloads are not accounted for.
  The admitted helper should take an owned byte permit with any transferred
  payload and move it into the job alongside its ticket. Queue accounting
  must be explicit at producers; closures cannot introspect their heap size.
  A true aggregate cap needs a reviewed shared payload budget rather than
  double-charging one semaphore (which can deadlock a full incoming batch).
- Protocol reads allocate a bounded frame before decoding a message. Reserve
  control/frame allowance independently from payload admission so handshake,
  ACK and shutdown traffic can still progress under data saturation. Do not
  exhaust the only capacity waiting for a peer to return that same capacity.

## Read-only health projection

Propose `SELECT murmur_health()` returning additive versioned JSON:

```json
{
  "version": 1,
  "replication": {"running": false, "last_error": null,
                  "last_error_code": null, "last_error_kind": null},
  "scheduler": {"local_admission_enforced": false,
                "scope": "remote_and_maintenance",
                "effective_config": null, "pending_config": null,
                "stats": null},
  "storage": {"committed_cursor": null, "durable_cursor": null,
              "counters": null},
  "budgets": {"serving_in_use_bytes": 0, "receiving_in_use_bytes": 0,
              "stage_bytes": null, "worker_payload_bytes": null}
}
```

Fill stats with the serialized SchedulerSnapshot (local/remote/maintenance
acquisitions, cancels, wait_nanos, service_nanos, dual_service_nanos,
dual_grants, waiters, oldest_wait_nanos; debt_nanos and shares).
Use Rust snapshots/atomics and short Store reads; do not flush or initiate
network activity to produce health. Preserve existing murmur_status output
compatibility. A stopped service can still have local scheduler state.

CRR's crsql_db_version is a logical version, not proof of RocksDB durability.
Do not synthesize a durable cursor from it. Until storage supplies a barrier
generation, leave durable_cursor null and expose the actual VFS stats/error.
Unknown/unimplemented metrics stay null, not zero or healthy. Background
failures retain structured code/kind with their message; never parse text.

## Verification gate for downstream integration

The isolated crate tests do not establish production fairness. Before marking
point 19 complete, test mixed Local/Remote contention, maintenance progress,
timeout/grant races, close with pending and active work, read-upgrade lock
ordering, cancellation after enqueue, peer reads while a transaction is
held, no admission across stalled network I/O, permit cleanup after malformed
transfers, and PRAGMA persistence/reopen/effective-config behavior. Compare
contended service time with configured shares across many bounded units;
non-preemptible long transactions make short-window percentages approximate.
