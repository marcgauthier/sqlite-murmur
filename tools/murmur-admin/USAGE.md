# murmur-admin — offline operational CLI

All commands are **offline-only**: a target whose store LOCK is held by
a live process is refused. Stop the database first.

The key travels from `--key-hex` into `sqlite_murmur_register` memory
only. It is wiped after registration and never printed, logged, or
embedded in `--json` output. There is no `--passphrase` yet: it arrives
with the D2 Argon2id protector (the archive's plain-SHA256 passphrase
is deliberately not ported).

## verify

```
murmur-admin verify <data-dir> [--backup] --key-hex=<64 hex>
```

Opens read-only, runs `PRAGMA integrity_check`, and — with `--backup`
— checks the D1 `manifest.json` artifact (exact schema, supported
version, known cipher). Exit 0 with `PASS`, else exit 1 with `FAIL`
and reasons. `--json` emits
`{target, valid, mode, database_id?, node_id?, cipher?, rocksdb_seq?,
tables_checked, errors[]}`.

## restore

```
murmur-admin restore <backup-dir> <dest-dir> [--fresh-node-id[=hex]]
    [--fresh-db-id[=hex]] --key-hex=<64 hex>
```

Copies the backup to a new destination (dest must not exist; any
failure removes it), optionally rehomes identity on the copy, then
verifies the copy as a backup artifact. Fresh ids may be given as
32 hex chars or drawn randomly when the flag has no value.

Rehome is a single atomic transaction on the copy: the old ordinal-0
site moves to a fresh ordinal, every `__crsql_clock` row pointing at
ordinal 0 is remapped, the new UUID is installed at ordinal 0, and
`murmur_identity` (node/site/incarnation) plus optionally
`murmur_config.group_id` are updated. History keeps the original UUID;
writes after reopen advertise the new one.

## repair

```
murmur-admin repair <data-dir> --key-hex=<64 hex>
```

Recovers a damaged store with RocksDB's repair, using the exact store
options. Live targets are refused (offline-only) and missing paths fail
without creating anything. Records stay opaque during repair; the key is
required uniformly but never used beyond registration. `--json` emits
`{data_dir, repaired}`.

## keys

```
murmur-admin keys <data-dir> --key-hex=<64 hex>
```

Shows the key envelope: sealed state, KDF, sequence, current key id,
and the key-id list. Unsealed (legacy) stores report no envelope;
this is reportable state, not an error. Live targets are refused
(offline-only) and missing paths fail without creating anything.
`--json` emits the inventory document verbatim (same schema as
`murmur_key_inventory()`). Key bytes never appear in either output.

## status / inspect

```
murmur-admin status <data-dir> --key-hex=<64 hex>
murmur-admin inspect <data-dir> --key-hex=<64 hex>
```

Read-only dumps. `status` reports identity (node/site/group,
incarnation), cipher, table count, and disk usage. `inspect` lists
tables (columns, CRR membership), the `murmur_config` map, and disk
usage. Both support `--json`. Config values for secret-like names are
shown as `<redacted>`.

## rekey

```
murmur-admin rekey <data-dir> (--key-hex=<64 hex> | --key-file=<path>)
    (--new-key-hex=<64 hex> | --new-key-file=<path>) [--json]
```

Offline master key rotation and store re-encryption:
- Checks clean refusal on unsealed databases (`SQLITE_ERROR`, code 1).
- Checks offline exclusivity (refuses running instances with store lock held).
- Accepts keys via either 64 hex characters or 32 raw bytes (via `--key-file` / `--new-key-file`).
- Cleanses sensitive key buffers in memory (`OPENSSL_cleanse`).
- Stages the new master envelope in `meta:keyring.REKEY`.
- Sweeps and re-encrypts all database pages under the fresh envelope's active data key (unkeyed pre-seal pages stay unkeyed). Supports crash recovery with staged-resume convergence (idempotent re-run after interrupted sweep).
- Atomically commits the new envelope to `meta:keyring` and cleans up staged markers.
- `--json` emits `{"rekeyed":true}`. Exit 0 on success, exit 1 on failure.

