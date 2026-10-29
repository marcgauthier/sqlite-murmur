# Key ceremony & trust policy (WP-D)

Owner: WP-D (`muse-worker-01a1839a`), §7a grant. Scope: every key class
in sqlite-murmur — page keys, file keys, bridge session secrets — and
the trust policy around them. Items marked **PENDING** name approved
but unlanded work; nothing pending is required to operate what ships.

## 0. Key inventory

| Class | Home | Provisioning | Rotation |
|---|---|---|---|
| Page master (32B) | `register()` memory only | Operator, out-of-band | Full rekey (PENDING impl) |
| Page data keys | `meta:keyring` envelope | Seal ceremony §1.2 | `murmur_rotate_key` (PENDING D2b impl) |
| File current/previous (32B ea) | `register_file_key` slots | Operator per node | Restart with new current + old previous |
| Bridge session secrets | Rust memory only | `murmur_bridge_configure` JSON args | Reconfigure + restart |
| TLS node credentials | `murmur_config` (cert) + memory (key) | Existing replication onboarding | Existing rotation |

Rules that hold everywhere: long-lived keys never appear in SQL text
(the register rule; the sole exception is session-scoped bridge secrets
in action args, ruled §7a); key material is wiped after use
(`OPENSSL_cleanse`, `Zeroizing`, `admin_wipe`); CLI/config dumps redact
secret-like names (never allowlist).

## 1. Provision ceremonies

### 1.1 Page master key

1. Generate 32 bytes from a CSPRNG (`/dev/urandom`, `RAND_bytes`).
   Never derive from a password without the Argon2 protector (§1.3).
2. Deliver out-of-band to the process host (secret store, sealed
   envelope — never chat, tickets, or shell history).
3. Pass to `sqlite_murmur_register()` (exactly 32 bytes are copied;
   registration is once per process; the copy is wiped at unregister).
4. The same master opens every database it created. There is NO escrow:
   a lost master with no backup means unrecoverable data (§3.2).

### 1.2 Envelope seal (data keys)

The seal creates `meta:keyring` (keys.enc v3) and switches page crypto
from the master to data key id 1. Seal-anytime: safe on a live store;
pre-seal master-encrypted pages stay readable via the master fallback
until a flush lazily re-encrypts them.

1. (Optional) Stage Argon2 costs first: `PRAGMA murmur_kdf_time`,
   `murmur_kdf_memory_kib`, `murmur_kdf_threads` (ranges 1–32,
   1024–16777216 KiB, 1–32). Requires autocommit, read-write,
   replication stopped.
2. Seal: `PRAGMA murmur_kdf='direct'` (argon2id: **PENDING** the
   Argon2-C vendor drop; selecting it today fails loudly with
   "requires a build with a vendored Argon2 unit"). Sealing is
   write-once: later writes fail with "KDF envelope is sealed".
3. Verify: re-read the pragmas (read-back comes from the envelope
   header, not config) and run `SELECT murmur_key_inventory()` —
   expect `sealed:true`, `current:1`, one active key. Direct seals
   record zero cost params (v3 format rule; read-back shows 0).

### 1.3 Passphrases (PENDING)

Passphrase unlock arrives with the Argon2 unit: a
`register_passphrase` C API (F2-approved shape) plus `--passphrase` /
`--passphrase-file` CLI flags. The archive's plain-SHA256 passphrase
is deliberately never ported. Until then: 32-byte masters only, and
`murmur_kdf='argon2id'` stays a loud error.

### 1.4 File keys (§7a)

1. Generate current (and, on rotation, previous) 32-byte keys per §1.1.
2. Provision identically on every group node (operator step; TLS-CA
   model precedent — no in-band group key agreement exists).
3. Call `sqlite_murmur_register_file_key(current, previous_or_NULL)`
   once per process before start; keys apply at configure (restart to
   rotate). Getters exist only for the Rust file layer; keys never
   enter SQL text, pragmas, or logs.

### 1.5 Bridge session secrets

`SELECT murmur_bridge_configure('<json>')` with
`{"role":"low","signer":hex64,"recipient":hex64}` or
`{"role":"high","receiver_secret":hex64,"trusted_domains":[...],...}`.
Session-scoped by ruling: secrets stay in memory (`Zeroizing` at
rest), nothing persists, and the JSON role must match the
`murmur_bridge_role` pragma. Read the transient-window caveat (§5)
before scripting this call.

## 2. Rotation ceremonies

### 2.1 Page data keys (PENDING D2b impl)

`SELECT murmur_rotate_key()` stages a successor key, persists the new
envelope first, then installs it (active key + ring swap, readers heal
via retry); returns the new id. Old pages keep decrypting via retired
keys; touched pages re-encrypt lazily. `SELECT murmur_prune_keys()`
drops retired keys only when provably unused (any unkeyed EXP1/EXP2
record blocks pruning and is reported). The master is NEVER pruned.
Until D2b lands: do not attempt manual envelope surgery — rotation is
the only supported path and it is not wired yet.

### 2.2 KDF migration direct→argon2id (PENDING)

Approved shape (F2): an offline privileged re-seal (same data keys,
new protector/salt/params, atomic envelope swap; secret transits
memory, never SQL text). No online SQL action will ever exist for
this. Downgrades (argon2id→direct) are rejected by design. Blocked on
the Argon2-C drop; fresh argon2id seals (§1.2) are the interim path.

### 2.3 File-key rotation (§7a mechanism)

Restart each node with the new key as current and the expiring key as
previous (`objects_config` enforces previous-required + one-step, so
skipped generations fail closed). Order within a group is
operator's choice (keys are symmetric per node); mixed generations
during a rolling restart interoperate by the previous-key rule. After
all nodes run the new key, the next rotation drops the old one.

### 2.4 Compromise response

- Page data key suspected: rotate (§2.1 when landed) + prune; the
  envelope + audit trail (inventory `seq`) prove the rotation.
- Page master suspected: full rekey (page rewrite under a new master;
  PENDING impl per the G2 design) + re-seal; treat pre-rekey backups
  as compromised (each carries its own envelope copy).
- File key suspected: emergency §2.3 rotation on all nodes.
- Bridge secret suspected: reconfigure both sides + restart (session
  scope bounds the blast radius to in-flight bundles).

## 3. Recovery ceremonies

### 3.1 Backup / verify / restore

- Online: `SELECT murmur_backup('/new/dir')` (committed state only;
  manifest inside the artifact; cross-filesystem copies, slower).
- Offline verify: `murmur-admin verify <dir> [--backup] --key-hex=…`.
- Restore: `murmur-admin restore <backup> <dest> [--fresh-node-id[=hex]]
  [--fresh-db-id[=hex]]`; identity rehome is atomic on the copy
  (ordinal MOVE + clock remap + new UUID at ordinal 0); history keeps
  the original UUID, post-reopen writes use the new one.
- Repair: `murmur-admin repair <dir>` (offline-only, live targets
  refused, missing paths fail without creating anything).

### 3.2 Key loss

No escrow exists by design. Lost master + no backup = unrecoverable;
restore the newest verified backup whose envelope you can open, then
rotate (§2) on the assumption the lost key is compromised. Lost file
key = re-provision per §1.4/§2.3 (file payloads sealed under it are
unreadable until then). Lost bridge secret = reconfigure (§1.5).

### 3.3 Integrity failure

`integrity_check` failure or envelope AUTH failure: stop writes, take
a filesystem copy of the directory for forensics, then `repair` (LSM
level) or restore from the newest verified backup. Envelope AUTH
failure specifically means wrong key (or tamper), never try keys from
untrusted sources against a live store — verify a COPY with
`murmur-admin verify`.

## 4. Trust policy

- **Holders.** Page masters, file keys, and bridge secrets are held by
  the node operator. No shared global keys; group scope is provisioned
  per node by the operator (file keys) or per database (page keys).
- **Group provisioning.** File keys: identical operator provision per
  node. Page keys: one master per database; group membership (peers,
  TLS) is independent of page encryption.
- **Audit.** Live: `SELECT murmur_key_inventory()` (ids, status,
  creation times, sequence — never bytes). CLI `keys` shell: PENDING
  the F3 C API. Config dumps redact secret-like names (`inspect`
  shows `<redacted>`). Envelope `seq` + inventory `current` prove
  rotations happened; manifest `rocksdb_seq` pins backups.
- **Non-goals (stated).** No remote attestation, no key escrow, no
  in-band group key agreement, no downgrade paths.

## 5. Transient-secret caveat (§7b finding 1)

Session secrets cross more memory than "while parsing" suggests, and
operators + implementors must assume all of it is readable to local
attackers (core dumps, `/proc` scrapes, crash reports):

- SQL text + prepared-statement caches on the caller side;
- `serde_json::Value` DOM during `murmur_bridge_configure`;
- pre-move `[u8;32]` stack copies (Rust moves are `memcpy`; sources
  are not wiped) before secrets reach `Zeroizing` config fields.

Mitigations in force: at-rest `Zeroizing`, no persistence, no value
echo in errors/logs, wiped CLI buffers. Requested hardening (WP-C
finding 1, non-blocking): decode into `Zeroizing` buffers or wipe
locals post-move. Operator guidance: prefer `--passphrase-file`-style
indirection (when it lands) over argv for anything secret; disable core
dumps on secret-handling hosts.

## 6. Pending ledger

- Argon2-C vendor drop (source needed; network off-limits) → unlocks
  argon2id seals, `register_passphrase`, KDF migration, real-argon2
  test vectors (the fixture generator already branches on their
  availability).
- D2b impl (EXP3/EXP4 codec + rotate/prune ops + UDFs; rulings P–T in
  hand) → unlocks §2.1 and D2b tests.
- F3 C API (`sqlite_murmur_key_inventory`) → unlocks CLI `keys` shell
  (audit) — implementer: WP-D, after G2 per grant 1112(c).
- Full page rekey (G2 design next) → unlocks §2.4 master-compromise
  path and CLI `rekey`.
- File-key rotation works today (§2.3); prune-sweep driver deferred
  by ruling R; nothing else in this document is gated.
