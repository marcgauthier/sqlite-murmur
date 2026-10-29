# PROVENANCE — tools/murmur-admin (WP-D slice D3a)

Command surface adapted from archive `MURMUR-RUST/src/bin/admin.rs`
(status/inspect/verify/backup/restore/keys verbs, `--json`,
`--key-hex`, `--fresh-node-id`/`--fresh-db-id` flags, exit 0/1, JSON
report shapes). No archive code is copied: all storage mechanics are
destination-native (`sqlite_murmur_*` C API over the RocksDB VFS).

Deliberate departures (lead-approved D3a design):

- No `backup` subcommand: online backup is the D1 `murmur_backup()` SQL
  action. No `keys` subcommand yet: it arrives with the D2 envelope.
- No `--passphrase`: the archive hashes it with plain SHA256, which is
  weak. Passphrases arrive with the D2 Argon2id protector.
- Offline-only is enforced (fcntl `F_SETLK` probe on the store LOCK plus
  the open as backstop). The archive opens the live store; this VFS is
  single-process, so a second opener must refuse.
- Restore identity rewrite follows the lead-approved MOVE procedure
  (codex peer review + runtime probe): move ordinal 0 to a fresh
  ordinal, remap every `__crsql_clock` `site_id`, install the new UUID
  at ordinal 0, update `murmur_identity`, commit, reopen. The naive
  append-at-max+1 was proven wrong (ordinal 0 is the active site).
- `inspect` redacts `murmur_config` values for secret-like names
  (`private_key`, `certificate`, `secret`, `password`): config rows can
  carry key material, which must never reach CLI output.
