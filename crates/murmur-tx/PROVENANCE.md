# Source provenance

`src/` reuses the archived Murmur Rust replication implementation at
`/media/marc/2TB/Archives/MURMUR-RUST` (`replication/src/`), Git commit
`e5a19c25cceaf93375872e3a7f01f05ffc3659ac` (same archive recorded in
`../murmur-replication/PROVENANCE.md`).

Copied verbatim: `protocol.rs`, `snapshot.rs`, `retention.rs`, `queue.rs`,
`verification.rs`, `crypto.rs`, `policies.rs`, `bridge.rs`, `files.rs`,
`filefetch.rs`, `ftp.rs`, `membership.rs`, `transport.rs`.
`Cargo.toml` keeps the archived dependency set (package renamed to
`murmur-tx`, `crate-type = ["rlib"]` added).

Deliberate divergences in `src/lib.rs` only:

- `mod engine` and its `Handle`/`Status`/`ReplicationMetrics`/
  `PeerDiagnostics` re-exports plus `start()` are excluded. The engine is
  CRR-adapted downstream in `murmur-replication`; the transaction sync
  loop arrives with WP-A.
- Archived `engine::parse_id` is provided locally as `parse_id`.
- `retention::{peer,touch,retire,status}` are `pub` (were `pub(crate)`):
  the downstream CRR adapter drives peer-retention bookkeeping through
  the `Backend` trait. `collect` stays `pub(crate)` (it needs the
  archived transport `Config`; the downstream floor collector adapts
  its floor rules to CRR progress).
- `snapshot::{STREAM_MAGIC,install_key,stream_read,validate_chunk,
  reap_forgotten_pins}` are `pub` (were private/`pub(crate)`): the
  downstream snapshot adapter reuses the chunk/pin/receipt state
  machine while sourcing policy (sizes, trust, signatures) from the
  embedded profile instead of the archived transport `Config`.
- `mod transfer_tests` is excluded. `mod test_support` is included verbatim
  but `#[cfg(test)]`-gated: the verbatim `files.rs`/`filefetch.rs` test
  modules reference its `Empty` backend, and without it the crate's test
  target does not compile. It ships in no build artifact.
- `membership`, `queue`, and `filefetch` are `pub` (they were private to
  the archived crate, whose in-crate engine consumed them; downstream
  WP-A/WP-C consume them from here).
- `filefetch::Service` and `transport::endpoint_with_alpn` are `pub`
  (were `pub(crate)`): the WP-C files host constructs the fetch
  service and the files ALPN endpoint from `murmur-files` instead of
  forking the TLS/QUIC endpoint stack.

The supplied archive has no top-level LICENSE or license field in its
replication Cargo manifest. This records provenance; it does not assign a
new license to the imported material. Retain any applicable author
permissions when redistributing.
