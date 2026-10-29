# Internal ipnet compatibility subset

Derived from the repository's vendored `ipnet` 2.12.2, upstream
<https://github.com/krisprice/ipnet>. Upstream commit: `13c5c5873c02127679b1e8e64b538e4e36942640`.
Original copyright and MIT / Apache-2.0
license notices are retained in this directory. This is extracted third-party
code maintained locally, not wholly original Murmur code.

The package retains the name `ipnet` and version `2.12.2` so the workspace
`[patch.crates-io]` redirects both direct consumers and `hyper-util` to one
local implementation. Direct consumers pin this version so the patch cannot
be displaced by a newer registry release during ordinary resolution.
Only the `std` and `serde` feature surface used by the
application is provided; this is not a complete replacement for upstream ipnet.
Cargo patches apply at the root workspace: applications building these Rust
crates outside this workspace must also apply the same patch if they want to
use the internal implementation. The supported CMake package already does so.

- `parser.rs`: upstream parser retained with formatting changes only to preserve accepted/rejected
  IPv4, IPv6, and prefix spellings, including legacy leading-zero syntax.
- `ipnet_serde.rs`: upstream human-readable and binary serializers/deserializers;
  unused heapless formatting and upstream serde_test tests are omitted.
- `ipnet.rs`: reduced type, formatting, constructor, and address-containment
  implementation; aggregation, address arithmetic/ranges, subnet iteration,
  and schema-generation APIs are omitted.

`../common/fixtures/ipnet.json` was captured from upstream 2.12.2 before the
patch. The integration test compares parsing, formatting, JSON, Postcard binary
encoding, and IPv4/IPv6 containment over all prefix lengths and invalid inputs.
Both allowlist enforcement and NO_PROXY matching must preserve these semantics.
