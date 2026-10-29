use crate::*;
use rusqlite::Connection;

#[derive(Clone)]
pub(crate) struct Credentials {
    pub chain: Vec<Vec<u8>>,
    pub private_key: zeroize::Zeroizing<Vec<u8>>,
    pub roots: Vec<Vec<u8>>,
}
#[derive(Clone)]
pub(crate) struct Config {
    pub group: NodeId,
    pub node: NodeId,
    pub site: NodeId,
    pub bind: SocketAddr,
    pub advertise: Option<SocketAddr>,
    pub credentials: Credentials,
    pub allowed_peers: BTreeSet<NodeId>,
    pub allowed_networks: Vec<ipnet::IpNet>,
    pub bootstrap: Vec<SocketAddr>,
    pub excluded: BTreeSet<NodeId>,
    pub incarnation: u64,
    pub sessions: usize,
    pub queue_bytes: usize,
    pub batch_bytes: usize,
    pub max_batch_bytes: usize,
    pub staging_bytes: usize,
    pub staging_timeout_ms: u64,
    pub poll_ms: u64,
    pub anti_entropy_ms: u64,
    pub gossip_fanout: usize,
}

/// A Go-identical DNS name: dot-separated labels of letters, digits,
/// hyphens, and underscores; labels never start or end with a hyphen.
fn valid_dns_name(host: &str) -> bool {
    if host.is_empty() || host.len() > 253 {
        return false;
    }
    let name = host.strip_suffix('.').unwrap_or(host);
    if name.is_empty() {
        return false;
    }
    name.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && label
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
            && !label.starts_with('-')
            && !label.ends_with('-')
    })
}

/// Validate `host:port` where the host is an IP literal or DNS name.
/// Never resolves: offline validation must not need DNS (Go parity).
fn check_socket_or_name(value: &str) -> Result<()> {
    if value.parse::<SocketAddr>().is_ok() {
        return Ok(());
    }
    let (host, port) = value.rsplit_once(':').ok_or_else(|| Error::Invalid(format!("expected IP:port or host:port, got {value:?}")))?;
    if host.is_empty() || !valid_dns_name(host) {
        return Err(Error::Invalid(format!("invalid host {host:?}")));
    }
    match port.parse::<u16>() {
        Ok(0) | Err(_) => Err(Error::Invalid(format!("invalid port {port:?}"))),
        Ok(_) => Ok(()),
    }
}

/// Resolve a validated `host:port` (literals pass through). Runs once
/// at config load; restarts re-resolve.
fn resolve_socket_or_name(value: &str) -> Result<Vec<SocketAddr>> {
    use std::net::ToSocketAddrs;
    if let Ok(addr) = value.parse::<SocketAddr>() {
        return Ok(vec![addr]);
    }
    let (host, port) = value.rsplit_once(':').ok_or_else(|| Error::Invalid(format!("expected IP:port or host:port, got {value:?}")))?;
    let port: u16 = port.parse().map_err(|_| Error::Invalid(format!("invalid port {port:?}")))?;
    if port == 0 || !valid_dns_name(host) {
        return Err(Error::Invalid(format!("invalid host {host:?}")));
    }
    (host, port)
        .to_socket_addrs()
        .map_err(|e| Error::Invalid(format!("cannot resolve {host:?}: {e}")))
        .map(|iter| iter.collect())
}
pub(crate) fn default_value(name: &str) -> Option<&'static str> {
    Some(match name {
        "group_id" | "advertise" | "certificate" | "private_key" | "ca" | "crsqlite_extension" => {
            ""
        }
        "bind" => "0.0.0.0:7443",
        "bootstrap" | "allowed_peers" | "allowed_networks" => "[]",
        "poll_ms" => "100",
        "anti_entropy_ms" => "10000",
        "gossip_fanout" => "3",
        "sessions" => "32",
        "batch_bytes" => "1048576",
        "max_batch_bytes" => "67108864",
        "queue_bytes" => "67108864",
        "staging_bytes" => "268435456",
        "staging_timeout_ms" => "300000",
        "writer_local_share" => "90",
        "writer_remote_share" => "10",
        "writer_max_debt_ms" => "1000",
        "writer_maintenance_reserve_every" => "100",
        "retention_min_kept" => "1000",
        "retention_max_offline_ms" => "604800000",
        "retention_gc" => "1",
        "table_replication" => "{}",
        "column_policies" => "{}",
        "snapshot_bytes" => "536870912",
        "snapshot_staging_bytes" => "536870912",
        "snapshot_lifetime_ms" => "604800000",
        "snapshot_staging_transfers" => "4096",
        "snapshot_trusted_sources" => "[]",
        "accept_remote_schema" => "1",
        "schema_master" => "",
        "schema_master_enabled" => "0",
        // WP-D slice D2a: KDF envelope params. Defaults mirror the archived
        // spool (direct-HKDF, argon2id t3/m64MiB/p4). Values live in the
        // envelope header, never murmur_config (exception J); the pragma
        // handler routes these four names to the Store FCTL.
        "kdf" => "direct",
        "kdf_time" => "3",
        "kdf_memory_kib" => "65536",
        "kdf_threads" => "4",
        // WP-C slice 3: file/bridge knobs delegate to murmur-files adapt
        // (full murmur_ names there; stripped here). All restart-applied.
        "bridge_dir" => murmur_files::adapt::DEFAULT_BRIDGE_DIR,
        "files_generation" => murmur_files::adapt::DEFAULT_FILES_GENERATION,
        "max_file_bytes" => murmur_files::adapt::DEFAULT_MAX_FILE_BYTES,
        "max_staging_bytes" => murmur_files::adapt::DEFAULT_MAX_STAGING_BYTES,
        "fetch_concurrency" => murmur_files::adapt::DEFAULT_FETCH_CONCURRENCY,
        "fetch_interval_ms" => murmur_files::adapt::DEFAULT_FETCH_INTERVAL_MS,
        "fetch_bind" => murmur_files::adapt::DEFAULT_FETCH_BIND,
        "bridge_role" => murmur_files::adapt::DEFAULT_BRIDGE_ROLE,
        "bridge_stream" => murmur_files::adapt::DEFAULT_BRIDGE_STREAM,
        "bridge_max_bundle_bytes" => {
            murmur_files::adapt::DEFAULT_BRIDGE_MAX_BUNDLE_BYTES
        }
        "bridge_max_payload_bytes" => {
            murmur_files::adapt::DEFAULT_BRIDGE_MAX_PAYLOAD_BYTES
        }
        "bridge_staging_bytes" => murmur_files::adapt::DEFAULT_BRIDGE_STAGING_BYTES,
        // Deprecated-inert (acceptance 2): accepted + persisted + read
        // back, zero effect. Slice-3 stores payloads in chunk rows.
        "files_dir" => "",
        _ => return murmur_query::adapt::default_value(name),
    })
}
/// Map query-plane errors onto local errors. Messages are always
/// preserved; categories collapse onto the eight local variants
/// (SchemaMismatch/Busy/Closed have no local counterpart yet).
pub(crate) fn query_err(e: murmur_query::Error) -> Error {
    match e {
        murmur_query::Error::Invalid(s) => Error::Invalid(s),
        murmur_query::Error::Limit(s) => Error::Limit(s),
        murmur_query::Error::Sql(e) => Error::Sql(e),
        murmur_query::Error::Io(e) => Error::Io(e),
        murmur_query::Error::Storage(s) => Error::Protocol(format!("query storage: {s}")),
        murmur_query::Error::Corrupt(s) => Error::Protocol(format!("query corrupt: {s}")),
        murmur_query::Error::SchemaMismatch => {
            Error::Invalid("database identity or schema history mismatch".into())
        }
        murmur_query::Error::Busy => Error::Overloaded,
        murmur_query::Error::Closed => Error::Unavailable("database is closed".into()),
        murmur_query::Error::CallbackPanicked => {
            Error::Protocol("SQL callback panicked; transaction rolled back".into())
        }
        murmur_query::Error::Unavailable(s) => Error::Unavailable(s),
    }
}
pub(crate) fn setting(db: &Connection, name: &str) -> Result<String> {
    use rusqlite::OptionalExtension;
    if name == "writer_remote_share" {
        // Derived read-only: two independently persisted shares could
        // disagree, so only the local share persists.
        let local: u32 = setting(db, "writer_local_share")?
            .parse()
            .map_err(|_| Error::Invalid("invalid writer_local_share".into()))?;
        if !(1..=99).contains(&local) {
            return Err(Error::Invalid("invalid writer_local_share".into()));
        }
        return Ok((100 - local).to_string());
    }
    let default = default_value(name)
        .ok_or_else(|| Error::Invalid(format!("unknown PRAGMA murmur_{name}")))?;
    Ok(db
        .query_row(
            "SELECT value FROM murmur_config WHERE name=?",
            [name],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or_else(|| default.into()))
}
pub(crate) fn validate(name: &str, value: &str) -> Result<()> {
    default_value(name).ok_or_else(|| Error::Invalid(format!("unknown PRAGMA murmur_{name}")))?;
    if value.len() > 65536 || value.contains('\0') {
        return Err(Error::Invalid("setting too large or contains NUL".into()));
    }
    match name {
        "group_id" => {
            parse_id(value)?;
        }
        "bind" => {
            value
                .parse::<SocketAddr>()
                .map_err(|_| Error::Invalid("expected IP:port".into()))?;
        }
        "advertise" => {
            // IP literal or DNS name (Go parity); resolved at load.
            if !value.is_empty() {
                check_socket_or_name(value)?;
            }
        }
        "bootstrap" => {
            let v: Vec<String> =
                serde_json::from_str(value).map_err(|e| Error::Invalid(e.to_string()))?;
            if v.len() > 4096 {
                return Err(Error::Overloaded);
            }
            for entry in &v {
                check_socket_or_name(entry)?;
            }
        }
        "gossip_fanout" => {
            let n = value
                .parse::<usize>()
                .map_err(|_| Error::Invalid("expected fanout 1..32".into()))?;
            if !(1..=32).contains(&n) {
                return Err(Error::Invalid("gossip_fanout outside 1..32".into()));
            }
        }
        "allowed_peers" => {
            let v: Vec<String> =
                serde_json::from_str(value).map_err(|e| Error::Invalid(e.to_string()))?;
            if v.len() > 4096 {
                return Err(Error::Overloaded);
            }
            for s in v {
                parse_id(&s)?;
            }
        }
        "allowed_networks" => {
            let v: Vec<ipnet::IpNet> =
                serde_json::from_str(value).map_err(|e| Error::Invalid(e.to_string()))?;
            if v.len() > 4096 {
                return Err(Error::Overloaded);
            }
        }
        "poll_ms" | "anti_entropy_ms" | "staging_timeout_ms" => {
            let n = value
                .parse::<u64>()
                .map_err(|_| Error::Invalid("expected milliseconds".into()))?;
            let minimum = if name == "staging_timeout_ms" {
                60000
            } else {
                10
            };
            if !(minimum..=86400000).contains(&n) {
                return Err(Error::Invalid(format!(
                    "interval outside {minimum}..86400000 ms"
                )));
            }
        }
        "sessions" => {
            let n = value
                .parse::<usize>()
                .map_err(|_| Error::Invalid("expected session count".into()))?;
            if !(1..=256).contains(&n) {
                return Err(Error::Invalid("sessions outside 1..256".into()));
            }
        }
        "batch_bytes" | "max_batch_bytes" | "queue_bytes" | "staging_bytes" => {
            let n = value
                .parse::<usize>()
                .map_err(|_| Error::Invalid("expected byte count".into()))?;
            if !(65536..=1073741824).contains(&n) {
                return Err(Error::Invalid("byte budget outside 64 KiB..1 GiB".into()));
            }
        }
        "writer_local_share" => {
            let n = value
                .parse::<u32>()
                .map_err(|_| Error::Invalid("expected local share 1..99".into()))?;
            if !(1..=99).contains(&n) {
                return Err(Error::Invalid("writer_local_share outside 1..99".into()));
            }
        }
        "writer_max_debt_ms" => {
            let n = value
                .parse::<u64>()
                .map_err(|_| Error::Invalid("expected max debt 1..60000 ms".into()))?;
            if !(1..=60000).contains(&n) {
                return Err(Error::Invalid("writer_max_debt_ms outside 1..60000".into()));
            }
        }
        "writer_maintenance_reserve_every" => {
            let n = value
                .parse::<u64>()
                .map_err(|_| Error::Invalid("expected maintenance reserve 1..1000000".into()))?;
            if !(1..=1000000).contains(&n) {
                return Err(Error::Invalid(
                    "writer_maintenance_reserve_every outside 1..1000000".into(),
                ));
            }
        }
        "writer_remote_share" => {
            return Err(Error::Invalid(
                "writer_remote_share is derived from writer_local_share; assign that instead"
                    .into(),
            ));
        }
        "retention_min_kept" => {
            let n = value
                .parse::<u64>()
                .map_err(|_| Error::Invalid("expected versions to keep 1..1000000".into()))?;
            if !(1..=1000000).contains(&n) {
                return Err(Error::Invalid("retention_min_kept outside 1..1000000".into()));
            }
        }
        "retention_max_offline_ms" => {
            let n = value
                .parse::<u64>()
                .map_err(|_| Error::Invalid("expected offline window 60000..2592000000 ms".into()))?;
            if !(60000..=2592000000).contains(&n) {
                return Err(Error::Invalid(
                    "retention_max_offline_ms outside 60000..2592000000".into(),
                ));
            }
        }
        "table_replication" => {
            // Name-keyed opt-out map: {"scratch": false}. Missing or true
            // follows CRR state (backward compatible); explicit false
            // keeps the table local in both wire directions. Lookups are
            // case-insensitive; renames drop flags (re-set after rename).
            let map: BTreeMap<String, bool> = serde_json::from_str(value)
                .map_err(|e| Error::Invalid(format!("table_replication map: {e}")))?;
            if map.len() > 1024 {
                return Err(Error::Overloaded);
            }
            for name in map.keys() {
                if name.is_empty() || name.len() > 128 {
                    return Err(Error::Invalid("bad table name in table_replication".into()));
                }
            }
        }
        "column_policies" => {
            // Flat "table.column" map to archived policy names. Unknown
            // tables/columns are inert (validated at use, when WP-B
            // lands); unknown policy names are rejected now. JSON/list
            // merge has no archived implementation and is rejected.
            let map: BTreeMap<String, String> = serde_json::from_str(value)
                .map_err(|e| Error::Invalid(format!("column_policies map: {e}")))?;
            if map.len() > 1024 {
                return Err(Error::Overloaded);
            }
            for (key, policy) in &map {
                let mut parts = key.split('.');
                match (parts.next(), parts.next(), parts.next()) {
                    (Some(t), Some(c), None) if !t.is_empty() && !c.is_empty() => {}
                    _ => {
                        return Err(Error::Invalid(format!(
                            "column_policies key must be table.column: {key}"
                        )));
                    }
                }
                match policy.as_str() {
                    "Lww" | "Counter" | "Set" | "Min" | "Max" => {}
                    _ => {
                        return Err(Error::Invalid(format!(
                            "unknown merge policy (want Lww/Counter/Set/Min/Max): {policy}"
                        )));
                    }
                }
            }
        }
        "snapshot_bytes" | "snapshot_staging_bytes" => {
            let n = value
                .parse::<usize>()
                .map_err(|_| Error::Invalid("expected byte count 1 MiB..2 GiB".into()))?;
            if !(1048576..=2147483648).contains(&n) {
                return Err(Error::Invalid("snapshot budget outside 1 MiB..2 GiB".into()));
            }
        }
        "snapshot_lifetime_ms" => {
            let n = value
                .parse::<u64>()
                .map_err(|_| Error::Invalid("expected lifetime 3600000..2592000000 ms".into()))?;
            if !(3600000..=2592000000).contains(&n) {
                return Err(Error::Invalid(
                    "snapshot_lifetime_ms outside 3600000..2592000000".into(),
                ));
            }
        }
        "snapshot_staging_transfers" => {
            let n = value
                .parse::<usize>()
                .map_err(|_| Error::Invalid("expected transfer count 1..65536".into()))?;
            if !(1..=65536).contains(&n) {
                return Err(Error::Invalid(
                    "snapshot_staging_transfers outside 1..65536".into(),
                ));
            }
        }
        "accept_remote_schema" | "schema_master_enabled" | "retention_gc" => {
            // SQLite-style booleans; exactly 0 or 1.
            if value != "0" && value != "1" {
                return Err(Error::Invalid(format!("{name} expects 0 or 1")));
            }
        }
        "schema_master" => {
            // Node id (32-hex or UUID) or empty for legacy symmetric
            // adoption. Master designation is checked against the live
            // node id at manifest build time, not here.
            if !value.is_empty() {
                parse_id(value).map_err(|_| {
                    Error::Invalid(format!("bad node id in schema_master: {value}"))
                })?;
            }
        }
        "kdf" => {
            // Ranges mirror Store::keyringControl; keep in sync.
            if value != "direct" && value != "argon2id" {
                return Err(Error::Invalid("kdf expects direct or argon2id".into()));
            }
        }
        "kdf_time" => {
            let n = value
                .parse::<u32>()
                .map_err(|_| Error::Invalid("kdf_time outside 1..32".into()))?;
            if !(1..=32).contains(&n) {
                return Err(Error::Invalid("kdf_time outside 1..32".into()));
            }
        }
        "kdf_memory_kib" => {
            let n = value
                .parse::<u32>()
                .map_err(|_| Error::Invalid("kdf_memory_kib outside 1024..16777216".into()))?;
            if !(1024..=16777216).contains(&n) {
                return Err(Error::Invalid("kdf_memory_kib outside 1024..16777216".into()));
            }
        }
        "kdf_threads" => {
            let n = value
                .parse::<u32>()
                .map_err(|_| Error::Invalid("kdf_threads outside 1..32".into()))?;
            if !(1..=32).contains(&n) {
                return Err(Error::Invalid("kdf_threads outside 1..32".into()));
            }
        }
        "bridge_dir" | "files_generation" | "max_file_bytes" | "max_staging_bytes"
        | "fetch_concurrency" | "fetch_interval_ms" | "fetch_bind" | "bridge_role"
        | "bridge_stream" | "bridge_max_bundle_bytes" | "bridge_max_payload_bytes"
        | "bridge_staging_bytes" => {
            // adapt keys full murmur_ names (stripped here); its error
            // text names the full pragma the user typed.
            let full = format!("murmur_{name}");
            debug_assert!(murmur_files::adapt::default_value(&full).is_some());
            murmur_files::adapt::validate(&full, value).map_err(crate::wait::backend_err)?;
        }
        // Deprecated-inert: any value accepted, persisted, read back;
        // nothing consumes it.
        "files_dir" => {}
        "snapshot_trusted_sources" => {
            // JSON array of node ids (32-hex or UUID). Empty denies all
            // snapshot donors (secure default; the smoke suite opts in).
            let ids: Vec<String> = serde_json::from_str(value)
                .map_err(|e| Error::Invalid(format!("snapshot_trusted_sources: {e}")))?;
            if ids.len() > 1024 {
                return Err(Error::Overloaded);
            }
            for id in &ids {
                parse_id(id).map_err(|_| {
                    Error::Invalid(format!("bad node id in snapshot_trusted_sources: {id}"))
                })?;
            }
        }
        _ => {
            // The pragma handler rejects truly unknown names via
            // default_value; validate() only sees known ones. Local
            // names without extra validation stay accepted; query
            // names defer to the query plane's ranges.
            if murmur_query::adapt::default_value(name).is_some() {
                murmur_query::adapt::validate(name, value).map_err(query_err)?;
            }
        }
    }
    Ok(())
}
fn certificates(path: &str) -> Result<Vec<Vec<u8>>> {
    let bytes = std::fs::read(path)?;
    let certs = <rustls::pki_types::CertificateDer as rustls::pki_types::pem::PemObject>::pem_slice_iter(&bytes)
        .map(|r| r.map(|c| c.to_vec()).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)))
        .collect::<std::io::Result<Vec<_>>>()?;
    if certs.is_empty() {
        return Err(Error::Invalid("empty PEM certificate file".into()));
    }
    Ok(certs)
}

fn private_key(bytes: &[u8]) -> Result<rustls::pki_types::PrivateKeyDer<'static>> {
    use rustls::pki_types::{PrivateKeyDer, pem::PemObject};
    PrivateKeyDer::pem_slice_iter(bytes).next().transpose()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
        .ok_or_else(|| Error::Invalid("missing PEM private key".into()))
}

impl Config {
    pub fn load(db: &Connection, site: NodeId) -> Result<Self> {
        let get = |s| setting(db, s);
        let num = |s| -> Result<usize> {
            get(s)?
                .parse()
                .map_err(|_| Error::Invalid(format!("invalid {s}")))
        };
        let chain = certificates(&get("certificate")?)?;
        let node = transport::node_id(&chain[0])?;
        let roots = certificates(&get("ca")?)?;
        let bytes = zeroize::Zeroizing::new(std::fs::read(get("private_key")?)?);
        let key = private_key(&bytes)?;
        let incarnation: i64 = db.query_row(
            "UPDATE murmur_identity SET incarnation=incarnation+1 WHERE id=1 RETURNING incarnation",
            [],
            |r| r.get(0),
        )?;
        let cfg = Self {
            group: parse_id(&get("group_id")?)?,
            node,
            site,
            bind: get("bind")?
                .parse()
                .map_err(|_| Error::Invalid("bind".into()))?,
            advertise: if get("advertise")?.is_empty() {
                None
            } else {
                Some(
                    resolve_socket_or_name(&get("advertise")?)?
                        .into_iter()
                        .next()
                        .ok_or_else(|| Error::Invalid("advertise".into()))?,
                )
            },
            credentials: Credentials {
                chain,
                roots,
                private_key: zeroize::Zeroizing::new(key.secret_der().to_vec()),
            },
            allowed_peers: serde_json::from_str::<Vec<String>>(&get("allowed_peers")?)
                .map_err(|e| Error::Invalid(e.to_string()))?
                .iter()
                .map(|s| parse_id(s))
                .collect::<Result<_>>()?,
            allowed_networks: serde_json::from_str(&get("allowed_networks")?)
                .map_err(|e| Error::Invalid(e.to_string()))?,
            bootstrap: {
                let entries: Vec<String> = serde_json::from_str(&get("bootstrap")?)
                    .map_err(|e| Error::Invalid(e.to_string()))?;
                let mut resolved = Vec::new();
                for entry in &entries {
                    resolved.extend(resolve_socket_or_name(entry)?);
                }
                resolved
            },
            excluded: {
                let mut s = db.prepare("SELECT node FROM murmur_excluded")?;
                let rows = s.query_map([], |r| r.get::<_, Vec<u8>>(0))?;
                rows.map(|r| {
                    r.map_err(Error::from).and_then(|v| {
                        v.try_into()
                            .map_err(|_| Error::Invalid("excluded identity".into()))
                    })
                })
                .collect::<Result<_>>()?
            },
            incarnation: incarnation as u64,
            sessions: num("sessions")?,
            queue_bytes: num("queue_bytes")?,
            batch_bytes: num("batch_bytes")?,
            max_batch_bytes: num("max_batch_bytes")?,
            staging_bytes: num("staging_bytes")?,
            staging_timeout_ms: num("staging_timeout_ms")? as u64,
            poll_ms: num("poll_ms")? as u64,
            anti_entropy_ms: num("anti_entropy_ms")? as u64,
            gossip_fanout: num("gossip_fanout")?,
        };
        if cfg.batch_bytes > cfg.max_batch_bytes
            || cfg.max_batch_bytes > 64 << 20
            || cfg.max_batch_bytes > cfg.queue_bytes
            || cfg.max_batch_bytes > cfg.staging_bytes
        {
            return Err(Error::Invalid(
                "require batch_bytes <= max_batch_bytes <= min(queue_bytes, staging_bytes, 64 MiB)"
                    .into(),
            ));
        }
        let old: Option<Vec<u8>> =
            db.query_row("SELECT node FROM murmur_identity WHERE id=1", [], |r| {
                r.get(0)
            })?;
        if old.as_deref().is_some_and(|v| v != node) {
            return Err(Error::Auth(
                "certificate changed node identity for this database".into(),
            ));
        }
        db.execute(
            "UPDATE murmur_identity SET node=? WHERE id=1",
            [node.as_slice()],
        )?;
        Ok(cfg)
    }

    /// Build the file-fetch transport config from the engine config.
    /// Carries ONLY what the fetch path reads (verified against
    /// murmur-tx transport.rs + filefetch.rs): credentials (endpoint
    /// TLS + node check), allowed_peers (NodeVerifier + peer_id),
    /// allowed_networks (network_allowed on serve-accept AND fetch
    /// dial — carried so the engine's network restriction is not
    /// silently dropped on the fetch port), sessions (bidi budget),
    /// bind (Config::new base; Service::start overrides it with
    /// fetch_bind). The `files` section is rebuilt from Knobs inside
    /// start_fetch; signing_key is a zeroed placeholder, never read
    /// on the fetch path. No new pragma, no second store: the cert
    /// pragmas stay the single source of truth.
    pub(crate) fn to_fetch_transport(&self) -> murmur_tx::Config {
        let mut tx = murmur_tx::Config::new(
            self.bind,
            murmur_tx::Credentials {
                chain: self.credentials.chain.clone(),
                private_key: zeroize::Zeroizing::new(self.credentials.private_key.to_vec()),
                roots: self.credentials.roots.clone(),
            },
            [0; 32],
        );
        tx.allowed_peers = self.allowed_peers.clone();
        tx.allowed_networks = self.allowed_networks.clone();
        tx.sessions = self.sessions;
        tx
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn memdb() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE murmur_config(name TEXT PRIMARY KEY, value TEXT NOT NULL)",
        )
        .unwrap();
        db
    }

    #[test]
    fn writer_pragma_defaults_ranges_and_readonly() {
        assert_eq!(default_value("writer_local_share"), Some("90"));
        assert_eq!(default_value("writer_remote_share"), Some("10"));
        assert_eq!(default_value("writer_max_debt_ms"), Some("1000"));
        assert_eq!(
            default_value("writer_maintenance_reserve_every"),
            Some("100")
        );
        for good in ["1", "90", "99"] {
            validate("writer_local_share", good).unwrap();
        }
        for bad in ["0", "100", "-1", "abc", ""] {
            assert!(validate("writer_local_share", bad).is_err(), "{bad}");
        }
        validate("writer_max_debt_ms", "1").unwrap();
        validate("writer_max_debt_ms", "60000").unwrap();
        assert!(validate("writer_max_debt_ms", "0").is_err());
        assert!(validate("writer_max_debt_ms", "60001").is_err());
        validate("writer_maintenance_reserve_every", "1").unwrap();
        validate("writer_maintenance_reserve_every", "1000000").unwrap();
        assert!(validate("writer_maintenance_reserve_every", "0").is_err());
        assert!(validate("writer_maintenance_reserve_every", "1000001").is_err());
        // Derived share rejects every assignment.
        assert!(validate("writer_remote_share", "10").is_err());
        assert!(validate("writer_remote_share", "").is_err());
    }

    #[test]
    fn writer_remote_share_derives_from_local() {
        let db = memdb();
        assert_eq!(setting(&db, "writer_remote_share").unwrap(), "10");
        db.execute(
            "INSERT INTO murmur_config VALUES('writer_local_share','75')",
            [],
        )
        .unwrap();
        assert_eq!(setting(&db, "writer_remote_share").unwrap(), "25");
        assert_eq!(setting(&db, "writer_local_share").unwrap(), "75");
    }

    #[test]
    fn setting_unknown_name_errors() {
        let db = memdb();
        assert!(setting(&db, "nope").is_err());
        assert!(validate("nope", "1").is_err());
    }

    #[test]
    fn table_replication_pragma_shape() {
        assert_eq!(default_value("table_replication"), Some("{}"));
        validate("table_replication", "{}").unwrap();
        validate("table_replication", "{\"scratch\":false}").unwrap();
        validate("table_replication", "{\"a\":true,\"b\":false}").unwrap();
        assert!(validate("table_replication", "[]").is_err());
        assert!(validate("table_replication", "{\"a\":1}").is_err());
        assert!(validate("table_replication", "{\"\":false}").is_err());
        assert!(validate("table_replication", "nope").is_err());
        let db = memdb();
        assert_eq!(setting(&db, "table_replication").unwrap(), "{}");
    }

    #[test]
    fn column_policies_pragma_shape() {
        assert_eq!(default_value("column_policies"), Some("{}"));
        validate("column_policies", "{}").unwrap();
        validate("column_policies", "{\"docs.title\":\"Lww\"}").unwrap();
        validate(
            "column_policies",
            "{\"a.b\":\"Counter\",\"c.d\":\"Set\",\"e.f\":\"Min\",\"g.h\":\"Max\"}",
        )
        .unwrap();
        assert!(validate("column_policies", "{\"docs\":\"Lww\"}").is_err());
        assert!(validate("column_policies", "{\"a.b.c\":\"Lww\"}").is_err());
        assert!(validate("column_policies", "{\"a.b\":\"Json\"}").is_err());
        assert!(validate("column_policies", "{\"a.b\":\"lww\"}").is_err());
        assert!(validate("column_policies", "[]").is_err());
    }

    #[test]
    fn snapshot_pragma_defaults_and_ranges() {
        assert_eq!(default_value("snapshot_bytes"), Some("536870912"));
        assert_eq!(
            default_value("snapshot_staging_bytes"),
            Some("536870912")
        );
        assert_eq!(default_value("snapshot_lifetime_ms"), Some("604800000"));
        assert_eq!(default_value("snapshot_staging_transfers"), Some("4096"));
        assert_eq!(default_value("snapshot_trusted_sources"), Some("[]"));
        validate("snapshot_bytes", "1048576").unwrap();
        validate("snapshot_bytes", "2147483648").unwrap();
        assert!(validate("snapshot_bytes", "1048575").is_err());
        validate("snapshot_staging_bytes", "1048576").unwrap();
        validate("snapshot_lifetime_ms", "3600000").unwrap();
        validate("snapshot_lifetime_ms", "2592000000").unwrap();
        assert!(validate("snapshot_lifetime_ms", "3599999").is_err());
        validate("snapshot_staging_transfers", "1").unwrap();
        assert!(validate("snapshot_staging_transfers", "0").is_err());
        assert!(validate("snapshot_staging_transfers", "65537").is_err());
        validate("snapshot_trusted_sources", "[]").unwrap();
        validate(
            "snapshot_trusted_sources",
            "[\"00000000000000000000000000000001\"]",
        )
        .unwrap();
        validate(
            "snapshot_trusted_sources",
            "[\"11111111-1111-4111-8111-111111111111\"]",
        )
        .unwrap();
        assert!(validate("snapshot_trusted_sources", "[\"zz\"]").is_err());
        assert!(validate("snapshot_trusted_sources", "{}").is_err());
    }

    #[test]
    fn gossip_pragma_defaults_ranges_and_dns_seeds() {
        assert_eq!(default_value("gossip_fanout"), Some("3"));
        validate("gossip_fanout", "1").unwrap();
        validate("gossip_fanout", "32").unwrap();
        assert!(validate("gossip_fanout", "0").is_err());
        assert!(validate("gossip_fanout", "33").is_err());
        // IP literals still pass.
        validate("bootstrap", "[\"127.0.0.1:7443\"]").unwrap();
        validate("advertise", "127.0.0.1:7443").unwrap();
        // DNS names pass offline (never resolved here).
        validate("bootstrap", "[\"peer.example.com:7443\"]").unwrap();
        validate("bootstrap", "[\"peer.example.com:7443\",\"10.0.0.2:7443\"]").unwrap();
        validate("advertise", "peer.example.com:7443").unwrap();
        validate("advertise", "").unwrap();
        // Bad names/ports fail offline.
        assert!(validate("bootstrap", "[\"-bad.example.com:7443\"]").is_err());
        assert!(validate("bootstrap", "[\"peer.example.com:0\"]").is_err());
        assert!(validate("bootstrap", "[\"peer.example.com\"]").is_err());
        assert!(validate("bootstrap", "[\"exa mple.com:7443\"]").is_err());
        assert!(validate("advertise", "peer.example.com:99999").is_err());
        // bind stays IP-only.
        assert!(validate("bind", "peer.example.com:7443").is_err());
        // Resolution runs at load, not validate.
        let addrs = resolve_socket_or_name("localhost:7443").unwrap();
        assert!(addrs.iter().all(|a| a.port() == 7443));
        assert!(!addrs.is_empty());
        assert!(resolve_socket_or_name("peer.invalid:7443").is_err());
    }

    #[test]
    fn schema_pragma_defaults_and_ranges() {
        assert_eq!(default_value("accept_remote_schema"), Some("1"));
        assert_eq!(default_value("schema_master"), Some(""));
        assert_eq!(default_value("schema_master_enabled"), Some("0"));
        validate("accept_remote_schema", "0").unwrap();
        validate("accept_remote_schema", "1").unwrap();
        assert!(validate("accept_remote_schema", "2").is_err());
        assert!(validate("accept_remote_schema", "true").is_err());
        validate("schema_master_enabled", "1").unwrap();
        assert!(validate("schema_master_enabled", "").is_err());
        validate("schema_master", "").unwrap();
        validate("schema_master", "00000000000000000000000000000001").unwrap();
        assert!(validate("schema_master", "zz").is_err());
    }

    #[test]
    fn kdf_pragma_defaults_and_ranges() {
        assert_eq!(default_value("kdf"), Some("direct"));
        assert_eq!(default_value("kdf_time"), Some("3"));
        assert_eq!(default_value("kdf_memory_kib"), Some("65536"));
        assert_eq!(default_value("kdf_threads"), Some("4"));
        validate("kdf", "direct").unwrap();
        validate("kdf", "argon2id").unwrap();
        assert!(validate("kdf", "scrypt").is_err());
        assert!(validate("kdf", "").is_err());
        validate("kdf_time", "1").unwrap();
        validate("kdf_time", "32").unwrap();
        assert!(validate("kdf_time", "0").is_err());
        assert!(validate("kdf_time", "33").is_err());
        validate("kdf_memory_kib", "1024").unwrap();
        validate("kdf_memory_kib", "16777216").unwrap();
        assert!(validate("kdf_memory_kib", "1023").is_err());
        assert!(validate("kdf_memory_kib", "16777217").is_err());
        validate("kdf_threads", "1").unwrap();
        validate("kdf_threads", "32").unwrap();
        assert!(validate("kdf_threads", "0").is_err());
        assert!(validate("kdf_threads", "x").is_err());
    }

    #[test]
    fn files_pragma_defaults_ranges_and_inert_dir() {
        // All 12 delegate to murmur-files adapt (defaults + ranges).
        for (full, default) in murmur_files::adapt::PRAGMAS {
            let stripped = full.strip_prefix("murmur_").unwrap();
            assert_eq!(default_value(stripped), Some(*default), "{stripped}");
            validate(stripped, default).unwrap();
        }
        validate("bridge_role", "low").unwrap();
        assert!(validate("bridge_role", "sideways").is_err());
        assert!(validate("fetch_bind", "").is_ok());
        assert!(validate("fetch_bind", "127.0.0.1:7444").is_ok());
        assert!(validate("fetch_bind", "nope").is_err());
        assert!(validate("bridge_stream", "a-b_c9").is_ok());
        assert!(validate("bridge_stream", "has space").is_err());
        assert!(validate("max_file_bytes", "1048575").is_err());
        assert!(validate("fetch_concurrency", "65").is_err());
        // Coupled rule: staging must cover one bundle.
        let get = |pairs: Vec<(String, String)>| {
            move |name: &str| {
                pairs
                    .iter()
                    .find(|(n, _)| n == name)
                    .map(|(_, v)| v.clone())
            }
        };
        murmur_files::adapt::validate_coupled(&get(vec![])).unwrap();
        assert!(murmur_files::adapt::validate_coupled(&get(vec![(
            "murmur_bridge_staging_bytes".to_string(),
            "4096".to_string()
        )]))
        .is_err());
        // Deprecated-inert: accepted, never validated, never consumed.
        assert_eq!(default_value("files_dir"), Some(""));
        validate("files_dir", "/anything/at/all").unwrap();
        validate("files_dir", "").unwrap();
    }

    #[test]
    fn retention_pragma_defaults_and_ranges() {
        assert_eq!(default_value("retention_min_kept"), Some("1000"));
        assert_eq!(
            default_value("retention_max_offline_ms"),
            Some("604800000")
        );
        assert_eq!(default_value("retention_gc"), Some("1"));
        validate("retention_gc", "0").unwrap();
        validate("retention_gc", "1").unwrap();
        assert!(validate("retention_gc", "2").is_err());
        validate("retention_min_kept", "1").unwrap();
        validate("retention_min_kept", "1000000").unwrap();
        assert!(validate("retention_min_kept", "0").is_err());
        assert!(validate("retention_min_kept", "1000001").is_err());
        validate("retention_max_offline_ms", "60000").unwrap();
        validate("retention_max_offline_ms", "2592000000").unwrap();
        assert!(validate("retention_max_offline_ms", "59999").is_err());
        assert!(validate("retention_max_offline_ms", "2592000001").is_err());
        let db = memdb();
        assert_eq!(setting(&db, "retention_min_kept").unwrap(), "1000");
    }
}

#[cfg(test)]
mod pem_compatibility_tests {
    use super::*;
    use rustls::pki_types::PrivateKeyDer;

    #[test]
    fn supported_key_labels_missing_keys_and_malformed_pem() {
        // PEM decoding identifies the container; TLS validates the DER later.
        for label in ["RSA PRIVATE KEY", "EC PRIVATE KEY", "PRIVATE KEY"] {
            let pem = format!("-----BEGIN {label}-----\nAQID\n-----END {label}-----\n");
            let key = private_key(pem.as_bytes()).unwrap();
            assert_eq!(key.secret_der(), &[1, 2, 3]);
            assert!(matches!((&key, label),
                (PrivateKeyDer::Pkcs1(_), "RSA PRIVATE KEY") |
                (PrivateKeyDer::Sec1(_), "EC PRIVATE KEY") |
                (PrivateKeyDer::Pkcs8(_), "PRIVATE KEY")));
        }
        assert!(matches!(private_key(b""), Err(Error::Invalid(_))));
        assert!(matches!(private_key(b"-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----"), Err(Error::Invalid(_))));
        for malformed in [
            "-----BEGIN PRIVATE KEY-----\n!!!!\n-----END PRIVATE KEY-----",
            "-----BEGIN PRIVATE KEY-----\nAQID\n",
        ] {
            assert!(private_key(malformed.as_bytes()).is_err());
        }
        let cert = rcgen::CertificateParams::new(vec!["localhost".into()]).unwrap()
            .self_signed(&rcgen::KeyPair::generate().unwrap()).unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), format!("{}{}", cert.pem(), cert.pem())).unwrap();
        assert_eq!(certificates(file.path().to_str().unwrap()).unwrap(), vec![cert.der().to_vec(); 2]);
        std::fs::write(file.path(), b"").unwrap();
        assert!(certificates(file.path().to_str().unwrap()).is_err());
    }
}
