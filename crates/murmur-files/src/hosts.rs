//! Host lifecycles: file store, fetch driver, and bridge.
//!
//! [`Hosts`] is the single object the lead's wiring holds: it opens the
//! SQLite-backed file objects against the shared [`Backend`](murmur_tx::Backend)
//! and the injected [`ChunkStore`](crate::store::ChunkStore), applies WP-D key
//! material and trust in memory only, and runs the throttled fetch tick on the
//! shared maintenance thread. Async payload I/O runs on one lazily created
//! runtime thread, and only while bridge publish is used (default-disabled
//! installs create zero threads).
//!
//! Slice 3: the fetch service and bridge file flows await the lead's generic
//! store split in canonical murmur-tx; their entry points fail closed with a
//! pending-split error until then. No filesystem directory backs the file
//! store (constraint 9).

use crate::adapt::{self, Knobs};
use crate::objects::SqliteObjects;
use crate::store::ChunkStore;
use crate::{Error, NodeId, Result, bridge, files};
use murmur_tx::Backend;
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Fetch service over the SQLite object store (the §6 split): the generic
/// [`murmur_tx::filefetch::Service`] instantiated with the `FetchObjects`
/// backend from `fetch_sqlite.rs`.
type FetchService = murmur_tx::filefetch::Service<SqliteObjects<Arc<dyn ChunkStore>>>;

pub struct Hosts {
    backend: Arc<dyn Backend>,
    store: Arc<dyn ChunkStore>,
    objects: Mutex<Option<Arc<SqliteObjects<Arc<dyn ChunkStore>>>>>,
    bridge: Mutex<Option<bridge::Bridge>>,
    runtime: Mutex<Option<tokio::runtime::Runtime>>,
    fetch: Mutex<Option<FetchService>>,
}

impl Hosts {
    /// Open the hosts against the shared backend and the lead's `rusqlite`
    /// chunk store (tests inject the in-memory store).
    pub fn open(backend: Arc<dyn Backend>, store: Arc<dyn ChunkStore>) -> Self {
        Self {
            backend,
            store,
            objects: Mutex::new(None),
            bridge: Mutex::new(None),
            runtime: Mutex::new(None),
            fetch: Mutex::new(None),
        }
    }

    pub fn backend(&self) -> &Arc<dyn Backend> {
        &self.backend
    }

    fn runtime(&self) -> Result<tokio::runtime::Handle> {
        let mut slot = self.runtime.lock().unwrap();
        if slot.is_none() {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("murmur-files")
                .enable_all()
                .build()
                .map_err(|e| Error::Unavailable(format!("file runtime: {e}")))?;
            *slot = Some(runtime);
        }
        Ok(slot.as_ref().unwrap().handle().clone())
    }

    pub fn objects(&self) -> Result<Arc<SqliteObjects<Arc<dyn ChunkStore>>>> {
        self.objects
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| Error::Invalid("file storage is not configured".into()))
    }

    pub fn with_bridge<R>(&self, f: impl FnOnce(&bridge::Bridge) -> Result<R>) -> Result<R> {
        let guard = self.bridge.lock().unwrap();
        let bridge = guard
            .as_ref()
            .ok_or_else(|| Error::Invalid("bridge is not configured".into()))?;
        f(bridge)
    }

    /// Open (or re-open) the file store over the injected chunk store. All
    /// file pragmas are restart-applied; the lead recreates hosts on reopen,
    /// so in-place reconfiguration only serves tests and explicit operator
    /// resets. The bridge file attachment lands with the lead's store split.
    pub fn configure_files(&self, config: crate::objects::ObjectsConfig) -> Result<()> {
        let objects = Arc::new(SqliteObjects::open(
            config,
            self.backend.clone(),
            self.store.clone(),
        )?);
        *self.objects.lock().unwrap() = Some(objects);
        Ok(())
    }

    fn install_bridge(&self, config: bridge::Config) -> Result<()> {
        let bridge = bridge::Bridge::open(config, self.backend.clone())?;
        *self.bridge.lock().unwrap() = Some(bridge);
        Ok(())
    }

    /// Configure a low-side exporter. Secrets arrive from WP-D via the caller
    /// and stay in memory; the pragma role must already read `low`.
    pub fn configure_bridge_exporter(
        &self,
        knobs: &Knobs,
        db_parent: &Path,
        signer: [u8; 32],
        recipient: [u8; 32],
    ) -> Result<()> {
        if adapt::bridge_role(knobs)? != Some(bridge::Role::LowExporter) {
            return Err(Error::Invalid(format!(
                "{} expects low for an exporter",
                adapt::BRIDGE_ROLE
            )));
        }
        let stream = adapt::bridge_stream(knobs)?.expect("role checked");
        let mut config = bridge::Config::exporter(
            self.backend.identity().0,
            adapt::bridge_dir(knobs, db_parent),
            stream,
            signer,
            recipient,
        );
        adapt::apply_bridge_limits(&mut config, knobs)?;
        self.install_bridge(config)
    }

    /// Configure a high-side receiver. Secrets and trust arrive from WP-D via
    /// the caller and stay in memory; the pragma role must read `high`.
    pub fn configure_bridge_receiver(
        &self,
        knobs: &Knobs,
        db_parent: &Path,
        secret: [u8; 32],
        trusted_domains: BTreeSet<NodeId>,
        trusted_signers: BTreeMap<NodeId, [u8; 32]>,
        trusted_origins: BTreeMap<NodeId, [u8; 32]>,
    ) -> Result<()> {
        if adapt::bridge_role(knobs)? != Some(bridge::Role::HighReceiver) {
            return Err(Error::Invalid(format!(
                "{} expects high for a receiver",
                adapt::BRIDGE_ROLE
            )));
        }
        let stream = adapt::bridge_stream(knobs)?.expect("role checked");
        let mut config = bridge::Config::receiver(
            self.backend.identity().0,
            adapt::bridge_dir(knobs, db_parent),
            stream,
            secret,
            trusted_domains,
            trusted_signers,
            trusted_origins,
        );
        adapt::apply_bridge_limits(&mut config, knobs)?;
        self.install_bridge(config)
    }

    /// Apply WP-D key material and trust from a JSON document, in memory only.
    /// Schema v1 (see USAGE.md): exporter
    /// `{"role":"low","signer":hex64,"recipient":hex64}`; receiver
    /// `{"role":"high","receiver_secret":hex64,"trusted_domains":[hex32],
    /// "trusted_signers":{hex32:hex64},"trusted_origins":{hex32:hex64}}`
    /// (trust maps optional, default empty). The JSON role must match the
    /// `murmur_bridge_role` pragma. Nothing here is persisted; transient
    /// copies of the input text exist while parsing.
    pub fn configure_bridge_json(&self, knobs: &Knobs, db_parent: &Path, json: &str) -> Result<()> {
        let value: serde_json::Value = serde_json::from_str(json)
            .map_err(|e| Error::Invalid(format!("bridge configure: {e}")))?;
        let role = value
            .get("role")
            .and_then(|role| role.as_str())
            .ok_or_else(|| Error::Invalid("bridge configure: role".into()))?;
        let expected = adapt::bridge_role(knobs)?
            .ok_or_else(|| Error::Invalid("bridge role is not configured".into()))?;
        match (role, expected) {
            ("low", bridge::Role::LowExporter) => {
                let signer = adapt::decode_hex_32(json_str(&value, "signer")?, "bridge signer")?;
                let recipient =
                    adapt::decode_hex_32(json_str(&value, "recipient")?, "bridge recipient")?;
                self.configure_bridge_exporter(knobs, db_parent, signer, recipient)
            }
            ("high", bridge::Role::HighReceiver) => {
                let secret = adapt::decode_hex_32(
                    json_str(&value, "receiver_secret")?,
                    "bridge receiver secret",
                )?;
                let domains = json_hex16_set(&value, "trusted_domains")?;
                let signers = json_hex_map(&value, "trusted_signers")?;
                let origins = json_hex_map(&value, "trusted_origins")?;
                self.configure_bridge_receiver(knobs, db_parent, secret, domains, signers, origins)
            }
            _ => Err(Error::Invalid("bridge configure: role mismatch".into())),
        }
    }

    /// Start the fetch service over the SQLite object store. Guard order:
    /// disabled bind, unconfigured storage, missing engine TLS credentials
    /// (the transport needs a chain + roots; checked here so a bare test or
    /// misconfigured engine fails with this error instead of panicking
    /// inside endpoint setup). Idempotent: starting twice is a no-op.
    /// Fetch dials come from the file pragmas (constraint 8): the engine
    /// config is cloned and its file section is rebuilt from `knobs`, so a
    /// stale archived section can never leak a directory or key into the
    /// SQLite path (the backend never reads them).
    pub fn start_fetch(&self, knobs: &Knobs, engine: &murmur_tx::Config) -> Result<()> {
        if !adapt::fetch_enabled(knobs) {
            return Err(Error::Invalid("file fetch is disabled".into()));
        }
        let objects = self.objects()?;
        if engine.credentials.chain.is_empty() || engine.credentials.roots.is_empty() {
            return Err(Error::Invalid(
                "file fetch requires engine TLS credentials".into(),
            ));
        }
        if self.fetch.lock().unwrap().is_some() {
            return Ok(());
        }
        let fetch_bind: SocketAddr = knobs
            .fetch_bind
            .parse()
            .map_err(|_| Error::Invalid("murmur_fetch_bind expects host:port".into()))?;
        let mut files = murmur_tx::files::Config::new(PathBuf::new(), [0; 32], fetch_bind);
        files.max_file_bytes = knobs.max_file_bytes;
        files.max_staging_bytes = knobs.max_staging_bytes;
        files.concurrent_fetches = knobs.fetch_concurrency;
        files.fetch_interval = knobs.fetch_interval;
        files
            .validate()
            .map_err(|e| Error::Invalid(format!("file fetch limits: {e}")))?;
        let mut transport = engine.clone();
        transport.files = Some(files);
        let handle = self.runtime()?;
        let service = handle.block_on(async {
            murmur_tx::filefetch::Service::start(&transport, self.backend.clone(), objects)
        })?;
        *self.fetch.lock().unwrap() = Some(service);
        Ok(())
    }

    /// Run one throttled fetch round against `sources`. Reports idle
    /// (`Ok(false)`) until the service starts; otherwise launches the round
    /// and reports `Ok(true)`. Errors (endpoint down, overloaded staging)
    /// propagate — the maintenance tick treats them as best-effort.
    pub fn fetch_tick(&self, sources: Vec<(NodeId, SocketAddr)>) -> Result<bool> {
        let service = match self.fetch.lock().unwrap().clone() {
            Some(service) => service,
            None => return Ok(false),
        };
        let handle = self.runtime()?;
        handle.block_on(service.fetch_missing(sources))?;
        Ok(true)
    }

    /// Stop the fetch service: close the endpoint (which ends the accept
    /// loop) and drop the service. No-op when never started.
    pub fn stop_fetch(&self) {
        let service = self.fetch.lock().unwrap().take();
        if let Some(service) = service {
            service.endpoint.close(0u32.into(), b"file fetch stopped");
        }
    }

    /// Publish one bundle file over FTP/FTPS. Roots and timeout are per-call
    /// (never persisted): `ftps://` requires pinned roots, `ftp://` ignores
    /// them and uses the 30s default timeout. A zero timeout selects 30s.
    pub fn publish_ftp(
        &self,
        path: &Path,
        url: &str,
        roots_pem: Vec<u8>,
        timeout: Duration,
    ) -> Result<()> {
        let tls = murmur_tx::ftp::FtpOptions { roots_pem, timeout };
        let reference = if url.starts_with("ftps://") {
            Some(&tls)
        } else {
            None
        };
        let handle = self.runtime()?;
        self.with_bridge(|bridge| handle.block_on(bridge.publish_ftp(path, url, reference)))
    }

    pub fn publish_http(&self, path: &Path, url: &str) -> Result<()> {
        let handle = self.runtime()?;
        self.with_bridge(|bridge| handle.block_on(bridge.publish_http(path, url)))
    }

    pub fn upload(&self, name: &str, path: &Path) -> Result<files::Metadata> {
        let objects = self.objects()?;
        adapt::actions::upload(&objects, name, path)
    }

    pub fn download(&self, name: &str, dest: &Path) -> Result<u64> {
        let objects = self.objects()?;
        adapt::actions::download(&objects, name, dest)
    }

    pub fn read_chunk(&self, name: &str, offset: u64, limit: usize) -> Result<Vec<u8>> {
        let objects = self.objects()?;
        adapt::actions::read_chunk(&objects, name, offset, limit)
    }

    pub fn delete(&self, name: &str) -> Result<()> {
        self.objects()?.delete(name)
    }

    pub fn list(&self, pattern: Option<&str>) -> Result<Vec<files::Metadata>> {
        let objects = self.objects()?;
        adapt::actions::list(&objects, pattern)
    }

    pub fn metadata(&self, name: &str) -> Result<Option<files::Metadata>> {
        self.objects()?.metadata(name)
    }

    pub fn availability(&self, name: &str) -> Result<String> {
        let objects = self.objects()?;
        adapt::actions::availability(&objects, name)
    }

    pub fn last_error(&self) -> Result<Option<String>> {
        let objects = self.objects()?;
        Ok(adapt::actions::last_error(&objects))
    }

    pub fn collect(&self, grace_ms: u64) -> Result<usize> {
        let objects = self.objects()?;
        adapt::actions::collect(&objects, grace_ms)
    }

    pub fn rotate(&self, limit: usize) -> Result<usize> {
        let objects = self.objects()?;
        adapt::actions::rotate(&objects, limit)
    }

    pub fn rotation_status(&self) -> Result<files::KeyRotationStatus> {
        self.objects()?.key_rotation_status()
    }

    pub fn bridge_export(&self, payload_hex: &str) -> Result<PathBuf> {
        self.with_bridge(|bridge| adapt::actions::bridge_export(bridge, payload_hex))
    }

    pub fn bridge_process(&self) -> Result<usize> {
        self.with_bridge(|bridge| adapt::actions::bridge_process(bridge))
    }

    pub fn bridge_capture(&self) -> Result<usize> {
        self.with_bridge(|bridge| adapt::actions::bridge_capture(bridge))
    }

    pub fn bridge_stage(&self, path: &Path) -> Result<bridge::Header> {
        self.with_bridge(|bridge| adapt::actions::bridge_stage(bridge, path))
    }

    pub fn bridge_export_file(&self, name: &str) -> Result<()> {
        self.with_bridge(|bridge| adapt::actions::bridge_export_file(bridge, name))
    }

    pub fn bridge_export_snapshot(&self) -> Result<PathBuf> {
        self.with_bridge(|bridge| adapt::actions::bridge_export_snapshot(bridge))
    }

    pub fn bridge_release(&self, table: &str, key_hex: &str, column: Option<&str>) -> Result<bool> {
        self.with_bridge(|bridge| adapt::actions::bridge_release(bridge, table, key_hex, column))
    }
}

fn json_str<'a>(value: &'a serde_json::Value, field: &'static str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(|entry| entry.as_str())
        .ok_or_else(|| Error::Invalid(format!("bridge configure: {field}")))
}

fn json_hex16_set(value: &serde_json::Value, field: &'static str) -> Result<BTreeSet<NodeId>> {
    let Some(list) = value.get(field) else {
        return Ok(BTreeSet::new());
    };
    let list = list
        .as_array()
        .ok_or_else(|| Error::Invalid(format!("bridge configure: {field}")))?;
    list.iter()
        .map(|entry| {
            entry
                .as_str()
                .ok_or_else(|| Error::Invalid(format!("bridge configure: {field}")))
                .and_then(|text| adapt::decode_hex_16(text, "bridge node"))
        })
        .collect()
}

fn json_hex_map(
    value: &serde_json::Value,
    field: &'static str,
) -> Result<BTreeMap<NodeId, [u8; 32]>> {
    let Some(map) = value.get(field) else {
        return Ok(BTreeMap::new());
    };
    let map = map
        .as_object()
        .ok_or_else(|| Error::Invalid(format!("bridge configure: {field}")))?;
    map.iter()
        .map(|(key, entry)| {
            let node = adapt::decode_hex_16(key, "bridge node")?;
            let public = entry
                .as_str()
                .ok_or_else(|| Error::Invalid(format!("bridge configure: {field}")))
                .and_then(|text| adapt::decode_hex_32(text, "bridge key"))?;
            Ok((node, public))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::MemoryStore;
    use crate::stub::Stub;
    use std::collections::HashMap;

    const SIGNER: [u8; 32] = [0x51; 32];
    const RECEIVER_SECRET: [u8; 32] = [0xbe; 32];

    fn knobs(pairs: &[(&str, &str)]) -> Knobs {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        Knobs::from_map(&|name| map.get(name).cloned()).unwrap()
    }

    fn files_hosts(tag: &str) -> (Hosts, tempfile::TempDir, PathBuf) {
        let store: Arc<dyn ChunkStore> = Arc::new(MemoryStore::new());
        let hosts = Hosts::open(Arc::new(Stub::new()), store);
        let temp = tempfile::TempDir::with_prefix(tag).unwrap();
        let parent = temp.path().to_path_buf();
        let config = adapt::objects_config(&knobs(&[]), [7; 32], None).unwrap();
        hosts.configure_files(config).unwrap();
        (hosts, temp, parent)
    }

    fn exporter(tag: &str) -> (Hosts, tempfile::TempDir, PathBuf) {
        let store: Arc<dyn ChunkStore> = Arc::new(MemoryStore::new());
        let hosts = Hosts::open(Arc::new(Stub::new()), store);
        let temp = tempfile::TempDir::with_prefix(tag).unwrap();
        let parent = temp.path().to_path_buf();
        let knobs = knobs(&[(adapt::BRIDGE_ROLE, "low"), (adapt::BRIDGE_STREAM, "main")]);
        hosts
            .configure_bridge_exporter(
                &knobs,
                &parent,
                SIGNER,
                bridge::recipient_public(RECEIVER_SECRET),
            )
            .unwrap();
        (hosts, temp, parent)
    }

    fn receiver(tag: &str) -> (Hosts, Arc<Stub>, tempfile::TempDir, PathBuf) {
        let backend = Arc::new(Stub::new());
        let store: Arc<dyn ChunkStore> = Arc::new(MemoryStore::new());
        let hosts = Hosts::open(backend.clone(), store);
        let temp = tempfile::TempDir::with_prefix(tag).unwrap();
        let parent = temp.path().to_path_buf();
        let knobs = knobs(&[(adapt::BRIDGE_ROLE, "high"), (adapt::BRIDGE_STREAM, "main")]);
        let (database, node) = backend.identity();
        let mut domains = BTreeSet::new();
        domains.insert(database);
        let mut signers = BTreeMap::new();
        signers.insert(node, murmur_tx::protocol::public_key(&SIGNER));
        hosts
            .configure_bridge_receiver(
                &knobs,
                &parent,
                RECEIVER_SECRET,
                domains,
                signers,
                BTreeMap::new(),
            )
            .unwrap();
        (hosts, backend, temp, parent)
    }

    #[test]
    fn guards_reject_unconfigured_hosts() {
        let store: Arc<dyn ChunkStore> = Arc::new(MemoryStore::new());
        let hosts = Hosts::open(Arc::new(Stub::new()), store);
        assert!(hosts.objects().is_err());
        assert!(hosts.upload("a", Path::new("/none")).is_err());
        assert!(hosts.with_bridge(|_| Ok(())).is_err());
        assert!(!hosts.fetch_tick(vec![]).unwrap());
        assert!(hosts.bridge_export("00").is_err());
    }

    #[test]
    fn files_round_trip() {
        let (hosts, _temp, parent) = files_hosts("murmur-files-rt");
        let source = parent.join("source.bin");
        let content: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&source, &content).unwrap();
        let meta = hosts.upload("report", &source).unwrap();
        assert_eq!(meta.bytes, 4096);
        assert!(!meta.deleted);
        let stored = hosts.metadata("report").unwrap().unwrap();
        assert_eq!(stored.digest, meta.digest);
        let head = hosts.read_chunk("report", 0, 16).unwrap();
        assert_eq!(head, content[..16]);
        let mid = hosts.read_chunk("report", 100, 32).unwrap();
        assert_eq!(mid, content[100..132]);
        assert!(hosts.read_chunk("report", 99999, 16).unwrap().is_empty());
        assert!(hosts.read_chunk("report", 0, 0).is_err());
        assert!(hosts.read_chunk("report", 0, (1 << 20) + 1).is_err());
        assert_eq!(hosts.list(None).unwrap().len(), 1);
        assert_eq!(hosts.list(Some("rep")).unwrap().len(), 1);
        assert!(hosts.list(Some("zzz")).unwrap().is_empty());
        let dest = parent.join("dest.bin");
        assert_eq!(hosts.download("report", &dest).unwrap(), 4096);
        assert_eq!(std::fs::read(&dest).unwrap(), content);
        assert_eq!(hosts.availability("report").unwrap(), "local");
        assert_eq!(hosts.availability("missing").unwrap(), "unavailable");
        let status = hosts.rotation_status().unwrap();
        assert_eq!(status.current_generation, 0);
        assert!(status.previous_key_unused_locally);
        hosts.delete("report").unwrap();
        assert!(hosts.list(None).unwrap().is_empty());
        assert_eq!(hosts.availability("report").unwrap(), "unavailable");
        assert_eq!(hosts.collect(0).unwrap(), 1);
        assert!(hosts.last_error().unwrap().is_none());
    }

    #[test]
    fn fetch_start_rejects_disabled_and_unconfigured() {
        let store: Arc<dyn ChunkStore> = Arc::new(MemoryStore::new());
        let hosts = Hosts::open(Arc::new(Stub::new()), store);
        let engine = bare_engine();
        let err = hosts.start_fetch(&knobs(&[]), &engine).unwrap_err();
        assert!(err.to_string().contains("disabled"), "{err}");
        let on = knobs(&[(adapt::FETCH_BIND, "127.0.0.1:0")]);
        let err = hosts.start_fetch(&on, &engine).unwrap_err();
        assert!(err.to_string().contains("not configured"), "{err}");
        assert!(!hosts.fetch_tick(vec![]).unwrap());
        hosts.stop_fetch();
    }

    // Self-signed P-256 test certificate (DER hex) minted for node
    // 1111..11, matching `Stub::new().identity().1` so the fetch
    // transport accepts it. Generated once with openssl (offline):
    // req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 with a
    // URI:replicateddb://node/1111..11 SAN and NO other extensions
    // (-config with an empty -extensions section); key exported
    // PKCS#8 DER. Plain end-entity form is REQUIRED: a CA:TRUE
    // self-signed cert is rejected as a TLS leaf (CaUsedAsEndEntity),
    // while the bare cert still anchors peer verification by SPKI.
    // Fixture bytes, not generated per run.
    const TEST_CERT_HEX: &str = concat!(
        "3082019c30820142a0030201020214288f92c4bcc1c472a3bd2f865d9a66b3760d3ec5300a06082a8648ce3d040302301c311a301806035504030c11",
        "6d75726d75722d66657463682d74657374301e170d3236313032383232313334375a170d3336313032353232313334375a301c311a30180603550403",
        "0c116d75726d75722d66657463682d746573743059301306072a8648ce3d020106082a8648ce3d03010703420004f22e2d14d0c32a04f36c7a863958",
        "50e4dc1d26a8f2db7caf24a4d204854ab4581a65cdff26dcc5fb293b9f89e85bda3fa1f0c6c377cf273f403ce4389a844346a3623060303f0603551d",
        "110438303686347265706c69636174656464623a2f2f6e6f64652f313131313131313131313131313131313131313131313131313131313131313130",
        "1d0603551d0e041604144182d0ce1f4761c00dcf50652c06d2e64e4eed8a300a06082a8648ce3d0403020348003045022100a9196d5234d6a4ed34cd",
        "9213da6d53f672d84919d005ac2ed236bab350650c86022076b3ed1b95a37fa749f3dceb34932bef9d20051c3bf3d20cd1d49303494e9c73",
    );
    const TEST_KEY_HEX: &str = concat!(
        "308187020100301306072a8648ce3d020106082a8648ce3d030107046d306b02010104203deb2d7498355af71dd2e58325d05a550a95b1cd6fe2d0e4",
        "48ea200f00876ae3a14403420004f22e2d14d0c32a04f36c7a86395850e4dc1d26a8f2db7caf24a4d204854ab4581a65cdff26dcc5fb293b9f89e85b",
        "da3fa1f0c6c377cf273f403ce4389a844346",
    );

    fn hex_bytes(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }

    fn bare_engine() -> murmur_tx::Config {
        murmur_tx::Config::new(
            "127.0.0.1:0".parse().unwrap(),
            murmur_tx::Credentials {
                chain: Vec::new(),
                private_key: zeroize::Zeroizing::new(Vec::new()),
                roots: Vec::new(),
            },
            [0; 32],
        )
    }

    fn test_engine() -> murmur_tx::Config {
        let chain = vec![hex_bytes(TEST_CERT_HEX)];
        murmur_tx::Config::new(
            "127.0.0.1:0".parse().unwrap(),
            murmur_tx::Credentials {
                chain: chain.clone(),
                private_key: zeroize::Zeroizing::new(hex_bytes(TEST_KEY_HEX)),
                roots: chain,
            },
            [0; 32],
        )
    }

    // Self-signed P-256 node-22 certificate (DER hex), minted with the
    // same offline openssl recipe as TEST_CERT_HEX (P-256, 10y, SAN
    // only — plain end-entity, see the CA note above; key PKCS#8 DER).
    // A and B cross-trust: each side's roots hold BOTH certs, so the
    // two-node transfer test needs no CA. Fixture bytes, not generated
    // per run.
    const TEST_B_CERT_HEX: &str = concat!(
        "3082019c30820142a0030201020214356c24e5a9904fd2792fee1e218e2d6d55c4d0cd300a06082a8648ce3d040302301c311a301806035504030c11",
        "6d75726d75722d66657463682d74657374301e170d3236313032383232313334385a170d3336313032353232313334385a301c311a30180603550403",
        "0c116d75726d75722d66657463682d746573743059301306072a8648ce3d020106082a8648ce3d030107034200043ea9402a17a1c5a316d96d6dd6d3",
        "e9a80141af4729e816995c8d73169fc6d397944ea7f9e714243300c018759bdfbffe52023b7e9bc5bf38e0068f569cf75a9aa3623060303f0603551d",
        "110438303686347265706c69636174656464623a2f2f6e6f64652f323232323232323232323232323232323232323232323232323232323232323230",
        "1d0603551d0e04160414fd047730a45bbb2c0c0c2ac629136eacf98fa5db300a06082a8648ce3d0403020348003045022100be79b213ee695a7eb3db",
        "e947f2f94fa83be49e6bd3615ebc6941b34d7ea0f2010220604408bf7068f29ffa73dc2bc6fa2c60a1f6694e2a692c5401aadfb2841e18d0",
    );
    const TEST_B_KEY_HEX: &str = concat!(
        "308187020100301306072a8648ce3d020106082a8648ce3d030107046d306b0201010420dcec9d1f26d09b4fadfc766fb99aa28df45ce9403ec6089f",
        "38d267d9bca9dd34a144034200043ea9402a17a1c5a316d96d6dd6d3e9a80141af4729e816995c8d73169fc6d397944ea7f9e714243300c018759bdf",
        "bffe52023b7e9bc5bf38e0068f569cf75a9a",
    );

    /// Cross-trusting engine pair for the two-node transfer test: each
    /// side presents its own cert and trusts both bare anchors (no CA —
    /// the fixtures are plain end-entity certs, see above).
    fn test_engine_pair() -> (murmur_tx::Config, murmur_tx::Config) {
        let cert_a = hex_bytes(TEST_CERT_HEX);
        let cert_b = hex_bytes(TEST_B_CERT_HEX);
        let roots = vec![cert_a.clone(), cert_b.clone()];
        let mk = |chain: Vec<u8>, key: Vec<u8>| {
            murmur_tx::Config::new(
                "127.0.0.1:0".parse().unwrap(),
                murmur_tx::Credentials {
                    chain: vec![chain],
                    private_key: zeroize::Zeroizing::new(key),
                    roots: roots.clone(),
                },
                [0; 32],
            )
        };
        (
            mk(cert_a, hex_bytes(TEST_KEY_HEX)),
            mk(cert_b, hex_bytes(TEST_B_KEY_HEX)),
        )
    }

    #[test]
    fn fetch_start_requires_engine_credentials() {
        let (hosts, _temp, _parent) = files_hosts("murmur-files-creds");
        let on = knobs(&[(adapt::FETCH_BIND, "127.0.0.1:0")]);
        // Bare credentials fail with a clean error, never a panic
        // inside endpoint setup.
        let err = hosts.start_fetch(&on, &bare_engine()).unwrap_err();
        assert!(err.to_string().contains("TLS credentials"), "{err}");
        assert!(!hosts.fetch_tick(vec![]).unwrap());
        hosts.stop_fetch();
    }

    #[test]
    fn fetch_start_tick_stop_live_loop() {
        let (hosts, _temp, _parent) = files_hosts("murmur-files-live");
        let on = knobs(&[(adapt::FETCH_BIND, "127.0.0.1:0")]);
        let engine = test_engine();
        hosts.start_fetch(&on, &engine).unwrap();
        // Idempotent: a second start is a no-op, not a double bind.
        hosts.start_fetch(&on, &engine).unwrap();
        // No objects, no sources: the round launches and finds nothing.
        assert!(hosts.fetch_tick(vec![]).unwrap());
        hosts.stop_fetch();
        assert!(!hosts.fetch_tick(vec![]).unwrap());
        // The endpoint closed cleanly: a restart rebinds fine.
        hosts.start_fetch(&on, &engine).unwrap();
        assert!(hosts.fetch_tick(vec![]).unwrap());
        hosts.stop_fetch();
    }

    #[test]
    fn fetch_start_rejects_identity_mismatch() {
        let store: Arc<dyn ChunkStore> = Arc::new(MemoryStore::new());
        let backend = Arc::new(Stub::with_identity(([0x22; 16], [0x22; 16])));
        let hosts = Hosts::open(backend, store);
        let config = adapt::objects_config(&knobs(&[]), [7; 32], None).unwrap();
        hosts.configure_files(config).unwrap();
        let on = knobs(&[(adapt::FETCH_BIND, "127.0.0.1:0")]);
        // The fixture cert is minted for node 11..11, not 22..22.
        let err = hosts.start_fetch(&on, &test_engine()).unwrap_err();
        assert!(err.to_string().contains("not for this node"), "{err}");
    }

    #[test]
    fn fetch_two_node_live_transfer() {
        // Two nodes, one database: A serves a sealed object over
        // SqliteObjects, B fetches it over a real QUIC connection.
        // Same cluster file key (gen 0) both sides — the digest is a
        // plaintext hash, so identical content addresses identically.
        let store_a: Arc<dyn ChunkStore> = Arc::new(MemoryStore::new());
        let store_b = Arc::new(MemoryStore::new());
        let hosts_a = Hosts::open(Arc::new(Stub::new()), store_a);
        let hosts_b = Hosts::open(
            Arc::new(Stub::with_identity(([0xdb; 16], [0x22; 16]))),
            store_b.clone(),
        );
        let config_a = adapt::objects_config(&knobs(&[]), [7; 32], None).unwrap();
        hosts_a.configure_files(config_a).unwrap();
        let config_b = adapt::objects_config(&knobs(&[]), [7; 32], None).unwrap();
        hosts_b.configure_files(config_b).unwrap();

        let temp = tempfile::TempDir::with_prefix("murmur-files-xfer").unwrap();
        // Two slices (64 KiB each): exercises the multi-chunk loop.
        let content: Vec<u8> = (0..70_000u32).map(|i| (i % 251) as u8).collect();
        let source = temp.path().join("source.bin");
        std::fs::write(&source, &content).unwrap();
        let meta_a = hosts_a.upload("report", &source).unwrap();
        let meta_b = hosts_b.upload("report", &source).unwrap();
        assert_eq!(meta_a.digest, meta_b.digest);
        assert_eq!(meta_a.generation, meta_b.generation);
        // B knows the object but holds no bytes: the fetch trigger
        // (metadata intact, chunk-completeness fails).
        store_b
            .delete_object(&meta_b.digest, meta_b.generation)
            .unwrap();
        assert!(hosts_a.objects().unwrap().available("report").unwrap());
        assert!(!hosts_b.objects().unwrap().available("report").unwrap());

        let (engine_a, engine_b) = test_engine_pair();
        let knobs_a = knobs(&[(adapt::FETCH_BIND, "127.0.0.1:0")]);
        hosts_a.start_fetch(&knobs_a, &engine_a).unwrap();
        // Uniform-port rule: B follows A's ephemeral port on its own
        // loopback (distinct 127.0.0.x, same port — no fixed-port
        // collision risk between parallel suites).
        let port = hosts_a
            .fetch
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .endpoint
            .local_addr()
            .unwrap()
            .port();
        let bind_b = format!("127.0.0.2:{port}");
        let knobs_b = knobs(&[(adapt::FETCH_BIND, bind_b.as_str())]);
        hosts_b.start_fetch(&knobs_b, &engine_b).unwrap();

        // Real sources: B dials A's fetch endpoint and pulls.
        let addr_a: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        assert!(hosts_b.fetch_tick(vec![([0x11; 16], addr_a)]).unwrap());
        // Completion is async (fetch_missing spawns per-object tasks):
        // poll for the bytes with a loud timeout.
        let mut landed = false;
        for _ in 0..200 {
            if hosts_b.objects().unwrap().available("report").unwrap() {
                landed = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert!(
            landed,
            "transfer timed out; B last_error: {:?}",
            hosts_b.last_error().unwrap()
        );
        let dest = temp.path().join("fetched.bin");
        let bytes = hosts_b.download("report", &dest).unwrap();
        assert_eq!(bytes, content.len() as u64);
        assert_eq!(std::fs::read(&dest).unwrap(), content);
        hosts_a.stop_fetch();
        hosts_b.stop_fetch();
    }

    #[test]
    fn bridge_export_stage_process_round_trip() {
        let (exporter, _ete, _ep) = exporter("murmur-files-exp");
        let (receiver, backend, _rte, _rp) = receiver("murmur-files-rec");
        let path = exporter.bridge_export("deadbeef").unwrap();
        assert!(path.is_file());
        let header = receiver.bridge_stage(&path).unwrap();
        assert_eq!(header.sequence, 1);
        assert_eq!(header.stream, "main");
        assert_eq!(receiver.bridge_process().unwrap(), 1);
        assert_eq!(backend.imported(), vec![vec![0xde, 0xad, 0xbe, 0xef]]);
        assert_eq!(receiver.bridge_process().unwrap(), 0);
        assert!(receiver.bridge_release("t", "00", None).unwrap());
        assert!(exporter.bridge_export_snapshot().unwrap().is_file());
    }

    #[test]
    fn bridge_process_quarantines_rejected_artifacts() {
        let (receiver, _backend, _temp, parent) = receiver("murmur-files-q");
        let dir = parent.join("murmur-bridge");
        std::fs::write(
            dir.join("junk-00000000000000000001.bundle"),
            b"not a bundle",
        )
        .unwrap();
        assert_eq!(receiver.bridge_process().unwrap(), 0);
        assert!(
            dir.join("quarantine")
                .join("junk-00000000000000000001.bundle")
                .is_file()
        );
    }

    #[test]
    fn bridge_export_file_needs_attached_files() {
        let (exporter, _temp, _parent) = exporter("murmur-files-ef");
        assert!(exporter.bridge_export_file("report").is_err());
    }

    #[test]
    fn bridge_export_file_pending_store_split() {
        // The canonical bridge stays concrete over the filesystem object store
        // until the lead's split, so no files attach and file export fails
        // closed even with a configured store and exporter.
        let (hosts, _temp, parent) = files_hosts("murmur-files-ef2");
        let bridge_knobs = knobs(&[(adapt::BRIDGE_ROLE, "low"), (adapt::BRIDGE_STREAM, "main")]);
        hosts
            .configure_bridge_exporter(
                &bridge_knobs,
                &parent,
                SIGNER,
                bridge::recipient_public(RECEIVER_SECRET),
            )
            .unwrap();
        let source = parent.join("data.bin");
        std::fs::write(&source, vec![0x33; 3000]).unwrap();
        hosts.upload("data", &source).unwrap();
        let err = hosts.bridge_export_file("data").unwrap_err();
        assert!(
            matches!(&err, Error::Invalid(message) if message.contains("bridge file storage")),
            "unexpected: {err:?}"
        );
    }

    #[test]
    fn bridge_configure_json() {
        let store: Arc<dyn ChunkStore> = Arc::new(MemoryStore::new());
        let hosts = Hosts::open(Arc::new(Stub::new()), store);
        let temp = tempfile::TempDir::with_prefix("murmur-files-json").unwrap();
        let knobs_low = knobs(&[(adapt::BRIDGE_ROLE, "low"), (adapt::BRIDGE_STREAM, "main")]);
        let signer_hex = "51".repeat(32);
        let recipient_hex = murmur_tx::hex(&bridge::recipient_public(RECEIVER_SECRET));
        let json =
            format!(r#"{{"role":"low","signer":"{signer_hex}","recipient":"{recipient_hex}"}}"#);
        hosts
            .configure_bridge_json(&knobs_low, temp.path(), &json)
            .unwrap();
        assert!(hosts.bridge_export("00").is_ok());
        let bad = json.replace("\"low\"", "\"high\"");
        assert!(
            hosts
                .configure_bridge_json(&knobs_low, temp.path(), &bad)
                .is_err()
        );

        let store: Arc<dyn ChunkStore> = Arc::new(MemoryStore::new());
        let hosts = Hosts::open(Arc::new(Stub::new()), store);
        let node_hex = murmur_tx::hex(&[0x11; 16]);
        let domain_hex = murmur_tx::hex(&[0xdb; 16]);
        let pub_hex = murmur_tx::hex(&murmur_tx::protocol::public_key(&SIGNER));
        let secret_hex = "be".repeat(32);
        let knobs_high = knobs(&[(adapt::BRIDGE_ROLE, "high"), (adapt::BRIDGE_STREAM, "main")]);
        let json = format!(
            r#"{{"role":"high","receiver_secret":"{secret_hex}","trusted_domains":["{domain_hex}"],"trusted_signers":{{"{node_hex}":"{pub_hex}"}}}}"#
        );
        hosts
            .configure_bridge_json(&knobs_high, temp.path(), &json)
            .unwrap();
        assert!(
            hosts
                .configure_bridge_json(&knobs_high, temp.path(), "{}")
                .is_err()
        );
    }

    #[test]
    fn publish_entrypoints_reject_bad_input_before_network() {
        let (exporter, _temp, _parent) = exporter("murmur-files-pub");
        let bundle = exporter.bridge_export("aa").unwrap();
        assert!(
            exporter
                .publish_ftp(&bundle, "gopher://x/y", vec![], Duration::ZERO)
                .is_err()
        );
        assert!(
            exporter
                .publish_ftp(&bundle, "ftps://x/y", vec![], Duration::ZERO)
                .is_err()
        );
        assert!(exporter.publish_http(&bundle, "gopher://x").is_err());
        let (receiver, _stub, _temp, _parent) = receiver("murmur-files-pub2");
        assert!(receiver.publish_http(&bundle, "https://x").is_err());
    }
}
