//! PRAGMA spec, pragma-to-config mapping, and SQL-action handler logic.
//!
//! All twelve file/bridge PRAGMAs are restart-applied (lead verdict, WP-C
//! ruling 4): the driver reads effective values at open, and pending edits
//! apply on reopen. Errors reuse [`murmur_tx::Error`]; there is no new error
//! seam. Key material and trust policy arrive through the host API from WP-D /
//! the engine, never through pragmas.
//!
//! Slice 3 (constraint 9): `murmur_files_dir` is dropped — payloads rest in
//! SQLite chunk rows, so no directory pragma exists. `murmur_bridge_dir` stays
//! per the R2 air-gap artifact exception.

use crate::objects::{ObjectsConfig, SqliteObjects};
use crate::store::ChunkStore;
use crate::{Error, Result, bridge, files};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const BRIDGE_DIR: &str = "murmur_bridge_dir";
pub const FILES_GENERATION: &str = "murmur_files_generation";
pub const MAX_FILE_BYTES: &str = "murmur_max_file_bytes";
pub const MAX_STAGING_BYTES: &str = "murmur_max_staging_bytes";
pub const FETCH_CONCURRENCY: &str = "murmur_fetch_concurrency";
pub const FETCH_INTERVAL_MS: &str = "murmur_fetch_interval_ms";
pub const FETCH_BIND: &str = "murmur_fetch_bind";
pub const BRIDGE_ROLE: &str = "murmur_bridge_role";
pub const BRIDGE_STREAM: &str = "murmur_bridge_stream";
pub const BRIDGE_MAX_BUNDLE_BYTES: &str = "murmur_bridge_max_bundle_bytes";
pub const BRIDGE_MAX_PAYLOAD_BYTES: &str = "murmur_bridge_max_payload_bytes";
pub const BRIDGE_STAGING_BYTES: &str = "murmur_bridge_staging_bytes";

pub const DEFAULT_BRIDGE_DIR: &str = "";
pub const DEFAULT_FILES_GENERATION: &str = "0";
pub const DEFAULT_MAX_FILE_BYTES: &str = "4294967296";
pub const DEFAULT_MAX_STAGING_BYTES: &str = "1073741824";
pub const DEFAULT_FETCH_CONCURRENCY: &str = "2";
pub const DEFAULT_FETCH_INTERVAL_MS: &str = "30000";
pub const DEFAULT_FETCH_BIND: &str = "";
pub const DEFAULT_BRIDGE_ROLE: &str = "";
pub const DEFAULT_BRIDGE_STREAM: &str = "";
pub const DEFAULT_BRIDGE_MAX_BUNDLE_BYTES: &str = "16777216";
pub const DEFAULT_BRIDGE_MAX_PAYLOAD_BYTES: &str = "67108864";
pub const DEFAULT_BRIDGE_STAGING_BYTES: &str = "1073741824";

/// (name, default) for every file/bridge pragma. All restart-applied.
pub const PRAGMAS: &[(&str, &str)] = &[
    (BRIDGE_DIR, DEFAULT_BRIDGE_DIR),
    (FILES_GENERATION, DEFAULT_FILES_GENERATION),
    (MAX_FILE_BYTES, DEFAULT_MAX_FILE_BYTES),
    (MAX_STAGING_BYTES, DEFAULT_MAX_STAGING_BYTES),
    (FETCH_CONCURRENCY, DEFAULT_FETCH_CONCURRENCY),
    (FETCH_INTERVAL_MS, DEFAULT_FETCH_INTERVAL_MS),
    (FETCH_BIND, DEFAULT_FETCH_BIND),
    (BRIDGE_ROLE, DEFAULT_BRIDGE_ROLE),
    (BRIDGE_STREAM, DEFAULT_BRIDGE_STREAM),
    (BRIDGE_MAX_BUNDLE_BYTES, DEFAULT_BRIDGE_MAX_BUNDLE_BYTES),
    (BRIDGE_MAX_PAYLOAD_BYTES, DEFAULT_BRIDGE_MAX_PAYLOAD_BYTES),
    (BRIDGE_STAGING_BYTES, DEFAULT_BRIDGE_STAGING_BYTES),
];

pub fn default_value(name: &str) -> Option<&'static str> {
    PRAGMAS
        .iter()
        .find(|(pragma, _)| *pragma == name)
        .map(|(_, default)| *default)
}

fn parse_u64(name: &str, value: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .map_err(|_| Error::Invalid(format!("{name} expects an unsigned integer")))
}

fn check_range(name: &str, value: u64, min: u64, max: u64) -> Result<()> {
    if value < min || value > max {
        return Err(Error::Invalid(format!("{name} out of range")));
    }
    Ok(())
}

/// Intrinsic validation for one pragma value. Coupled rules (bridge staging
/// >= bundle) live in [`validate_coupled`], which sees the whole map.
pub fn validate(name: &str, value: &str) -> Result<()> {
    match name {
        BRIDGE_DIR => Ok(()),
        FILES_GENERATION => {
            parse_u64(FILES_GENERATION, value)?;
            Ok(())
        }
        MAX_FILE_BYTES => check_range(name, parse_u64(name, value)?, 1 << 20, 32 << 30),
        MAX_STAGING_BYTES => check_range(name, parse_u64(name, value)?, 64 << 10, 32 << 30),
        FETCH_CONCURRENCY => check_range(name, parse_u64(name, value)?, 1, 64),
        FETCH_INTERVAL_MS => check_range(name, parse_u64(name, value)?, 1000, 86_400_000),
        FETCH_BIND => {
            if value.is_empty() {
                return Ok(());
            }
            value
                .parse::<SocketAddr>()
                .map_err(|_| Error::Invalid(format!("{name} expects host:port")))?;
            Ok(())
        }
        BRIDGE_ROLE => match value {
            "" | "low" | "high" => Ok(()),
            _ => Err(Error::Invalid(format!("{name} expects \"\", low, or high"))),
        },
        BRIDGE_STREAM => {
            if value.len() > 64
                || !value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            {
                return Err(Error::Invalid(format!(
                    "{name} expects [A-Za-z0-9-_], at most 64"
                )));
            }
            Ok(())
        }
        BRIDGE_MAX_BUNDLE_BYTES => check_range(name, parse_u64(name, value)?, 4 << 10, 16 << 20),
        BRIDGE_MAX_PAYLOAD_BYTES => check_range(name, parse_u64(name, value)?, 4 << 10, 64 << 20),
        BRIDGE_STAGING_BYTES => {
            check_range(name, parse_u64(name, value)?, 4 << 10, u64::MAX)?;
            Ok(())
        }
        _ => Err(Error::Invalid(format!("unknown file pragma {name}"))),
    }
}

/// Coupled validation over the effective map: bridge staging must cover one
/// full bundle. Missing entries read as defaults.
pub fn validate_coupled(get: &dyn Fn(&str) -> Option<String>) -> Result<()> {
    let bundle = parse_u64(
        BRIDGE_MAX_BUNDLE_BYTES,
        &get(BRIDGE_MAX_BUNDLE_BYTES)
            .unwrap_or_else(|| DEFAULT_BRIDGE_MAX_BUNDLE_BYTES.to_string()),
    )?;
    let staging = parse_u64(
        BRIDGE_STAGING_BYTES,
        &get(BRIDGE_STAGING_BYTES).unwrap_or_else(|| DEFAULT_BRIDGE_STAGING_BYTES.to_string()),
    )?;
    if staging < bundle {
        return Err(Error::Invalid(
            "murmur_bridge_staging_bytes must cover murmur_bridge_max_bundle_bytes".into(),
        ));
    }
    Ok(())
}

/// Effective, validated knob set. Built from the persisted/effective pragma
/// map by the lead's config wiring; restart-applied.
#[derive(Clone, Debug)]
pub struct Knobs {
    pub bridge_dir: String,
    pub generation: u64,
    pub max_file_bytes: u64,
    pub max_staging_bytes: u64,
    pub fetch_concurrency: usize,
    pub fetch_interval: Duration,
    pub fetch_bind: String,
    pub bridge_role: String,
    pub bridge_stream: String,
    pub bridge_max_bundle_bytes: usize,
    pub bridge_max_payload_bytes: usize,
    pub bridge_staging_bytes: usize,
}

impl Knobs {
    pub fn from_map(get: &dyn Fn(&str) -> Option<String>) -> Result<Self> {
        let text = |name: &'static str| -> Result<String> {
            let value = get(name)
                .or_else(|| default_value(name).map(str::to_string))
                .ok_or_else(|| Error::Invalid(format!("unknown file pragma {name}")))?;
            validate(name, &value)?;
            Ok(value)
        };
        let number = |name: &'static str| -> Result<u64> { parse_u64(name, &text(name)?) };
        let knobs = Self {
            bridge_dir: text(BRIDGE_DIR)?,
            generation: number(FILES_GENERATION)?,
            max_file_bytes: number(MAX_FILE_BYTES)?,
            max_staging_bytes: number(MAX_STAGING_BYTES)?,
            fetch_concurrency: number(FETCH_CONCURRENCY)? as usize,
            fetch_interval: Duration::from_millis(number(FETCH_INTERVAL_MS)?),
            fetch_bind: text(FETCH_BIND)?,
            bridge_role: text(BRIDGE_ROLE)?,
            bridge_stream: text(BRIDGE_STREAM)?,
            bridge_max_bundle_bytes: number(BRIDGE_MAX_BUNDLE_BYTES)? as usize,
            bridge_max_payload_bytes: number(BRIDGE_MAX_PAYLOAD_BYTES)? as usize,
            bridge_staging_bytes: number(BRIDGE_STAGING_BYTES)? as usize,
        };
        validate_coupled(get)?;
        Ok(knobs)
    }
}

/// Resolve a directory pragma. Empty selects `<db-parent>/<leaf>`; explicit
/// relative paths resolve under the DB parent; absolute paths pass through.
pub fn resolve_dir(value: &str, db_parent: &Path, leaf: &str) -> PathBuf {
    if value.is_empty() {
        return db_parent.join(leaf);
    }
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else {
        db_parent.join(path)
    }
}

pub fn bridge_dir(knobs: &Knobs, db_parent: &Path) -> PathBuf {
    resolve_dir(&knobs.bridge_dir, db_parent, "murmur-bridge")
}

pub fn fetch_enabled(knobs: &Knobs) -> bool {
    !knobs.fetch_bind.trim().is_empty()
}

/// Build the SQLite-backed file-store config. Key material arrives from WP-D
/// via the caller; this maps validated knobs onto store fields only. Fetch
/// dials stay in [`Knobs`] for the lead's fetch-service split; there is no
/// directory (constraint 9).
pub fn objects_config(
    knobs: &Knobs,
    key: [u8; 32],
    previous: Option<[u8; 32]>,
) -> Result<ObjectsConfig> {
    let config = ObjectsConfig {
        key: zeroize::Zeroizing::new(key),
        previous_key: previous.map(zeroize::Zeroizing::new),
        generation: knobs.generation,
        max_file_bytes: knobs.max_file_bytes,
        max_staging_bytes: knobs.max_staging_bytes,
    };
    config.validate()?;
    Ok(config)
}

pub fn bridge_role(knobs: &Knobs) -> Result<Option<bridge::Role>> {
    match knobs.bridge_role.as_str() {
        "" => Ok(None),
        "low" => Ok(Some(bridge::Role::LowExporter)),
        "high" => Ok(Some(bridge::Role::HighReceiver)),
        _ => Err(Error::Invalid(format!(
            "{BRIDGE_ROLE} expects \"\", low, or high"
        ))),
    }
}

/// Validated stream name: `None` while the bridge is disabled, `Some` (always
/// non-empty) once a role is set.
pub fn bridge_stream(knobs: &Knobs) -> Result<Option<String>> {
    if bridge_role(knobs)?.is_none() {
        return Ok(None);
    }
    if knobs.bridge_stream.is_empty() {
        return Err(Error::Invalid(format!(
            "{BRIDGE_STREAM} is required once a bridge role is set"
        )));
    }
    Ok(Some(knobs.bridge_stream.clone()))
}

/// Copy the validated bridge budgets onto an exporter/receiver config built
/// with secrets from WP-D. Re-checks the coupled staging rule defensively.
pub fn apply_bridge_limits(config: &mut bridge::Config, knobs: &Knobs) -> Result<()> {
    if knobs.bridge_staging_bytes < knobs.bridge_max_bundle_bytes {
        return Err(Error::Invalid(
            "murmur_bridge_staging_bytes must cover murmur_bridge_max_bundle_bytes".into(),
        ));
    }
    config.max_bundle_bytes = knobs.bridge_max_bundle_bytes;
    config.max_payload_bytes = knobs.bridge_max_payload_bytes;
    config.staging_bytes = knobs.bridge_staging_bytes;
    Ok(())
}

pub fn decode_hex(text: &str, what: &'static str) -> Result<Vec<u8>> {
    if text.len() % 2 != 0 || !text.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(Error::Invalid(format!("{what} expects hex")));
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16))
        .collect::<std::result::Result<Vec<u8>, _>>()
        .map_err(|_| Error::Invalid(format!("{what} expects hex")))
}

fn decode_hex_array<const N: usize>(text: &str, what: &'static str) -> Result<[u8; N]> {
    let bytes = decode_hex(text, what)?;
    bytes
        .try_into()
        .map_err(|_| Error::Invalid(format!("{what} has the wrong length")))
}

pub fn decode_hex_16(text: &str, what: &'static str) -> Result<[u8; 16]> {
    decode_hex_array(text, what)
}

pub fn decode_hex_32(text: &str, what: &'static str) -> Result<[u8; 32]> {
    decode_hex_array(text, what)
}

/// SQL-action handler logic: pure functions over open stores. The lead's
/// `sql.rs` wiring maps arguments/results onto SQLite values; guards for
/// unconfigured stores live on [`crate::hosts::Hosts`].
pub mod actions {
    use super::*;
    use std::fs::File;
    use std::sync::Arc;

    /// Largest single `murmur_file_read` result: 1 MiB (matches the archive
    /// `Download::next` bound).
    pub const MAX_READ_CHUNK: usize = 1 << 20;

    pub fn upload(
        objects: &SqliteObjects<Arc<dyn ChunkStore>>,
        name: &str,
        path: &Path,
    ) -> Result<files::Metadata> {
        let file =
            File::open(path).map_err(|e| Error::Unavailable(format!("upload source: {e}")))?;
        objects.upload(name, file)
    }

    pub fn download(
        objects: &SqliteObjects<Arc<dyn ChunkStore>>,
        name: &str,
        dest: &Path,
    ) -> Result<u64> {
        let file = private_file(dest)?;
        let meta = objects.read(name, file)?;
        Ok(meta.bytes)
    }

    /// Open `dest` for writing with owner-only permissions, mirroring the
    /// archive staging-file mode (0600 on unix).
    fn private_file(path: &Path) -> Result<File> {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options
            .open(path)
            .map_err(|e| Error::Unavailable(format!("download destination: {e}")))
    }

    /// Read up to `limit` plaintext bytes starting at `offset`.
    pub fn read_chunk(
        objects: &SqliteObjects<Arc<dyn ChunkStore>>,
        name: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<u8>> {
        if limit == 0 || limit > MAX_READ_CHUNK {
            return Err(Error::Invalid("file chunk limit".into()));
        }
        let mut download = objects.download(name)?;
        let mut skip = offset;
        while skip > 0 {
            let (_, chunk, done) = download.next(skip.min(MAX_READ_CHUNK as u64) as usize)?;
            if chunk.is_empty() {
                break;
            }
            skip -= chunk.len() as u64;
            if done {
                break;
            }
        }
        if skip > 0 {
            return Ok(Vec::new());
        }
        let (_, chunk, _) = download.next(limit)?;
        Ok(chunk)
    }

    /// List live metadata, optionally filtered by substring (covers the
    /// archived `search_files` facade; results stay name-sorted).
    pub fn list(
        objects: &SqliteObjects<Arc<dyn ChunkStore>>,
        pattern: Option<&str>,
    ) -> Result<Vec<files::Metadata>> {
        let mut out = objects.list()?;
        if let Some(pattern) = pattern {
            out.retain(|meta| meta.name.contains(pattern));
        }
        Ok(out)
    }

    /// Stable one-line availability rendering for the SQL action.
    pub fn availability(
        objects: &SqliteObjects<Arc<dyn ChunkStore>>,
        name: &str,
    ) -> Result<String> {
        match objects.availability(name)? {
            files::Availability::Local => Ok("local".to_string()),
            files::Availability::Unavailable => Ok("unavailable".to_string()),
            files::Availability::Remote(peers) => Ok(format!(
                "remote:{}",
                peers
                    .iter()
                    .map(|node| murmur_tx::hex(node))
                    .collect::<Vec<_>>()
                    .join(",")
            )),
        }
    }

    pub fn last_error(objects: &SqliteObjects<Arc<dyn ChunkStore>>) -> Option<String> {
        objects.last_fetch_error()
    }

    pub fn collect(objects: &SqliteObjects<Arc<dyn ChunkStore>>, grace_ms: u64) -> Result<usize> {
        objects.collect(Duration::from_millis(grace_ms))
    }

    pub fn rotate(objects: &SqliteObjects<Arc<dyn ChunkStore>>, limit: usize) -> Result<usize> {
        objects.rotate_keys(limit)
    }

    pub fn bridge_export(bridge: &bridge::Bridge, payload_hex: &str) -> Result<PathBuf> {
        let payload = decode_hex(payload_hex, "bridge payload")?;
        bridge.export(&payload)
    }

    pub fn bridge_process(bridge: &bridge::Bridge) -> Result<usize> {
        bridge.process()
    }

    pub fn bridge_capture(bridge: &bridge::Bridge) -> Result<usize> {
        bridge.capture()
    }

    pub fn bridge_stage(bridge: &bridge::Bridge, path: &Path) -> Result<bridge::Header> {
        bridge.stage(path)
    }

    pub fn bridge_export_file(bridge: &bridge::Bridge, name: &str) -> Result<()> {
        bridge.export_file(name)
    }

    pub fn bridge_export_snapshot(bridge: &bridge::Bridge) -> Result<PathBuf> {
        bridge.export_snapshot()
    }

    pub fn bridge_release(
        bridge: &bridge::Bridge,
        table: &str,
        key_hex: &str,
        column: Option<&str>,
    ) -> Result<bool> {
        bridge.release(
            table.to_string(),
            decode_hex(key_hex, "bridge release key")?,
            column.map(str::to_string),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn get(map: &HashMap<String, String>) -> impl Fn(&str) -> Option<String> + '_ {
        move |name| map.get(name).cloned()
    }

    #[test]
    fn pragma_table_covers_all_names_with_working_defaults() {
        assert_eq!(PRAGMAS.len(), 12);
        for (name, default) in PRAGMAS {
            assert_eq!(default_value(name), Some(*default));
            validate(name, default).unwrap();
        }
        assert_eq!(default_value("murmur_nope"), None);
        assert!(validate("murmur_nope", "1").is_err());
    }

    #[test]
    fn validate_rejects_bad_values() {
        for (name, value) in [
            (FILES_GENERATION, "-1"),
            (FILES_GENERATION, "abc"),
            (MAX_FILE_BYTES, "0"),
            (MAX_FILE_BYTES, "1048575"),
            (MAX_FILE_BYTES, "34359738369"),
            (MAX_STAGING_BYTES, "0"),
            (MAX_STAGING_BYTES, "65535"),
            (FETCH_CONCURRENCY, "0"),
            (FETCH_CONCURRENCY, "65"),
            (FETCH_INTERVAL_MS, "999"),
            (FETCH_INTERVAL_MS, "86400001"),
            (FETCH_BIND, "not-an-addr"),
            (FETCH_BIND, "127.0.0.1"),
            (BRIDGE_ROLE, "medium"),
            (BRIDGE_STREAM, "has space"),
            (BRIDGE_STREAM, "semi;colon"),
            (BRIDGE_MAX_BUNDLE_BYTES, "0"),
            (BRIDGE_MAX_BUNDLE_BYTES, "16777217"),
            (BRIDGE_MAX_PAYLOAD_BYTES, "0"),
            (BRIDGE_MAX_PAYLOAD_BYTES, "67108865"),
            (BRIDGE_STAGING_BYTES, "0"),
            (BRIDGE_STAGING_BYTES, "4095"),
        ] {
            assert!(validate(name, value).is_err(), "{name}={value}");
        }
        assert!(validate(BRIDGE_STREAM, &"x".repeat(65)).is_err());
        for (name, value) in [
            (MAX_FILE_BYTES, "1048576"),
            (FETCH_CONCURRENCY, "1"),
            (FETCH_CONCURRENCY, "64"),
            (FETCH_BIND, "127.0.0.1:7444"),
            (FETCH_BIND, "[::1]:0"),
            (BRIDGE_ROLE, "low"),
            (BRIDGE_ROLE, "high"),
            (BRIDGE_STREAM, "ok-stream_1"),
            (BRIDGE_STREAM, ""),
        ] {
            validate(name, value).unwrap();
        }
    }

    #[test]
    fn coupled_staging_must_cover_bundle() {
        let empty = HashMap::new();
        validate_coupled(&get(&empty)).unwrap();
        let mut tight = HashMap::new();
        tight.insert(BRIDGE_MAX_BUNDLE_BYTES.to_string(), "16777216".to_string());
        tight.insert(BRIDGE_STAGING_BYTES.to_string(), "16777216".to_string());
        validate_coupled(&get(&tight)).unwrap();
        tight.insert(BRIDGE_STAGING_BYTES.to_string(), "16777215".to_string());
        assert!(validate_coupled(&get(&tight)).is_err());
    }

    #[test]
    fn resolve_dir_shapes() {
        let parent = Path::new("/db");
        assert_eq!(
            resolve_dir("", parent, "murmur-bridge"),
            PathBuf::from("/db/murmur-bridge")
        );
        assert_eq!(
            resolve_dir("/abs/dir", parent, "murmur-bridge"),
            PathBuf::from("/abs/dir")
        );
        assert_eq!(
            resolve_dir("rel/dir", parent, "murmur-bridge"),
            PathBuf::from("/db/rel/dir")
        );
    }

    #[test]
    fn knobs_default_to_disabled_transports() {
        let empty = HashMap::new();
        let knobs = Knobs::from_map(&get(&empty)).unwrap();
        assert!(!fetch_enabled(&knobs));
        assert_eq!(knobs.fetch_concurrency, 2);
        assert_eq!(knobs.fetch_interval, Duration::from_millis(30000));
        let config = objects_config(&knobs, [7; 32], None).unwrap();
        assert_eq!(config.generation, 0);
        assert_eq!(config.max_file_bytes, 4294967296);
        assert_eq!(config.max_staging_bytes, 1073741824);
        assert!(config.previous_key.is_none());
        assert!(bridge_role(&knobs).unwrap().is_none());
        assert!(bridge_stream(&knobs).unwrap().is_none());
    }

    #[test]
    fn knobs_enabled_bind_and_role() {
        let mut map = HashMap::new();
        map.insert(FETCH_BIND.to_string(), "127.0.0.1:7444".to_string());
        map.insert(BRIDGE_ROLE.to_string(), "low".to_string());
        map.insert(BRIDGE_STREAM.to_string(), "main".to_string());
        let knobs = Knobs::from_map(&get(&map)).unwrap();
        assert!(fetch_enabled(&knobs));
        let config = objects_config(&knobs, [7; 32], Some([8; 32])).unwrap();
        assert!(config.previous_key.is_some());
        assert_eq!(
            bridge_role(&knobs).unwrap(),
            Some(bridge::Role::LowExporter)
        );
        assert_eq!(bridge_stream(&knobs).unwrap().as_deref(), Some("main"));
    }

    #[test]
    fn bridge_stream_required_once_role_set() {
        let mut map = HashMap::new();
        map.insert(BRIDGE_ROLE.to_string(), "high".to_string());
        let knobs = Knobs::from_map(&get(&map)).unwrap();
        assert!(bridge_stream(&knobs).is_err());
    }

    #[test]
    fn hex_decoding() {
        assert_eq!(decode_hex("00ff", "t").unwrap(), vec![0, 255]);
        assert_eq!(decode_hex_16(&"ab".repeat(16), "t").unwrap(), [0xab; 16]);
        assert!(decode_hex("abc", "t").is_err());
        assert!(decode_hex("zz", "t").is_err());
        assert!(decode_hex_32("00", "t").is_err());
    }
}
