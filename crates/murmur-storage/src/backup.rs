//! Online hot backup helper. Rust port of `src/sqlite_murmur_backup.cpp`.
//!
//! Provenance and caller contract live in `include/sqlite_murmur_backup.h`:
//! dest-must-not-exist, remove-on-any-failure, 0600 manifest with fsync
//! data + dir discipline, manifest inside the artifact carrying identity
//! and consistency markers only (never key material).
//!
//! RocksDB itself stays behind two caller-supplied callbacks: the C++
//! store still owns its C++ `rocksdb::DB*`, so checkpoint/compaction run
//! through tiny `extern "C"` shims while validation, orchestration, the
//! manifest, and cleanup live here. Once the store is ported (holding a
//! C-API handle natively), these callbacks go away with the shim.

use crate::error::{SQLITE_ERROR, SQLITE_IOERR_WRITE, SQLITE_MISUSE, SQLITE_OK};
use crate::guard;
use std::ffi::{CStr, c_char, c_int, c_void};
use std::path::Path;

pub const MANIFEST: &str = "manifest.json";
pub const FORMAT_VERSION: u32 = 1;

/// Create a RocksDB checkpoint of the caller's database at `dest`,
/// reporting the checkpoint sequence number. Implemented by the C++
/// shim until the store is ported.
pub type CheckpointFn = unsafe extern "C" fn(ctx: *mut c_void, dest: *const c_char, seq: *mut u64) -> c_int;
/// Force a full-range synchronous compaction. Implemented by the C++ shim.
pub type CompactFn = unsafe extern "C" fn(ctx: *mut c_void) -> c_int;

/// True when nothing exists at `dest` (file, dir, or symlink). A dangling
/// symlink still refuses: writing through attacker-placed links is never
/// acceptable. Unstatable paths report free; the checkpoint call itself
/// fails without creating anything.
fn path_is_free(dest: &Path) -> bool {
    std::fs::symlink_metadata(dest).is_err()
}

fn cipher_charset_ok(cipher: &[u8]) -> bool {
    cipher
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

fn to_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 0xf) as usize] as char);
    }
    out
}

/// Best-effort durable write: 0600 file created exclusively, fsync data
/// (close errors fail), remove the file on any failure, then fsync the
/// containing dir (best-effort, like the C++).
#[cfg(unix)]
fn write_file_sync(path: &Path, body: &[u8]) -> bool {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::io::IntoRawFd;

    let mut file = match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
    {
        Ok(f) => f,
        Err(_) => return false,
    };
    let mut ok = file.write_all(body).is_ok();
    if ok {
        ok = file.sync_all().is_ok();
    }
    // SAFETY: fd came from a live File; closed exactly once here.
    let fd = file.into_raw_fd();
    if unsafe { libc::close(fd) } != 0 {
        ok = false;
    }
    if !ok {
        let _ = std::fs::remove_file(path);
        return false;
    }
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }
    }
    true
}

/// Non-Unix fallback mirroring the C++ WIN32 branch (truncate + flush).
#[cfg(not(unix))]
fn write_file_sync(path: &Path, body: &[u8]) -> bool {
    use std::io::Write;
    let mut out = match std::fs::File::create(path) {
        Ok(f) => f,
        Err(_) => return false,
    };
    out.write_all(body).is_ok() && out.flush().is_ok()
}

fn write_manifest(
    dest: &Path,
    cipher: &[u8],
    group_id: &[u8; 16],
    node_id: &[u8; 16],
    seq: u64,
) -> c_int {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    // Byte-exact schema: flat object, no spaces, trailing newline. The
    // cipher is charset-checked before this runs, so no escaping is needed.
    let body = format!(
        "{{\"format_version\":{},\"cipher\":\"{}\",\"group_id\":\"{}\",\"node_id\":\"{}\",\
         \"rocksdb_seq\":{},\"created_unix_ms\":{}}}\n",
        FORMAT_VERSION,
        String::from_utf8_lossy(cipher),
        to_hex(group_id),
        to_hex(node_id),
        seq,
        now_ms,
    );
    if !write_file_sync(&dest.join(MANIFEST), body.as_bytes()) {
        return SQLITE_IOERR_WRITE;
    }
    #[cfg(unix)]
    {
        // Sync the parent so the new directory entry itself is durable.
        if let Some(parent) = dest.parent() {
            if !parent.as_os_str().is_empty() {
                if let Ok(dir) = std::fs::File::open(parent) {
                    let _ = dir.sync_all();
                }
            }
        }
    }
    SQLITE_OK
}

fn remove_best_effort(dest: &Path) {
    let _ = std::fs::remove_dir_all(dest)
        .or_else(|_| std::fs::remove_file(dest).map(|_| ()));
}

/// Run a consistent online backup. `dest` is the original NUL-terminated
/// argument (also passed to the checkpoint callback); `dest_path` is its
/// parsed form for manifest/cleanup work. `seq_out` receives the
/// checkpoint sequence number on success only.
#[allow(clippy::too_many_arguments)]
pub fn backup_run(
    dest_path: &Path,
    dest_cstr: *const c_char,
    cipher: &[u8],
    group_id: &[u8; 16],
    node_id: &[u8; 16],
    seq_out: Option<&mut u64>,
    ctx: *mut c_void,
    checkpoint: CheckpointFn,
) -> c_int {
    if dest_path.as_os_str().is_empty() || cipher.is_empty() {
        return SQLITE_MISUSE;
    }
    if !cipher_charset_ok(cipher) {
        return SQLITE_ERROR;
    }
    if !path_is_free(dest_path) {
        return SQLITE_ERROR;
    }
    let mut seq = 0u64;
    // SAFETY: callback + ctx come from the caller per contract.
    if unsafe { checkpoint(ctx, dest_cstr, &mut seq) } != SQLITE_OK {
        remove_best_effort(dest_path);
        return SQLITE_IOERR_WRITE;
    }
    if write_manifest(dest_path, cipher, group_id, node_id, seq) != SQLITE_OK {
        remove_best_effort(dest_path);
        return SQLITE_IOERR_WRITE;
    }
    if let Some(out) = seq_out {
        *out = seq;
    }
    SQLITE_OK
}

/// Force a full-range synchronous compaction.
pub fn compact_run(ctx: *mut c_void, compact: CompactFn) -> c_int {
    // SAFETY: callback + ctx come from the caller per contract.
    if unsafe { compact(ctx) } != SQLITE_OK {
        return SQLITE_IOERR_WRITE;
    }
    SQLITE_OK
}

// ---------------------------------------------------------------------------
// C ABI: declared in include/sqlite_murmur_backup.h. String arguments use
// strlen semantics.

/// Convert a NUL-terminated path argument. Unix takes raw bytes (exact C++
/// behavior); other platforms fail closed on non-UTF-8.
#[cfg(unix)]
fn path_from_cstr(ptr: *const c_char) -> std::path::PathBuf {
    use std::os::unix::ffi::OsStrExt;
    // SAFETY: NUL-terminated per contract.
    let bytes = unsafe { CStr::from_ptr(ptr) }.to_bytes();
    std::ffi::OsStr::from_bytes(bytes).into()
}

#[cfg(not(unix))]
fn path_from_cstr(ptr: *const c_char) -> Option<std::path::PathBuf> {
    // SAFETY: NUL-terminated per contract. Non-UTF-8 fails closed where
    // the platform cannot represent raw bytes (the C++ only builds the
    // Unix discipline on Unix either).
    let s = unsafe { CStr::from_ptr(ptr) }.to_str().ok()?;
    Some(std::path::PathBuf::from(s))
}

#[cfg(unix)]
fn path_from_cstr_opt(ptr: *const c_char) -> Option<std::path::PathBuf> {
    Some(path_from_cstr(ptr))
}

#[cfg(not(unix))]
fn path_from_cstr_opt(ptr: *const c_char) -> Option<std::path::PathBuf> {
    path_from_cstr(ptr)
}

/// Run a consistent online backup. Returns SQLITE_OK (dest holds
/// checkpoint + manifest) or a SQLITE_* error; on ANY failure dest is
/// removed. `seq_out` (nullable) receives the checkpoint sequence number.
#[unsafe(no_mangle)]
pub extern "C" fn sqlite_murmur_backup_rs_run(
    dest: *const c_char,
    cipher: *const c_char,
    group_id: *const u8,
    node_id: *const u8,
    seq_out: *mut u64,
    ctx: *mut c_void,
    checkpoint: Option<CheckpointFn>,
) -> c_int {
    guard(
        || {
            if dest.is_null()
                || cipher.is_null()
                || group_id.is_null()
                || node_id.is_null()
                || ctx.is_null()
                || checkpoint.is_none()
            {
                return SQLITE_MISUSE;
            }
            // SAFETY: validated above; fixed-size buffers per contract.
            let dest_path = match path_from_cstr_opt(dest) {
                Some(p) => p,
                None => return SQLITE_MISUSE,
            };
            let cipher = unsafe { CStr::from_ptr(cipher) }.to_bytes();
            let group: &[u8; 16] = unsafe { &*(group_id as *const [u8; 16]) };
            let node: &[u8; 16] = unsafe { &*(node_id as *const [u8; 16]) };
            let seq_out = unsafe { seq_out.as_mut() };
            backup_run(
                &dest_path,
                dest,
                cipher,
                group,
                node,
                seq_out,
                ctx,
                checkpoint.unwrap(),
            )
        },
        SQLITE_ERROR,
    )
}

/// Force a full-range synchronous compaction.
#[unsafe(no_mangle)]
pub extern "C" fn sqlite_murmur_backup_rs_compact(
    ctx: *mut c_void,
    compact: Option<CompactFn>,
) -> c_int {
    guard(
        || {
            let Some(compact) = compact else {
                return SQLITE_MISUSE;
            };
            if ctx.is_null() {
                return SQLITE_MISUSE;
            }
            compact_run(ctx, compact)
        },
        SQLITE_ERROR,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn scratch(name: &str) -> std::path::PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "murmur-storage-backup-{}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed),
            name
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn ids() -> ([u8; 16], [u8; 16]) {
        let mut group = [0u8; 16];
        let mut node = [0u8; 16];
        for (i, b) in group.iter_mut().enumerate() {
            *b = 0x10 + i as u8;
        }
        for (i, b) in node.iter_mut().enumerate() {
            *b = 0xa0 + i as u8;
        }
        (group, node)
    }

    unsafe extern "C" fn stub_checkpoint(
        _ctx: *mut c_void,
        dest: *const c_char,
        seq: *mut u64,
    ) -> c_int {
        let path = path_from_cstr_opt(dest).unwrap();
        std::fs::create_dir(&path).unwrap();
        unsafe { *seq = 42 };
        SQLITE_OK
    }

    unsafe extern "C" fn failing_checkpoint(
        _ctx: *mut c_void,
        dest: *const c_char,
        _seq: *mut u64,
    ) -> c_int {
        // Leave a partial artifact: the runner must remove it.
        let path = path_from_cstr_opt(dest).unwrap();
        let _ = std::fs::create_dir(&path);
        SQLITE_IOERR_WRITE
    }

    unsafe extern "C" fn stub_compact(_ctx: *mut c_void) -> c_int {
        SQLITE_OK
    }

    unsafe extern "C" fn failing_compact(_ctx: *mut c_void) -> c_int {
        SQLITE_ERROR
    }

    #[cfg(unix)]
    fn cstring(path: &Path) -> CString {
        CString::new(path.as_os_str().as_encoded_bytes()).unwrap()
    }

    #[cfg(not(unix))]
    fn cstring(path: &Path) -> CString {
        CString::new(path.to_string_lossy().as_bytes()).unwrap()
    }

    #[test]
    fn round_trip_writes_schema_exact_manifest() {
        let root = scratch("roundtrip");
        let dest = root.join("backup");
        let dest_c = cstring(&dest);
        let (group, node) = ids();
        let mut seq = 0u64;
        let rc = backup_run(
            &dest,
            dest_c.as_ptr(),
            b"aes256gcm",
            &group,
            &node,
            Some(&mut seq),
            std::ptr::null_mut(),
            stub_checkpoint,
        );
        assert_eq!(rc, SQLITE_OK);
        assert_eq!(seq, 42);
        let body = std::fs::read_to_string(dest.join(MANIFEST)).unwrap();
        assert!(body.starts_with(
            "{\"format_version\":1,\"cipher\":\"aes256gcm\",\
             \"group_id\":\"101112131415161718191a1b1c1d1e1f\",\
             \"node_id\":\"a0a1a2a3a4a5a6a7a8a9aaabacadaeaf\",\
             \"rocksdb_seq\":42,\"created_unix_ms\":"
        ));
        assert!(body.ends_with("}\n"));
        let marker = "\"created_unix_ms\":";
        let start = body.find(marker).unwrap() + marker.len();
        let stamp: u64 = body[start..].trim_end_matches("}\n").parse().unwrap();
        assert!(stamp > 0);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dest.join(MANIFEST))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn validation_rejects_without_touching_fs() {
        let root = scratch("validation");
        let (group, node) = ids();
        let mut seq = 0u64;
        // Empty dest / cipher are misuse.
        assert_eq!(
            backup_run(
                Path::new(""),
                std::ptr::null(),
                b"aes256gcm",
                &group,
                &node,
                None,
                std::ptr::null_mut(),
                stub_checkpoint
            ),
            SQLITE_MISUSE
        );
        let dest = root.join("nope");
        let dest_c = cstring(&dest);
        assert_eq!(
            backup_run(
                &dest,
                dest_c.as_ptr(),
                b"",
                &group,
                &node,
                Some(&mut seq),
                std::ptr::null_mut(),
                stub_checkpoint
            ),
            SQLITE_MISUSE
        );
        // Bad cipher charset is an error (injection attempt).
        assert_eq!(
            backup_run(
                &dest,
                dest_c.as_ptr(),
                b"aes256gcm\",evil\":1,\"x\":\"",
                &group,
                &node,
                None,
                std::ptr::null_mut(),
                stub_checkpoint
            ),
            SQLITE_ERROR
        );
        assert!(!dest.exists());
        // Existing file / dir / dangling symlink are refused, unclobbered.
        let file = root.join("file-dest");
        std::fs::write(&file, "sentinel").unwrap();
        let file_c = cstring(&file);
        assert_eq!(
            backup_run(
                &file,
                file_c.as_ptr(),
                b"aes256gcm",
                &group,
                &node,
                None,
                std::ptr::null_mut(),
                stub_checkpoint
            ),
            SQLITE_ERROR
        );
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "sentinel");
        let dir = root.join("dir-dest");
        std::fs::create_dir(&dir).unwrap();
        let dir_c = cstring(&dir);
        assert_eq!(
            backup_run(
                &dir,
                dir_c.as_ptr(),
                b"aes256gcm",
                &group,
                &node,
                None,
                std::ptr::null_mut(),
                stub_checkpoint
            ),
            SQLITE_ERROR
        );
        #[cfg(unix)]
        {
            let link = root.join("dangling");
            std::os::unix::fs::symlink(root.join("nope"), &link).unwrap();
            let link_c =
                cstring(&link);
            assert_eq!(
                backup_run(
                    &link,
                    link_c.as_ptr(),
                    b"aes256gcm",
                    &group,
                    &node,
                    None,
                    std::ptr::null_mut(),
                    stub_checkpoint
                ),
                SQLITE_ERROR
            );
            assert!(std::fs::symlink_metadata(&link).is_ok());
        }
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn checkpoint_failure_removes_partial_artifact() {
        let root = scratch("cleanup");
        let dest = root.join("backup");
        let dest_c = cstring(&dest);
        let (group, node) = ids();
        assert_eq!(
            backup_run(
                &dest,
                dest_c.as_ptr(),
                b"aes256gcm",
                &group,
                &node,
                None,
                std::ptr::null_mut(),
                failing_checkpoint
            ),
            SQLITE_IOERR_WRITE
        );
        assert!(!dest.exists());
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn compact_maps_callback_result() {
        assert_eq!(compact_run(std::ptr::null_mut(), stub_compact), SQLITE_OK);
        assert_eq!(
            compact_run(std::ptr::null_mut(), failing_compact),
            SQLITE_IOERR_WRITE
        );
    }
}
