//! Helper connections for drain and apply.
//!
//! Applied writes must not loop back into any app connection's session, so
//! drain and apply run on dedicated connections. Those connections must use
//! the same VFS as the calling connection: a custom-VFS database path (for
//! example a directory owned by another storage engine) is meaningless to
//! the default VFS.

use rusqlite::{Connection, OpenFlags};

/// Open `path` on `vfs` (`None` selects the default VFS), with the same
/// flags `Connection::open` uses.
pub fn open_helper(path: &str, vfs: Option<&str>) -> Result<Connection, String> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;
    match vfs {
        Some(name) => Connection::open_with_flags_and_vfs(path, flags, name),
        None => Connection::open_with_flags(path, flags),
    }
    .map_err(|e| format!("murmur: helper connection failed for '{path}': {e}"))
}
