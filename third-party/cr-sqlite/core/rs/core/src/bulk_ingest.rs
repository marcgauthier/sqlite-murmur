//! Bulk ingestion: stage many changes in a TEMP table, then apply them
//! grouped by row in one call.
//!
//! Row-by-row `INSERT INTO crsql_changes` pays full per-change dispatch:
//! unpack, key probe, clock probes, base write, clock write. Bulk ingest
//! amortizes the per-row probes and base writes across each row's changes:
//! one key resolve, one clock scan, one multi-column base write per row.
//!
//! Observable results are identical to row-by-row ingestion in staging
//! order: same winners, same clocks, same versions, same seqs, same
//! rowsImpacted. Internal surrogate keys may differ (they never leave the
//! node).
//!
//! Usage, all inside the caller's transaction:
//!   SELECT crsql_ingest_begin();           -- create/clear staging
//!   INSERT INTO crsql_staging VALUES ...;  -- N rows, executemany
//!   SELECT crsql_ingest_commit();           -- grouped apply, returns count
//!
//! On commit error nothing from the failing chunk is applied (savepoint),
//! staging is left intact for inspection, and rows from earlier chunks stay
//! in the caller's transaction (roll back or fix and retry).

use alloc::borrow::Cow;
use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use alloc::string::ToString;
use alloc::vec::Vec;
use core::ffi::{c_char, c_int};
use core::mem::ManuallyDrop;
use sqlite_nostd as sqlite;
use sqlite_nostd::{sqlite3, Connection, Context, ResultCode, Stmt, Value};

use crate::c::crsql_ExtData;
use crate::pack_columns::{unpack_columns, ColumnValue};
use crate::stmt_cache::reset_cached_stmt;
use crate::tableinfo::{crsql_ensure_table_infos_are_up_to_date, TableInfo};

/// Staging mirrors the crsql_changes column order exactly so callers can
/// copy rows verbatim.
const STAGING_DDL: &str = "CREATE TEMP TABLE IF NOT EXISTS crsql_staging (
  tbl TEXT NOT NULL,
  pk BLOB NOT NULL,
  cid TEXT NOT NULL,
  val BLOB,
  col_vrsn INTEGER,
  db_vrsn INTEGER,
  site_id BLOB,
  cl INTEGER,
  seq INTEGER
)";
/**
 * site_id and the four versions are nullable exactly like row-by-row
 * ingestion: xUpdate reads them via int64()/blob(), which yield 0/empty
 * for NULL, and the loader below does the same through column_int64 and
 * column_bytes_owned. tbl/pk/cid stay NOT NULL: row-by-row rejects NULL
 * there (unknown table/column, unpack failure) and staging errors early
 * instead.
 */
/**
 * val uses BLOB affinity (no conversions) so staged values keep their exact
 * storage class. ANY would mean NUMERIC affinity and coerce '1' to 1.
 */

/// Rows applied per chunk. Bounds memory; chunks chain exact version state.
const CHUNK_ROWS: i64 = 20_000;

/// One staged change plus its load-time validation results. Payloads are
/// borrowed when they come from a packed blob (zero-copy) and owned when
/// they come from the staging table.
struct StagedChange<'a> {
    rowid: i64,
    tbl_idx: usize,
    pk: Cow<'a, [u8]>,
    cid: Cow<'a, str>,
    /// None = sentinel ('-1'), Some(i) = non_pks[i]. Resolved at load.
    cid_idx: Option<usize>,
    val: OwnedValue<'a>,
    col_vrsn: i64,
    db_vrsn: i64,
    site_id: Cow<'a, [u8]>,
    cl: i64,
    seq: i64,
}

/// Staged sqlite value (owned by the staging layer, unlike live values).
/// Compared with [`compare_owned_to_value`] using the exact ordering of
/// `crsql_compare_sqlite_values`.
enum OwnedValue<'a> {
    Null,
    Integer(i64),
    Float(f64),
    Text(Cow<'a, [u8]>),
    Blob(Cow<'a, [u8]>),
}

/// Blob/text bytes that tolerate zero-length values (whose pointer is
/// NULL in the column API, unlike the value API).
fn column_bytes_owned(
    stmt: &sqlite::ManagedStmt,
    i: i32,
) -> Result<Vec<u8>, ResultCode> {
    if stmt.stmt.column_bytes(i) == 0 {
        return Ok(Vec::new());
    }
    Ok(stmt.column_blob(i)?.to_vec())
}

fn column_text_str(stmt: &sqlite::ManagedStmt, i: i32) -> Result<&str, ResultCode> {
    if stmt.stmt.column_bytes(i) == 0 {
        return Ok("");
    }
    stmt.column_text(i)
}

fn read_owned_value(
    stmt: &sqlite::ManagedStmt,
    i: i32,
) -> Result<OwnedValue<'static>, ResultCode> {
    match stmt.column_type(i)? {
        sqlite::ColumnType::Null => Ok(OwnedValue::Null),
        sqlite::ColumnType::Integer => Ok(OwnedValue::Integer(stmt.column_int64(i))),
        sqlite::ColumnType::Float => Ok(OwnedValue::Float(stmt.column_double(i))),
        sqlite::ColumnType::Text => Ok(OwnedValue::Text(Cow::Owned(column_bytes_owned(
            stmt, i,
        )?))),
        sqlite::ColumnType::Blob => Ok(OwnedValue::Blob(Cow::Owned(column_bytes_owned(
            stmt, i,
        )?))),
    }
}

fn bind_owned_value(
    stmt: *mut sqlite::stmt,
    i: i32,
    val: &OwnedValue<'_>,
) -> Result<ResultCode, ResultCode> {
    // Raw statement: the Stmt trait carries the full bind set incl. double.
    match val {
        OwnedValue::Null => stmt.bind_null(i),
        OwnedValue::Integer(v) => stmt.bind_int64(i, *v),
        OwnedValue::Float(v) => stmt.bind_double(i, *v),
        // TEXT came out of sqlite so it is valid UTF-8 by construction.
        OwnedValue::Text(v) => core::str::from_utf8(v)
            .or(Err(ResultCode::ERROR))
            .and_then(|s| stmt.bind_text(i, s, sqlite::Destructor::STATIC)),
        OwnedValue::Blob(v) => stmt.bind_blob(i, v, sqlite::Destructor::STATIC),
    }
}

/**
 * Compare an owned (staged) value against a live sqlite value with the
 * exact ordering of `crsql_compare_sqlite_values`: type ordinal first
 * (null sorts below everything), then payload. NaN handling mirrors the
 * original branch-for-branch (unordered NaN compares equal).
 */
fn compare_owned_to_value(owned: &OwnedValue<'_>, live: *mut sqlite::value) -> c_int {
    let l_type = match owned {
        OwnedValue::Integer(_) => sqlite::ColumnType::Integer,
        OwnedValue::Float(_) => sqlite::ColumnType::Float,
        OwnedValue::Text(_) => sqlite::ColumnType::Text,
        OwnedValue::Blob(_) => sqlite::ColumnType::Blob,
        OwnedValue::Null => sqlite::ColumnType::Null,
    };
    let r_type = live.value_type();
    if l_type != r_type {
        return (r_type as i32) - (l_type as i32);
    }
    match owned {
        OwnedValue::Blob(b) => (&**b).cmp(live.blob()) as c_int,
        OwnedValue::Float(f) => {
            let r_double = live.double();
            if *f < r_double {
                -1
            } else if *f > r_double {
                1
            } else {
                0
            }
        }
        OwnedValue::Integer(v) => {
            let r_int = live.int64();
            if *v < r_int {
                -1
            } else if *v > r_int {
                1
            } else {
                0
            }
        }
        OwnedValue::Null => 0,
        OwnedValue::Text(t) => (&**t).cmp(live.blob()) as c_int,
    }
}

/**
 * `SELECT crsql_ingest_begin()` -- (re)create and empty the staging table.
 * Safe to call again to discard a staged batch.
 */
pub unsafe extern "C" fn x_crsql_ingest_begin(
    ctx: *mut sqlite::context,
    _argc: i32,
    _argv: *mut *mut sqlite::value,
) {
    let db = ctx.db_handle();
    if let Err(_) = db.exec_safe(STAGING_DDL) {
        ctx.result_error("crsql_ingest_begin: failed to create staging table");
        return;
    }
    if let Err(_) = db.exec_safe("DELETE FROM crsql_staging") {
        ctx.result_error("crsql_ingest_begin: failed to clear staging table");
        return;
    }
    ctx.result_int(0);
}

/**
 * `SELECT crsql_ingest_commit()` -- apply all staged rows grouped by row.
 * Returns the number of applied changes (same count crsql_rows_impacted
 * would report). Staging is cleared on success and preserved on error.
 */
pub unsafe extern "C" fn x_crsql_ingest_commit(
    ctx: *mut sqlite::context,
    _argc: i32,
    _argv: *mut *mut sqlite::value,
) {
    let ext_data = ctx.user_data() as *mut crsql_ExtData;
    let db = ctx.db_handle();
    match ingest_commit(db, ext_data) {
        Ok(applied) => sqlite::result_int64(ctx, applied),
        Err(msg) => ctx.result_error(&msg),
    }
}

/// Packed-changes blob format (v1, little-endian), shared by
/// `crsql_ingest_blob` and every packer (see py packing helper):
///   u32 magic 0x42535243 ('CRSB'), u32 version 1, u32 count, then per
///   change: u32+tbl, u32+pk, u32+cid, u8 valtype + val payload
///   (0=null, 1=i64, 2=f64, 3=u32+text, 4=u32+blob), i64 col_vrsn,
///   i64 db_vrsn, u32+site_id, i64 cl, i64 seq.
const BLOB_MAGIC: u32 = 0x42535243;
const BLOB_VERSION: u32 = 1;

struct BlobCursor<'a> {
    data: &'a [u8],
    pos: usize,
}

fn blob_tag(cursor: &BlobCursor) -> String {
    format!("blob offset {}", cursor.pos)
}

impl<'a> BlobCursor<'a> {
    fn rest(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }
    fn take(&mut self, n: usize, what: &str) -> Result<&'a [u8], String> {
        if self.rest() < n {
            return Err(format!("crsql_ingest_blob: truncated {} at {}", what, blob_tag(self)));
        }
        let s = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    fn u32(&mut self, what: &str) -> Result<u32, String> {
        let b = self.take(4, what)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn i64(&mut self, what: &str) -> Result<i64, String> {
        let b = self.take(8, what)?;
        Ok(i64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
    fn bytes(&mut self, what: &str) -> Result<&'a [u8], String> {
        let n = self.u32(what)? as usize;
        if n > 1_000_000_000 {
            return Err(format!("crsql_ingest_blob: absurd {} length at {}", what, blob_tag(self)));
        }
        self.take(n, what)
    }
}

/// Parse one chunk of staged changes from a blob cursor. Arrival order is
/// pack order; rowids are synthetic (1-based positions) for error messages.
fn parse_blob_chunk<'a>(
    cursor: &mut BlobCursor<'a>,
    tbl_infos: &Vec<TableInfo>,
    remaining: &mut u32,
    next_rowid: &mut i64,
) -> Result<Vec<StagedChange<'a>>, String> {
    let mut staged: Vec<StagedChange<'a>> = Vec::new();
    let mut n = 0i64;
    while *remaining > 0 && n < CHUNK_ROWS {
        *remaining -= 1;
        *next_rowid += 1;
        let rowid = *next_rowid;
        let fail = |why: &str| format!("crsql_ingest_blob: row {}: {}", rowid, why);
        let tbl_b = cursor.bytes("tbl").map_err(|e| fail(&e))?;
        let tbl = core::str::from_utf8(tbl_b).or(Err(fail("bad tbl")))?;
        if tbl.len() > crate::consts::MAX_TBL_NAME_LEN as usize {
            return Err(fail("table name exceeded max length"));
        }
        let tbl_idx = tbl_infos
            .iter()
            .position(|x| x.tbl_name == tbl)
            .ok_or(fail("unknown table"))?;
        let pk = Cow::Borrowed(cursor.bytes("pk").map_err(|e| fail(&e))?);
        let cid_b = cursor.bytes("cid").map_err(|e| fail(&e))?;
        let cid = core::str::from_utf8(cid_b).or(Err(fail("bad cid")))?;
        if cid.len() > crate::consts::MAX_TBL_NAME_LEN as usize {
            return Err(fail("column name exceeded max length"));
        }
        let cid_idx = if cid == crate::c::INSERT_SENTINEL {
            None
        } else {
            tbl_infos[tbl_idx]
                .non_pks
                .iter()
                .position(|c| c.name == cid)
                .ok_or(fail("unknown column"))
                .map(Some)?
        };
        let vt = cursor.take(1, "valtype").map_err(|e| fail(&e))?[0];
        let val = match vt {
            0 => OwnedValue::Null,
            1 => OwnedValue::Integer(cursor.i64("val").map_err(|e| fail(&e))?),
            2 => OwnedValue::Float(f64::from_bits(
                cursor.i64("val").map_err(|e| fail(&e))? as u64,
            )),
            3 => OwnedValue::Text(Cow::Borrowed(
                cursor.bytes("val").map_err(|e| fail(&e))?,
            )),
            4 => OwnedValue::Blob(Cow::Borrowed(
                cursor.bytes("val").map_err(|e| fail(&e))?,
            )),
            _ => return Err(fail("bad valtype")),
        };
        if let OwnedValue::Text(t) = &val {
            core::str::from_utf8(t).or(Err(fail("bad text val")))?;
        }
        let col_vrsn = cursor.i64("col_vrsn").map_err(|e| fail(&e))?;
        let db_vrsn = cursor.i64("db_vrsn").map_err(|e| fail(&e))?;
        let site_id = Cow::Borrowed(cursor.bytes("site_id").map_err(|e| fail(&e))?);
        if site_id.len() > crate::consts::SITE_ID_LEN as usize {
            return Err(fail("site id exceeded max length"));
        }
        let cl = cursor.i64("cl").map_err(|e| fail(&e))?;
        let seq = cursor.i64("seq").map_err(|e| fail(&e))?;
        staged.push(StagedChange {
            rowid,
            tbl_idx,
            pk,
            cid: Cow::Borrowed(cid),
            cid_idx,
            val,
            col_vrsn,
            db_vrsn,
            site_id,
            cl,
            seq,
        });
        n += 1;
    }
    Ok(staged)
}

/**
 * Take the main-database write lock before reading version state: staging
 * lives in TEMP (no main lock), so without this an external commit could
 * land between our baseline read and our first main write and regress
 * assigned versions. A same-value no-op write takes the lock cheaply.
 */
fn lock_main_db_for_bulk(db: *mut sqlite3) -> Result<ResultCode, String> {
    let key_stmt = db
        .prepare_v2("SELECT key FROM crsql_master LIMIT 1")
        .or(Err("crsql_ingest_commit: failed to lock".to_string()))?;
    let has_row = match key_stmt.step() {
        Ok(ResultCode::ROW) => true,
        _ => false,
    };
    if has_row {
        let key = key_stmt
            .column_text(0)
            .or(Err("crsql_ingest_commit: failed to lock".to_string()))?
            .to_string();
        db.prepare_v2("UPDATE crsql_master SET value = value WHERE key = ?")
            .and_then(|s| {
                s.bind_text(1, &key, sqlite::Destructor::TRANSIENT)?;
                s.step()
            })
            .or(Err("crsql_ingest_commit: failed to lock".to_string()))?;
    } else {
        // No master rows yet: net-zero scratch write takes the lock.
        db.exec_safe(
            "INSERT INTO crsql_master (key, value) VALUES ('crsql_bulk_lock', 1);
             DELETE FROM crsql_master WHERE key = 'crsql_bulk_lock'",
        )
        .or(Err("crsql_ingest_commit: failed to lock".to_string()))?;
    }
    Ok(ResultCode::OK)
}

/// One deferred versioned clock write. The version is assigned by the
/// arrival replica so numbering matches row-by-row ingestion exactly.
struct ClockOp {
    /// None = sentinel ('-1'), Some(i) = non_pks[i].
    col_idx: Option<usize>,
    col_vrsn: i64,
    seq: i64,
    site_id: Vec<u8>,
    merging: i64,
    assigned: i64,
}

/// Deferred per-change plan: decisions made, writes pending.
/// Base winners live in the run's pending list (grouped at apply).
struct ChangePlan {
    /// Zero-out before the sentinel write (resurrect/sentinel paths).
    zero: Option<ZeroOp>,
    ops: Vec<ClockOp>,
    /// merge_delete's unversioned non-sentinel drop.
    drop_clocks: bool,
    /// rowsImpacted contribution, counted exactly like row-by-row.
    impact: i64,
}

struct ZeroOp {
    merging: i64,
    assigned: i64,
}

impl ChangePlan {
    fn skip() -> ChangePlan {
        ChangePlan {
            zero: None,
            ops: Vec::new(),
            drop_clocks: false,
            impact: 0,
        }
    }
}

/// Narrow-table clock cache entry. `dirty_site` is Some exactly when this
/// run planned a write to the column (site known without probing).
struct ScanEntry {
    name: Vec<u8>,
    vrsn: i64,
    dirty_site: Option<Vec<u8>>,
}

/// Per-row-run decide state. Runs are maximal consecutive same-(table, pk)
/// slices of the stable-sorted chunk, so intra-run order is arrival order.
struct RunState {
    tbl_idx: usize,
    key: i64,
    pks: Vec<ColumnValue>,
    narrow: bool,
    scan: Vec<ScanEntry>,
    /// Wide tables: exact (cid, vrsn, dirty_site) for touched columns.
    /// Becomes complete on zero (one exact scan, rare path).
    wide_dirty: Vec<(Vec<u8>, i64, Option<Vec<u8>>)>,
    wide_exact: bool,
    cur_cl: i64,
    /// Pending winning base writes: (non_pk_idx, staged_idx). Last wins.
    pending_base: Vec<(usize, usize)>,
    /// Staged indices in run order.
    members: Vec<usize>,
}

/**
 * Arrival replica: assign db_versions exactly as row-by-row ingestion in
 * staging order would. Walks arrival order over all plans, advancing a
 * local pending copy with the same max() rule; sets the real pending once.
 */
fn assign_versions_arrival(
    db_version: i64,
    pending_start: i64,
    staged: &Vec<StagedChange<'_>>,
    plans: &mut Vec<ChangePlan>,
) -> i64 {
    let mut pend = pending_start;
    let floor = db_version + 1;
    for (idx, _s) in staged.iter().enumerate() {
        let plan = &mut plans[idx];
        if let Some(zero) = plan.zero.as_mut() {
            let mut ret = floor;
            if ret < pend {
                ret = pend;
            }
            if ret < zero.merging {
                ret = zero.merging;
            }
            pend = ret;
            zero.assigned = ret;
        }
        for op in plan.ops.iter_mut() {
            let mut ret = floor;
            if ret < pend {
                ret = pend;
            }
            if ret < op.merging {
                ret = op.merging;
            }
            pend = ret;
            op.assigned = ret;
        }
    }
    pend
}

/// Load every clock row for a key into owned entries (narrow tables, and
/// wide tables on the rare zero path where exactness demands the full set).
fn load_scan_entries(
    db: *mut sqlite3,
    tbl_info: &TableInfo,
    key: sqlite::int64,
) -> Result<Vec<ScanEntry>, ResultCode> {
    let scan_ref = tbl_info.get_clock_scan_stmt(db)?;
    let scan_stmt = scan_ref.as_ref().ok_or(ResultCode::ERROR)?;
    if let Err(rc) = scan_stmt.bind_int64(1, key) {
        reset_cached_stmt(scan_stmt.stmt)?;
        return Err(rc);
    }
    let mut entries = Vec::new();
    loop {
        match scan_stmt.step() {
            Ok(ResultCode::ROW) => {
                entries.push(ScanEntry {
                    name: scan_stmt.column_blob(0)?.to_vec(),
                    vrsn: scan_stmt.column_int64(1),
                    dirty_site: None,
                });
            }
            Ok(ResultCode::DONE) => break,
            Ok(rc) | Err(rc) => {
                reset_cached_stmt(scan_stmt.stmt)?;
                return Err(rc);
            }
        }
    }
    reset_cached_stmt(scan_stmt.stmt)?;
    Ok(entries)
}

/// Flush a run's pending winning base writes as one multi-column upsert.
/// Last write per column wins (same final state as sequential upserts).
/// Callers hold the chunk sync-bit; never toggles it.
fn flush_pending_base(
    db: *mut sqlite3,
    tbl_info: &TableInfo,
    run: &mut RunState,
    staged: &Vec<StagedChange<'_>>,
) -> Result<ResultCode, ResultCode> {
    if run.pending_base.is_empty() {
        return Ok(ResultCode::OK);
    }
    // Dedup last-wins, then sort for a stable cache key (SET order is
    // irrelevant across distinct columns).
    let mut seen: Vec<usize> = Vec::new();
    let mut cols: Vec<usize> = Vec::new();
    for (col_idx, _) in run.pending_base.iter().rev() {
        if !seen.contains(col_idx) {
            seen.push(*col_idx);
            cols.push(*col_idx);
        }
    }
    cols.sort_unstable();
    // Winner staged index per column (last occurrence in pending order).
    let mut winners: Vec<usize> = Vec::with_capacity(cols.len());
    for col_idx in cols.iter() {
        let pos = run
            .pending_base
            .iter()
            .rposition(|(c, _)| c == col_idx)
            .ok_or(ResultCode::ERROR)?;
        winners.push(run.pending_base[pos].1);
    }
    run.pending_base.clear();

    let stmt_ref = tbl_info.get_merge_group_stmt(db, &cols)?;
    let stmt = stmt_ref.stmt;
    crate::pack_columns::bind_package_to_stmt(stmt, &run.pks, 0)?;
    let npks = run.pks.len() as i32;
    for (k, staged_idx) in winners.iter().enumerate() {
        bind_owned_value(stmt, npks + k as i32 + 1, &staged[*staged_idx].val)?;
    }
    for (k, staged_idx) in winners.iter().enumerate() {
        bind_owned_value(
            stmt,
            npks + winners.len() as i32 + k as i32 + 1,
            &staged[*staged_idx].val,
        )?;
    }
    let rc = stmt.step();
    reset_cached_stmt(stmt)?;
    rc
}

/// Immediate base pk-only insert (sentinel/resurrect paths). One per row.
fn base_pk_insert_now(
    db: *mut sqlite3,
    tbl_info: &TableInfo,
    pks: &Vec<ColumnValue>,
) -> Result<ResultCode, ResultCode> {
    let stmt_ref = tbl_info.get_merge_pk_only_insert_stmt(db)?;
    let stmt = stmt_ref.as_ref().ok_or(ResultCode::ERROR)?;
    crate::pack_columns::bind_package_to_stmt(stmt.stmt, pks, 0)?;
    let rc = stmt.step();
    reset_cached_stmt(stmt.stmt)?;
    rc
}

/// Immediate base delete (delete path). One per row.
fn base_delete_now(
    db: *mut sqlite3,
    tbl_info: &TableInfo,
    pks: &Vec<ColumnValue>,
) -> Result<ResultCode, ResultCode> {
    let stmt_ref = tbl_info.get_merge_delete_stmt(db)?;
    let stmt = stmt_ref.as_ref().ok_or(ResultCode::ERROR)?;
    crate::pack_columns::bind_package_to_stmt(stmt.stmt, pks, 0)?;
    let rc = stmt.step();
    reset_cached_stmt(stmt.stmt)?;
    rc
}

/// Unversioned non-sentinel clock drop (delete path, after sentinel write).
fn drop_clocks_now(
    db: *mut sqlite3,
    tbl_info: &TableInfo,
    key: sqlite::int64,
) -> Result<ResultCode, ResultCode> {
    let stmt_ref = tbl_info.get_merge_delete_drop_clocks_stmt(db)?;
    let stmt = stmt_ref.as_ref().ok_or(ResultCode::ERROR)?;
    let rc = stmt
        .bind_int64(1, key)
        .and_then(|_| stmt.step());
    reset_cached_stmt(stmt.stmt)?;
    rc
}

/// Probe the live base value for a tie-break. Mirrors did_cid_win's value
/// section, comparing the staged value with the exact legacy ordering.
fn probe_col_value_tie(
    db: *mut sqlite3,
    tbl_info: &TableInfo,
    pks: &Vec<ColumnValue>,
    col_name: &str,
    staged_val: &OwnedValue<'_>,
) -> Result<c_int, String> {
    let stmt_ref = tbl_info
        .get_col_value_stmt(db, col_name)
        .or(Err("failed getting col value stmt".to_string()))?;
    let stmt = stmt_ref
        .as_ref()
        .ok_or("failed getting col value stmt".to_string())?;
    crate::pack_columns::bind_package_to_stmt(stmt.stmt, pks, 0)
        .or(Err("failed binding col value stmt".to_string()))?;
    match stmt.step() {
        Ok(ResultCode::ROW) => {
            let local_value = stmt
                .column_value(0)
                .or(Err("failed reading local value".to_string()))?;
            let ret = compare_owned_to_value(staged_val, local_value);
            reset_cached_stmt(stmt.stmt).or(Err("failed resetting".to_string()))?;
            Ok(ret)
        }
        _ => {
            let _ = reset_cached_stmt(stmt.stmt);
            Err(format!(
                "could not find row to merge with for tbl {}",
                tbl_info.tbl_name
            ))
        }
    }
}

/// Probe the live site id for a tie-break. Mirrors did_cid_win's site
/// section byte-for-byte.
fn probe_col_site_tie(
    db: *mut sqlite3,
    tbl_info: &TableInfo,
    key: sqlite::int64,
    col_name: &str,
    insert_site_id: &[u8],
) -> Result<c_int, String> {
    let stmt_ref = tbl_info
        .get_col_site_id_stmt(db)
        .or(Err("failed getting col site id stmt".to_string()))?;
    let stmt = stmt_ref
        .as_ref()
        .ok_or("failed getting col site id stmt".to_string())?;
    if let Err(_) = stmt.bind_int64(1, key) {
        let _ = reset_cached_stmt(stmt.stmt);
        return Err("failed binding col site id stmt".to_string());
    }
    if let Err(_) = stmt.bind_text(2, col_name, sqlite::Destructor::STATIC) {
        let _ = reset_cached_stmt(stmt.stmt);
        return Err("failed binding col site id stmt".to_string());
    }
    match stmt.step() {
        Ok(ResultCode::ROW) => {
            let local_site_id = stmt
                .column_blob(0)
                .or(Err("failed reading local site id".to_string()))?;
            let ret = insert_site_id.cmp(local_site_id) as c_int;
            reset_cached_stmt(stmt.stmt).or(Err("failed resetting".to_string()))?;
            Ok(ret)
        }
        Ok(ResultCode::DONE) => {
            let _ = reset_cached_stmt(stmt.stmt);
            Err(format!(
                "could not find site_id for previous change, cr-sqlite clock table might be corrupt for tbl {}",
                tbl_info.tbl_name
            ))
        }
        _ => {
            let _ = reset_cached_stmt(stmt.stmt);
            Err("Bad return code when selecting local column site_id".to_string())
        }
    }
}

fn scan_local_cl(scan: &Vec<ScanEntry>) -> i64 {
    for e in scan.iter() {
        if e.name.as_slice() == crate::c::DELETE_SENTINEL.as_bytes() {
            return e.vrsn;
        }
    }
    if scan.is_empty() {
        0
    } else {
        1
    }
}

/// Record a planned clock write in the run caches (narrow scan or wide
/// dirty map) so later same-run decisions see it without probing.
fn run_record_clock(run: &mut RunState, cid: &str, vrsn: i64, site: &[u8]) {
    if run.narrow {
        match run
            .scan
            .iter_mut()
            .find(|e| e.name.as_slice() == cid.as_bytes())
        {
            Some(e) => {
                e.vrsn = vrsn;
                e.dirty_site = Some(site.to_vec());
            }
            None => run.scan.push(ScanEntry {
                name: cid.as_bytes().to_vec(),
                vrsn,
                dirty_site: Some(site.to_vec()),
            }),
        }
    } else {
        match run
            .wide_dirty
            .iter_mut()
            .find(|(n, _, _)| n.as_slice() == cid.as_bytes())
        {
            Some(e) => {
                e.1 = vrsn;
                e.2 = Some(site.to_vec());
            }
            None => run.wide_dirty.push((
                cid.as_bytes().to_vec(),
                vrsn,
                Some(site.to_vec()),
            )),
        }
    }
}

/// Apply a planned zero-out to the run caches. Wide tables load one exact
/// scan first (zero keeps rows, so the unknown set must become known).
fn run_zero_all(
    db: *mut sqlite3,
    tbl_info: &TableInfo,
    run: &mut RunState,
) -> Result<(), String> {
    let sentinel = crate::c::DELETE_SENTINEL.as_bytes();
    if run.narrow {
        for e in run.scan.iter_mut() {
            if e.name.as_slice() != sentinel {
                // Zero touches versions only; dirty sites stay valid.
                e.vrsn = 0;
            }
        }
        return Ok(());
    }
    if !run.wide_exact {
        let entries =
            load_scan_entries(db, tbl_info, run.key).or(Err("clock scan failed".to_string()))?;
        run.wide_dirty = entries
            .into_iter()
            .map(|e| {
                let v = if e.name.as_slice() == sentinel {
                    e.vrsn
                } else {
                    0
                };
                (e.name, v, None)
            })
            .collect();
        run.wide_exact = true;
        return Ok(());
    }
    for (name, vrsn, _) in run.wide_dirty.iter_mut() {
        if name.as_slice() != sentinel {
            *vrsn = 0;
        }
    }
    Ok(())
}

/// Apply a planned non-sentinel drop to the run caches (delete path).
fn run_drop_nonsentinels(run: &mut RunState) {
    let sentinel = crate::c::DELETE_SENTINEL.as_bytes();
    if run.narrow {
        run.scan.retain(|e| e.name.as_slice() == sentinel);
    } else {
        run.wide_dirty
            .retain(|(n, _, _)| n.as_slice() == sentinel);
        // Post-drop the full set is known: sentinel plus nothing.
        run.wide_exact = true;
    }
}

/// Pending-or-live column version for a change: cache hit, exact miss, or
/// (wide tables, untouched column) one live probe, which is fresh because
/// nothing is pending for that column.
fn run_target_vrsn(
    db: *mut sqlite3,
    tbl_info: &TableInfo,
    run: &RunState,
    cid: &str,
) -> Result<Option<i64>, String> {
    if run.narrow {
        for e in run.scan.iter() {
            if e.name.as_slice() == cid.as_bytes() {
                return Ok(Some(e.vrsn));
            }
        }
        return Ok(None);
    }
    for (name, vrsn, _) in run.wide_dirty.iter() {
        if name.as_slice() == cid.as_bytes() {
            return Ok(Some(*vrsn));
        }
    }
    if run.wide_exact {
        return Ok(None);
    }
    let stmt_ref = tbl_info
        .get_col_version_stmt(db)
        .or(Err("failed getting col version stmt".to_string()))?;
    let stmt = stmt_ref
        .as_ref()
        .ok_or("failed getting col version stmt".to_string())?;
    if stmt.bind_int64(1, run.key).is_err() {
        let _ = reset_cached_stmt(stmt.stmt);
        return Err("failed binding col version stmt".to_string());
    }
    if stmt
        .bind_text(2, cid, sqlite::Destructor::STATIC)
        .is_err()
    {
        let _ = reset_cached_stmt(stmt.stmt);
        return Err("failed binding col version stmt".to_string());
    }
    match stmt.step() {
        Ok(ResultCode::ROW) => {
            let v = stmt.column_int64(0);
            let _ = reset_cached_stmt(stmt.stmt);
            Ok(Some(v))
        }
        Ok(ResultCode::DONE) => {
            let _ = reset_cached_stmt(stmt.stmt);
            Ok(None)
        }
        _ => {
            let _ = reset_cached_stmt(stmt.stmt);
            Err("Bad return code when selecting local column version".to_string())
        }
    }
}

fn run_dirty_site<'a>(run: &'a RunState, cid: &str) -> Option<&'a [u8]> {
    if run.narrow {
        run.scan
            .iter()
            .find(|e| e.name.as_slice() == cid.as_bytes())
            .and_then(|e| e.dirty_site.as_deref())
    } else {
        run.wide_dirty
            .iter()
            .find(|(n, _, _)| n.as_slice() == cid.as_bytes())
            .and_then(|(_, _, s)| s.as_deref())
    }
}

/// Value-then-site tie-break. Pending base hits flush first so probes see
/// exactly the state row-by-row ingestion would have written by now;
/// pending clock hits compare bytes directly (same bytes, no probe).
fn tie_break(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    tbl_info: &TableInfo,
    run: &mut RunState,
    staged: &Vec<StagedChange<'_>>,
    s_idx: usize,
) -> Result<bool, String> {
    let s = &staged[s_idx];
    let cid_idx = s.cid_idx.ok_or("sentinel reached tie-break".to_string())?;
    if run.pending_base.iter().any(|(c, _)| *c == cid_idx) {
        flush_pending_base(db, tbl_info, run, staged)
            .or(Err("failed flushing pending base writes".to_string()))?;
    }
    let cmp = probe_col_value_tie(db, tbl_info, &run.pks, &s.cid, &s.val)?;
    if cmp != 0 {
        return Ok(cmp > 0);
    }
    if unsafe { (*ext_data).mergeEqualValues } != 1 {
        return Ok(false);
    }
    if let Some(dirty) = run_dirty_site(run, &s.cid) {
        return Ok(((&*s.site_id).cmp(dirty) as c_int) > 0);
    }
    let site_cmp = probe_col_site_tie(db, tbl_info, run.key, &s.cid, &s.site_id)?;
    Ok(site_cmp > 0)
}

/// Decide one staged change, mirroring merge_insert's branches exactly.
/// Base pk-inserts/deletes apply immediately (per-row, ungroupable); cid
/// base writes collect in the run; clock writes become plans.
fn decide_one(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    tbl_info: &TableInfo,
    run: &mut RunState,
    staged: &Vec<StagedChange<'_>>,
    s_idx: usize,
    plan: &mut ChangePlan,
) -> Result<(), String> {
    let s = &staged[s_idx];
    let local_cl = run.cur_cl;
    if s.cl < local_cl {
        return Ok(());
    }
    let is_delete = s.cl % 2 == 0;
    let needs_resurrect = s.cl > local_cl && s.cl % 2 == 1;
    let row_exists = local_cl != 0;
    let is_sentinel = s.cid_idx.is_none();

    if is_delete {
        if s.cl == local_cl {
            return Ok(());
        }
        base_delete_now(db, tbl_info, &run.pks)
            .or(Err("failed applying bulk delete".to_string()))?;
        plan.ops.push(ClockOp {
            col_idx: None,
            col_vrsn: s.col_vrsn,
            seq: s.seq,
            site_id: s.site_id.clone().into_owned(),
            merging: s.db_vrsn,
            assigned: 0,
        });
        plan.drop_clocks = true;
        plan.impact = 1;
        run.cur_cl = s.cl;
        run_drop_nonsentinels(run);
        run_record_clock(run, crate::c::DELETE_SENTINEL, s.col_vrsn, &s.site_id);
        return Ok(());
    }

    if is_sentinel {
        if s.cl == local_cl {
            return Ok(());
        }
        base_pk_insert_now(db, tbl_info, &run.pks)
            .or(Err("failed applying bulk pk insert".to_string()))?;
        plan.zero = Some(ZeroOp {
            merging: s.db_vrsn,
            assigned: 0,
        });
        plan.ops.push(ClockOp {
            col_idx: None,
            col_vrsn: s.col_vrsn,
            seq: s.seq,
            site_id: s.site_id.clone().into_owned(),
            merging: s.db_vrsn,
            assigned: 0,
        });
        plan.impact = 1;
        run.cur_cl = s.cl;
        run_zero_all(db, tbl_info, run)?;
        run_record_clock(run, crate::c::INSERT_SENTINEL, s.col_vrsn, &s.site_id);
        return Ok(());
    }

    if needs_resurrect && (row_exists || (!row_exists && s.cl > 1)) {
        base_pk_insert_now(db, tbl_info, &run.pks)
            .or(Err("failed applying bulk pk insert".to_string()))?;
        plan.zero = Some(ZeroOp {
            merging: s.db_vrsn,
            assigned: 0,
        });
        // Legacy passes the causal length as the sentinel's column version.
        plan.ops.push(ClockOp {
            col_idx: None,
            col_vrsn: s.cl,
            seq: s.seq,
            site_id: s.site_id.clone().into_owned(),
            merging: s.db_vrsn,
            assigned: 0,
        });
        plan.impact += 1;
        run.cur_cl = s.cl;
        run_zero_all(db, tbl_info, run)?;
        run_record_clock(run, crate::c::INSERT_SENTINEL, s.cl, &s.site_id);
    }

    let won = if needs_resurrect || !row_exists {
        true
    } else {
        match run_target_vrsn(db, tbl_info, run, &s.cid)? {
            None => true,
            Some(local_vrsn) => {
                if s.col_vrsn > local_vrsn {
                    true
                } else if s.col_vrsn < local_vrsn {
                    false
                } else {
                    tie_break(db, ext_data, tbl_info, run, staged, s_idx)?
                }
            }
        }
    };
    if !won {
        return Ok(());
    }
    let cid_idx = s.cid_idx.ok_or("sentinel reached cid path".to_string())?;
    plan.ops.push(ClockOp {
        col_idx: Some(cid_idx),
        col_vrsn: s.col_vrsn,
        seq: s.seq,
        site_id: s.site_id.clone().into_owned(),
        merging: s.db_vrsn,
        assigned: 0,
    });
    run.pending_base.push((cid_idx, s_idx));
    plan.impact += 1;
    run_record_clock(run, &s.cid, s.col_vrsn, &s.site_id);
    Ok(())
}

/// Pass A: stable-sort chunk indices by (table, pk bytes) so each row's
/// changes are consecutive in arrival order, then decide per run with
/// grouped key resolve and clock reads.
fn decide_chunk(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    tbl_infos: &Vec<TableInfo>,
    staged: &Vec<StagedChange<'_>>,
    plans: &mut Vec<ChangePlan>,
) -> Result<Vec<RunState>, String> {
    let mut order: Vec<usize> = (0..staged.len()).collect();
    order.sort_by(|&a, &b| {
        (staged[a].tbl_idx, &staged[a].pk).cmp(&(staged[b].tbl_idx, &staged[b].pk))
    });

    let mut runs: Vec<RunState> = Vec::new();
    let mut i = 0;
    while i < order.len() {
        let first = &staged[order[i]];
        let mut j = i + 1;
        while j < order.len()
            && staged[order[j]].tbl_idx == first.tbl_idx
            && staged[order[j]].pk == first.pk
        {
            j += 1;
        }
        let tbl_info = &tbl_infos[first.tbl_idx];
        let pks = unpack_columns(&first.pk)
            .or(Err("failed unpacking staged pks".to_string()))?;
        let key = tbl_info
            .get_or_create_key(db, &pks)
            .or(Err("failed resolving staged key".to_string()))?;
        let narrow =
            tbl_info.non_pks.len() <= crate::changes_vtab_write::CLOCK_SCAN_MAX_NON_PKS;
        let (scan, cur_cl) = if narrow {
            let scan =
                load_scan_entries(db, tbl_info, key).or(Err("clock scan failed".to_string()))?;
            let cl = scan_local_cl(&scan);
            (scan, cl)
        } else {
            let cl = crate::changes_vtab_write::get_local_cl(db, tbl_info, key)
                .or(Err("failed reading local cl".to_string()))?;
            (Vec::new(), cl)
        };
        let mut run = RunState {
            tbl_idx: first.tbl_idx,
            key,
            pks,
            narrow,
            scan,
            wide_dirty: Vec::new(),
            wide_exact: false,
            cur_cl,
            pending_base: Vec::new(),
            members: order[i..j].to_vec(),
        };
        for k in i..j {
            let s_idx = order[k];
            decide_one(
                db,
                ext_data,
                tbl_info,
                &mut run,
                staged,
                s_idx,
                &mut plans[s_idx],
            )?;
        }
        runs.push(run);
        i = j;
    }
    Ok(runs)
}

/// Pass B: apply one run's plans with replica-assigned versions: grouped
/// base upsert first, then per-change clocks in run order (zero, then ops,
/// then the delete drop -- the legacy intra-change order).
fn apply_run(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    tbl_infos: &Vec<TableInfo>,
    run: &mut RunState,
    staged: &Vec<StagedChange<'_>>,
    plans: &Vec<ChangePlan>,
) -> Result<i64, String> {
    let tbl_info = &tbl_infos[run.tbl_idx];
    flush_pending_base(db, tbl_info, run, staged)
        .or(Err("failed flushing bulk base writes".to_string()))?;
    let mut impact: i64 = 0;
    for s_idx in run.members.iter() {
        let plan = &plans[*s_idx];
        if let Some(zero) = plan.zero.as_ref() {
            crate::changes_vtab_write::zero_clocks_on_resurrect_with_version(
                db,
                tbl_info,
                run.key,
                zero.assigned,
            )
            .or(Err("failed zeroing bulk clocks".to_string()))?;
        }
        for op in plan.ops.iter() {
            let col_name: &str = match op.col_idx {
                None => crate::c::INSERT_SENTINEL,
                Some(i) => &tbl_info.non_pks[i].name,
            };
            crate::changes_vtab_write::set_winner_clock_with_version(
                db,
                ext_data,
                tbl_info,
                run.key,
                col_name,
                op.col_vrsn,
                op.assigned,
                &op.site_id,
                op.seq,
            )
            .or(Err("failed writing bulk clock".to_string()))?;
        }
        if plan.drop_clocks {
            drop_clocks_now(db, tbl_info, run.key)
                .or(Err("failed dropping bulk clocks".to_string()))?;
        }
        impact += plan.impact;
    }
    Ok(impact)
}

/// Load one arrival-ordered chunk of staged rows with owned copies.
fn load_chunk<'a>(
    db: *mut sqlite3,
    tbl_infos: &Vec<TableInfo>,
    after_rowid: i64,
) -> Result<Vec<StagedChange<'a>>, String> {
    let stmt = db
        .prepare_v2(
            "SELECT rowid, tbl, pk, cid, val, col_vrsn, db_vrsn, site_id, cl, seq
             FROM crsql_staging WHERE rowid > ? ORDER BY rowid LIMIT ?",
        )
        .or(Err(
            "crsql_ingest_commit: staging table missing, call crsql_ingest_begin first"
                .to_string(),
        ))?;
    stmt.bind_int64(1, after_rowid)
        .or(Err("failed reading staging table".to_string()))?;
    stmt.bind_int64(2, CHUNK_ROWS)
        .or(Err("failed reading staging table".to_string()))?;
    let mut staged: Vec<StagedChange<'_>> = Vec::new();
    loop {
        match stmt.step() {
            Ok(ResultCode::ROW) => {
                let rowid = stmt.column_int64(0);
                let tbl = column_text_str(&stmt, 1)
                    .or(Err(format!("staged row {}: bad tbl", rowid)))?;
                if tbl.len() > crate::consts::MAX_TBL_NAME_LEN as usize {
                    return Err(format!("staged row {}: table name exceeded max length", rowid));
                }
                let tbl_idx = tbl_infos
                    .iter()
                    .position(|x| x.tbl_name == tbl)
                    .ok_or(format!("staged row {}: unknown table", rowid))?;
                let cid = column_text_str(&stmt, 3)
                    .or(Err(format!("staged row {}: bad cid", rowid)))?
                    .to_string();
                if cid.len() > crate::consts::MAX_TBL_NAME_LEN as usize {
                    return Err(format!("staged row {}: column name exceeded max length", rowid));
                }
                let cid_idx = if cid == crate::c::INSERT_SENTINEL {
                    None
                } else {
                    let info = &tbl_infos[tbl_idx];
                    let pos = info
                        .non_pks
                        .iter()
                        .position(|c| c.name == cid)
                        .ok_or(format!("staged row {}: unknown column", rowid))?;
                    Some(pos)
                };
                staged.push(StagedChange {
                    rowid,
                    tbl_idx,
                    pk: Cow::Owned(
                        column_bytes_owned(&stmt, 2)
                            .or(Err(format!("staged row {}: bad pk", rowid)))?,
                    ),
                    cid: Cow::Owned(cid),
                    cid_idx,
                    val: read_owned_value(&stmt, 4).or(Err(format!(
                        "staged row {}: bad val",
                        rowid
                    )))?,
                    col_vrsn: stmt.column_int64(5),
                    db_vrsn: stmt.column_int64(6),
                    site_id: Cow::Owned(
                        column_bytes_owned(&stmt, 7)
                            .or(Err(format!("staged row {}: bad site_id", rowid)))?,
                    ),
                    cl: stmt.column_int64(8),
                    seq: stmt.column_int64(9),
                });
                if staged[staged.len() - 1].site_id.len()
                    > crate::consts::SITE_ID_LEN as usize
                {
                    return Err(format!("staged row {}: site id exceeded max length", rowid));
                }
            }
            Ok(ResultCode::DONE) => break,
            _ => return Err("failed reading staging table".to_string()),
        }
    }
    Ok(staged)
}

/// Chunk sources feeding the grouped apply: the staging table (keyed by
/// rowid) or a packed blob (unpacked in arrival order).
enum ChunkSource<'a> {
    Staging { after_rowid: i64 },
    Blob {
        cursor: BlobCursor<'a>,
        remaining: u32,
        next_rowid: i64,
    },
}

fn next_chunk<'a>(
    db: *mut sqlite3,
    tbl_infos: &Vec<TableInfo>,
    source: &mut ChunkSource<'a>,
) -> Result<Vec<StagedChange<'a>>, String> {
    match source {
        ChunkSource::Staging { after_rowid } => {
            let staged = load_chunk(db, tbl_infos, *after_rowid)?;
            if let Some(last) = staged.last() {
                *after_rowid = last.rowid;
            }
            Ok(staged)
        }
        ChunkSource::Blob {
            cursor,
            remaining,
            next_rowid,
        } => parse_blob_chunk(cursor, tbl_infos, remaining, next_rowid),
    }
}

/**
 * `SELECT crsql_ingest_blob(?)` -- apply a packed-changes blob grouped by
 * row, without a staging table. Same results as ingesting the packed rows
 * in order. Returns the applied count.
 */
pub unsafe extern "C" fn x_crsql_ingest_blob(
    ctx: *mut sqlite::context,
    argc: i32,
    argv: *mut *mut sqlite::value,
) {
    if argc != 1 {
        ctx.result_error("crsql_ingest_blob: expected 1 argument");
        return;
    }
    let args = sqlite::args!(argc, argv);
    let blob = args[0].blob();
    let ext_data = ctx.user_data() as *mut crsql_ExtData;
    let db = ctx.db_handle();
    match ingest_blob(db, ext_data, blob) {
        Ok(applied) => sqlite::result_int64(ctx, applied),
        Err(msg) => ctx.result_error(&msg),
    }
}

fn ingest_blob(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    blob: &[u8],
) -> Result<i64, String> {
    let mut cursor = BlobCursor { data: blob, pos: 0 };
    if cursor.u32("magic")? != BLOB_MAGIC {
        return Err("crsql_ingest_blob: bad magic".to_string());
    }
    if cursor.u32("version")? != BLOB_VERSION {
        return Err("crsql_ingest_blob: unsupported version".to_string());
    }
    let count = cursor.u32("count")?;
    let mut source = ChunkSource::Blob {
        cursor,
        remaining: count,
        next_rowid: 0,
    };
    let total = ingest_with_source(db, ext_data, &mut source, false)?;
    Ok(total)
}

fn ingest_commit(db: *mut sqlite3, ext_data: *mut crsql_ExtData) -> Result<i64, String> {
    let mut source = ChunkSource::Staging { after_rowid: 0 };
    ingest_with_source(db, ext_data, &mut source, true)
}

fn ingest_with_source(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    source: &mut ChunkSource<'_>,
    clear_staging: bool,
) -> Result<i64, String> {
    let mut errmsg: *mut c_char = core::ptr::null_mut();
    let rc = crsql_ensure_table_infos_are_up_to_date(db, ext_data, &mut errmsg);
    if rc != ResultCode::OK as c_int {
        if !errmsg.is_null() {
            let msg = unsafe {
                core::ffi::CStr::from_ptr(errmsg as *const c_char)
                    .to_string_lossy()
                    .into_owned()
            };
            unsafe { alloc::ffi::CString::from_raw(errmsg) };
            return Err(msg);
        }
        return Err("Failed to update CRR table information".to_string());
    }
    let tbl_infos = ManuallyDrop::new(unsafe {
        Box::from_raw((*ext_data).tableInfos as *mut Vec<TableInfo>)
    });

    lock_main_db_for_bulk(db)?;
    crate::db_version::fill_db_version_if_needed(db, ext_data)?;
    unsafe {
        (*ext_data).dbVersionCheckedForWrite = 1;
    }
    let (db_version, mut pend) = unsafe { ((*ext_data).dbVersion, (*ext_data).pendingDbVersion) };

    // Whole-commit savepoint: a failing chunk undoes the commit, never a
    // partial prefix (same atomicity as one multi-row statement).
    db.exec_safe("SAVEPOINT crsql_bulk_9f3a")
        .or(Err("failed starting bulk savepoint".to_string()))?;
    // Chunk sync-bit: suppress triggers for our base writes. Cleared on
    // every exit below; never leaked.
    unsafe {
        *(*ext_data).syncBit = 1;
    }
    let result = ingest_commit_inner(db, ext_data, &tbl_infos, db_version, &mut pend, source);
    unsafe {
        *(*ext_data).syncBit = 0;
    }
    match result {
        Ok(total) => {
            // Memory state lands BEFORE the release: in autocommit (a bare
            // SELECT with no outer tx) RELEASE commits the implicit txn and
            // the commit hook must observe the final pendingDbVersion and
            // reset rowsImpacted after the accumulation, exactly as
            // row-by-row's per-statement xCommit does. With an outer tx the
            // release commits nothing and both stay visible until commit.
            unsafe {
                (*ext_data).pendingDbVersion = pend;
                (*ext_data).rowsImpacted += total as c_int;
            }
            db.exec_safe("RELEASE crsql_bulk_9f3a")
                .or(Err("failed releasing bulk savepoint".to_string()))?;
            if clear_staging {
                db.exec_safe("DELETE FROM crsql_staging")
                    .or(Err("failed clearing staging table".to_string()))?;
            }
            Ok(total)
        }
        Err(msg) => {
            let _ = db.exec_safe("ROLLBACK TO crsql_bulk_9f3a");
            let _ = db.exec_safe("RELEASE crsql_bulk_9f3a");
            Err(msg)
        }
    }
}

fn ingest_commit_inner(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    tbl_infos: &Vec<TableInfo>,
    db_version: i64,
    pend: &mut i64,
    source: &mut ChunkSource<'_>,
) -> Result<i64, String> {
    let mut total: i64 = 0;
    loop {
        let staged = next_chunk(db, tbl_infos, source)?;
        if staged.is_empty() {
            break;
        }
        let mut plans: Vec<ChangePlan> =
            (0..staged.len()).map(|_| ChangePlan::skip()).collect();
        let mut runs = decide_chunk(db, ext_data, tbl_infos, &staged, &mut plans)?;
        *pend = assign_versions_arrival(db_version, *pend, &staged, &mut plans);
        for run in runs.iter_mut() {
            total += apply_run(db, ext_data, tbl_infos, run, &staged, &plans)?;
        }
    }
    // Blob exactness: no truncation residue, no trailing garbage. Runs
    // inside the savepoint, so a failure here rolls everything back.
    if let ChunkSource::Blob {
        cursor,
        remaining,
        ..
    } = source
    {
        if *remaining != 0 || cursor.pos != cursor.data.len() {
            return Err("crsql_ingest_blob: trailing garbage".to_string());
        }
    }
    Ok(total)
}
