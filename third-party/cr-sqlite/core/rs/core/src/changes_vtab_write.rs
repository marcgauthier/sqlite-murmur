use alloc::boxed::Box;
use alloc::ffi::CString;
use alloc::format;
use alloc::vec::Vec;
use core::ffi::{c_char, c_int};
use core::mem;
use sqlite::Stmt;
use sqlite_nostd as sqlite;
use sqlite_nostd::{sqlite3, ResultCode, Value};

use crate::c::crsql_ExtData;
use crate::c::{crsql_Changes_vtab, CrsqlChangesColumn};
use crate::compare_values::crsql_compare_sqlite_values;
use crate::pack_columns::bind_package_to_stmt;
use crate::pack_columns::{unpack_columns, ColumnValue};
use crate::stmt_cache::reset_cached_stmt;
use crate::tableinfo::{crsql_ensure_table_infos_are_up_to_date, TableInfo};
use crate::util::slab_rowid;

/**
 * did_cid_win does not take into account the causal length.
 * The expectation is that all causal length concerns have already been handle
 * via:
 * - early return because insert_cl < local_cl
 * - automatic win because insert_cl > local_cl
 * - come here to did_cid_win if insert_cl = local_cl
 */
fn did_cid_win(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    insert_tbl: &str,
    tbl_info: &TableInfo,
    unpacked_pks: &Vec<ColumnValue>,
    key: sqlite::int64,
    insert_val: *mut sqlite::value,
    insert_site_id: &[u8],
    col_name: &str,
    col_version: sqlite::int64,
    errmsg: *mut *mut c_char,
) -> Result<bool, ResultCode> {
    let col_vrsn_stmt_ref = tbl_info.get_col_version_stmt(db)?;
    let col_vrsn_stmt = col_vrsn_stmt_ref.as_ref().ok_or(ResultCode::ERROR)?;

    let bind_result = col_vrsn_stmt.bind_int64(1, key);
    if let Err(rc) = bind_result {
        reset_cached_stmt(col_vrsn_stmt.stmt)?;
        return Err(rc);
    }
    if let Err(rc) = col_vrsn_stmt.bind_text(2, col_name, sqlite::Destructor::STATIC) {
        reset_cached_stmt(col_vrsn_stmt.stmt)?;
        return Err(rc);
    }

    match col_vrsn_stmt.step() {
        Ok(ResultCode::ROW) => {
            let local_version = col_vrsn_stmt.column_int64(0);
            reset_cached_stmt(col_vrsn_stmt.stmt)?;
            // causal lengths are the same. Fall back to original algorithm.
            if let Some(won) = compare_col_version(col_version, local_version) {
                return Ok(won);
            }
            return did_cid_win_with_local_version(
                db,
                ext_data,
                insert_tbl,
                tbl_info,
                unpacked_pks,
                key,
                insert_val,
                insert_site_id,
                col_name,
                errmsg,
            );
        }
        Ok(ResultCode::DONE) => {
            reset_cached_stmt(col_vrsn_stmt.stmt)?;
            // no rows returned
            // of course the incoming change wins if there's nothing there locally.
            return Ok(true);
        }
        Ok(rc) | Err(rc) => {
            reset_cached_stmt(col_vrsn_stmt.stmt)?;
            let err = CString::new("Bad return code when selecting local column version")?;
            unsafe { *errmsg = err.into_raw() };
            return Err(rc);
        }
    }
}

/**
 * Three-way column version compare shared by the legacy probe path and the
 * clock-scan path. Returns None on equality so the caller falls through to
 * value and site-id tie-breaks.
 */
fn compare_col_version(
    insert_col_vrsn: sqlite::int64,
    local_vrsn: sqlite::int64,
) -> Option<bool> {
    if insert_col_vrsn > local_vrsn {
        Some(true)
    } else if insert_col_vrsn < local_vrsn {
        Some(false)
    } else {
        None
    }
}

fn did_cid_win_with_local_version(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    insert_tbl: &str,
    tbl_info: &TableInfo,
    unpacked_pks: &Vec<ColumnValue>,
    key: sqlite::int64,
    insert_val: *mut sqlite::value,
    insert_site_id: &[u8],
    col_name: &str,
    errmsg: *mut *mut c_char,
) -> Result<bool, ResultCode> {
    // versions are equal
    // need to compare values
    let col_val_stmt_ref = tbl_info.get_col_value_stmt(db, col_name)?;
    let col_val_stmt = col_val_stmt_ref.as_ref().ok_or(ResultCode::ERROR)?;

    let bind_result = bind_package_to_stmt(col_val_stmt.stmt, &unpacked_pks, 0);
    if let Err(rc) = bind_result {
        reset_cached_stmt(col_val_stmt.stmt)?;
        return Err(rc);
    }

    let step_result = col_val_stmt.step();
    match step_result {
        Ok(ResultCode::ROW) => {
            let local_value = col_val_stmt.column_value(0)?;
            let mut ret = crsql_compare_sqlite_values(insert_val, local_value);
            reset_cached_stmt(col_val_stmt.stmt)?;
            if ret == 0 && unsafe { (*ext_data).mergeEqualValues == 1 } {
                // values are the same (ret == 0) and the option to tie break on site_id is true
                let col_site_id_stmt_ref = tbl_info.get_col_site_id_stmt(db)?;
                let col_site_id_stmt = col_site_id_stmt_ref.as_ref().ok_or(ResultCode::ERROR)?;

                let bind_result = col_site_id_stmt.bind_int64(1, key);
                if let Err(rc) = bind_result {
                    reset_cached_stmt(col_site_id_stmt.stmt)?;
                    return Err(rc);
                }
                if let Err(rc) = col_site_id_stmt.bind_text(2, col_name, sqlite::Destructor::STATIC)
                {
                    reset_cached_stmt(col_site_id_stmt.stmt)?;
                    return Err(rc);
                }

                match col_site_id_stmt.step() {
                    Ok(ResultCode::ROW) => {
                        let local_site_id = col_site_id_stmt.column_blob(0)?;
                        ret = insert_site_id.cmp(local_site_id) as c_int;

                        // reset the stmt after, we're accessing a slice in-memory
                        reset_cached_stmt(col_site_id_stmt.stmt)?;
                    }
                    Ok(ResultCode::DONE) => {
                        reset_cached_stmt(col_site_id_stmt.stmt)?;
                        let err = CString::new(format!(
                            "could not find site_id for previous change, cr-sqlite clock table might be corrupt for tbl {}",
                            insert_tbl
                        ))?;
                        unsafe { *errmsg = err.into_raw() };
                        return Err(ResultCode::ERROR);
                    }
                    Ok(rc) | Err(rc) => {
                        reset_cached_stmt(col_site_id_stmt.stmt)?;
                        let err =
                            CString::new("Bad return code when selecting local column site_id")?;
                        unsafe { *errmsg = err.into_raw() };
                        return Err(rc);
                    }
                }
            }
            return Ok(ret > 0);
        }
        _ => {
            // ResultCode::DONE would happen if clock values exist but actual values are missing.
            // should we just allow the insert anyway?
            reset_cached_stmt(col_val_stmt.stmt)?;
            let err = CString::new(format!(
                "could not find row to merge with for tbl {}",
                insert_tbl
            ))?;
            unsafe { *errmsg = err.into_raw() };
            return Err(ResultCode::ERROR);
        }
    }
}

fn set_winner_clock(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    tbl_info: &TableInfo,
    key: sqlite::int64,
    insert_col_name: &str,
    insert_col_vrsn: sqlite::int64,
    insert_db_vrsn: sqlite::int64,
    insert_site_id: &[u8],
    insert_seq: sqlite::int64,
) -> Result<sqlite::int64, ResultCode> {
    // Same version the embedded crsql_next_db_version(?) call produced, but
    // computed once in Rust with a once-per-tx data_version check.
    let db_vrsn_to_write =
        crate::db_version::next_db_version_for_write(db, ext_data, Some(insert_db_vrsn))
            .or(Err(ResultCode::ERROR))?;
    set_winner_clock_with_version(
        db,
        ext_data,
        tbl_info,
        key,
        insert_col_name,
        insert_col_vrsn,
        db_vrsn_to_write,
        insert_site_id,
        insert_seq,
    )
}

/**
 * Resolve a site id blob to its ordinal, inserting a mapping on first sight.
 * Shared by row-by-row and bulk ingestion (same cache behavior).
 */
pub(crate) fn ordinal_for_site(
    _db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    insert_site_id: &[u8],
) -> Result<Option<sqlite::int64>, ResultCode> {
    // set the site_id ordinal
    // get the returned ordinal
    // use that in place of insert_site_id in the metadata table(s)

    // on changes read, join to gather the proper site id.
    // A last-value cache skips the ordinal SELECT for the common single-site
    // bulk ingest case. Only pre-tx ordinals are cached (see
    // insertedSiteIdThisTx) and the cache dies on commit/rollback, so a
    // savepoint rollback can never strand a cached ordinal.
    let ordinal = unsafe {
        if insert_site_id.is_empty() {
            None
        } else if (*ext_data).lastSiteIdOrdinalValid == 1
            && (*ext_data).lastSiteIdLen as usize == insert_site_id.len()
            && insert_site_id == &(*ext_data).lastSiteId[..insert_site_id.len()]
        {
            Some((*ext_data).lastSiteIdOrdinal)
        } else {
            (*ext_data).pSelectSiteIdOrdinalStmt.bind_blob(
                1,
                insert_site_id,
                sqlite::Destructor::STATIC,
            )?;
            let rc = (*ext_data).pSelectSiteIdOrdinalStmt.step()?;
            if rc == ResultCode::ROW {
                let ordinal = (*ext_data).pSelectSiteIdOrdinalStmt.column_int64(0);
                (*ext_data).pSelectSiteIdOrdinalStmt.clear_bindings()?;
                (*ext_data).pSelectSiteIdOrdinalStmt.reset()?;

                if (*ext_data).insertedSiteIdThisTx == 0
                    && insert_site_id.len() <= (*ext_data).lastSiteId.len()
                {
                    (*ext_data).lastSiteId[..insert_site_id.len()]
                        .copy_from_slice(insert_site_id);
                    (*ext_data).lastSiteIdLen = insert_site_id.len() as c_int;
                    (*ext_data).lastSiteIdOrdinal = ordinal;
                    (*ext_data).lastSiteIdOrdinalValid = 1;
                }

                Some(ordinal)
            } else {
                (*ext_data).pSelectSiteIdOrdinalStmt.clear_bindings()?;
                (*ext_data).pSelectSiteIdOrdinalStmt.reset()?;
                // site id had no ordinal yet.
                // set one and return the ordinal.
                (*ext_data).pSetSiteIdOrdinalStmt.bind_blob(
                    1,
                    insert_site_id,
                    sqlite::Destructor::STATIC,
                )?;
                let rc = (*ext_data).pSetSiteIdOrdinalStmt.step()?;
                if rc == ResultCode::DONE {
                    (*ext_data).pSetSiteIdOrdinalStmt.clear_bindings()?;
                    (*ext_data).pSetSiteIdOrdinalStmt.reset()?;
                    return Err(ResultCode::ABORT);
                }
                let ordinal = (*ext_data).pSetSiteIdOrdinalStmt.column_int64(0);
                (*ext_data).pSetSiteIdOrdinalStmt.clear_bindings()?;
                (*ext_data).pSetSiteIdOrdinalStmt.reset()?;
                (*ext_data).insertedSiteIdThisTx = 1;
                (*ext_data).lastSiteIdOrdinalValid = 0;
                Some(ordinal)
            }
        }
    };

    Ok(ordinal)
}

pub(crate) fn set_winner_clock_with_version(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    tbl_info: &TableInfo,
    key: sqlite::int64,
    insert_col_name: &str,
    insert_col_vrsn: sqlite::int64,
    db_vrsn_to_write: sqlite::int64,
    insert_site_id: &[u8],
    insert_seq: sqlite::int64,
) -> Result<sqlite::int64, ResultCode> {
    let ordinal = ordinal_for_site(db, ext_data, insert_site_id)?;

    let set_stmt_ref = tbl_info.get_set_winner_clock_stmt(db)?;
    let set_stmt = set_stmt_ref.as_ref().ok_or(ResultCode::ERROR)?;

    let bind_result = set_stmt.bind_int64(1, key);
    if let Err(rc) = bind_result {
        reset_cached_stmt(set_stmt.stmt)?;
        return Err(rc);
    }
    let bind_result = set_stmt
        .bind_text(2, insert_col_name, sqlite::Destructor::STATIC)
        .and_then(|_| set_stmt.bind_int64(3, insert_col_vrsn))
        .and_then(|_| set_stmt.bind_int64(4, db_vrsn_to_write))
        .and_then(|_| set_stmt.bind_int64(5, insert_seq))
        .and_then(|_| match ordinal {
            Some(ordinal) => set_stmt.bind_int64(6, ordinal),
            // site_id is NOT NULL DEFAULT 0: legacy OR REPLACE mapped a
            // NULL bind to the default via its NOT NULL substitution rule.
            // The upsert has no such rule, so bind the 0 explicitly.
            None => set_stmt.bind_int64(6, 0),
        });

    if let Err(rc) = bind_result {
        reset_cached_stmt(set_stmt.stmt)?;
        return Err(rc);
    }

    match set_stmt.step() {
        Ok(ResultCode::ROW) => {
            let rowid = set_stmt.column_int64(0);
            reset_cached_stmt(set_stmt.stmt)?;
            Ok(rowid)
        }
        _ => {
            reset_cached_stmt(set_stmt.stmt)?;
            Err(ResultCode::ERROR)
        }
    }
}

fn merge_sentinel_only_insert(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    tbl_info: &TableInfo,
    unpacked_pks: &Vec<ColumnValue>,
    key: sqlite::int64,
    remote_col_vrsn: sqlite::int64,
    remote_db_vsn: sqlite::int64,
    remote_site_id: &[u8],
    remote_seq: sqlite::int64,
) -> Result<sqlite::int64, ResultCode> {
    let merge_stmt_ref = tbl_info.get_merge_pk_only_insert_stmt(db)?;
    let merge_stmt = merge_stmt_ref.as_ref().ok_or(ResultCode::ERROR)?;

    let rc = bind_package_to_stmt(merge_stmt.stmt, unpacked_pks, 0);
    if let Err(rc) = rc {
        reset_cached_stmt(merge_stmt.stmt)?;
        return Err(rc);
    }
    // Suppress local triggers while applying the remote row. Direct write to
    // the shared sync bit integer; equivalent to stepping the set/clear
    // statements without the per-row SQL overhead.
    unsafe {
        *(*ext_data).syncBit = 1;
    }
    let rc = merge_stmt.step();

    // TODO: report err?
    let _ = reset_cached_stmt(merge_stmt.stmt);

    unsafe {
        *(*ext_data).syncBit = 0;
    }

    if let Err(rc) = rc {
        return Err(rc);
    }

    if let Ok(_) = rc {
        zero_clocks_on_resurrect(db, ext_data, tbl_info, key, remote_db_vsn)?;
        return set_winner_clock(
            db,
            ext_data,
            tbl_info,
            key,
            crate::c::INSERT_SENTINEL,
            remote_col_vrsn,
            remote_db_vsn,
            remote_site_id,
            remote_seq,
        );
    }

    Ok(-1)
}

fn zero_clocks_on_resurrect(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    tbl_info: &TableInfo,
    key: sqlite::int64,
    insert_db_vrsn: sqlite::int64,
) -> Result<ResultCode, ResultCode> {
    // Same version the embedded crsql_next_db_version(?) call produced.
    let db_vrsn_to_write =
        crate::db_version::next_db_version_for_write(db, ext_data, Some(insert_db_vrsn))
            .or(Err(ResultCode::ERROR))?;
    zero_clocks_on_resurrect_with_version(db, tbl_info, key, db_vrsn_to_write)
}

pub(crate) fn zero_clocks_on_resurrect_with_version(
    db: *mut sqlite3,
    tbl_info: &TableInfo,
    key: sqlite::int64,
    db_vrsn_to_write: sqlite::int64,
) -> Result<ResultCode, ResultCode> {
    let zero_stmt_ref = tbl_info.get_zero_clocks_on_resurrect_stmt(db)?;
    let zero_stmt = zero_stmt_ref.as_ref().ok_or(ResultCode::ERROR)?;

    let ret = zero_stmt
        .bind_int64(1, db_vrsn_to_write)
        .and_then(|_| zero_stmt.bind_int64(2, key))
        .and_then(|_| zero_stmt.step());
    reset_cached_stmt(zero_stmt.stmt)?;
    return ret;
}

unsafe fn merge_delete(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    tbl_info: &TableInfo,
    unpacked_pks: &Vec<ColumnValue>,
    key: sqlite::int64,
    remote_col_vrsn: sqlite::int64,
    remote_db_vrsn: sqlite::int64,
    remote_site_id: &[u8],
    remote_seq: sqlite::int64,
) -> Result<sqlite::int64, ResultCode> {
    let delete_stmt_ref = tbl_info.get_merge_delete_stmt(db)?;
    let delete_stmt = delete_stmt_ref.as_ref().ok_or(ResultCode::ERROR)?;

    if let Err(rc) = bind_package_to_stmt(delete_stmt.stmt, unpacked_pks, 0) {
        reset_cached_stmt(delete_stmt.stmt)?;
        return Err(rc);
    }
    unsafe {
        *(*ext_data).syncBit = 1;
    }
    let rc = delete_stmt.step();

    reset_cached_stmt(delete_stmt.stmt)?;

    unsafe {
        *(*ext_data).syncBit = 0;
    }

    if let Err(rc) = rc {
        return Err(rc);
    }

    let ret = set_winner_clock(
        db,
        ext_data,
        tbl_info,
        key,
        crate::c::DELETE_SENTINEL,
        remote_col_vrsn,
        remote_db_vrsn,
        remote_site_id,
        remote_seq,
    )?;

    // Drop clocks _after_ setting the winner clock so we don't lose track of the max db_version!!
    // This must never come before `set_winner_clock`
    let drop_clocks_stmt_ref = tbl_info.get_merge_delete_drop_clocks_stmt(db)?;
    let drop_clocks_stmt = drop_clocks_stmt_ref.as_ref().ok_or(ResultCode::ERROR)?;

    let rc = drop_clocks_stmt
        .bind_int64(1, key)
        .and_then(|_| drop_clocks_stmt.step());
    reset_cached_stmt(drop_clocks_stmt.stmt)?;
    rc?;

    return Ok(ret);
}

#[no_mangle]
pub unsafe extern "C" fn crsql_merge_insert(
    vtab: *mut sqlite::vtab,
    argc: c_int,
    argv: *mut *mut sqlite::value,
    rowid: *mut sqlite::int64,
    errmsg: *mut *mut c_char,
) -> c_int {
    match merge_insert(vtab, argc, argv, rowid, errmsg) {
        Err(rc) | Ok(rc) => rc as c_int,
    }
}

/**
 * Tables this narrow (or narrower) read the row's clocks in one scan.
 * Wider tables keep the legacy point probes: a full scan would read more
 * rows than the probes it replaces.
 */
pub(crate) const CLOCK_SCAN_MAX_NON_PKS: usize = 8;

fn scan_local_clocks(
    db: *mut sqlite::sqlite3,
    tbl_info: &TableInfo,
    key: sqlite::int64,
    col_name: &str,
) -> Result<(sqlite::int64, Option<sqlite::int64>), ResultCode> {
    let scan_stmt_ref = tbl_info.get_clock_scan_stmt(db)?;
    let scan_stmt = scan_stmt_ref.as_ref().ok_or(ResultCode::ERROR)?;

    if let Err(rc) = scan_stmt.bind_int64(1, key) {
        reset_cached_stmt(scan_stmt.stmt)?;
        return Err(rc);
    }

    let mut any_row = false;
    let mut sentinel_cl: Option<sqlite::int64> = None;
    let mut target_vrsn: Option<sqlite::int64> = None;
    loop {
        match scan_stmt.step() {
            Ok(ResultCode::ROW) => {
                any_row = true;
                let name = scan_stmt.column_text(0)?;
                let vrsn = scan_stmt.column_int64(1);
                if name == crate::c::DELETE_SENTINEL {
                    sentinel_cl = Some(vrsn);
                } else if name == col_name {
                    target_vrsn = Some(vrsn);
                }
            }
            Ok(ResultCode::DONE) => break,
            Ok(rc) | Err(rc) => {
                reset_cached_stmt(scan_stmt.stmt)?;
                return Err(rc);
            }
        }
    }
    reset_cached_stmt(scan_stmt.stmt)?;

    // Same semantics as the legacy COALESCE(sentinel, row-exists ? 1 : 0).
    let local_cl = match sentinel_cl {
        Some(cl) => cl,
        None if any_row => 1,
        None => 0,
    };
    Ok((local_cl, target_vrsn))
}

pub(crate) fn get_local_cl(
    db: *mut sqlite::sqlite3,
    tbl_info: &TableInfo,
    key: sqlite::int64,
) -> Result<sqlite::int64, ResultCode> {
    let local_cl_stmt_ref = tbl_info.get_local_cl_stmt(db)?;
    let local_cl_stmt = local_cl_stmt_ref.as_ref().ok_or(ResultCode::ERROR)?;

    let rc = local_cl_stmt.bind_int64(1, key);
    if let Err(rc) = rc {
        reset_cached_stmt(local_cl_stmt.stmt)?;
        return Err(rc);
    }
    let rc = local_cl_stmt.bind_int64(2, key);
    if let Err(rc) = rc {
        reset_cached_stmt(local_cl_stmt.stmt)?;
        return Err(rc);
    }

    let step_result = local_cl_stmt.step();
    match step_result {
        Ok(ResultCode::ROW) => {
            let ret = local_cl_stmt.column_int64(0);
            reset_cached_stmt(local_cl_stmt.stmt)?;
            Ok(ret)
        }
        Ok(ResultCode::DONE) => {
            reset_cached_stmt(local_cl_stmt.stmt)?;
            Ok(0)
        }
        Ok(rc) | Err(rc) => {
            reset_cached_stmt(local_cl_stmt.stmt)?;
            Err(rc)
        }
    }
}

unsafe fn merge_insert(
    vtab: *mut sqlite::vtab,
    argc: c_int,
    argv: *mut *mut sqlite::value,
    rowid: *mut sqlite::int64,
    errmsg: *mut *mut c_char,
) -> Result<ResultCode, ResultCode> {
    let tab = vtab.cast::<crsql_Changes_vtab>();
    let db = (*tab).db;

    let rc = crsql_ensure_table_infos_are_up_to_date(db, (*tab).pExtData, errmsg);
    if rc != ResultCode::OK as i32 {
        let err = CString::new("Failed to update CRR table information")?;
        *errmsg = err.into_raw();
        return Err(ResultCode::ERROR);
    }

    let args = sqlite::args!(argc, argv);
    let insert_tbl = args[2 + CrsqlChangesColumn::Tbl as usize];
    if insert_tbl.bytes() > crate::consts::MAX_TBL_NAME_LEN {
        let err = CString::new("crsql - table name exceeded max length")?;
        *errmsg = err.into_raw();
        return Err(ResultCode::ERROR);
    }

    let insert_tbl = insert_tbl.text();
    let insert_pks = args[2 + CrsqlChangesColumn::Pk as usize];
    let insert_col = args[2 + CrsqlChangesColumn::Cid as usize];
    if insert_col.bytes() > crate::consts::MAX_TBL_NAME_LEN {
        let err = CString::new("crsql - column name exceeded max length")?;
        *errmsg = err.into_raw();
        return Err(ResultCode::ERROR);
    }

    let insert_col = insert_col.text();
    let insert_val = args[2 + CrsqlChangesColumn::Cval as usize];
    let insert_col_vrsn = args[2 + CrsqlChangesColumn::ColVrsn as usize].int64();
    let insert_db_vrsn = args[2 + CrsqlChangesColumn::DbVrsn as usize].int64();
    let insert_site_id = args[2 + CrsqlChangesColumn::SiteId as usize];
    let insert_cl = args[2 + CrsqlChangesColumn::Cl as usize].int64();
    let insert_seq = args[2 + CrsqlChangesColumn::Seq as usize].int64();

    if insert_site_id.bytes() > crate::consts::SITE_ID_LEN {
        let err = CString::new("crsql - site id exceeded max length")?;
        *errmsg = err.into_raw();
        return Err(ResultCode::ERROR);
    }

    let insert_site_id = insert_site_id.blob();
    let tbl_infos = mem::ManuallyDrop::new(Box::from_raw(
        (*(*tab).pExtData).tableInfos as *mut Vec<TableInfo>,
    ));
    // TODO: will this work given `insert_tbl` is null termed?
    let tbl_info_index = tbl_infos.iter().position(|x| x.tbl_name == insert_tbl);

    if tbl_info_index.is_none() {
        let err = CString::new(format!(
            "crsql - could not find the schema information for table {}",
            insert_tbl
        ))?;
        *errmsg = err.into_raw();
        return Err(ResultCode::ERROR);
    }
    // TODO: technically safe since we checked `is_none` but this should be more idiomatic
    let tbl_info_index = tbl_info_index.unwrap();

    let tbl_info = &tbl_infos[tbl_info_index];
    let unpacked_pks = unpack_columns(insert_pks.blob())?;

    // Get or create key as the first thing we do.
    // We'll need the key for all later operations.
    let key = tbl_info.get_or_create_key(db, &unpacked_pks)?;

    // Narrow tables fold the local causal length and column version probes
    // into one clock scan; wide tables keep the legacy probes.
    let use_clock_scan = tbl_info.non_pks.len() <= CLOCK_SCAN_MAX_NON_PKS;
    let (local_cl, prefetched_col_vrsn) = if use_clock_scan {
        scan_local_clocks(db, &tbl_info, key, insert_col)?
    } else {
        (get_local_cl(db, &tbl_info, key)?, None)
    };

    // We can ignore all updates from older causal lengths.
    // They won't win at anything.
    if insert_cl < local_cl {
        return Ok(ResultCode::OK);
    }

    let is_delete = insert_cl % 2 == 0;
    // Resurrect or update to latest cl.
    // The current node might have missed the delete preceeding this causal length
    // in out-of-order delivery setups but we still call it a resurrect as special
    // handling needs to happen in the "alive -> missed_delete -> alive" case.
    let needs_resurrect = insert_cl > local_cl && insert_cl % 2 == 1;
    let row_exists_locally = local_cl != 0;
    let is_sentinel_only = crate::c::INSERT_SENTINEL == insert_col;

    if is_delete {
        // We got a delete event but we've already processed a delete at that version.
        // Just bail.
        if insert_cl == local_cl {
            return Ok(ResultCode::OK);
        }
        // else, it is a delete and the cl is > than ours. Drop the row.
        let merge_result = merge_delete(
            db,
            (*tab).pExtData,
            &tbl_info,
            &unpacked_pks,
            key,
            insert_col_vrsn,
            insert_db_vrsn,
            insert_site_id,
            insert_seq,
        );
        match merge_result {
            Err(rc) => {
                return Err(rc);
            }
            Ok(inner_rowid) => {
                (*(*tab).pExtData).rowsImpacted += 1;
                *rowid = slab_rowid(tbl_info_index as i32, inner_rowid);
                return Ok(ResultCode::OK);
            }
        }
    }

    /*
    || crsql_columnExists(
            // TODO: only safe because we _know_ this is actually a cstr
            insert_col.as_ptr() as *const c_char,
            (*tbl_info).nonPks,
            (*tbl_info).nonPksLen,
        ) == 0
     */
    if is_sentinel_only {
        // If it is a sentinel but the local_cl already matches, nothing to do
        // as the local sentinel already has the same data!
        if insert_cl == local_cl {
            return Ok(ResultCode::OK);
        }
        let merge_result = merge_sentinel_only_insert(
            db,
            (*tab).pExtData,
            &tbl_info,
            &unpacked_pks,
            key,
            insert_col_vrsn,
            insert_db_vrsn,
            insert_site_id,
            insert_seq,
        );
        match merge_result {
            Err(rc) => {
                return Err(rc);
            }
            Ok(inner_rowid) => {
                // a success & rowid of -1 means the merge was a no-op
                if inner_rowid != -1 {
                    (*(*tab).pExtData).rowsImpacted += 1;
                    *rowid = slab_rowid(tbl_info_index as i32, inner_rowid);
                    return Ok(ResultCode::OK);
                } else {
                    return Ok(ResultCode::OK);
                }
            }
        }
    }

    // we got a causal length which would resurrect the row.
    // In an in-order delivery situation then `sentinel_only` would have already resurrected the row
    // In out-of-order delivery, we need to resurrect the row as soon as we get a value
    // which should resurrect the row. I.e., don't wait on the sentinel value to resurrect the row!
    // If the row does not exist locally and the insert_cl is > 1 then we need to create a sentinel to record the insert cl.
    // Not doing so will cause us to assume a cl of 1.
    if needs_resurrect && (row_exists_locally || (!row_exists_locally && insert_cl > 1)) {
        // this should work -- same as `merge_sentinel_only_insert` except we're not done once we do it
        // and the version to set to is the cl not col_vrsn of current insert
        merge_sentinel_only_insert(
            db,
            (*tab).pExtData,
            &tbl_info,
            &unpacked_pks,
            key,
            insert_cl,
            insert_db_vrsn,
            insert_site_id,
            insert_seq,
        )?;
        (*(*tab).pExtData).rowsImpacted += 1;
    }

    // we can short-circuit via needs_resurrect
    // given the greater cl automatically means a win.
    // or if we realize that the row does not exist locally at all.
    let does_cid_win = if needs_resurrect || !row_exists_locally {
        true
    } else if use_clock_scan {
        match prefetched_col_vrsn {
            // target column unseen locally: same as the legacy missing-row win.
            None => true,
            Some(local_vrsn) => {
                match compare_col_version(insert_col_vrsn, local_vrsn) {
                    Some(won) => won,
                    None => did_cid_win_with_local_version(
                        db,
                        (*tab).pExtData,
                        insert_tbl,
                        &tbl_info,
                        &unpacked_pks,
                        key,
                        insert_val,
                        insert_site_id,
                        insert_col,
                        errmsg,
                    )?,
                }
            }
        }
    } else {
        did_cid_win(
            db,
            (*tab).pExtData,
            insert_tbl,
            &tbl_info,
            &unpacked_pks,
            key,
            insert_val,
            insert_site_id,
            insert_col,
            insert_col_vrsn,
            errmsg,
        )?
    };

    if !does_cid_win {
        // doesCidWin == 0? compared against our clocks, nothing wins. OK and
        // Done.
        return Ok(ResultCode::OK);
    }

    // TODO: this is all almost identical between all three merge cases!
    let merge_stmt_ref = tbl_info.get_merge_insert_stmt(db, insert_col)?;
    let merge_stmt = merge_stmt_ref.as_ref().ok_or(ResultCode::ERROR)?;

    let bind_result = bind_package_to_stmt(merge_stmt.stmt, &unpacked_pks, 0)
        .and_then(|_| merge_stmt.bind_value(unpacked_pks.len() as i32 + 1, insert_val))
        .and_then(|_| merge_stmt.bind_value(unpacked_pks.len() as i32 + 2, insert_val));
    if let Err(rc) = bind_result {
        reset_cached_stmt(merge_stmt.stmt)?;
        return Err(rc);
    }

    unsafe {
        *(*(*tab).pExtData).syncBit = 1;
    }
    let rc = merge_stmt.step();

    reset_cached_stmt(merge_stmt.stmt)?;

    unsafe {
        *(*(*tab).pExtData).syncBit = 0;
    }

    if let Err(rc) = rc {
        return Err(rc);
    }

    let merge_result = set_winner_clock(
        db,
        (*tab).pExtData,
        &tbl_info,
        key,
        insert_col,
        insert_col_vrsn,
        insert_db_vrsn,
        insert_site_id,
        insert_seq,
    );
    match merge_result {
        Err(rc) => {
            return Err(rc);
        }
        Ok(inner_rowid) => {
            (*(*tab).pExtData).rowsImpacted += 1;
            *rowid = slab_rowid(tbl_info_index as i32, inner_rowid);
            return Ok(ResultCode::OK);
        }
    }
}
