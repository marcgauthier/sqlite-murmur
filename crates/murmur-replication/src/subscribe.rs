//! C bridge for live query subscriptions (point 17).
//!
//! `murmur_subscribe` resolves the database's [`Control`], snapshots the
//! current cr-sqlite version, and starts a worker-fed subscription whose
//! fetch closure re-runs the user's SELECT on the replication worker
//! connection. Events serialize to JSON; polling maps timeout/closed
//! distinctly. All entry points catch panics and never unwind into C.

use crate::{storage::Worker, *};
use std::{
    ffi::{CStr, c_char, c_int},
    sync::Arc,
    time::Duration,
};

/// Mirrors `murmur_sub_opts` (repr order must match the C header).
#[repr(C)]
pub struct SubOpts {
    pub buffer_size: c_int,
    pub emit_unchanged: c_int,
    pub resume_cursor_json: *const c_char,
}

/// Opaque subscription handle (`murmur_sub`).
pub struct MurmurSub {
    sub: murmur_query::subscription::Subscription<String>,
}

fn cstr_to_string(p: *const c_char) -> Option<String> {
    if p.is_null() {
        return None;
    }
    unsafe { CStr::from_ptr(p) }.to_str().ok().map(str::to_owned)
}

fn fetch_rows(worker: &Arc<Worker>, sql: &str) -> murmur_query::Result<Vec<String>> {
    let sql = sql.to_owned();
    let rows = worker
        .call(move |s| {
            let mut q = s.db.prepare(&sql)?;
            let names: Vec<String> =
                q.column_names().iter().map(|n| n.to_string()).collect();
            let mut out = Vec::new();
            let mut rows = q.query([])?;
            while let Some(r) = rows.next()? {
                let mut obj = serde_json::Map::new();
                for (i, name) in names.iter().enumerate() {
                    let value = match r.get_ref(i)? {
                        rusqlite::types::ValueRef::Null => serde_json::Value::Null,
                        rusqlite::types::ValueRef::Integer(v) => v.into(),
                        rusqlite::types::ValueRef::Real(v) => {
                            serde_json::Number::from_f64(v)
                                .map_or(serde_json::Value::Null, serde_json::Value::Number)
                        }
                        rusqlite::types::ValueRef::Text(v) => {
                            String::from_utf8_lossy(v).into_owned().into()
                        }
                        rusqlite::types::ValueRef::Blob(v) => {
                            v.iter().map(|b| format!("{b:02x}")).collect::<String>().into()
                        }
                    };
                    obj.insert(name.clone(), value);
                }
                out.push(serde_json::Value::Object(obj).to_string());
            }
            Ok(out)
        })
        .map_err(|e| murmur_query::Error::Invalid(e.to_string()))?;
    Ok(rows)
}

/// # Safety
/// `db`, `sql`, and `tables_json` must be valid non-null pointers; `opts`
/// may be null for defaults. The caller owns the returned handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn murmur_subscribe(
    db: *mut rusqlite::ffi::sqlite3,
    sql: *const c_char,
    tables_json: *const c_char,
    opts: *const SubOpts,
) -> *mut MurmurSub {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let control = crate::sql::control_for(db)?;
        let sql = cstr_to_string(sql)?;
        let tables: Vec<String> =
            serde_json::from_str(&cstr_to_string(tables_json)?).ok()?;
        let (buffer_size, emit_unchanged, resume) = if opts.is_null() {
            (32usize, false, None)
        } else {
            let opts = unsafe { &*opts };
            let resume = cstr_to_string(opts.resume_cursor_json)
                .filter(|s| !s.is_empty())
                .map(|s| serde_json::from_str(&s))
                .transpose()
                .ok()?;
            (
                (opts.buffer_size as usize).clamp(1, 4096),
                opts.emit_unchanged != 0,
                resume,
            )
        };
        let worker = control.worker().clone();
        let seq = worker
            .call(|s| {
                s.ensure_crsql()?;
                murmur_query::adapt::crsql_db_version(&s.db).map_err(config::query_err)
            })
            .ok()?;
        let sub_mgr = worker.call(|s| Ok(s.sub_manager())).ok()?;
        let fetch_worker = worker.clone();
        let fetch_sql = sql.clone();
        let sub = murmur_query::subscription::start_subscription(
            &sub_mgr,
            seq,
            move || fetch_rows(&fetch_worker, &fetch_sql),
            |row: &String| murmur_query::digest::hash(row.as_bytes()).to_vec(),
            murmur_query::subscription::SubscriptionOptions {
                buffer_size,
                emit_unchanged,
                resume_from: resume,
                tables,
            },
        )
        .ok()?;
        Some(Box::into_raw(Box::new(MurmurSub { sub })))
    }));
    result.ok().flatten().unwrap_or(std::ptr::null_mut())
}

/// # Safety
/// `s` must be a live handle from `murmur_subscribe`; `event_json` must
/// be a valid out-pointer. See the header for return codes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn murmur_sub_poll(
    s: *mut MurmurSub,
    timeout_ms: c_int,
    event_json: *mut *mut c_char,
) -> c_int {
    if s.is_null() || event_json.is_null() {
        return -1;
    }
    unsafe { *event_json = std::ptr::null_mut() };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let sub = unsafe { &(*s).sub };
        let event = if timeout_ms < 0 {
            sub.recv()
        } else {
            sub.recv_timeout(Duration::from_millis(timeout_ms as u64))
        };
        match event {
            Ok(e) => {
                let json = serde_json::to_string(&e).map_err(|_| -2)?;
                let owned = unsafe { crate::sql::sqlite_string(&json) };
                if owned.is_null() {
                    return Err(-3);
                }
                unsafe { *event_json = owned };
                Ok(1)
            }
            Err(murmur_query::Error::Busy) => Ok(0),
            Err(_) => Err(-1),
        }
    }));
    result.unwrap_or(Err(-4)).unwrap_or_else(|code| code)
}

/// # Safety
/// `event_json` must be NULL or a string from `murmur_sub_poll`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn murmur_sub_free_event(event_json: *mut c_char) {
    if !event_json.is_null() {
        unsafe { rusqlite::ffi::sqlite3_free(event_json.cast()) };
    }
}

/// # Safety
/// `s` must be NULL or a live handle; closes at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn murmur_sub_close(s: *mut MurmurSub) {
    if s.is_null() {
        return;
    }
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drop(unsafe { Box::from_raw(s) });
    }));
}

