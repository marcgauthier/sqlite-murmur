//! Point 8 host wiring: High/Low ownership events, reduced owners, SQL.
//!
//! Pure kernels live in `murmur-policy` (`high_low` authority selection,
//! `ownership` event envelopes). This module owns the SQLite side: the
//! `murmur_owner_events` carrier table (a CRR so grants/releases
//! replicate), reduced-owner persistence in `murmur_kv`, and the
//! grant/release/projection UDF backends.
//!
//! v1 scope (documented limits):
//! - Single-column-PK (or rowid) targets: native metadata reads match
//!   the row's clock entries by raw pk value.
//! - Stamp is the native `(row_cl, col_version)` pair plus causal position: deterministic
//!   and convergent, but cross-node ordering is only meaningful under
//!   the single-grantor-per-target deployment (the sane one: the High
//!   domain grants on its own targets). Concurrent multi-node grants
//!   resolve deterministically, not authoritatively.
//! - Reduction is lazy (on grant/release/projection) plus explicit
//!   `murmur_hl_reduce()`; there is no post-apply hook.

use crate::{
    Error, Result,
    policy_ingest::{self, Resolved},
};
use murmur_policy::{
    high_low::{self, Owner},
    model::Value,
    ownership::{self, Event, EventKind, EventRef, Target, TargetKind},
};
use rusqlite::{Connection, OptionalExtension, types::Value as SqlValue};
use serde::{Deserialize, Serialize};

/// Coordinator-owned reduced-owner envelope (v1 postcard).
/// Host stamp: native `(row_cl, col_version)` plus the event's
/// position in deterministic order, so same-clock grant/release pairs
/// still order (a release stored without an intervening write must
/// not tie its grant's stamp). Deterministic across nodes holding the
/// same event set.
pub(crate) type Stamp = (i64, i64, i64);

/// Coordinator-owned reduced-owner envelope (v1 postcard).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct ReducedOwner {
    pub version: u8,
    pub epoch: Stamp,
    pub release_epoch: Stamp,
    pub owned: bool,
    pub value: Option<Value>,
    pub native_version: Option<Stamp>,
    pub applied: u64,
    pub skipped: u64,
}

/// Install the event carrier table (plain DDL; CRR-ified separately).
pub(crate) fn install(db: &Connection) -> Result<()> {
    let prior: Option<String> = db
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?",
            [ownership::EVENT_TABLE],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(existing) = prior {
        let strip = |s: &str| s.trim_end().trim_end_matches(';').trim_end().to_string();
        if strip(&existing) != strip(ownership::CREATE_EVENT_SQL) {
            return Err(Error::Protocol("murmur_owner_events diverges".into()));
        }
    } else {
        db.execute_batch(ownership::CREATE_EVENT_SQL)?;
    }
    Ok(())
}

pub(crate) fn ensure_owner_crr(db: &Connection) -> Result<()> {
    let clock = format!("{}__crsql_clock", ownership::EVENT_TABLE);
    let crr: i64 = db.query_row(
        "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name=?",
        [&clock],
        |r| r.get(0),
    )?;
    if crr != 1 {
        db.execute_batch(&format!(
            "SELECT crsql_as_crr('main', \"{}\")",
            ownership::EVENT_TABLE
        ))?;
        let crr: i64 = db.query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name=?",
            [&clock],
            |r| r.get(0),
        )?;
        if crr != 1 {
            return Err(Error::Protocol("owner event table is not a CRR".into()));
        }
    }
    Ok(())
}

fn policy_err(e: murmur_policy::Error) -> Error {
    match e {
        murmur_policy::Error::Invalid(m) => Error::Invalid(m),
        murmur_policy::Error::Corrupt(m) => Error::Protocol(m),
        murmur_policy::Error::Limit(m) => Error::Limit(m),
        murmur_policy::Error::SchemaMismatch => Error::Protocol("ownership schema mismatch".into()),
    }
}

fn kv_get(db: &Connection, key: &[u8]) -> Result<Option<Vec<u8>>> {
    Ok(db
        .query_row("SELECT value FROM murmur_kv WHERE key=?", [key], |r| {
            r.get(0)
        })
        .optional()?)
}

fn kv_put(db: &Connection, key: &[u8], value: &[u8]) -> Result<()> {
    db.execute(
        "INSERT OR REPLACE INTO murmur_kv(key,value) VALUES(?,?)",
        rusqlite::params![key, value],
    )?;
    Ok(())
}

/// Native `(row_cl, col_version)` for a target cell, read from the
/// row's clock entries. Clock keys hold the raw pk value (verified
/// live: integer pk 1 reads back key 1), so the pk binds directly.
fn native_meta(db: &Connection, target: &Resolved, pk: &SqlValue) -> Result<(i64, i64)> {
    let clock = format!("{}__crsql_clock", target.table);
    let has_cl: i64 = db.query_row(
        &format!("SELECT count(*) FROM pragma_table_info(\"{clock}\") WHERE name='cl'"),
        [],
        |r| r.get(0),
    )?;
    // Older clock shapes lack `cl`; db_version is the honest fallback
    // ordering token on those databases.
    let causal = if has_cl == 1 { "cl" } else { "db_version" };
    let hit: Option<(i64, i64)> = db
        .query_row(
            &format!(
                "SELECT \"{causal}\", col_version FROM \"{clock}\" WHERE key=? AND col_name=?"
            ),
            rusqlite::params![pk, target.column],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    hit.filter(|(_, cv)| *cv > 0)
        .ok_or_else(|| Error::Invalid("ownership needs a clocked row".into()))
}

fn hl_target(target: &Target) -> high_low::Target {
    high_low::Target {
        table: target.table.clone(),
        pk: target.pk.clone(),
        column: match &target.kind {
            TargetKind::Row => high_low::ROW.to_string(),
            TargetKind::Column(c) => c.clone(),
        },
    }
}

fn make_target(table: &str, pk_bytes: Vec<u8>, column: &str) -> Result<Target> {
    let kind = if column.is_empty() {
        TargetKind::Row
    } else {
        TargetKind::Column(column.to_string())
    };
    Target::new(table.to_string(), pk_bytes, kind).map_err(policy_err)
}

fn digest(data: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    sha2::Sha256::digest(data).into()
}

/// One stored event with its row coordinates.
struct Stored {
    origin: Vec<u8>,
    row_cl: i64,
    col_version: i64,
    event: Event,
    digest: [u8; 32],
}

fn load_events(db: &Connection, target: &Target, budget: usize) -> Result<Vec<Stored>> {
    let (kind_no, column) = match &target.kind {
        TargetKind::Row => (0i64, ""),
        TargetKind::Column(c) => (1i64, c.as_str()),
    };
    let mut q = db.prepare(
        "SELECT origin_register,row_cl,col_version,payload FROM murmur_owner_events \
         WHERE target_table=? AND target_pk=? AND target_kind=? AND target_column=?",
    )?;
    let rows: Vec<(Vec<u8>, i64, i64, Vec<u8>)> = q
        .query_map(
            rusqlite::params![target.table, target.pk, kind_no, column],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut out = Vec::new();
    for (origin, row_cl, col_version, payload) in rows {
        if payload.is_empty() {
            continue;
        }
        let event = match ownership::decode_event(&payload, budget) {
            Ok(e) if e.target == *target => e,
            _ => continue,
        };
        out.push(Stored {
            origin,
            row_cl,
            col_version,
            digest: digest(&payload),
            event,
        });
    }
    Ok(out)
}

/// Causal order for one target's events: topological over prior refs
/// (the same event set folds identically on every node), ready ties
/// by native version then digest, unresolvable leftovers last.
fn ordered_events(db: &Connection, target: &Target, budget: usize) -> Result<Vec<Stored>> {
    let stored = load_events(db, target, budget)?;
    let index_of: std::collections::BTreeMap<[u8; 32], usize> = stored
        .iter()
        .enumerate()
        .map(|(i, s)| (s.digest, i))
        .collect();
    let mut deps: Vec<Vec<usize>> = vec![Vec::new(); stored.len()];
    for (i, s) in stored.iter().enumerate() {
        for prior in [&s.event.prior_grant, &s.event.prior_release]
            .into_iter()
            .flatten()
        {
            if let Some(&j) = index_of.get(&prior.payload_digest) {
                if j != i {
                    deps[i].push(j);
                }
            }
        }
    }
    let rank = |s: &Stored| (s.row_cl, s.col_version, s.digest);
    let mut done = vec![false; stored.len()];
    let mut order = Vec::with_capacity(stored.len());
    loop {
        let mut ready: Vec<usize> = (0..stored.len())
            .filter(|&i| !done[i] && deps[i].iter().all(|&j| done[j]))
            .collect();
        if ready.is_empty() {
            break;
        }
        ready.sort_by_key(|&i| rank(&stored[i]));
        done[ready[0]] = true;
        order.push(ready[0]);
    }
    let mut tail: Vec<usize> = (0..stored.len()).filter(|&i| !done[i]).collect();
    tail.sort_by_key(|&i| rank(&stored[i]));
    order.extend(tail);
    let mut slots: Vec<Option<Stored>> = stored.into_iter().map(Some).collect();
    Ok(order
        .into_iter()
        .map(|i| slots[i].take().expect("topo index"))
        .collect())
}

/// Reference to the chain-head event of one kind for a target, if any.
fn last_ref(
    db: &Connection,
    target: &Target,
    is_release: bool,
    budget: usize,
) -> Result<Option<EventRef>> {
    for s in ordered_events(db, target, budget)?.into_iter().rev() {
        if matches!(s.event.kind, EventKind::Release) == is_release {
            let origin_register: [u8; 16] = s
                .origin
                .try_into()
                .map_err(|_| Error::Protocol("bad origin register".into()))?;
            return ownership::event_ref(
                &s.event,
                origin_register,
                s.row_cl,
                s.col_version,
                budget,
            )
            .map(Some)
            .map_err(policy_err);
        }
    }
    Ok(None)
}

fn store_event(
    db: &Connection,
    target: &Target,
    origin: [u8; 16],
    row_cl: i64,
    col_version: i64,
    payload: &[u8],
    budget: usize,
) -> Result<()> {
    // Validate the row fields against the decoded event before writing.
    let event = ownership::decode_event(payload, budget).map_err(policy_err)?;
    if event.target != *target {
        return Err(Error::Protocol("event target mismatch".into()));
    }
    let (kind_no, column) = match &target.kind {
        TargetKind::Row => (0i64, ""),
        TargetKind::Column(c) => (1i64, c.as_str()),
    };
    db.execute(
        "INSERT OR IGNORE INTO murmur_owner_events(\
         target_table,target_pk,target_kind,target_column,\
         origin_register,col_version,row_cl,payload_digest,payload) \
         VALUES(?,?,?,?,?,?,?,?,?)",
        rusqlite::params![
            target.table,
            target.pk,
            kind_no,
            column,
            origin.as_slice(),
            col_version,
            row_cl,
            digest(payload).as_slice(),
            payload,
        ],
    )?;
    Ok(())
}

/// Fold every stored event for one target into the reduced owner and
/// persist it. Deterministic order; stale/corrupt events are skipped
/// and counted, never fatal (convergent on all nodes).
pub(crate) fn reduce_target(db: &Connection, target: &Target) -> Result<ReducedOwner> {
    let budget = policy_ingest::byte_budget(db)?;
    let ordered = ordered_events(db, target, budget)?;
    // Empty/invalid payloads never reach the fold (filtered at load);
    // count them from the raw row total for the envelope stats.
    let (kind_no, column) = match &target.kind {
        TargetKind::Row => (0i64, ""),
        TargetKind::Column(c) => (1i64, c.as_str()),
    };
    let raw: i64 = db.query_row(
        "SELECT count(*) FROM murmur_owner_events \
         WHERE target_table=? AND target_pk=? AND target_kind=? AND target_column=?",
        rusqlite::params![target.table, target.pk, kind_no, column],
        |r| r.get(0),
    )?;
    let key = ownership::target_key(target).map_err(policy_err)?;
    let mut running: Option<Owner<Stamp>> = None;
    let mut applied = 0u64;
    let mut skipped = raw.saturating_sub(ordered.len() as i64).max(0) as u64;
    for (position, s) in ordered.into_iter().enumerate() {
        let accepted: Stamp = (s.row_cl, s.col_version, position as i64);
        let event = s.event;
        let next = (|| -> murmur_policy::Result<Owner<Stamp>> {
            match event.kind {
                EventKind::Grant { value } => match &target.kind {
                    TargetKind::Row => {
                        high_low::own_deletion(hl_target(target), accepted, (0, 0, 0))
                    }
                    TargetKind::Column(_) => high_low::promote_value(
                        hl_target(target),
                        running.as_ref(),
                        accepted,
                        value.ok_or_else(|| {
                            murmur_policy::Error::Invalid("column grant needs value".into())
                        })?,
                        (0, 0, 0),
                    ),
                },
                EventKind::Release => {
                    let current = running.as_ref().ok_or_else(|| {
                        murmur_policy::Error::Invalid("release without owner".into())
                    })?;
                    high_low::release(current, accepted)
                }
            }
        })();
        match next {
            Ok(owner) => {
                running = Some(owner);
                applied += 1;
            }
            Err(_) => skipped += 1,
        }
    }
    let reduced = match running {
        Some(o) => ReducedOwner {
            version: 1,
            epoch: o.epoch.clone(),
            release_epoch: o.release_epoch.clone(),
            owned: o.owned,
            value: o.value.clone().map(|v| v.value),
            native_version: o.value.as_ref().map(|v| v.native_version.clone()),
            applied,
            skipped,
        },
        None => ReducedOwner {
            version: 1,
            epoch: (0, 0, 0),
            release_epoch: (0, 0, 0),
            owned: false,
            value: None,
            native_version: None,
            applied,
            skipped,
        },
    };
    let bytes = crate::encode(&reduced)?;
    kv_put(db, &key, &bytes)?;
    Ok(reduced)
}

fn load_reduced(db: &Connection, target: &Target) -> Result<Option<ReducedOwner>> {
    let key = ownership::target_key(target).map_err(policy_err)?;
    kv_get(db, &key)?
        .map(|b| {
            let reduced: ReducedOwner = crate::decode(&b)?;
            if reduced.version != 1 {
                return Err(Error::Protocol("owner envelope version".into()));
            }
            Ok(reduced)
        })
        .transpose()
}

/// Row-level native metadata (no column filter): the row's latest
/// clock entry. Used for row grants/releases.
fn row_meta(db: &Connection, table: &str, pk: &SqlValue) -> Result<(i64, i64)> {
    if !crate::schema::valid_ident(table) {
        return Err(Error::Invalid("rejected ownership table".into()));
    }
    let clock = format!("{table}__crsql_clock");
    let has: i64 = db.query_row(
        "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name=?",
        [&clock],
        |r| r.get(0),
    )?;
    if has != 1 {
        return Err(Error::Invalid("ownership target must be a CRR".into()));
    }
    let has_cl: i64 = db.query_row(
        &format!("SELECT count(*) FROM pragma_table_info(\"{clock}\") WHERE name='cl'"),
        [],
        |r| r.get(0),
    )?;
    let causal = if has_cl == 1 { "cl" } else { "db_version" };
    let hit: Option<(i64, i64)> = db
        .query_row(
            &format!(
                "SELECT \"{causal}\", col_version FROM \"{clock}\" WHERE key=? \
                 ORDER BY \"{causal}\" DESC, col_version DESC"
            ),
            [pk],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    hit.filter(|(_, cv)| *cv > 0)
        .ok_or_else(|| Error::Invalid("ownership needs a clocked row".into()))
}

/// Grant High ownership of a row (column "") or column. Returns a
/// JSON summary of the reduced owner.
pub(crate) fn grant(
    db: &Connection,
    table: &str,
    pk: &SqlValue,
    column: &str,
) -> Result<serde_json::Value> {
    let budget = policy_ingest::byte_budget(db)?;
    let pk_bytes = policy_ingest::encode_pk(pk)?;
    let target = make_target(table, pk_bytes, column)?;
    let (row_cl, col_version, value) = if column.is_empty() {
        let meta = row_meta(db, table, pk)?;
        (meta.0, meta.1, None)
    } else {
        let resolved = policy_ingest::resolve_target(db, table, column, pk)?;
        if !policy_ingest::row_live(db, &resolved, pk)? {
            return Err(Error::Invalid("ownership target row is gone".into()));
        }
        let meta = native_meta(db, &resolved, pk)?;
        let cell = policy_ingest::current_cell(db, &resolved, pk)?
            .ok_or_else(|| Error::Invalid("ownership target row is gone".into()))?;
        (meta.0, meta.1, Some(policy_ingest::sql_to_policy(&cell)?))
    };
    let (node, _) = policy_ingest::actor(db)?;
    let event = Event {
        target: target.clone(),
        kind: EventKind::Grant { value },
        prior_grant: last_ref(db, &target, false, budget)?,
        prior_release: last_ref(db, &target, true, budget)?,
    };
    let payload = ownership::encode_event(&event, budget).map_err(policy_err)?;
    store_event(db, &target, node, row_cl, col_version, &payload, budget)?;
    let reduced = reduce_target(db, &target)?;
    Ok(serde_json::json!({
        "target": format!("{}.{}(row={})", target.table,
            if column.is_empty() { "*" } else { column }, column.is_empty()),
        "owned": reduced.owned,
        "epoch": reduced.epoch,
        "applied": reduced.applied,
        "skipped": reduced.skipped,
    }))
}

/// Release High ownership. The target must currently be owned.
pub(crate) fn release(
    db: &Connection,
    table: &str,
    pk: &SqlValue,
    column: &str,
) -> Result<serde_json::Value> {
    let budget = policy_ingest::byte_budget(db)?;
    let pk_bytes = policy_ingest::encode_pk(pk)?;
    let target = make_target(table, pk_bytes, column)?;
    let current = reduce_target(db, &target)?;
    if !current.owned {
        return Err(Error::Invalid("target is not owned".into()));
    }
    let (row_cl, col_version) = if column.is_empty() {
        row_meta(db, table, pk)?
    } else {
        let resolved = policy_ingest::resolve_target(db, table, column, pk)?;
        native_meta(db, &resolved, pk)?
    };
    let (node, _) = policy_ingest::actor(db)?;
    let prior_grant = last_ref(db, &target, false, budget)?
        .ok_or_else(|| Error::Invalid("release needs a prior grant".into()))?;
    let event = Event {
        target: target.clone(),
        kind: EventKind::Release,
        prior_grant: Some(prior_grant),
        prior_release: last_ref(db, &target, true, budget)?,
    };
    let payload = ownership::encode_event(&event, budget).map_err(policy_err)?;
    store_event(db, &target, node, row_cl, col_version, &payload, budget)?;
    let reduced = reduce_target(db, &target)?;
    Ok(serde_json::json!({
        "owned": reduced.owned,
        "release_epoch": reduced.release_epoch,
        "applied": reduced.applied,
        "skipped": reduced.skipped,
    }))
}

/// Authority selection for a column: `low`, `suppress-row`, or
/// `high:<json value>`. Reduces lazily so remote events apply on read.
/// `column == ""` reports row ownership (`owned`/`unowned`).
pub(crate) fn projection(
    db: &Connection,
    table: &str,
    pk: &SqlValue,
    column: &str,
) -> Result<String> {
    let pk_bytes = policy_ingest::encode_pk(pk)?;
    if column.is_empty() {
        let target = make_target(table, pk_bytes, "")?;
        let reduced = reduce_target(db, &target)?;
        return Ok(if reduced.owned {
            "owned".into()
        } else {
            "unowned".into()
        });
    }
    let row_target = make_target(table, pk_bytes.clone(), "")?;
    let col_target = make_target(table, pk_bytes, column)?;
    let row_reduced = reduce_target(db, &row_target)?;
    let col_reduced = reduce_target(db, &col_target)?;
    let row_owner = reduced_to_owner(&row_target, &row_reduced);
    let col_owner = reduced_to_owner(&col_target, &col_reduced);
    let target = high_low::Target {
        table: row_target.table.clone(),
        pk: row_target.pk.clone(),
        column: column.to_ascii_lowercase(),
    };
    match high_low::select_projection(&target, Some(&row_owner), Some(&col_owner))
        .map_err(policy_err)?
    {
        high_low::Projection::SuppressRow => Ok("suppress-row".into()),
        high_low::Projection::Low => Ok("low".into()),
        high_low::Projection::High(v) => Ok(format!(
            "high:{}",
            serde_json::to_string(&v.value).map_err(|e| Error::Protocol(e.to_string()))?
        )),
    }
}

/// Eagerly reduce every target holding events. Returns targets reduced.
pub(crate) fn reduce_all(db: &Connection) -> Result<u64> {
    let mut q = db.prepare(
        "SELECT DISTINCT target_table,target_pk,target_kind,target_column \
         FROM murmur_owner_events",
    )?;
    let rows: Vec<(String, Vec<u8>, i64, String)> = q
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut count = 0u64;
    for (table, pk, kind_no, column) in rows {
        let kind = match (kind_no, column.as_str()) {
            (0, "") => TargetKind::Row,
            (1, c) => TargetKind::Column(c.to_string()),
            _ => return Err(Error::Protocol("bad owner target kind".into())),
        };
        let target = Target::new(table, pk, kind).map_err(policy_err)?;
        reduce_target(db, &target)?;
        count += 1;
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE murmur_config(name TEXT PRIMARY KEY,value TEXT NOT NULL);
             CREATE TABLE murmur_identity(id INTEGER PRIMARY KEY CHECK(id=1),node BLOB,site BLOB,incarnation INTEGER NOT NULL DEFAULT 0);
             INSERT INTO murmur_identity(id,node,site) VALUES(1,X'01010101010101010101010101010101',X'02020202020202020202020202020202');
             CREATE TABLE murmur_kv(key BLOB PRIMARY KEY,value BLOB NOT NULL);
             CREATE TABLE docs(id INTEGER PRIMARY KEY, n INTEGER);
             CREATE TABLE docs__crsql_clock(key BLOB, col_name TEXT, col_version INTEGER, db_version INTEGER, site_id BLOB, seq INTEGER, cl INTEGER);",
        )
        .unwrap();
        install(&db).unwrap();
        db.execute("INSERT INTO docs(id,n) VALUES(1,42)", [])
            .unwrap();
        db.execute(
            "INSERT INTO docs__crsql_clock(key,col_name,col_version,db_version,site_id,seq,cl) VALUES(?,?,?,?,?,?,?)",
            rusqlite::params![1i64, "n", 1i64, 2i64, vec![3u8; 16], 1i64, 2i64,],
        )
        .unwrap();
        db
    }

    #[test]
    fn column_grant_projects_high_then_release_goes_low() {
        let db = setup();
        let pk = SqlValue::Integer(1);
        assert_eq!(projection(&db, "docs", &pk, "n").unwrap(), "low");
        let g = grant(&db, "docs", &pk, "n").unwrap();
        assert_eq!(g["owned"], true);
        let p = projection(&db, "docs", &pk, "n").unwrap();
        assert!(p.starts_with("high:"), "{p}");
        assert!(p.contains("42"), "{p}");
        let r = release(&db, "docs", &pk, "n").unwrap();
        assert_eq!(r["owned"], false);
        assert_eq!(projection(&db, "docs", &pk, "n").unwrap(), "low");
        // Releasing an unowned target fails; kv holds the reduced owner.
        assert!(release(&db, "docs", &pk, "n").is_err());
        assert_eq!(reduce_all(&db).unwrap(), 1);
    }

    #[test]
    fn row_grant_suppresses_column_projection() {
        let db = setup();
        let pk = SqlValue::Integer(1);
        grant(&db, "docs", &pk, "").unwrap();
        assert_eq!(projection(&db, "docs", &pk, "").unwrap(), "owned");
        assert_eq!(projection(&db, "docs", &pk, "n").unwrap(), "suppress-row");
        release(&db, "docs", &pk, "").unwrap();
        assert_eq!(projection(&db, "docs", &pk, "").unwrap(), "unowned");
        assert_eq!(projection(&db, "docs", &pk, "n").unwrap(), "low");
    }

    #[test]
    fn ownership_rejects_bad_targets() {
        let db = setup();
        let pk = SqlValue::Integer(1);
        assert!(grant(&db, "missing", &pk, "n").is_err());
        assert!(grant(&db, "docs", &SqlValue::Text("x".into()), "n").is_err());
        assert!(grant(&db, "docs", &SqlValue::Integer(2), "n").is_err());
        assert!(release(&db, "docs", &pk, "n").is_err());
    }
}

fn reduced_to_owner(target: &Target, reduced: &ReducedOwner) -> Owner<Stamp> {
    Owner {
        target: hl_target(target),
        epoch: reduced.epoch.clone(),
        release_epoch: reduced.release_epoch.clone(),
        owned: reduced.owned,
        value: match (&reduced.value, &reduced.native_version) {
            (Some(value), Some(native_version)) => Some(high_low::OwnedValue {
                value: value.clone(),
                native_version: native_version.clone(),
            }),
            _ => None,
        },
    }
}
