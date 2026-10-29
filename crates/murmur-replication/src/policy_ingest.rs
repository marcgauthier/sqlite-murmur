//! Point 7 host ingest/projection adapter for murmur-policy carriers.
//!
//! The pure kernels (counter/set join, carrier validation/reduction,
//! extrema) live in `murmur-policy`; this module owns the SQLite side:
//! carrier-table install as a CRR (components ride native replication),
//! local-op component writes, and reduce-then-project after local ops
//! and after ingesting remote batches.
//!
//! v1 scope (documented limits):
//! - Single-incarnation: every component uses incarnation 0. Row
//!   delete/reinsert does not fence old components; a reinserted row
//!   recomputes over surviving components. No carrier GC (per proposal).
//! - Single-column-PK (or rowid) targets only; multi-column PKs error.
//! - Projections are write-only outputs: received projected scalars are
//!   never read as merge inputs, so LWW flapping cannot corrupt totals.
//! - Recompute after a remote batch covers every configured target that
//!   has carrier rows (bounded by the 1024-entry config map).

use crate::{Error, NodeId, Result};
use murmur_policy::{
    carrier::{self, Payload, Row, Target},
    causal::State,
    model::Value,
    policies::{Causal, Counter, Policy, Set, Tag},
};
use rusqlite::{Connection, OptionalExtension, types::Value as SqlValue};
use std::collections::BTreeMap;

const KIND_COUNTER: i64 = 1;
const KIND_SET_ADD: i64 = 2;
const KIND_SET_REMOVE: i64 = 3;
const KIND_MIN: i64 = 4;
const KIND_MAX: i64 = 5;

/// Install the carrier table (plain DDL; shared with the bare test
/// store). CRR-ification is separate ([`ensure_carrier_crr`]) because
/// the bare store has no cr-sqlite.
pub(crate) fn install(db: &Connection) -> Result<()> {
    let prior: Option<String> = db
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?",
            [carrier::TABLE],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(existing) = prior {
        let strip = |s: &str| s.trim_end().trim_end_matches(';').trim_end().to_string();
        if strip(&existing) != strip(carrier::CREATE_SQL) {
            return Err(Error::Protocol("murmur_policy_components diverges".into()));
        }
    } else {
        db.execute_batch(carrier::CREATE_SQL)?;
    }
    Ok(())
}

/// Make the carrier table a CRR so components replicate. Hard error
/// when cr-sqlite is present but conversion fails; callers on the
/// bare (non-crsqlite) store must skip this.
pub(crate) fn ensure_carrier_crr(db: &Connection) -> Result<()> {
    let clock = format!("{}__crsql_clock", carrier::TABLE);
    let crr: i64 = db.query_row(
        "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name=?",
        [&clock],
        |r| r.get(0),
    )?;
    if crr != 1 {
        db.execute_batch(&format!(
            "SELECT crsql_as_crr('main', \"{}\")",
            carrier::TABLE
        ))?;
        let crr: i64 = db.query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name=?",
            [&clock],
            |r| r.get(0),
        )?;
        if crr != 1 {
            return Err(Error::Protocol("carrier table is not a CRR".into()));
        }
    }
    Ok(())
}

fn policy_err(e: murmur_policy::Error) -> Error {
    match e {
        murmur_policy::Error::Invalid(m) => Error::Invalid(m),
        murmur_policy::Error::Corrupt(m) => Error::Protocol(m),
        murmur_policy::Error::Limit(m) => Error::Limit(m),
        murmur_policy::Error::SchemaMismatch => Error::Protocol("policy schema mismatch".into()),
    }
}

/// Parse the `column_policies` map into (table, column) -> policy.
/// `Lww` entries are inert (no carrier handling). Config validation
/// already ran at set time; malformed content here is corruption.
pub(crate) fn policies(db: &Connection) -> Result<BTreeMap<(String, String), Policy>> {
    let raw = crate::config::setting(db, "column_policies")?;
    let map: BTreeMap<String, String> =
        serde_json::from_str(&raw).map_err(|e| Error::Protocol(format!("column_policies: {e}")))?;
    let mut out = BTreeMap::new();
    for (key, name) in map {
        let policy = match name.as_str() {
            "Lww" => continue,
            "Counter" => Policy::Counter,
            "Set" => Policy::Set,
            "Min" => Policy::Min,
            "Max" => Policy::Max,
            _ => return Err(Error::Protocol(format!("unknown merge policy: {name}"))),
        };
        let (table, column) = key
            .split_once('.')
            .ok_or_else(|| Error::Protocol(format!("bad column_policies key: {key}")))?;
        out.insert((table.to_string(), column.to_string()), policy);
    }
    Ok(out)
}

/// Local actor pair (node, site); both must be provisioned non-zero.
pub(crate) fn actor(db: &Connection) -> Result<(NodeId, NodeId)> {
    let pair: Option<(Vec<u8>, Vec<u8>)> = db
        .query_row(
            "SELECT node, site FROM murmur_identity WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let (node, site) = pair.ok_or_else(|| Error::Invalid("policy ops need identity".into()))?;
    let take = |b: Vec<u8>| {
        <[u8; 16]>::try_from(b)
            .ok()
            .filter(|id| *id != [0; 16])
            .ok_or_else(|| Error::Invalid("policy ops need provisioned identity".into()))
    };
    Ok((take(node)?, take(site)?))
}

/// Canonical reversible pk bytes: tag 1 = i64 BE, 2 = UTF-8 text,
/// 3 = raw blob. Used both for carrier `target_pk` and for the
/// caller's pk argument to the op UDFs.
pub(crate) fn encode_pk(value: &SqlValue) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    match value {
        SqlValue::Integer(n) => {
            out.push(1);
            out.extend(n.to_be_bytes());
        }
        SqlValue::Text(s) => {
            out.push(2);
            out.extend(s.as_bytes());
        }
        SqlValue::Blob(b) => {
            out.push(3);
            out.extend(b);
        }
        SqlValue::Real(_) => return Err(Error::Invalid("float primary keys unsupported".into())),
        SqlValue::Null => return Err(Error::Invalid("null primary key".into())),
    }
    Ok(out)
}

pub(crate) fn decode_pk(bytes: &[u8]) -> Result<SqlValue> {
    let (tag, rest) = bytes
        .split_first()
        .ok_or_else(|| Error::Protocol("empty carrier pk".into()))?;
    match tag {
        1 if rest.len() == 8 => Ok(SqlValue::Integer(i64::from_be_bytes(
            rest.try_into()
                .map_err(|_| Error::Protocol("bad carrier pk".into()))?,
        ))),
        2 => Ok(SqlValue::Text(
            std::str::from_utf8(rest)
                .map_err(|_| Error::Protocol("bad carrier pk".into()))?
                .to_string(),
        )),
        3 => Ok(SqlValue::Blob(rest.to_vec())),
        _ => Err(Error::Protocol("bad carrier pk".into())),
    }
}

/// Resolved target: validated table/column, pk column (or rowid), and
/// the canonical pk bytes. v1 supports single-column PKs and rowid.
pub(crate) struct Resolved {
    pub(crate) table: String,
    pub(crate) column: String,
    pub(crate) pk_column: Option<String>,
    pub(crate) pk_bytes: Vec<u8>,
}

pub(crate) fn resolve_target(db: &Connection, table: &str, column: &str, pk: &SqlValue) -> Result<Resolved> {
    if !crate::schema::valid_ident(table) || !crate::schema::valid_ident(column) {
        return Err(Error::Invalid("rejected policy target".into()));
    }
    let mut info = db.prepare(&format!(
        "SELECT name, pk FROM pragma_table_info(\"{table}\")"
    ))?;
    let mut pk_cols = Vec::new();
    let mut has_column = false;
    let mut column_is_pk = false;
    let mut rows = info.query([])?;
    while let Some(r) = rows.next()? {
        let name: String = r.get(0)?;
        let pk: i64 = r.get(1)?;
        if name == column {
            has_column = true;
            column_is_pk = pk > 0;
        }
        if pk > 0 {
            pk_cols.push(name);
        }
    }
    if !has_column {
        return Err(Error::Invalid("unknown policy column".into()));
    }
    if column_is_pk {
        return Err(Error::Invalid("policy column must not be the PK".into()));
    }
    if pk_cols.len() > 1 {
        return Err(Error::Invalid("multi-column PKs unsupported".into()));
    }
    if crsql_present(db)? && !is_crr(db, table)? {
        return Err(Error::Invalid("policy target must be a CRR".into()));
    }
    Ok(Resolved {
        table: table.to_string(),
        column: column.to_string(),
        pk_column: pk_cols.into_iter().next(),
        pk_bytes: encode_pk(pk)?,
    })
}

fn crsql_present(db: &Connection) -> Result<bool> {
    let n: i64 = db.query_row(
        "SELECT count(*) FROM pragma_function_list WHERE name='crsql_as_crr'",
        [],
        |r| r.get(0),
    )?;
    Ok(n == 1)
}

fn is_crr(db: &Connection, table: &str) -> Result<bool> {
    let clock = format!("{table}__crsql_clock");
    let n: i64 = db.query_row(
        "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name=?",
        [&clock],
        |r| r.get(0),
    )?;
    Ok(n == 1)
}

pub(crate) fn row_live(db: &Connection, target: &Resolved, pk: &SqlValue) -> Result<bool> {
    let found: Option<i64> = match &target.pk_column {
        Some(col) => db
            .query_row(
                &format!("SELECT 1 FROM \"{}\" WHERE \"{}\"=?", target.table, col),
                [pk],
                |r| r.get(0),
            )
            .optional()?,
        None => db
            .query_row(
                &format!("SELECT 1 FROM \"{}\" WHERE rowid=?", target.table),
                [pk],
                |r| r.get(0),
            )
            .optional()?,
    };
    Ok(found.is_some())
}

pub(crate) fn sql_to_policy(value: &SqlValue) -> Result<Value> {
    match value {
        SqlValue::Null => Ok(Value::Null),
        SqlValue::Integer(n) => Ok(Value::Integer(*n)),
        SqlValue::Real(f) => {
            if f.is_nan() {
                return Err(Error::Invalid("NaN policy value".into()));
            }
            Ok(Value::Real(*f))
        }
        SqlValue::Text(s) => Ok(Value::Text(s.clone())),
        SqlValue::Blob(b) => Ok(Value::Blob(b.clone())),
    }
}

pub(crate) fn current_cell(db: &Connection, target: &Resolved, pk: &SqlValue) -> Result<Option<SqlValue>> {
    let cell: Option<SqlValue> = match &target.pk_column {
        Some(col) => db
            .query_row(
                &format!(
                    "SELECT \"{}\" FROM \"{}\" WHERE \"{}\"=?",
                    target.column, target.table, col
                ),
                [pk],
                |r| r.get(0),
            )
            .optional()?,
        None => db
            .query_row(
                &format!(
                    "SELECT \"{}\" FROM \"{}\" WHERE rowid=?",
                    target.column, target.table
                ),
                [pk],
                |r| r.get(0),
            )
            .optional()?,
    };
    Ok(cell)
}

pub(crate) fn policy_to_sql(value: &Value) -> SqlValue {
    match value {
        Value::Null => SqlValue::Null,
        Value::Integer(n) => SqlValue::Integer(*n),
        Value::Real(f) => SqlValue::Real(*f),
        Value::Text(s) => SqlValue::Text(s.clone()),
        Value::Blob(b) => SqlValue::Blob(b.clone()),
    }
}

fn load_rows(db: &Connection, target: &Resolved, incarnation: i64) -> Result<Vec<Row>> {
    let mut q = db.prepare(
        "SELECT target_table,target_column,target_pk,incarnation,\
         actor_database,actor_node,kind,tag,payload \
         FROM murmur_policy_components \
         WHERE target_table=? AND target_column=? AND target_pk=? AND incarnation=?",
    )?;
    let rows = q.query_map(
        rusqlite::params![target.table, target.column, target.pk_bytes, incarnation],
        |r| {
            Ok(Row {
                table: r.get(0)?,
                column: r.get(1)?,
                pk: r.get(2)?,
                incarnation: r.get(3)?,
                actor_database: r.get(4)?,
                actor_node: r.get(5)?,
                kind: r.get(6)?,
                tag: r.get(7)?,
                payload: r.get(8)?,
            })
        },
    )?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Error::from)
}

pub(crate) fn byte_budget(db: &Connection) -> Result<usize> {
    crate::config::setting(db, "max_batch_bytes")?
        .parse()
        .map_err(|_| Error::Invalid("invalid max_batch_bytes".into()))
}

fn empty_baseline(table: &str, column: &str, policy: Policy) -> Result<State<i64>> {
    let causal = match policy {
        Policy::Counter => Causal::Counter(Counter::default()),
        Policy::Set => Causal::Set(Set::default()),
        _ => return Err(Error::Invalid("causal baseline for extremum".into())),
    };
    Ok(State {
        version: 0,
        domain: [0; 16],
        branch: 0,
        scope: 0,
        baseline: Value::Integer(0),
        basis: 0,
        table: table.to_string(),
        column: column.to_string(),
        key: Vec::new(),
        generation: 0,
        causal,
    })
}

/// Reduce one target over its incarnation-0 components and project
/// the scalar into the target cell when live. Returns the projection
/// (None when the row is gone). Corrupt carrier rows fail the call:
/// the caller decides retry vs quarantine.
pub(crate) fn recompute(
    db: &Connection,
    table: &str,
    column: &str,
    pk: &SqlValue,
    policy: Policy,
) -> Result<Option<SqlValue>> {
    let target = resolve_target(db, table, column, pk)?;
    let live = row_live(db, &target, pk)?;
    let rows = load_rows(db, &target, 0)?;
    let ct = Target {
        table: target.table.clone(),
        column: target.column.clone(),
        pk: target.pk_bytes.clone(),
        incarnation: 0,
        live,
    };
    let budget = byte_budget(db)?;
    let projection = match policy {
        Policy::Counter | Policy::Set => {
            let base = empty_baseline(&target.table, &target.column, policy)?;
            carrier::reduce_causal(&ct, policy, &base, &rows, budget)
                .map_err(policy_err)?
                .projection
        }
        Policy::Min | Policy::Max => {
            // Baseline is the current cell: with no components the
            // projection is a no-op, and observed extrema fold over it.
            let base = current_cell(db, &target, pk)?
                .map(|v| sql_to_policy(&v))
                .transpose()?;
            let base = base.unwrap_or(Value::Null);
            carrier::reduce_extremum(&ct, policy, &base, &rows, budget)
                .map_err(policy_err)?
                .projection
        }
        Policy::Lww => return Err(Error::Invalid("no carrier reduce for Lww".into())),
    };
    if live {
        let value = projection
            .as_ref()
            .map(policy_to_sql)
            .unwrap_or(SqlValue::Null);
        project(db, &target, pk, &value)?;
        Ok(Some(value))
    } else {
        Ok(None)
    }
}

fn project(db: &Connection, target: &Resolved, pk: &SqlValue, value: &SqlValue) -> Result<()> {
    match &target.pk_column {
        Some(col) => db.execute(
            &format!(
                "UPDATE \"{}\" SET \"{}\"=? WHERE \"{}\"=?",
                target.table, target.column, col
            ),
            rusqlite::params![value, pk],
        )?,
        None => db.execute(
            &format!(
                "UPDATE \"{}\" SET \"{}\"=? WHERE rowid=?",
                target.table, target.column
            ),
            rusqlite::params![value, pk],
        )?,
    };
    Ok(())
}

/// Post-ingest hook: recompute every configured target that holds
/// carrier rows. Runs inside the caller's transaction.
pub(crate) fn recompute_configured(db: &Connection) -> Result<usize> {
    let map = policies(db)?;
    if map.is_empty() {
        return Ok(0);
    }
    let mut distinct: Vec<(String, String, Vec<u8>)> = Vec::new();
    {
        let mut q = db.prepare(
            "SELECT DISTINCT target_table,target_column,target_pk \
             FROM murmur_policy_components WHERE incarnation=0",
        )?;
        let mut rows = q.query([])?;
        while let Some(r) = rows.next()? {
            distinct.push((r.get(0)?, r.get(1)?, r.get(2)?));
        }
    }
    let mut count = 0;
    for (table, column, pk_bytes) in distinct {
        let policy = match map.get(&(table.clone(), column.clone())) {
            Some(p) => *p,
            None => continue,
        };
        let pk = decode_pk(&pk_bytes)?;
        // Fail-closed: corrupt carriers or drifted targets abort the
        // batch (the caller rolls back) rather than project wrongly.
        recompute(db, &table, &column, &pk, policy)?;
        count += 1;
    }
    Ok(count)
}

fn put_component(
    db: &Connection,
    target: &Resolved,
    actor: (NodeId, NodeId),
    kind: i64,
    tag: &[u8],
    payload: &Payload,
) -> Result<()> {
    let bytes = carrier::encode_payload(payload).map_err(policy_err)?;
    db.execute(
        "INSERT OR REPLACE INTO murmur_policy_components(\
         target_table,target_column,target_pk,incarnation,\
         actor_database,actor_node,kind,tag,payload) \
         VALUES(?,?,?,?,?,?,?,?,?)",
        rusqlite::params![
            target.table,
            target.column,
            target.pk_bytes,
            0i64,
            actor.0.as_slice(),
            actor.1.as_slice(),
            kind,
            tag,
            bytes,
        ],
    )?;
    Ok(())
}

fn own_component(
    db: &Connection,
    target: &Resolved,
    actor: (NodeId, NodeId),
    kind: i64,
) -> Result<Option<Payload>> {
    let bytes: Option<Vec<u8>> = db
        .query_row(
            "SELECT payload FROM murmur_policy_components \
             WHERE target_table=? AND target_column=? AND target_pk=? \
             AND incarnation=0 AND actor_database=? AND actor_node=? \
             AND kind=? AND tag=X''",
            rusqlite::params![
                target.table,
                target.column,
                target.pk_bytes,
                actor.0.as_slice(),
                actor.1.as_slice(),
                kind,
            ],
            |r| r.get(0),
        )
        .optional()?;
    bytes
        .map(|b| carrier::decode_payload(&b).map_err(policy_err))
        .transpose()
}

/// Local op: append the caller's component, recompute, project.
/// Runs inside the caller's transaction. Ops per policy:
/// Counter `add` (integer delta), Set `add`/`remove` (any value),
/// Min/Max `observe` (any non-NaN value). Returns the projection.
pub(crate) fn apply_op(
    db: &Connection,
    table: &str,
    column: &str,
    pk: &SqlValue,
    op: &str,
    value: &SqlValue,
) -> Result<Option<SqlValue>> {
    let map = policies(db)?;
    let policy = map
        .get(&(table.to_string(), column.to_string()))
        .copied()
        .ok_or_else(|| Error::Invalid("column has no merge policy".into()))?;
    let target = resolve_target(db, table, column, pk)?;
    if !row_live(db, &target, pk)? {
        return Err(Error::Invalid("policy target row is gone".into()));
    }
    let actor_pair = actor(db)?;
    let pvalue = sql_to_policy(value)?;
    match (policy, op) {
        (Policy::Counter, "add") => {
            let delta = match pvalue {
                Value::Integer(n) => n,
                _ => return Err(Error::Invalid("counter delta must be integer".into())),
            };
            let (mut positive, mut negative) =
                match own_component(db, &target, actor_pair, KIND_COUNTER)? {
                    Some(Payload::Counter { positive, negative }) => (positive, negative),
                    Some(_) => return Err(Error::Protocol("counter component kind".into())),
                    None => (0, 0),
                };
            if delta >= 0 {
                positive = positive
                    .checked_add(delta as u64)
                    .ok_or_else(|| Error::Limit("counter overflow".into()))?;
            } else {
                negative = negative
                    .checked_add(delta.unsigned_abs())
                    .ok_or_else(|| Error::Limit("counter overflow".into()))?;
            }
            put_component(
                db,
                &target,
                actor_pair,
                KIND_COUNTER,
                &[],
                &Payload::Counter { positive, negative },
            )?;
        }
        (Policy::Set, "add") => {
            let tag = Tag {
                database: actor_pair.0,
                node: actor_pair.1,
                transaction: rand::random(),
                operation: 0,
            };
            let tag_bytes = murmur_policy::model::encode(&tag).map_err(policy_err)?;
            put_component(
                db,
                &target,
                actor_pair,
                KIND_SET_ADD,
                &tag_bytes,
                &Payload::SetAdd { tag, value: pvalue },
            )?;
        }
        (Policy::Set, "remove") => {
            // Emit one removal per observed add-tag naming this value.
            let rows = load_rows(db, &target, 0)?;
            let mut removed = 0;
            for row in rows {
                if row.kind != KIND_SET_ADD || row.payload.is_empty() {
                    continue;
                }
                let payload = match carrier::decode_payload(&row.payload) {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                if let Payload::SetAdd { tag, value: v } = payload
                    && v == pvalue
                {
                    let tag_bytes = murmur_policy::model::encode(&tag).map_err(policy_err)?;
                    put_component(
                        db,
                        &target,
                        actor_pair,
                        KIND_SET_REMOVE,
                        &tag_bytes,
                        &Payload::SetRemove { tag },
                    )?;
                    removed += 1;
                }
            }
            if removed == 0 {
                return Err(Error::Invalid("set value was never added".into()));
            }
        }
        (Policy::Min, "observe") | (Policy::Max, "observe") => {
            let kind = if policy == Policy::Min {
                KIND_MIN
            } else {
                KIND_MAX
            };
            let keep = match own_component(db, &target, actor_pair, kind)? {
                Some(Payload::Min(v)) | Some(Payload::Max(v)) => {
                    let order = murmur_policy::compare::compare(&pvalue, &v).map_err(policy_err)?;
                    let better = (policy == Policy::Min && order.is_lt())
                        || (policy == Policy::Max && order.is_gt());
                    if better { pvalue } else { v }
                }
                Some(_) => return Err(Error::Protocol("extremum component kind".into())),
                None => pvalue,
            };
            let payload = if policy == Policy::Min {
                Payload::Min(keep)
            } else {
                Payload::Max(keep)
            };
            put_component(db, &target, actor_pair, kind, &[], &payload)?;
        }
        _ => {
            return Err(Error::Invalid(
                "op must be add/remove/observe for the column policy".into(),
            ));
        }
    }
    recompute(db, table, column, pk, policy)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup(policies_json: &str) -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE murmur_config(name TEXT PRIMARY KEY,value TEXT NOT NULL);
             CREATE TABLE murmur_identity(id INTEGER PRIMARY KEY CHECK(id=1),node BLOB,site BLOB,incarnation INTEGER NOT NULL DEFAULT 0);
             INSERT INTO murmur_identity(id,node,site) VALUES(1,X'01010101010101010101010101010101',X'02020202020202020202020202020202');",
        )
        .unwrap();
        db.execute(
            "INSERT INTO murmur_config(name,value) VALUES('column_policies',?)",
            [policies_json],
        )
        .unwrap();
        install(&db).unwrap();
        db.execute_batch("CREATE TABLE docs(id INTEGER PRIMARY KEY, n INTEGER, t TEXT);")
            .unwrap();
        db
    }

    #[test]
    fn pk_codec_round_trips_and_rejects() {
        for v in [
            SqlValue::Integer(-9),
            SqlValue::Text("k".into()),
            SqlValue::Blob(vec![0, 255]),
        ] {
            assert_eq!(decode_pk(&encode_pk(&v).unwrap()).unwrap(), v);
        }
        assert!(encode_pk(&SqlValue::Real(1.5)).is_err());
        assert!(encode_pk(&SqlValue::Null).is_err());
        assert!(decode_pk(&[]).is_err());
        assert!(decode_pk(&[9, 1, 2]).is_err());
    }

    #[test]
    fn counter_add_projects_running_total() {
        let db = setup(r#"{"docs.n":"Counter"}"#);
        db.execute("INSERT INTO docs(id,n) VALUES(1,0)", [])
            .unwrap();
        let pk = SqlValue::Integer(1);
        assert_eq!(
            apply_op(&db, "docs", "n", &pk, "add", &SqlValue::Integer(5)).unwrap(),
            Some(SqlValue::Integer(5))
        );
        assert_eq!(
            apply_op(&db, "docs", "n", &pk, "add", &SqlValue::Integer(-2)).unwrap(),
            Some(SqlValue::Integer(3))
        );
        let cell: i64 = db
            .query_row("SELECT n FROM docs WHERE id=1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(cell, 3);
        // Wrong op and wrong value type fail; unconfigured column fails.
        assert!(apply_op(&db, "docs", "n", &pk, "observe", &SqlValue::Integer(1)).is_err());
        assert!(apply_op(&db, "docs", "n", &pk, "add", &SqlValue::Text("x".into())).is_err());
        assert!(apply_op(&db, "docs", "t", &pk, "add", &SqlValue::Integer(1)).is_err());
    }

    #[test]
    fn set_add_remove_projects_membership() {
        let db = setup(r#"{"docs.t":"Set"}"#);
        db.execute("INSERT INTO docs(id,t) VALUES(1,NULL)", [])
            .unwrap();
        let pk = SqlValue::Integer(1);
        let v = SqlValue::Text("a".into());
        apply_op(&db, "docs", "t", &pk, "add", &v).unwrap();
        apply_op(&db, "docs", "t", &pk, "add", &SqlValue::Text("b".into())).unwrap();
        // Projection is the postcard-encoded member list blob.
        let blob: Vec<u8> = db
            .query_row("SELECT t FROM docs WHERE id=1", [], |r| r.get(0))
            .unwrap();
        let members: Vec<Value> = murmur_policy::model::decode(&blob).unwrap();
        assert_eq!(members.len(), 2);
        apply_op(&db, "docs", "t", &pk, "remove", &v).unwrap();
        let blob: Vec<u8> = db
            .query_row("SELECT t FROM docs WHERE id=1", [], |r| r.get(0))
            .unwrap();
        let members: Vec<Value> = murmur_policy::model::decode(&blob).unwrap();
        assert_eq!(members, vec![Value::Text("b".into())]);
        // Removing a never-added value fails.
        assert!(apply_op(&db, "docs", "t", &pk, "remove", &SqlValue::Text("z".into())).is_err());
    }

    #[test]
    fn min_max_observe_folds_extrema() {
        let db = setup(r#"{"docs.n":"Min"}"#);
        db.execute("INSERT INTO docs(id,n) VALUES(1,100)", [])
            .unwrap();
        let pk = SqlValue::Integer(1);
        assert_eq!(
            apply_op(&db, "docs", "n", &pk, "observe", &SqlValue::Integer(7)).unwrap(),
            Some(SqlValue::Integer(7))
        );
        // A worse observation keeps the extremum (per-actor fold + reduce).
        assert_eq!(
            apply_op(&db, "docs", "n", &pk, "observe", &SqlValue::Integer(50)).unwrap(),
            Some(SqlValue::Integer(7))
        );
        let db = setup(r#"{"docs.n":"Max"}"#);
        db.execute("INSERT INTO docs(id,n) VALUES(1,0)", [])
            .unwrap();
        apply_op(&db, "docs", "n", &pk, "observe", &SqlValue::Integer(7)).unwrap();
        assert_eq!(
            apply_op(&db, "docs", "n", &pk, "observe", &SqlValue::Integer(50)).unwrap(),
            Some(SqlValue::Integer(50))
        );
    }

    #[test]
    fn recompute_configured_covers_carrier_targets() {
        let db = setup(r#"{"docs.n":"Counter"}"#);
        db.execute("INSERT INTO docs(id,n) VALUES(1,0)", [])
            .unwrap();
        let pk = SqlValue::Integer(1);
        apply_op(&db, "docs", "n", &pk, "add", &SqlValue::Integer(4)).unwrap();
        // Clobber the projection; the post-ingest hook must restore it.
        db.execute("UPDATE docs SET n=999 WHERE id=1", []).unwrap();
        assert_eq!(recompute_configured(&db).unwrap(), 1);
        let cell: i64 = db
            .query_row("SELECT n FROM docs WHERE id=1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(cell, 4);
    }

    #[test]
    fn unconfigured_database_recomputes_nothing() {
        let db = setup("{}");
        assert_eq!(recompute_configured(&db).unwrap(), 0);
    }
}
