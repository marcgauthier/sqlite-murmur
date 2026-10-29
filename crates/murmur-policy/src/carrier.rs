//! Pure carrier validation/reduction. The host owns native CRR key decoding,
//! lifecycle, baseline selection, transactions and projection writes.
use crate::{
    Error, Result,
    causal::{self, State},
    compare::compare,
    model::{Value, decode, encode},
    policies::{Causal, Counter, Policy, Set, Tag},
};
use serde::{Deserialize, Serialize};

pub const TABLE: &str = "murmur_policy_components";
pub const PAYLOAD_VERSION: u8 = 1;
pub const CREATE_SQL: &str = "CREATE TABLE murmur_policy_components(\
target_table TEXT NOT NULL,target_column TEXT NOT NULL,target_pk BLOB NOT NULL,\
incarnation INTEGER NOT NULL,actor_database BLOB NOT NULL,actor_node BLOB NOT NULL,\
kind INTEGER NOT NULL,tag BLOB NOT NULL,payload BLOB NOT NULL DEFAULT X'',\
PRIMARY KEY(target_table,target_column,target_pk,incarnation,actor_database,actor_node,kind,tag)\
) WITHOUT ROWID";

/// Decoded SQL fields; native packed key bytes stay opaque to this crate.
#[derive(Clone, Debug)]
pub struct Row {
    pub table: String,
    pub column: String,
    pub pk: Vec<u8>,
    pub incarnation: i64,
    pub actor_database: Vec<u8>,
    pub actor_node: Vec<u8>,
    pub kind: i64,
    pub tag: Vec<u8>,
    pub payload: Vec<u8>,
}

/// Host-supplied lifecycle; no parity or live/dead rule is inferred here.
#[derive(Clone, Debug)]
pub struct Target {
    pub table: String,
    pub column: String,
    pub pk: Vec<u8>,
    pub incarnation: i64,
    pub live: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Payload {
    Counter { positive: u64, negative: u64 },
    SetAdd { tag: Tag, value: Value },
    SetRemove { tag: Tag },
    Min(Value),
    Max(Value),
}

impl Payload {
    pub fn kind(&self) -> i64 {
        match self {
            Self::Counter { .. } => 1,
            Self::SetAdd { .. } => 2,
            Self::SetRemove { .. } => 3,
            Self::Min(_) => 4,
            Self::Max(_) => 5,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Envelope {
    version: u8,
    payload: Payload,
}

/// Encode state, not a new operation: this allocates no actor/tag/clock.
pub fn encode_payload(payload: &Payload) -> Result<Vec<u8>> {
    encode(&Envelope {
        version: PAYLOAD_VERSION,
        payload: payload.clone(),
    })
}

/// Decode a v1 payload envelope (host read-modify-write for per-actor
/// components). Version-checked like the reducer; canonical-form check
/// is the reducer's job at reduce time.
pub fn decode_payload(bytes: &[u8]) -> Result<Payload> {
    let envelope: Envelope = decode(bytes)?;
    if envelope.version != PAYLOAD_VERSION {
        return Err(invalid("unsupported carrier payload version"));
    }
    Ok(envelope.payload)
}

fn invalid(message: &str) -> Error {
    Error::Invalid(message.into())
}

fn identity(bytes: &[u8]) -> Result<[u8; 16]> {
    let id: [u8; 16] = bytes
        .try_into()
        .map_err(|_| invalid("carrier actor must be 16 bytes"))?;
    if id == [0; 16] {
        return Err(invalid("zero carrier actor"));
    }
    Ok(id)
}

fn valid_value(value: &Value) -> Result<()> {
    if matches!(value, Value::Real(v) if v.is_nan()) {
        return Err(invalid("NaN carrier value"));
    }
    Ok(())
}

fn row_size(row: &Row) -> Result<usize> {
    [
        row.table.len(),
        row.column.len(),
        row.pk.len(),
        row.actor_database.len(),
        row.actor_node.len(),
        row.tag.len(),
        row.payload.len(),
        16,
    ]
    .into_iter()
    .try_fold(0usize, |total, n| total.checked_add(n))
    .ok_or_else(|| Error::Limit("carrier byte count overflow".into()))
}

fn check_target(target: &Target) -> Result<()> {
    if target.table.is_empty()
        || target.column.is_empty()
        || target.pk.is_empty()
        || target.table.contains('\0')
        || target.column.contains('\0')
        || target.incarnation < 0
    {
        return Err(invalid("invalid carrier target"));
    }
    Ok(())
}

struct Validated {
    actor: ([u8; 16], [u8; 16]),
    payload: Option<Payload>,
}

fn validate(row: &Row) -> Result<Validated> {
    let actor = (identity(&row.actor_database)?, identity(&row.actor_node)?);
    let tag = match row.kind {
        2 | 3 => {
            let tag: Tag = decode(&row.tag)?;
            if encode(&tag)? != row.tag {
                return Err(invalid("noncanonical carrier tag"));
            }
            Some(tag)
        }
        1 | 4 | 5 if row.tag.is_empty() => None,
        _ => return Err(invalid("carrier kind or tag")),
    };
    // A native PK/sentinel may be paged before its single payload column.
    if row.payload.is_empty() {
        return Ok(Validated {
            actor,
            payload: None,
        });
    }
    let envelope: Envelope = decode(&row.payload)?;
    if envelope.version != PAYLOAD_VERSION {
        return Err(invalid("unsupported carrier payload version"));
    }
    if envelope.payload.kind() != row.kind {
        return Err(invalid("carrier payload kind mismatch"));
    }
    if encode(&envelope)? != row.payload {
        return Err(invalid("noncanonical carrier payload"));
    }
    match &envelope.payload {
        Payload::SetAdd {
            tag: payload_tag,
            value,
        } => {
            if tag.as_ref() != Some(payload_tag)
                || payload_tag.database != actor.0
                || payload_tag.node != actor.1
            {
                return Err(invalid("carrier addition tag identity"));
            }
            valid_value(value)?;
        }
        Payload::SetRemove { tag: payload_tag } => {
            // The remover may differ from the original tag's actor, including
            // removals of synthetic baseline tags. No tag is generated here.
            if tag.as_ref() != Some(payload_tag) {
                return Err(invalid("carrier removal tag identity"));
            }
        }
        Payload::Min(value) | Payload::Max(value) => valid_value(value)?,
        Payload::Counter { .. } => {}
    }
    Ok(Validated {
        actor,
        payload: Some(envelope.payload),
    })
}

fn validate_rows(
    target: &Target,
    policy: Policy,
    rows: &[Row],
    byte_budget: usize,
) -> Result<Vec<Validated>> {
    check_target(target)?;
    if byte_budget == 0 {
        return Err(invalid("carrier byte budget is zero"));
    }
    let mut total = 0usize;
    let mut validated = Vec::new();
    for row in rows {
        total = total
            .checked_add(row_size(row)?)
            .ok_or_else(|| Error::Limit("carrier byte count overflow".into()))?;
        if total > byte_budget {
            return Err(Error::Limit("carrier byte budget exceeded".into()));
        }
        if row.table != target.table
            || row.column != target.column
            || row.pk != target.pk
            || row.incarnation < 0
        {
            return Err(invalid("carrier target identity"));
        }
        let permitted = matches!(
            (policy, row.kind),
            (Policy::Counter, 1) | (Policy::Set, 2 | 3) | (Policy::Min, 4) | (Policy::Max, 5)
        );
        if !permitted {
            return Err(invalid("carrier policy mismatch"));
        }
        validated.push(validate(row)?);
    }
    Ok(validated)
}

pub struct Reduction<Stamp> {
    /// None when the native host says the target is not live.
    pub projection: Option<Value>,
    pub state: State<Stamp>,
    /// Includes incomplete payloads and noncurrent incarnations. The host
    /// decides pending/stale retention; this crate does not garbage-collect.
    pub deferred_rows: usize,
}

/// Join validated components into a private candidate, leaving the caller's
/// baseline untouched on every error. Baseline/branch selection is host-owned.
pub fn reduce_causal<Stamp: Clone + Ord>(
    target: &Target,
    policy: Policy,
    baseline: &State<Stamp>,
    rows: &[Row],
    byte_budget: usize,
) -> Result<Reduction<Stamp>> {
    let matches_policy = matches!(
        (policy, &baseline.causal),
        (Policy::Counter, Causal::Counter(_)) | (Policy::Set, Causal::Set(_))
    );
    if !matches_policy
        || baseline.table != target.table
        || baseline.column != target.column
        || u64::try_from(target.incarnation).ok() != Some(baseline.generation)
    {
        return Err(invalid("carrier baseline identity or policy"));
    }
    let validated = validate_rows(target, policy, rows, byte_budget)?;
    let mut candidate = baseline.clone();
    let mut source = baseline.clone();
    let mut deferred_rows = 0;
    for (row, validated) in rows.iter().zip(validated) {
        if !target.live || row.incarnation != target.incarnation || validated.payload.is_none() {
            deferred_rows += 1;
            continue;
        }
        let causal = match validated.payload.unwrap() {
            Payload::Counter { positive, negative } => {
                let mut counter = Counter::default();
                counter.positive.insert(validated.actor, positive);
                counter.negative.insert(validated.actor, negative);
                Causal::Counter(counter)
            }
            Payload::SetAdd { tag, value } => {
                let encoded = encode(&value)?;
                if let Causal::Set(known) = &candidate.causal
                    && known
                        .additions
                        .iter()
                        .any(|(prior, tags)| tags.contains(&tag) && prior != &encoded)
                {
                    return Err(Error::Corrupt("one set tag names different values".into()));
                }
                let mut set = Set::default();
                set.add(encoded, tag);
                Causal::Set(set)
            }
            Payload::SetRemove { tag } => {
                let mut set = Set::default();
                set.removals.insert(tag);
                Causal::Set(set)
            }
            _ => return Err(invalid("noncausal carrier in causal reducer")),
        };
        source.causal = causal;
        causal::join_state(&mut candidate, &source)?;
    }
    let projection = if target.live {
        Some(causal::projection(&candidate)?)
    } else {
        None
    };
    Ok(Reduction {
        projection,
        state: candidate,
        deferred_rows,
    })
}

pub struct ExtremumReduction {
    pub projection: Option<Value>,
    pub deferred_rows: usize,
}

/// Reduce every supplied candidate, including native-LWW losers. The A4
/// adapter must supply those candidates before discarding them.
pub fn reduce_extremum(
    target: &Target,
    policy: Policy,
    baseline: &Value,
    rows: &[Row],
    byte_budget: usize,
) -> Result<ExtremumReduction> {
    if !matches!(policy, Policy::Min | Policy::Max) {
        return Err(invalid("not an extremum policy"));
    }
    valid_value(baseline)?;
    let validated = validate_rows(target, policy, rows, byte_budget)?;
    let mut value = baseline.clone();
    let mut deferred_rows = 0;
    for (row, validated) in rows.iter().zip(validated) {
        if !target.live || row.incarnation != target.incarnation || validated.payload.is_none() {
            deferred_rows += 1;
            continue;
        }
        let incoming = match validated.payload.unwrap() {
            Payload::Min(value) | Payload::Max(value) => value,
            _ => return Err(invalid("non-extremum carrier in extremum reducer")),
        };
        let order = compare(&incoming, &value)?;
        if policy == Policy::Min && order.is_lt() || policy == Policy::Max && order.is_gt() {
            value = incoming;
        }
    }
    Ok(ExtremumReduction {
        projection: target.live.then_some(value),
        deferred_rows,
    })
}
