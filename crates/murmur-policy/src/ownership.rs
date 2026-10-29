//! Native CRR ownership event envelopes. No SQL, clocks or mutation of rows.
//!
//! Events reference prior immutable events rather than recursively embedding
//! their own native stamp. The host authenticates origins, resolves references
//! and commits reduced owners with native row changes in the same database.
use crate::{
    Error, Result,
    model::{Value, decode, encode},
};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

pub const VERSION: u8 = 1;
pub const KEY_PREFIX: &[u8] = b"09/bridge/owner-crr/v1/";
pub const EVENT_TABLE: &str = "murmur_owner_events";
pub const CREATE_EVENT_SQL: &str = "CREATE TABLE murmur_owner_events(\
target_table TEXT NOT NULL,target_pk BLOB NOT NULL,target_kind INTEGER NOT NULL,\
target_column TEXT NOT NULL,origin_register BLOB NOT NULL,col_version INTEGER NOT NULL,\
row_cl INTEGER NOT NULL,payload_digest BLOB NOT NULL,payload BLOB NOT NULL DEFAULT X'',\
PRIMARY KEY(target_table,target_pk,target_kind,target_column,origin_register,row_cl,col_version,payload_digest)\
) WITHOUT ROWID";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TargetKind {
    Row,
    Column(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Target {
    pub table: String,
    pub pk: Vec<u8>,
    pub kind: TargetKind,
}

fn invalid(message: &str) -> Error {
    Error::Invalid(message.into())
}

fn valid_name(name: &str) -> bool {
    !name.is_empty() && !name.contains('\0') && name == name.to_ascii_lowercase()
}

impl Target {
    /// Canonicalize SQLite's ASCII-insensitive identifiers; retain native PK
    /// bytes exactly. This function does not unpack or allocate a native key.
    pub fn new(table: String, pk: Vec<u8>, kind: TargetKind) -> Result<Self> {
        let kind = match kind {
            TargetKind::Row => TargetKind::Row,
            TargetKind::Column(column) => TargetKind::Column(column.to_ascii_lowercase()),
        };
        let target = Self {
            table: table.to_ascii_lowercase(),
            pk,
            kind,
        };
        target.validate()?;
        Ok(target)
    }

    fn validate(&self) -> Result<()> {
        if !valid_name(&self.table) || self.pk.is_empty() {
            return Err(invalid("noncanonical ownership target"));
        }
        if let TargetKind::Column(column) = &self.kind
            && (!valid_name(column) || column == crate::high_low::ROW)
        {
            return Err(invalid("invalid or reserved ownership column"));
        }
        Ok(())
    }
}

/// Immutable carrier identity. origin_register is supplied and authenticated
/// by the native host; col_version is observed, never allocated here.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventRef {
    pub target: Target,
    pub origin_register: [u8; 16],
    pub row_cl: i64,
    pub col_version: i64,
    pub payload_digest: [u8; 32],
}

impl EventRef {
    fn validate(&self, target: &Target) -> Result<()> {
        self.target.validate()?;
        if &self.target != target
            || self.origin_register == [0; 16]
            || self.row_cl < 0
            || self.col_version <= 0
        {
            return Err(invalid("ownership event reference identity"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum EventKind {
    Grant { value: Option<Value> },
    Release,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub target: Target,
    pub kind: EventKind,
    pub prior_grant: Option<EventRef>,
    pub prior_release: Option<EventRef>,
}

impl Event {
    fn validate(&self) -> Result<()> {
        self.target.validate()?;
        for reference in [&self.prior_grant, &self.prior_release]
            .into_iter()
            .flatten()
        {
            reference.validate(&self.target)?;
        }
        match (&self.target.kind, &self.kind) {
            (TargetKind::Row, EventKind::Grant { value: None }) => {}
            (TargetKind::Column(_), EventKind::Grant { value: Some(value) }) => {
                if matches!(value, Value::Real(v) if v.is_nan()) {
                    return Err(invalid("NaN ownership grant"));
                }
            }
            (_, EventKind::Release) if self.prior_grant.is_some() => {}
            _ => return Err(invalid("ownership event kind, value or grant reference")),
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Envelope {
    version: u8,
    event: Event,
}

fn check_budget(size: usize, max_bytes: usize) -> Result<()> {
    if max_bytes == 0 {
        return Err(invalid("ownership byte budget is zero"));
    }
    if size > max_bytes {
        return Err(Error::Limit(
            "ownership envelope exceeds byte budget".into(),
        ));
    }
    Ok(())
}

pub fn encode_event(event: &Event, max_bytes: usize) -> Result<Vec<u8>> {
    event.validate()?;
    // Include the in-memory variable-size fields before serialization, so a
    // huge value cannot allocate an additional encoded copy just to fail.
    let mut size = event
        .target
        .table
        .len()
        .checked_add(event.target.pk.len())
        .ok_or_else(|| Error::Limit("ownership byte count overflow".into()))?;
    let mut add = |n: usize| -> Result<()> {
        size = size
            .checked_add(n)
            .ok_or_else(|| Error::Limit("ownership byte count overflow".into()))?;
        check_budget(size, max_bytes)
    };
    if let TargetKind::Column(column) = &event.target.kind {
        add(column.len())?;
    }
    if let EventKind::Grant {
        value: Some(Value::Text(text)),
    } = &event.kind
    {
        add(text.len())?;
    }
    if let EventKind::Grant {
        value: Some(Value::Blob(bytes)),
    } = &event.kind
    {
        add(bytes.len())?;
    }
    for reference in [&event.prior_grant, &event.prior_release]
        .into_iter()
        .flatten()
    {
        add(reference.target.table.len())?;
        add(reference.target.pk.len())?;
        if let TargetKind::Column(column) = &reference.target.kind {
            add(column.len())?;
        }
        add(64)?; // native origin + i64 lifecycle/version + SHA-256 digest
    }
    check_budget(size, max_bytes)?;
    let bytes = encode(&Envelope {
        version: VERSION,
        event: event.clone(),
    })?;
    check_budget(bytes.len(), max_bytes)?;
    Ok(bytes)
}

pub fn decode_event(bytes: &[u8], max_bytes: usize) -> Result<Event> {
    check_budget(bytes.len(), max_bytes)?;
    let envelope: Envelope = decode(bytes)?;
    if envelope.version != VERSION {
        return Err(invalid("unsupported ownership envelope version"));
    }
    envelope.event.validate()?;
    if encode_event(&envelope.event, max_bytes)? != bytes {
        return Err(invalid("noncanonical ownership envelope"));
    }
    Ok(envelope.event)
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    murmur_tx::protocol::schema_digest(bytes)
}

pub fn event_ref(
    event: &Event,
    origin_register: [u8; 16],
    row_cl: i64,
    col_version: i64,
    max_bytes: usize,
) -> Result<EventRef> {
    let reference = EventRef {
        target: event.target.clone(),
        origin_register,
        row_cl,
        col_version,
        payload_digest: digest(&encode_event(event, max_bytes)?),
    };
    reference.validate(&event.target)?;
    Ok(reference)
}

pub fn target_key(target: &Target) -> Result<Vec<u8>> {
    target.validate()?;
    let mut key = KEY_PREFIX.to_vec();
    key.extend(murmur_tx::hex(&digest(&encode(&(VERSION, target))?)).as_bytes());
    Ok(key)
}

pub fn verify_target_key(key: &[u8], target: &Target) -> Result<()> {
    if key != target_key(target)? {
        return Err(Error::Corrupt("ownership target key mismatch".into()));
    }
    Ok(())
}

/// Versioned event staging record, not the canonical reduced Owner envelope.
pub fn encode_record(event: &Event, max_bytes: usize) -> Result<Vec<u8>> {
    encode_event(event, max_bytes)
}

pub fn decode_record(key: &[u8], bytes: &[u8], max_bytes: usize) -> Result<Event> {
    let event = decode_event(bytes, max_bytes)?;
    verify_target_key(key, &event.target)?;
    Ok(event)
}

/// Native accepted metadata for a single target control register. Deliberately
/// has no Ord impl: ordering different registers would fabricate chronology.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeStamp {
    target: Target,
    row_cl: i64,
    col_version: i64,
    accepted_blob: Vec<u8>,
    site_id: Option<[u8; 16]>,
    merge_equal_values: bool,
}

impl NativeStamp {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        target: Target,
        row_cl: i64,
        col_version: i64,
        accepted_blob: Vec<u8>,
        site_id: Option<[u8; 16]>,
        merge_equal_values: bool,
        max_bytes: usize,
    ) -> Result<Self> {
        target.validate()?;
        if row_cl < 0 || col_version <= 0 || (merge_equal_values && site_id.is_none()) {
            return Err(invalid("invalid native ownership metadata"));
        }
        let event = decode_event(&accepted_blob, max_bytes)?;
        if event.target != target {
            return Err(invalid("native stamp target mismatch"));
        }
        Ok(Self {
            target,
            row_cl,
            col_version,
            accepted_blob,
            site_id: if merge_equal_values { site_id } else { None },
            merge_equal_values,
        })
    }

    pub fn target(&self) -> &Target {
        &self.target
    }
    pub fn row_cl(&self) -> i64 {
        self.row_cl
    }
    pub fn col_version(&self) -> i64 {
        self.col_version
    }
    pub fn accepted_blob(&self) -> &[u8] {
        &self.accepted_blob
    }
    pub fn site_id(&self) -> Option<[u8; 16]> {
        self.site_id
    }
    pub fn merge_equal_values(&self) -> bool {
        self.merge_equal_values
    }

    pub fn compare_same_target(&self, other: &Self) -> Result<Ordering> {
        if self.target != other.target || self.merge_equal_values != other.merge_equal_values {
            return Err(invalid("native ownership stamps have incompatible scopes"));
        }
        Ok((
            self.row_cl,
            self.col_version,
            &self.accepted_blob,
            self.site_id,
        )
            .cmp(&(
                other.row_cl,
                other.col_version,
                &other.accepted_blob,
                other.site_id,
            )))
    }

    pub fn reference(&self, origin_register: [u8; 16]) -> Result<EventRef> {
        let reference = EventRef {
            target: self.target.clone(),
            origin_register,
            row_cl: self.row_cl,
            col_version: self.col_version,
            payload_digest: digest(&self.accepted_blob),
        };
        reference.validate(&self.target)?;
        Ok(reference)
    }
}
