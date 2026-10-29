//! Ownership/control kernel adapted from archived src/high_low.rs.
//! Native CRR event ordering and row lifecycle are supplied by the host.
//! There are no shadows, row merges, storage calls, transport or clocks here.
use crate::{Error, Result, model::Value};
use murmur_tx::bridge::{Header, Role};
use std::collections::BTreeSet;

pub const ROW: &str = "__row__";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target {
    pub table: String,
    pub pk: Vec<u8>,
    pub column: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OwnedValue<Stamp> {
    pub value: Value,
    pub native_version: Stamp,
}

/// Minimal ownership provenance, with opaque native PK bytes and no row model.
/// There is no approved production encoding/control-carrier schema yet.
#[derive(Clone, Debug, PartialEq)]
pub struct Owner<Stamp> {
    pub target: Target,
    pub epoch: Stamp,
    pub release_epoch: Stamp,
    pub owned: bool,
    pub value: Option<OwnedValue<Stamp>>,
}

fn check_target(target: &Target) -> Result<()> {
    if target.table.is_empty()
        || target.pk.is_empty()
        || target.column.is_empty()
        || target.table.contains('\0')
        || target.column.contains('\0')
    {
        return Err(Error::Invalid("ownership target".into()));
    }
    Ok(())
}

fn check_owner<Stamp: Ord>(owner: &Owner<Stamp>) -> Result<()> {
    check_target(&owner.target)?;
    if owner.release_epoch > owner.epoch {
        return Err(Error::Corrupt("ownership release exceeds epoch".into()));
    }
    if owner.owned && owner.target.column != ROW && owner.value.is_none() {
        return Err(Error::Corrupt("owned column has no value".into()));
    }
    if owner
        .value
        .as_ref()
        .is_some_and(|value| matches!(value.value, Value::Real(v) if v.is_nan()))
    {
        return Err(Error::Invalid("NaN ownership value".into()));
    }
    Ok(())
}

/// Preserve the archive's (owner epoch, cell version) winner selection.
/// Equal tokens choose incoming, as in the archive. The host must supply
/// native accepted tie-break ordering; this function invents none.
pub fn join_owner<Stamp: Clone + Ord>(
    previous: &Owner<Stamp>,
    incoming: &Owner<Stamp>,
) -> Result<Owner<Stamp>> {
    check_owner(previous)?;
    check_owner(incoming)?;
    if previous.target != incoming.target {
        return Err(Error::Corrupt("ownership identity".into()));
    }
    let prior = (
        &previous.epoch,
        previous.value.as_ref().map(|v| &v.native_version),
    );
    let next = (
        &incoming.epoch,
        incoming.value.as_ref().map(|v| &v.native_version),
    );
    Ok(if prior > next {
        previous.clone()
    } else {
        incoming.clone()
    })
}

/// Promote a host-accepted local cell change, retaining an active ownership
/// epoch or the previous release fence. No event token is allocated here.
pub fn promote_value<Stamp: Clone + Ord>(
    target: Target,
    previous: Option<&Owner<Stamp>>,
    accepted: Stamp,
    value: Value,
    initial_release: Stamp,
) -> Result<Owner<Stamp>> {
    check_target(&target)?;
    if target.column == ROW {
        return Err(Error::Invalid("row marker is not a value column".into()));
    }
    if let Some(previous) = previous {
        check_owner(previous)?;
        if previous.target != target {
            return Err(Error::Corrupt("ownership identity".into()));
        }
        if accepted < previous.epoch
            || previous
                .value
                .as_ref()
                .is_some_and(|v| accepted < v.native_version)
            || (!previous.owned && accepted <= previous.release_epoch)
        {
            return Err(Error::Invalid("stale native ownership promotion".into()));
        }
    }
    let owner = Owner {
        target,
        epoch: previous
            .filter(|p| p.owned)
            .map(|p| p.epoch.clone())
            .unwrap_or_else(|| accepted.clone()),
        release_epoch: previous
            .map(|p| p.release_epoch.clone())
            .unwrap_or(initial_release),
        owned: true,
        value: Some(OwnedValue {
            value,
            native_version: accepted,
        }),
    };
    check_owner(&owner)?;
    Ok(owner)
}

/// Adapt the archived High row-deletion ownership record; row deletion itself
/// stays with native CRR. The host supplies the initial-release marker.
pub fn own_deletion<Stamp: Clone + Ord>(
    target: Target,
    accepted: Stamp,
    initial_release: Stamp,
) -> Result<Owner<Stamp>> {
    if target.column != ROW {
        return Err(Error::Invalid(
            "deletion requires row ownership target".into(),
        ));
    }
    let owner = Owner {
        target,
        epoch: accepted,
        release_epoch: initial_release,
        owned: true,
        value: None,
    };
    check_owner(&owner)?;
    Ok(owner)
}

/// Release with a host-accepted newer native event. Delayed promotion cannot
/// overwrite this fence when resolved through join_owner.
pub fn release<Stamp: Clone + Ord>(owner: &Owner<Stamp>, accepted: Stamp) -> Result<Owner<Stamp>> {
    check_owner(owner)?;
    if !owner.owned {
        return Err(Error::Invalid("field is not High-owned".into()));
    }
    if accepted <= owner.epoch
        || owner
            .value
            .as_ref()
            .is_some_and(|v| accepted < v.native_version)
    {
        return Err(Error::Invalid("stale native ownership release".into()));
    }
    let mut released = owner.clone();
    released.owned = false;
    released.epoch = accepted.clone();
    released.release_epoch = accepted;
    Ok(released)
}

#[derive(Clone, Debug, PartialEq)]
pub enum Projection<Stamp> {
    SuppressRow,
    High(OwnedValue<Stamp>),
    Low,
}

/// Select authority without merging or writing a row. The host separately
/// resolves eligible Low values using native CRR and checks lifecycle fences.
pub fn select_projection<Stamp: Clone + Ord>(
    target: &Target,
    row_owner: Option<&Owner<Stamp>>,
    column_owner: Option<&Owner<Stamp>>,
) -> Result<Projection<Stamp>> {
    check_target(target)?;
    if target.column == ROW {
        return Err(Error::Invalid("projection requires a value column".into()));
    }
    if let Some(owner) = row_owner {
        check_owner(owner)?;
        if owner.target.table != target.table
            || owner.target.pk != target.pk
            || owner.target.column != ROW
        {
            return Err(Error::Corrupt("row ownership identity".into()));
        }
    }
    if let Some(owner) = column_owner {
        check_owner(owner)?;
        if owner.target != *target {
            return Err(Error::Corrupt("column ownership identity".into()));
        }
    }
    if row_owner.is_some_and(|owner| owner.owned) {
        return Ok(Projection::SuppressRow);
    }
    if let Some(owner) = column_owner.filter(|owner| owner.owned) {
        return Ok(Projection::High(owner.value.clone().ok_or_else(|| {
            Error::Corrupt("owned column has no value".into())
        })?));
    }
    Ok(Projection::Low)
}

/// Whether ownership allows a native Low deletion to be projected. Native
/// CRR still decides if the Low deletion is causally eligible.
pub fn permits_low_delete<Stamp: Ord>(row: &Target, owners: &[Owner<Stamp>]) -> Result<bool> {
    check_target(row)?;
    for owner in owners {
        check_owner(owner)?;
        if owner.target.table != row.table || owner.target.pk != row.pk {
            return Err(Error::Corrupt("deletion ownership identity".into()));
        }
    }
    Ok(!owners.iter().any(|owner| owner.owned))
}

/// Already-resolved native Low cell provenance; not a Shadow or row model.
#[derive(Clone, Debug)]
pub struct LowSource<Stamp> {
    pub target: Target,
    pub database: [u8; 16],
    pub native_version: Stamp,
    pub incarnation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Scope<Stamp> {
    pub domain: [u8; 16],
    pub branch: Stamp,
    pub incarnation: u64,
    pub basis: Stamp,
}

/// Adapt the archive's policy_scope branch selection without scanning a
/// store, resolving shadows, advancing clocks or merging native rows.
pub fn select_scope<Stamp: Clone + Ord>(
    target: &Target,
    local_database: [u8; 16],
    local_incarnation: u64,
    local_basis: Stamp,
    initial_branch: Stamp,
    owner: Option<&Owner<Stamp>>,
    low_sources: &[LowSource<Stamp>],
) -> Result<Scope<Stamp>> {
    check_target(target)?;
    if let Some(owner) = owner {
        check_owner(owner)?;
        if owner.target != *target {
            return Err(Error::Corrupt("policy ownership identity".into()));
        }
    }
    for source in low_sources {
        if source.target != *target {
            return Err(Error::Corrupt("Low provenance identity".into()));
        }
    }
    if let Some(owner) = owner.filter(|owner| owner.owned) {
        return Ok(Scope {
            domain: local_database,
            branch: owner.release_epoch.clone(),
            incarnation: local_incarnation,
            basis: owner.epoch.clone(),
        });
    }
    let latest = low_sources.iter().max_by(|a, b| {
        (&a.native_version, a.database, a.incarnation).cmp(&(
            &b.native_version,
            b.database,
            b.incarnation,
        ))
    });
    if let Some(source) = latest {
        return Ok(Scope {
            domain: source.database,
            branch: initial_branch,
            incarnation: source.incarnation,
            basis: source.native_version.clone(),
        });
    }
    Ok(Scope {
        domain: local_database,
        branch: owner
            .map(|owner| owner.release_epoch.clone())
            .unwrap_or(initial_branch),
        incarnation: local_incarnation,
        basis: local_basis,
    })
}

fn archived_id(bytes: &[u8]) -> String {
    // Archived model::id hashes then hex-encodes. Reuse the existing tx
    // SHA-256 helper rather than importing another digest implementation.
    murmur_tx::hex(&murmur_tx::protocol::schema_digest(bytes))
}

/// Preserve the archive's receipt namespace and forwarded-origin identity.
/// No new control-carrier schema or persistence is introduced by this helper.
pub fn receipt_key(header: &Header) -> Vec<u8> {
    format!(
        "09/bridge/receipt/{}/{}",
        archived_id(&header.source_database),
        header
            .origin
            .as_ref()
            .map(|origin| format!("{}-{:020}", archived_id(&origin.node), origin.sequence))
            .unwrap_or_else(|| archived_id(&header.bundle))
    )
    .into_bytes()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ImportDecision {
    /// Commit the receipt atomically with the host's accepted native apply.
    Fresh {
        key: Vec<u8>,
        digest: [u8; 32],
    },
    Replay,
}

/// Apply the archived role/domain/receipt checks AFTER the canonical bridge
/// has authenticated/decrypted the bundle. This is not signature verification.
/// previous_receipt must be read under receipt_key(header) by the host.
pub fn check_import(
    role: Role,
    trusted_domains: &BTreeSet<[u8; 16]>,
    header: &Header,
    payload_database: Option<[u8; 16]>,
    previous_receipt: Option<&[u8]>,
) -> Result<ImportDecision> {
    if role != Role::HighReceiver || !trusted_domains.contains(&header.source_database) {
        return Err(Error::Invalid("untrusted bridge domain".into()));
    }
    if let Some(previous) = previous_receipt {
        if previous != header.digest {
            return Err(Error::Corrupt("conflicting bridge receipt".into()));
        }
        return Ok(ImportDecision::Replay);
    }
    if payload_database.is_some_and(|database| database != header.source_database) {
        return Err(Error::SchemaMismatch);
    }
    Ok(ImportDecision::Fresh {
        key: receipt_key(header),
        digest: header.digest,
    })
}
