//! Snapshot join over CRR state (point 3, slice A3a).
//!
//! Downstream adapter over `murmur_tx::snapshot`'s chunk/pin/receipt
//! state machine. Policy (budgets, trust, signatures) comes from the
//! embedded profile, not the archived transport `Config`:
//!
//! - Sizes, lifetime, and transfer counts: `murmur_snapshot_*` pragmas.
//! - Trust: mutual TLS already authenticates both endpoints, so manifest
//!   signatures are replaced by transport authentication — the donor
//!   must be the authenticated source AND listed in `trusted_sources` —
//!   plus the archived identity/expiry/budget/schema checks verbatim.
//!   The signature field stays empty; mixed archived/embedded networks
//!   were already impossible (CRSB is intentionally incompatible).
//! - Payload: prefix-consistent CRSB segments (a full export from
//!   version 0), not a point-in-time image; concurrent writes arrive
//!   via follow-up pulls. Install applies them through
//!   `Backend::apply`, which is idempotent.
//!
//! Reused verbatim (they take no `Config`): `receive`, `missing`,
//! `incoming`, `outgoing`, `cancel`, `maintain`, `prune_limit`,
//! `reap_forgotten_pins`. Forked below: `verify_transport`,
//! `build_streaming`, `accept`, `finish`.

use crate::*;
use murmur_tx::snapshot::{Incoming, Manifest, Pin};

#[derive(Clone, Debug)]
pub(crate) struct SnapshotParams {
    pub snapshot_bytes: usize,
    pub snapshot_staging_bytes: usize,
    pub transfer_lifetime_ms: u64,
    pub staging_transfers: usize,
    pub max_transaction_bytes: usize,
    pub batch_bytes: usize,
    pub trusted_sources: BTreeSet<murmur_tx::NodeId>,
}

pub(crate) fn deadline(params: &SnapshotParams, now: u64) -> u64 {
    now.saturating_add(params.transfer_lifetime_ms)
}

/// Load snapshot params from `murmur_config` (engine + UDFs). Batch
/// knobs come from the shared batch settings; trust defaults to empty
/// (deny) until the operator lists donors.
pub(crate) fn params_from(db: &rusqlite::Connection) -> Result<SnapshotParams> {
    let num = |name: &str| -> Result<u64> {
        config::setting(db, name)?
            .parse()
            .map_err(|_| Error::Invalid(format!("invalid {name}")))
    };
    let trusted: Vec<String> = serde_json::from_str(
        &config::setting(db, "snapshot_trusted_sources")?,
    )
    .map_err(|e| Error::Invalid(format!("snapshot_trusted_sources: {e}")))?;
    let mut trusted_sources = BTreeSet::new();
    for id in &trusted {
        trusted_sources.insert(parse_id(id)?);
    }
    Ok(SnapshotParams {
        snapshot_bytes: num("snapshot_bytes")? as usize,
        snapshot_staging_bytes: num("snapshot_staging_bytes")? as usize,
        transfer_lifetime_ms: num("snapshot_lifetime_ms")?,
        staging_transfers: num("snapshot_staging_transfers")? as usize,
        max_transaction_bytes: num("max_batch_bytes")? as usize,
        batch_bytes: num("batch_bytes")? as usize,
        trusted_sources,
    })
}

/// Archived `Manifest::verify` minus the Ed25519 check: mutual TLS binds
/// the donor identity instead. All other checks are verbatim.
pub(crate) fn verify_transport(
    m: &Manifest,
    backend: &dyn murmur_tx::Backend,
    params: &SnapshotParams,
    source: murmur_tx::NodeId,
    now: u64,
) -> murmur_tx::Result<()> {
    let (database, recipient) = backend.identity();
    if m.database != database
        || m.donor != source
        || m.recipient != recipient
        || !params.trusted_sources.contains(&source)
        || m.expires <= now
    {
        return Err(murmur_tx::Error::Auth(
            "snapshot identity, trust or expiry".into(),
        ));
    }
    let bytes =
        usize::try_from(m.bytes).map_err(|_| murmur_tx::Error::Overloaded)?;
    if bytes > params.snapshot_bytes
        || bytes > params.snapshot_staging_bytes
        || m.progress.len() > 4096
        || m.chunks.len() != bytes.div_ceil(murmur_tx::protocol::CHUNK_BYTES)
    {
        return Err(murmur_tx::Error::Overloaded);
    }
    let mut origins = BTreeSet::new();
    for p in &m.progress {
        if !origins.insert(p.origin)
            || p.observed < p.applied
            || (p.retained_from > p.retained_through && p.retained_through != 0)
        {
            return Err(murmur_tx::Error::Protocol("snapshot progress".into()));
        }
    }
    Ok(())
}

/// Archived `build_stream` with embedded params and no signing. `pages`
/// are CRSB export segments; `emit` persists bounded groups. Only the
/// final group publishes the manifest and pin.
pub(crate) fn build_streaming(
    identity: (murmur_tx::NodeId, murmur_tx::NodeId),
    recipient: murmur_tx::NodeId,
    pages: impl IntoIterator<Item = murmur_tx::Result<Vec<u8>>>,
    progress: Vec<murmur_tx::protocol::Progress>,
    schema: [u8; 32],
    params: &SnapshotParams,
    now: u64,
    mut emit: impl FnMut(Vec<murmur_tx::Record>) -> murmur_tx::Result<()>,
) -> murmur_tx::Result<Manifest> {
    use murmur_tx::snapshot::{
        BUILD_GRACE_MILLIS, STREAM_MAGIC, chunk, header, pin_key,
    };
    use murmur_tx::encode;
    use crate::crypto_provider::Sha256;
    if progress.len() > 4096 {
        return Err(murmur_tx::Error::Overloaded);
    }
    let transfer: murmur_tx::NodeId = crate::randomness::bytes();
    // Phase 1: preliminary pin+header, chunkless by construction.
    let prelim = Manifest {
        database: identity.0,
        donor: identity.1,
        recipient,
        transfer,
        expires: now.saturating_add(BUILD_GRACE_MILLIS),
        bytes: 0,
        digest: [0; 32],
        schema,
        progress: progress.clone(),
        chunks: Vec::new(),
        signature: Vec::new(),
    };
    emit(vec![
        murmur_tx::Record::put(
            pin_key(transfer),
            encode(&Pin {
                recipient,
                boundary: progress.iter().map(|p| (p.origin, p.applied)).collect(),
                target: None,
                expires: prelim.expires,
                digest: prelim.digest,
            })?,
        ),
        murmur_tx::Record::put(
            header(murmur_tx::snapshot::OUT, transfer),
            encode(&prelim)?,
        ),
    ])?;
    let mut hashes = Vec::new();
    let mut digest = Sha256::new();
    let mut buffer = Vec::with_capacity(murmur_tx::protocol::CHUNK_BYTES);
    let mut records = Vec::new();
    let mut total = 0usize;
    let mut push = |bytes: &[u8],
                    hashes: &mut Vec<[u8; 32]>,
                    buffer: &mut Vec<u8>,
                    records: &mut Vec<murmur_tx::Record>,
                    total: &mut usize,
                    digest: &mut Sha256|
     -> murmur_tx::Result<()> {
        *total = total
            .checked_add(bytes.len())
            .ok_or(murmur_tx::Error::Overloaded)?;
        if *total > params.snapshot_bytes || *total > params.snapshot_staging_bytes {
            return Err(murmur_tx::Error::Overloaded);
        }
        digest.update(bytes);
        let mut rest = bytes;
        while !rest.is_empty() {
            let n = (murmur_tx::protocol::CHUNK_BYTES - buffer.len()).min(rest.len());
            buffer.extend_from_slice(&rest[..n]);
            rest = &rest[n..];
            if buffer.len() == murmur_tx::protocol::CHUNK_BYTES {
                hashes.push(Sha256::digest(&buffer).into());
                records.push(murmur_tx::Record::put(
                    chunk(
                        murmur_tx::snapshot::OUT,
                        transfer,
                        (hashes.len() - 1) as u32,
                    ),
                    std::mem::replace(
                        buffer,
                        Vec::with_capacity(murmur_tx::protocol::CHUNK_BYTES),
                    ),
                ));
                if records.len() == 16 {
                    emit(std::mem::take(records))?;
                }
            }
        }
        Ok(())
    };
    push(
        STREAM_MAGIC,
        &mut hashes,
        &mut buffer,
        &mut records,
        &mut total,
        &mut digest,
    )?;
    for page in pages {
        let page = page?;
        if page.is_empty() || page.len() > params.max_transaction_bytes {
            return Err(murmur_tx::Error::Overloaded);
        }
        push(
            &(page.len() as u32).to_be_bytes(),
            &mut hashes,
            &mut buffer,
            &mut records,
            &mut total,
            &mut digest,
        )?;
        push(
            &page,
            &mut hashes,
            &mut buffer,
            &mut records,
            &mut total,
            &mut digest,
        )?;
    }
    if !buffer.is_empty() {
        hashes.push(Sha256::digest(&buffer).into());
        records.push(murmur_tx::Record::put(
            chunk(
                murmur_tx::snapshot::OUT,
                transfer,
                (hashes.len() - 1) as u32,
            ),
            buffer,
        ));
    }
    let manifest = Manifest {
        database: identity.0,
        donor: identity.1,
        recipient,
        transfer,
        expires: deadline(params, now),
        bytes: total as u64,
        digest: digest.finalize().into(),
        schema,
        progress,
        chunks: hashes,
        signature: Vec::new(),
    };
    let pin = Pin {
        recipient,
        boundary: manifest
            .progress
            .iter()
            .map(|p| (p.origin, p.applied))
            .collect(),
        target: None,
        expires: manifest.expires,
        digest: manifest.digest,
    };
    records.push(murmur_tx::Record::put(pin_key(transfer), encode(&pin)?));
    records.push(murmur_tx::Record::put(
        header(murmur_tx::snapshot::OUT, transfer),
        encode(&manifest)?,
    ));
    emit(records)?;
    Ok(manifest)
}

/// Archived `accept` with transport verification.
pub(crate) fn accept(
    backend: &dyn murmur_tx::Backend,
    params: &SnapshotParams,
    source: murmur_tx::NodeId,
    m: Manifest,
    now: u64,
) -> murmur_tx::Result<()> {
    use murmur_tx::snapshot::{IN, RECEIPTS, header};
    use murmur_tx::{decode, encode};
    verify_transport(&m, backend, params, source, now)?;
    if m.chunks.is_empty() {
        return Err(murmur_tx::Error::Unavailable(
            "snapshot build in progress".into(),
        ));
    }
    if let Some(bytes) = backend.get(&murmur_tx::snapshot::receipt_key(m.transfer))? {
        let done: Incoming = decode(&bytes)?;
        if encode(&done.manifest)? != encode(&m)? {
            return Err(murmur_tx::Error::Auth(
                "conflicting snapshot receipt".into(),
            ));
        }
        return Ok(());
    }
    if let Some(old) = murmur_tx::snapshot::incoming(backend)? {
        if encode(&old.manifest)? != encode(&m)? {
            return Err(murmur_tx::Error::Unavailable(
                "another snapshot is staged".into(),
            ));
        }
        return Ok(());
    }
    if backend.keys(RECEIPTS)?.len() >= params.staging_transfers {
        return Err(murmur_tx::Error::Overloaded);
    }
    let mut used = m.bytes;
    for key in backend
        .keys(IN)?
        .into_iter()
        .filter(|k| !k.ends_with(b"/header"))
    {
        used = used
            .checked_add(backend.get(&key)?.map_or(0, |v| v.len() as u64))
            .ok_or(murmur_tx::Error::Overloaded)?;
        if used > params.snapshot_staging_bytes as u64 {
            return Err(murmur_tx::Error::Overloaded);
        }
    }
    let staged = Incoming {
        deadline: m.expires.min(deadline(params, now)),
        manifest: m,
    };
    backend.commit(vec![murmur_tx::Record::put(
        header(IN, staged.manifest.transfer),
        encode(&staged)?,
    )])
}

/// Archived `finish` with transport verification. Returns true once the
/// receipt is durable (install may take several calls for big streams).
pub(crate) fn finish(
    backend: &dyn murmur_tx::Backend,
    params: &SnapshotParams,
    source: murmur_tx::NodeId,
    transfer: murmur_tx::NodeId,
    now: u64,
) -> murmur_tx::Result<bool> {
    use murmur_tx::snapshot::{
        STREAM_MAGIC, install_key, receipt_key, stream_read, validate_chunk,
    };
    use murmur_tx::protocol::progress_key;
    use murmur_tx::{decode, encode};
    use crate::crypto_provider::Sha256;
    if let Some(b) = backend.get(&receipt_key(transfer))? {
        let r: Incoming = decode(&b)?;
        if r.manifest.donor != source {
            return Err(murmur_tx::Error::Auth("snapshot receipt donor".into()));
        }
        return Ok(true);
    }
    let Some(b) = backend.get(&murmur_tx::snapshot::header(
        murmur_tx::snapshot::IN,
        transfer,
    ))?
    else {
        return Ok(false);
    };
    let r: Incoming = decode(&b)?;
    verify_transport(&r.manifest, backend, params, source, now)?;
    if r.deadline <= now {
        return Ok(false);
    }
    if r.manifest.schema != murmur_tx::protocol::schema_digest(&backend.schema()?) {
        return Err(murmur_tx::Error::Unavailable(
            "snapshot schema mismatch".into(),
        ));
    }
    if !murmur_tx::snapshot::missing(backend, &r)?.is_empty() {
        return Ok(false);
    }
    let install = backend
        .get(&install_key(transfer))?
        .map(|bytes| decode::<u64>(&bytes))
        .transpose()?;
    let streamed = if r.manifest.bytes >= STREAM_MAGIC.len() as u64 {
        stream_read(backend, &r.manifest, 0, STREAM_MAGIC.len())? == STREAM_MAGIC
    } else {
        false
    };
    let mut data = Vec::new();
    if install.is_none() {
        let mut digest = Sha256::new();
        let mut total = 0u64;
        for i in 0..r.manifest.chunks.len() {
            let Some(bytes) = backend.get(&murmur_tx::snapshot::chunk(
                murmur_tx::snapshot::IN,
                transfer,
                i as u32,
            ))?
            else {
                return Ok(false);
            };
            validate_chunk(&r.manifest, source, i as u32, &bytes)?;
            digest.update(&bytes);
            total += bytes.len() as u64;
            if !streamed {
                data.extend(bytes);
            }
        }
        if total != r.manifest.bytes
            || <[u8; 32]>::from(digest.finalize()) != r.manifest.digest
        {
            return Err(murmur_tx::Error::Auth("snapshot payload digest".into()));
        }
    }
    if streamed {
        let mut cursor = install.unwrap_or(STREAM_MAGIC.len() as u64);
        if cursor < STREAM_MAGIC.len() as u64 || cursor > r.manifest.bytes {
            return Err(murmur_tx::Error::Protocol(
                "snapshot installation cursor".into(),
            ));
        }
        if cursor < r.manifest.bytes {
            let mut payloads = Vec::new();
            let mut bytes = 0usize;
            while cursor < r.manifest.bytes && payloads.len() < 16 {
                let length = u32::from_be_bytes(
                    stream_read(backend, &r.manifest, cursor, 4)?
                        .try_into()
                        .unwrap(),
                ) as usize;
                if length == 0 || length > params.max_transaction_bytes {
                    return Err(murmur_tx::Error::Overloaded);
                }
                if !payloads.is_empty() && bytes.saturating_add(length) > params.batch_bytes {
                    break;
                }
                bytes = bytes.saturating_add(length);
                payloads.push(stream_read(backend, &r.manifest, cursor + 4, length)?);
                cursor += 4 + length as u64;
            }
            if !backend.apply(
                payloads,
                vec![murmur_tx::Record::put(
                    install_key(transfer),
                    encode(&cursor)?,
                )],
            )? {
                return Ok(false);
            }
            if cursor < r.manifest.bytes {
                return Ok(false);
            }
        }
    }
    let mut receipt = r.clone();
    receipt.deadline = deadline(params, now);
    let mut records = vec![
        murmur_tx::Record::put(receipt_key(transfer), encode(&receipt)?),
        murmur_tx::Record::delete(murmur_tx::snapshot::header(
            murmur_tx::snapshot::IN,
            transfer,
        )),
        murmur_tx::Record::delete(install_key(transfer)),
    ];
    for p in &r.manifest.progress {
        let old = backend
            .get(&progress_key(p.origin))?
            .map(|b| decode::<u64>(&b))
            .transpose()?
            .unwrap_or(0);
        records.push(murmur_tx::Record::put(
            progress_key(p.origin),
            encode(&old.max(p.applied))?,
        ));
    }
    backend.apply(if streamed { Vec::new() } else { vec![data] }, records)
}

/// Donor side of A3b: build a snapshot for `recipient` over a
/// prefix-consistent full export (pages from version 0). Runs on
/// `spawn_blocking`: export runs through blocking worker calls and
/// chunks commit through the `Backend`. Chunks land in the donor's
/// local kv only (export reads `crsql_changes`, never kv, so OUT
/// state never replicates). The manifest must fit one frame; past
/// ~192MB of chunk hashes the transfer is refused (fragmented
/// manifests are a follow-up, not this slice).
pub(crate) fn build_for(
    worker: &std::sync::Arc<storage::Worker>,
    backend: &dyn murmur_tx::Backend,
    cfg: config::Config,
    params: &SnapshotParams,
    recipient: murmur_tx::NodeId,
    now: u64,
) -> murmur_tx::Result<Manifest> {
    let schema = murmur_tx::protocol::schema_digest(&backend.schema()?);
    let progress = worker
        .call(|s| s.tx_progress())
        .map_err(tx_err_local)?;
    let mut pages = Vec::new();
    let mut cursor = protocol::Cursor {
        from: 0,
        through: None,
        after: None,
    };
    loop {
        let step = cursor.clone();
        let step_cfg = cfg.clone();
        let (next, blob) = worker
            .call(move |s| s.export(step, &step_cfg))
            .map_err(tx_err_local)?;
        pages.push(blob);
        if next.through.is_none() {
            break;
        }
        cursor = next;
    }
    let identity = backend.identity();
    let manifest = build_streaming(
        identity,
        recipient,
        pages.into_iter().map(Ok),
        progress,
        schema,
        params,
        now,
        |records| backend.commit(records),
    )?;
    let encoded = murmur_tx::encode(&manifest)?;
    if encoded.len() > protocol::FRAME_BYTES {
        return Err(murmur_tx::Error::Overloaded);
    }
    Ok(manifest)
}

fn tx_err_local(e: crate::Error) -> murmur_tx::Error {
    crate::backend::tx_err(e)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{backend::CrrBackend, storage::Worker};
    use murmur_tx::Backend as _;

    fn backend(number: u8) -> (tempfile::TempDir, CrrBackend) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db").to_str().unwrap().to_owned();
        let worker = Worker::open_test(path).unwrap();
        // Clock-table stubs (no extension in unit tests): schema_bytes
        // only checks sibling table names. Identical DDL on both sides
        // keeps donor/recipient digests equal.
        worker
            .call(|s| {
                s.db.execute_batch(
                    "CREATE TABLE docs(id INTEGER PRIMARY KEY, v TEXT); \
                     CREATE TABLE docs__crsql_clock(a);",
                )?;
                Ok(())
            })
            .unwrap();
        let mut node = [0u8; 16];
        node[15] = number;
        (dir, CrrBackend::new_offline(worker, [1; 16], node))
    }

    fn params_for(trusted: murmur_tx::NodeId) -> SnapshotParams {
        SnapshotParams {
            snapshot_bytes: 64 << 20,
            snapshot_staging_bytes: 64 << 20,
            transfer_lifetime_ms: 3_600_000,
            staging_transfers: 16,
            max_transaction_bytes: 4 << 20,
            batch_bytes: 1 << 20,
            trusted_sources: BTreeSet::from([trusted]),
        }
    }

    /// Full donor/recipient round trip with empty pages: exercises
    /// manifest, accept, chunk fetch, missing, finish, progress, and
    /// receipt without cr-sqlite (no CRSB install when the stream
    /// carries only its magic).
    #[test]
    fn round_trip_empty_pages() {
        let (_d1, donor) = backend(1);
        let (_d2, recipient) = backend(2);
        let donor_id = donor.identity().1;
        let recipient_id = recipient.identity().1;
        // Donor progress the snapshot will carry.
        donor
            .commit(vec![murmur_tx::Record::put(
                murmur_tx::protocol::progress_key(donor_id),
                murmur_tx::encode(&41u64).unwrap(),
            )])
            .unwrap();
        let progress = donor.progress().unwrap();
        let schema = murmur_tx::protocol::schema_digest(&donor.schema().unwrap());
        let dparams = params_for(recipient_id);
        let manifest = build_streaming(
            donor.identity(),
            recipient_id,
            Vec::<murmur_tx::Result<Vec<u8>>>::new(),
            progress.clone(),
            schema,
            &dparams,
            1_000,
            |records| donor.commit(records),
        )
        .unwrap();
        assert_eq!(manifest.donor, donor_id);
        assert_eq!(manifest.recipient, recipient_id);
        assert!(!manifest.chunks.is_empty()); // magic chunk present
        assert!(manifest.signature.is_empty()); // transport-auth profile
        // Donor serves its own build back (reuse path).
        let served =
            murmur_tx::snapshot::outgoing(&donor, recipient_id, manifest.transfer, 2_000)
                .unwrap()
                .unwrap();
        assert_eq!(served.transfer, manifest.transfer);
        // Recipient accepts, fetches every chunk, finishes.
        let rparams = params_for(donor_id);
        accept(&recipient, &rparams, donor_id, manifest.clone(), 2_000).unwrap();
        for (i, _hash) in manifest.chunks.iter().enumerate() {
            let key = murmur_tx::snapshot::chunk(
                murmur_tx::snapshot::OUT,
                manifest.transfer,
                i as u32,
            );
            let data = donor.get(&key).unwrap().unwrap();
            murmur_tx::snapshot::receive(
                &recipient,
                donor_id,
                manifest.transfer,
                i as u32,
                data,
                2_000,
            )
            .unwrap();
        }
        let state = murmur_tx::snapshot::incoming(&recipient)
            .unwrap()
            .unwrap();
        assert!(murmur_tx::snapshot::missing(&recipient, &state)
            .unwrap()
            .is_empty());
        assert!(finish(&recipient, &rparams, donor_id, manifest.transfer, 3_000).unwrap());
        // Progress carried over; receipt durable; second finish idempotent.
        let heads = recipient.progress().unwrap();
        let carried = heads.iter().find(|p| p.origin == donor_id).unwrap();
        assert_eq!(carried.applied, 41);
        assert!(finish(&recipient, &rparams, donor_id, manifest.transfer, 4_000).unwrap());
    }

    #[test]
    fn verify_rejects_distrust_and_mismatch() {
        let (_d1, donor) = backend(1);
        let (_d2, recipient) = backend(2);
        let progress = donor.progress().unwrap();
        let schema = murmur_tx::protocol::schema_digest(&donor.schema().unwrap());
        let dparams = params_for(recipient.identity().1);
        let manifest = build_streaming(
            donor.identity(),
            recipient.identity().1,
            Vec::<murmur_tx::Result<Vec<u8>>>::new(),
            progress,
            schema,
            &dparams,
            1_000,
            |records| donor.commit(records),
        )
        .unwrap();
        // Untrusted donor.
        let nobody = params_for([9u8; 16]);
        assert!(
            accept(&recipient, &nobody, donor.identity().1, manifest.clone(), 2_000).is_err()
        );
        // Wrong source.
        let rparams = params_for(donor.identity().1);
        assert!(accept(&recipient, &rparams, [9u8; 16], manifest.clone(), 2_000).is_err());
        // Expired.
        assert!(accept(&recipient, &rparams, donor.identity().1, manifest.clone(), u64::MAX)
            .is_err());
        // Oversized.
        let mut big = manifest.clone();
        big.bytes = u64::MAX;
        assert!(accept(&recipient, &rparams, donor.identity().1, big, 2_000).is_err());
    }
}
