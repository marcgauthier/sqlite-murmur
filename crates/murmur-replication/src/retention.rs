//! Retention floors over CRR progress (point 4, slice A2).
//!
//! Adapts the archived `murmur_tx::retention::collect` floor rules to a
//! store with no per-sequence log: peer ack floors, startup grace,
//! unrecorded-online pinning, and snapshot pins decide per-origin floors.
//! Floors persist as `floor_key` records and surface as the
//! `retained_from`/`retained_through` window in progress.
//!
//! A2 performs NO physical deletion. A2b adds floor-bounded tombstone
//! pruning: cr-sqlite renumbers db_version at merge (probe-verified), so
//! per-origin floors cannot address clock rows. Instead A2b deletes
//! tombstone (`'-1'`) clock rows at or below the my-stream floor — the
//! minimum over live peers of what they acked of OUR stream — minus the
//! minimum-kept tail. Live cells keep their single clock row each (the
//! clock table is already a frontier); only delete markers prune.
//! Pruned cursors fail loud (`cursor pruned; snapshot required`) instead
//! of serving silent gaps. No time knob exists: clock rows carry no
//! timestamps, so GC is ack-bounded only.

use crate::*;

/// Floor inputs. No time floor: without per-version ages a retention
/// interval cannot honestly gate anything, and A2b physical deletion is
/// ack-bounded for the same reason — there is deliberately no time knob.
pub(crate) struct RetentionParams {
    pub minimum_retained: u64,
    pub maximum_offline_ms: u64,
    pub bootstrap_nonempty: bool,
}

/// Mirrors the archived startup grace: a young node with configured seeds
/// cannot distinguish "single-node" from "seeds still connecting".
const STARTUP_GRACE_MILLIS: u64 = 60_000;

/// Same key the archived collector writes for `retention::status`.
const LAST_KEY: &[u8] = b"07/replication/retention/last";

/// Advance per-origin retention floors. Returns the number of origins
/// whose floor moved. Floors never decrease.
pub(crate) fn collect_floors(
    backend: &dyn murmur_tx::Backend,
    params: &RetentionParams,
    online: &BTreeSet<murmur_tx::NodeId>,
    now_millis: u64,
    boot_millis: u64,
) -> murmur_tx::Result<u64> {
    let peers = murmur_tx::retention::status(backend)?.peers;
    // A live session without a retention record yet pins the floor:
    // pruning before its first ack would strand it.
    let unrecorded_online = online
        .iter()
        .any(|id| !peers.iter().any(|peer| peer.node == *id));
    let young = params.bootstrap_nonempty
        && now_millis.saturating_sub(boot_millis) < STARTUP_GRACE_MILLIS;
    let mut moved = 0u64;
    let mut records = Vec::new();
    for head in backend.progress()? {
        let recorded_floor = peers
            .iter()
            .filter(|peer| {
                !peer.retired
                    && (online.contains(&peer.node)
                        || now_millis.saturating_sub(peer.last_seen_millis)
                            <= params.maximum_offline_ms)
            })
            .map(|peer| peer.acknowledged.get(&head.origin).copied().unwrap_or(0))
            .min();
        let peer_floor = match (recorded_floor, unrecorded_online, young) {
            (_, true, _) => 0,
            (Some(floor), _, _) => floor,
            (None, _, true) => 0,
            (None, _, false) => u64::MAX,
        };
        let snapshot_floor =
            murmur_tx::snapshot::prune_limit(backend, head.origin, now_millis)?;
        let tail_cap = head.applied.saturating_sub(params.minimum_retained);
        let floor = peer_floor.min(snapshot_floor).min(tail_cap);
        let prev: u64 = backend
            .get(&murmur_tx::retention::floor_key(head.origin))?
            .map(|bytes| murmur_tx::decode(&bytes))
            .transpose()?
            .unwrap_or(0);
        if floor > prev {
            records.push(murmur_tx::Record::put(
                murmur_tx::retention::floor_key(head.origin),
                murmur_tx::encode(&floor)?,
            ));
            // Retained window is [floor+1 .. applied]; A2 keeps all
            // physical state and only moves the window.
            let mut head = head.clone();
            head.retained_from = floor.saturating_add(1);
            head.retained_through = head.applied;
            records.push(murmur_tx::Record::put(
                murmur_tx::protocol::retained_key(head.origin),
                murmur_tx::encode(&head)?,
            ));
            moved += 1;
        }
    }
    if !records.is_empty() {
        backend.commit(records)?;
    }
    backend.commit(vec![murmur_tx::Record::put(
        LAST_KEY.to_vec(),
        murmur_tx::encode(&now_millis)?,
    )])?;
    Ok(moved)
}

/// My-stream GC floor: the minimum over live, non-retired peers of what
/// they acked of OUR stream, minus the minimum-kept tail. Same liveness
/// rules as [`collect_floors`]: unrecorded online peers and young seeded
/// nodes pin to 0; a mature node with no qualifying peers prunes through
/// its own applied head (single-node meshes need no history).
pub(crate) fn my_stream_floor(
    backend: &dyn murmur_tx::Backend,
    params: &RetentionParams,
    online: &BTreeSet<murmur_tx::NodeId>,
    own: murmur_tx::NodeId,
    own_applied: u64,
    now_millis: u64,
    boot_millis: u64,
) -> murmur_tx::Result<u64> {
    let peers = murmur_tx::retention::status(backend)?.peers;
    let unrecorded_online = online
        .iter()
        .any(|id| !peers.iter().any(|peer| peer.node == *id));
    if unrecorded_online {
        return Ok(0);
    }
    let acked = peers
        .iter()
        .filter(|peer| {
            !peer.retired
                && (online.contains(&peer.node)
                    || now_millis.saturating_sub(peer.last_seen_millis)
                        <= params.maximum_offline_ms)
        })
        .map(|peer| peer.acknowledged.get(&own).copied().unwrap_or(0))
        .min();
    let young = params.bootstrap_nonempty
        && now_millis.saturating_sub(boot_millis) < STARTUP_GRACE_MILLIS;
    let floor = match (acked, young) {
        (Some(floor), _) => floor.min(own_applied),
        (None, true) => 0,
        (None, false) => own_applied,
    };
    Ok(floor.saturating_sub(params.minimum_retained))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{backend::CrrBackend, storage::Worker};
    use murmur_tx::Backend as _;

    fn setup() -> (tempfile::TempDir, CrrBackend) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db").to_str().unwrap().to_owned();
        let worker = Worker::open_test(path).unwrap();
        (dir, CrrBackend::new_offline(worker, [1; 16], [2; 16]))
    }

    fn put_applied(b: &CrrBackend, origin: murmur_tx::NodeId, seq: u64) {
        b.commit(vec![murmur_tx::Record::put(
            murmur_tx::protocol::progress_key(origin),
            murmur_tx::encode(&seq).unwrap(),
        )])
        .unwrap();
    }

    fn params() -> RetentionParams {
        RetentionParams {
            minimum_retained: 1000,
            maximum_offline_ms: 604800000,
            bootstrap_nonempty: false,
        }
    }

    fn ack(b: &CrrBackend, peer: murmur_tx::NodeId, origin: murmur_tx::NodeId, seq: u64) {
        murmur_tx::retention::touch(
            b,
            peer,
            &[murmur_tx::protocol::Progress {
                origin,
                applied: seq,
                observed: seq,
                retained_from: 0,
                retained_through: 0,
            }],
        )
        .unwrap();
    }

    #[test]
    fn my_stream_floor_tracks_min_live_ack() {
        let (_dir, b) = setup();
        let own = [2u8; 16];
        let (fast, slow) = ([9u8; 16], [8u8; 16]);
        ack(&b, fast, own, 5000);
        ack(&b, slow, own, 3000);
        let online = BTreeSet::from([fast, slow]);
        // Min ack 3000 minus the 1000 tail.
        assert_eq!(
            my_stream_floor(&b, &params(), &online, own, 9000, 1_000_000, 0).unwrap(),
            2000
        );
        // Capped by our own applied head.
        assert_eq!(
            my_stream_floor(&b, &params(), &online, own, 2500, 1_000_000, 0).unwrap(),
            1500
        );
        // Retired peers leave the set; the survivor decides.
        murmur_tx::retention::retire(&b, slow).unwrap();
        assert_eq!(
            my_stream_floor(&b, &params(), &online, own, 9000, 1_000_000, 0).unwrap(),
            4000
        );
        // Unrecorded online peers pin to zero.
        let online = BTreeSet::from([fast, slow, [7u8; 16]]);
        assert_eq!(
            my_stream_floor(&b, &params(), &online, own, 9000, 1_000_000, 0).unwrap(),
            0
        );
    }

    #[test]
    fn my_stream_floor_single_node_uses_own_head() {
        let (_dir, b) = setup();
        let own = [2u8; 16];
        assert_eq!(
            my_stream_floor(&b, &params(), &BTreeSet::new(), own, 5000, 1_000_000, 0).unwrap(),
            4000
        );
        // Young seeded nodes pin to zero (still discovering).
        let mut seeded = params();
        seeded.bootstrap_nonempty = true;
        assert_eq!(
            my_stream_floor(&b, &seeded, &BTreeSet::new(), own, 5000, 30_000, 0).unwrap(),
            0
        );
    }

    #[test]
    fn floors_advance_to_tail_cap_without_peers() {
        let (_dir, b) = setup();
        let origin = [7u8; 16];
        put_applied(&b, origin, 5000);
        let moved = collect_floors(&b, &params(), &BTreeSet::new(), 1_000_000, 0).unwrap();
        assert_eq!(moved, 1);
        let floor: u64 = murmur_tx::decode(
            &b.get(&murmur_tx::retention::floor_key(origin))
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(floor, 4000);
        let head = b
            .progress()
            .unwrap()
            .into_iter()
            .find(|p| p.origin == origin)
            .unwrap();
        assert_eq!(head.retained_from, 4001);
        assert_eq!(head.retained_through, 5000);
        // Second pass moves nothing (floors never decrease, nothing new).
        let moved = collect_floors(&b, &params(), &BTreeSet::new(), 2_000_000, 0).unwrap();
        assert_eq!(moved, 0);
    }

    #[test]
    fn unrecorded_online_peer_pins_floor() {
        let (_dir, b) = setup();
        let origin = [7u8; 16];
        put_applied(&b, origin, 5000);
        let online = BTreeSet::from([[9u8; 16]]);
        let moved = collect_floors(&b, &params(), &online, 1_000_000, 0).unwrap();
        assert_eq!(moved, 0);
        assert!(b
            .get(&murmur_tx::retention::floor_key(origin))
            .unwrap()
            .is_none());
    }

    #[test]
    fn retired_peer_ignored_by_floors() {
        let (_dir, b) = setup();
        let origin = [7u8; 16];
        let peer = [9u8; 16];
        put_applied(&b, origin, 5000);
        murmur_tx::retention::touch(
            &b,
            peer,
            &[murmur_tx::protocol::Progress {
                origin,
                applied: 100,
                observed: 100,
                retained_from: 0,
                retained_through: 0,
            }],
        )
        .unwrap();
        // Live peer ack pins the floor at their ack...
        let online = BTreeSet::from([peer]);
        let moved = collect_floors(&b, &params(), &online, 1_000_000, 0).unwrap();
        assert_eq!(moved, 1);
        let floor: u64 = murmur_tx::decode(
            &b.get(&murmur_tx::retention::floor_key(origin))
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(floor, 100);
        // ...but once retired they no longer pin: floor jumps to the cap.
        murmur_tx::retention::retire(&b, peer).unwrap();
        let moved = collect_floors(&b, &params(), &BTreeSet::new(), 2_000_000, 0).unwrap();
        assert_eq!(moved, 1);
        let floor: u64 = murmur_tx::decode(
            &b.get(&murmur_tx::retention::floor_key(origin))
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(floor, 4000);
    }

    #[test]
    fn touch_retire_status_round_trip() {
        let (_dir, b) = setup();
        let peer = [9u8; 16];
        let origin = [7u8; 16];
        murmur_tx::retention::touch(
            &b,
            peer,
            &[murmur_tx::protocol::Progress {
                origin,
                applied: 42,
                observed: 42,
                retained_from: 0,
                retained_through: 0,
            }],
        )
        .unwrap();
        let status = murmur_tx::retention::status(&b).unwrap();
        assert_eq!(status.peers.len(), 1);
        assert_eq!(status.peers[0].acknowledged.get(&origin), Some(&42));
        assert!(!status.peers[0].retired);
        murmur_tx::retention::retire(&b, peer).unwrap();
        let status = murmur_tx::retention::status(&b).unwrap();
        assert!(status.peers[0].retired);
        assert!(murmur_tx::retention::touch(&b, peer, &[]).is_err());
    }
}
