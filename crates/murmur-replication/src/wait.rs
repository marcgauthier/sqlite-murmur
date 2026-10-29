//! Read-your-write and bounded-staleness waits over backend progress.
//!
//! Archive semantics with synchronous polling: SQL scalars run on
//! application threads with no async runtime, so waits poll
//! `Backend::progress` every [`POLL_INTERVAL`] until the condition holds
//! or the timeout elapses. A zero timeout polls exactly once.
//!
//! TODO(A2): short-circuit `wait_for_applied` when the origin is retired
//! (progress will never advance); retention status arrives with A2.

use crate::*;
use murmur_tx::protocol::Progress;

pub const POLL_INTERVAL: Duration = Duration::from_millis(100);

pub(crate) fn backend_err(e: murmur_tx::Error) -> Error {
    match e {
        murmur_tx::Error::Invalid(s) => Error::Invalid(s),
        murmur_tx::Error::Protocol(s) => Error::Protocol(s),
        murmur_tx::Error::Auth(s) => Error::Auth(s),
        murmur_tx::Error::Storage(s) => Error::Unavailable(s),
        murmur_tx::Error::Overloaded => Error::Overloaded,
        murmur_tx::Error::Unavailable(s) => Error::Unavailable(s),
        murmur_tx::Error::Io(e) => Error::Io(e),
    }
}

/// Worst per-origin applied deficit behind observed heads: how stale this
/// node is against the newest versions it has seen. Origins where applied
/// covers observed contribute nothing.
pub fn observed_deficit(heads: &[Progress]) -> u64 {
    heads
        .iter()
        .map(|p| p.observed.saturating_sub(p.applied))
        .max()
        .unwrap_or(0)
}

/// Block until `origin`'s applied head reaches `seq` or `timeout` elapses.
/// Waiting for sequence zero returns immediately. Returns true when the
/// head was reached, false on timeout.
pub fn wait_for_applied(
    backend: &dyn murmur_tx::Backend,
    origin: murmur_tx::NodeId,
    seq: u64,
    timeout: Duration,
) -> Result<bool> {
    if seq == 0 {
        return Ok(true);
    }
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let heads = backend.progress().map_err(backend_err)?;
        if heads
            .iter()
            .any(|p| p.origin == origin && p.applied >= seq)
        {
            return Ok(true);
        }
        let now = std::time::Instant::now();
        if now >= deadline {
            return Ok(false);
        }
        std::thread::sleep(POLL_INTERVAL.min(deadline - now));
    }
}

/// Block until the worst observed deficit is at most `bound`, or `timeout`
/// elapses. With no known-newer state the deficit is zero and any bound
/// holds. Returns true when met, false on timeout.
pub fn wait_for_lag(
    backend: &dyn murmur_tx::Backend,
    bound: u64,
    timeout: Duration,
) -> Result<bool> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let heads = backend.progress().map_err(backend_err)?;
        if observed_deficit(&heads) <= bound {
            return Ok(true);
        }
        let now = std::time::Instant::now();
        if now >= deadline {
            return Ok(false);
        }
        std::thread::sleep(POLL_INTERVAL.min(deadline - now));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{backend::CrrBackend, storage::Worker};

    fn setup() -> (tempfile::TempDir, CrrBackend) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db").to_str().unwrap().to_owned();
        let worker = Worker::open_test(path).unwrap();
        (dir, CrrBackend::new_offline(worker, [1; 16], [2; 16]))
    }

    fn put_applied(b: &CrrBackend, origin: murmur_tx::NodeId, seq: u64) {
        use murmur_tx::Backend as _;
        b.commit(vec![murmur_tx::Record::put(
            murmur_tx::protocol::progress_key(origin),
            murmur_tx::encode(&seq).unwrap(),
        )])
        .unwrap();
    }

    #[test]
    fn deficit_takes_worst_origin_gap() {
        let heads = vec![
            Progress {
                origin: [1; 16],
                applied: 10,
                observed: 12,
                retained_from: 0,
                retained_through: 0,
            },
            Progress {
                origin: [2; 16],
                applied: 5,
                observed: 5,
                retained_from: 0,
                retained_through: 0,
            },
        ];
        assert_eq!(observed_deficit(&heads), 2);
        assert_eq!(observed_deficit(&[]), 0);
    }

    #[test]
    fn applied_zero_returns_without_touching_storage() {
        let (_dir, b) = setup();
        assert!(wait_for_applied(&b, [9; 16], 0, Duration::from_millis(0)).unwrap());
    }

    #[test]
    fn applied_reached_returns_true() {
        let (_dir, b) = setup();
        put_applied(&b, [7; 16], 10);
        assert!(wait_for_applied(&b, [7; 16], 10, Duration::from_millis(50)).unwrap());
        assert!(wait_for_applied(&b, [7; 16], 9, Duration::from_millis(0)).unwrap());
    }

    #[test]
    fn applied_missing_times_out_false() {
        let (_dir, b) = setup();
        assert!(!wait_for_applied(&b, [7; 16], 1, Duration::from_millis(150)).unwrap());
        put_applied(&b, [7; 16], 3);
        assert!(!wait_for_applied(&b, [7; 16], 4, Duration::from_millis(0)).unwrap());
    }

    #[test]
    fn lag_bound_met_and_unmet() {
        use murmur_tx::Backend as _;
        let (_dir, b) = setup();
        // No heads: any bound holds immediately.
        assert!(wait_for_lag(&b, 0, Duration::from_millis(0)).unwrap());
        b.commit(vec![murmur_tx::Record::put(
            murmur_tx::protocol::observed_key([7; 16]),
            murmur_tx::encode(&10u64).unwrap(),
        )])
        .unwrap();
        assert!(wait_for_lag(&b, 10, Duration::from_millis(0)).unwrap());
        assert!(!wait_for_lag(&b, 9, Duration::from_millis(150)).unwrap());
        put_applied(&b, [7; 16], 10);
        assert!(wait_for_lag(&b, 0, Duration::from_millis(50)).unwrap());
    }
}
