//! Reactive query subscriptions and observer cursors.
//!
//! Provides live query change streams with initial snapshots, row-level diffs,
//! generation-bound resume cursors, and reset handling for slow consumers.
//!
//! Ported from MURMUR-RUST `src/subscription.rs`; sqlite-murmur adaptation
//! (see `PROVENANCE.md`): [`start_subscription`] takes an explicit fetch
//! closure, [`SubscriptionManager`], and commit sequence instead of the
//! archive `Database` handle. Queue, manager, diff, and worker logic are
//! otherwise unchanged.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{
    Arc, Condvar, Mutex, RwLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
    mpsc,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Row-level change type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeType {
    Added,
    Updated,
    Removed,
}

/// One row diff entry with its primary key identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowChange<T> {
    pub change_type: ChangeType,
    pub key: Vec<u8>,
    pub row: Option<T>,
}

/// Generation-bound resume token for subscriptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionCursor {
    pub database_id: [u8; 16],
    pub epoch: u64,
    pub sequence: u64,
}

/// Events emitted by a reactive subscription.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SubscriptionEvent<T> {
    /// Initial snapshot of matching rows when the subscription starts.
    Initial {
        rows: Vec<T>,
        cursor: SubscriptionCursor,
    },
    /// Incremental update event with latest rows and key-level diffs.
    Update {
        rows: Vec<T>,
        changes: Vec<RowChange<T>>,
        cursor: SubscriptionCursor,
    },
    /// Reset event sent when a slow consumer overflows its buffer,
    /// a schema migration invalidates state, or the database closes.
    Reset {
        cursor: SubscriptionCursor,
        reason: String,
    },
}

/// Configuration options for creating a subscription.
#[derive(Debug, Clone)]
pub struct SubscriptionOptions {
    /// Capacity of the event buffer for this subscriber.
    pub buffer_size: usize,
    /// Emit an event even when rows haven't changed.
    pub emit_unchanged: bool,
    /// Attempt to resume after this cursor.
    pub resume_from: Option<SubscriptionCursor>,
    /// Limit notifications to writes touching these table names (empty = watch all).
    pub tables: Vec<String>,
}

impl Default for SubscriptionOptions {
    fn default() -> Self {
        Self {
            buffer_size: 32,
            emit_unchanged: false,
            resume_from: None,
            tables: Vec::new(),
        }
    }
}

pub(crate) struct EventQueue<T> {
    queue: Mutex<VecDeque<SubscriptionEvent<T>>>,
    condvar: Condvar,
    closed: AtomicBool,
}

impl<T> EventQueue<T> {
    fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            condvar: Condvar::new(),
            closed: AtomicBool::new(false),
        }
    }

    /// Push an event. If the queue is at or exceeds buffer_size,
    /// discard stale backlog, enqueue Reset event, and return false
    /// to signal buffer overflow (slow consumer).
    fn push(
        &self,
        event: SubscriptionEvent<T>,
        buffer_size: usize,
        cursor: SubscriptionCursor,
    ) -> bool {
        let mut q = self.queue.lock().unwrap();
        if self.closed.load(Ordering::SeqCst) {
            return false;
        }
        if q.len() >= buffer_size {
            // Buffer overflow! Discard stale events and enqueue Reset.
            q.clear();
            q.push_back(SubscriptionEvent::Reset {
                cursor,
                reason: "buffer overflow: consumer fell behind".into(),
            });
            self.closed.store(true, Ordering::SeqCst);
            self.condvar.notify_all();
            return false;
        }
        q.push_back(event);
        self.condvar.notify_one();
        true
    }

    fn push_reset(&self, cursor: SubscriptionCursor, reason: String) {
        let mut q = self.queue.lock().unwrap();
        if self.closed.load(Ordering::SeqCst) {
            return;
        }
        q.clear();
        q.push_back(SubscriptionEvent::Reset { cursor, reason });
        self.closed.store(true, Ordering::SeqCst);
        self.condvar.notify_all();
    }

    fn recv(&self) -> Result<SubscriptionEvent<T>> {
        let mut q = self.queue.lock().unwrap();
        loop {
            if let Some(event) = q.pop_front() {
                return Ok(event);
            }
            if self.closed.load(Ordering::SeqCst) {
                return Err(Error::Closed);
            }
            q = self.condvar.wait(q).unwrap();
        }
    }

    fn recv_timeout(&self, timeout: Duration) -> Result<SubscriptionEvent<T>> {
        let mut q = self.queue.lock().unwrap();
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if let Some(event) = q.pop_front() {
                return Ok(event);
            }
            if self.closed.load(Ordering::SeqCst) {
                return Err(Error::Closed);
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                return Err(Error::Busy);
            }
            let remaining = deadline - now;
            let (new_q, res) = self.condvar.wait_timeout(q, remaining).unwrap();
            q = new_q;
            if res.timed_out() && q.is_empty() {
                if self.closed.load(Ordering::SeqCst) {
                    return Err(Error::Closed);
                }
                return Err(Error::Busy);
            }
        }
    }

    fn try_recv(&self) -> Result<SubscriptionEvent<T>> {
        let mut q = self.queue.lock().unwrap();
        if let Some(event) = q.pop_front() {
            return Ok(event);
        }
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        Err(Error::Busy)
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.condvar.notify_all();
    }
}

/// Active subscription stream.
pub struct Subscription<T> {
    queue: Arc<EventQueue<T>>,
    closed: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl<T> Subscription<T> {
    /// Receive the next event, blocking until available or subscription ends.
    pub fn recv(&self) -> Result<SubscriptionEvent<T>> {
        self.queue.recv()
    }

    /// Receive the next event with a timeout.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<SubscriptionEvent<T>> {
        self.queue.recv_timeout(timeout)
    }

    /// Try to receive the next event without blocking.
    pub fn try_recv(&self) -> Result<SubscriptionEvent<T>> {
        self.queue.try_recv()
    }

    /// Close this subscription.
    pub fn close(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
        self.queue.close();
        if let Some(h) = self.worker.take() {
            let _ = h.join();
        }
    }
}

impl<T> Drop for Subscription<T> {
    fn drop(&mut self) {
        self.close();
    }
}

pub(crate) struct Listener {
    tables: HashSet<String>,
    tx: mpsc::Sender<u64>,
}

pub struct SubscriptionManager {
    database_id: [u8; 16],
    epoch: u64,
    next_id: AtomicU64,
    listeners: Mutex<HashMap<u64, Listener>>,
    history: RwLock<Vec<u64>>,
    max_history: usize,
    closed: AtomicBool,
}

impl SubscriptionManager {
    pub fn new(database_id: [u8; 16], max_history: usize) -> Self {
        let epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(1);

        Self {
            database_id,
            epoch,
            next_id: AtomicU64::new(1),
            listeners: Mutex::new(HashMap::new()),
            history: RwLock::new(Vec::with_capacity(max_history)),
            max_history,
            closed: AtomicBool::new(false),
        }
    }

    pub fn cursor(&self, sequence: u64) -> SubscriptionCursor {
        SubscriptionCursor {
            database_id: self.database_id,
            epoch: self.epoch,
            sequence,
        }
    }

    pub fn notify_commit(&self, sequence: u64, affected_tables: &[String]) {
        if self.closed.load(Ordering::SeqCst) {
            return;
        }

        {
            let mut hist = self.history.write().unwrap();
            hist.push(sequence);
            if hist.len() > self.max_history {
                let overflow = hist.len() - self.max_history;
                hist.drain(0..overflow);
            }
        }

        let listeners = self.listeners.lock().unwrap();
        for listener in listeners.values() {
            if listener.tables.is_empty()
                || affected_tables.iter().any(|t| listener.tables.contains(t))
            {
                let _ = listener.tx.send(sequence);
            }
        }
    }

    pub fn notify_reset(&self, _reason: &str) {
        if self.closed.load(Ordering::SeqCst) {
            return;
        }
        let listeners = self.listeners.lock().unwrap();
        for listener in listeners.values() {
            let _ = listener.tx.send(u64::MAX); // u64::MAX signals reset
        }
    }

    pub fn validate_resume(&self, cursor: SubscriptionCursor, current_seq: u64) -> Result<()> {
        if cursor.database_id != self.database_id || cursor.epoch != self.epoch {
            return Err(Error::Invalid("subscription cursor expired: epoch mismatch".into()));
        }
        if cursor.sequence > current_seq {
            return Err(Error::Invalid("subscription cursor expired: future sequence".into()));
        }
        let hist = self.history.read().unwrap();
        if let Some(&min_retained) = hist.first() {
            if cursor.sequence < min_retained {
                return Err(Error::Invalid("subscription cursor expired: sequence purged".into()));
            }
        }
        Ok(())
    }

    pub fn register(&self, tables: &[String]) -> (u64, mpsc::Receiver<u64>) {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = mpsc::channel();
        let mut map = self.listeners.lock().unwrap();
        map.insert(
            id,
            Listener {
                tables: tables.iter().cloned().collect(),
                tx,
            },
        );
        (id, rx)
    }

    pub fn unregister(&self, id: u64) {
        let mut map = self.listeners.lock().unwrap();
        map.remove(&id);
    }

    #[allow(dead_code)]
    pub fn has_listeners(&self) -> bool {
        !self.listeners.lock().unwrap().is_empty()
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        let mut map = self.listeners.lock().unwrap();
        for listener in map.values() {
            let _ = listener.tx.send(u64::MAX);
        }
        map.clear();
    }
}

/// Start a live subscription. `manager` is the shared notification hub,
/// `current_seq` the present commit sequence (sqlite-murmur uses
/// `crsql_db_version()`), and `fetch` re-runs the subscribed query on the
/// embedding worker's read path. The rest matches the archive exactly:
/// initial snapshot, per-wake re-query with key-level diffs, and Reset on
/// overflow, schema reset, or query error.
pub fn start_subscription<T, F, K>(
    manager: &Arc<SubscriptionManager>,
    current_seq: u64,
    fetch: F,
    key_fn: K,
    options: SubscriptionOptions,
) -> Result<Subscription<T>>
where
    T: Clone + PartialEq + Send + 'static,
    F: Fn() -> Result<Vec<T>> + Send + Sync + 'static,
    K: Fn(&T) -> Vec<u8> + Send + Sync + 'static,
{
    let sub_mgr = manager.clone();

    if let Some(resume) = options.resume_from {
        sub_mgr.validate_resume(resume, current_seq)?;
    }

    let fetch = Arc::new(fetch);
    let key_fn = Arc::new(key_fn);

    let initial_fetch = fetch.clone();
    let initial_rows = initial_fetch()?;
    let cursor = sub_mgr.cursor(current_seq);

    let queue = Arc::new(EventQueue::new());
    let closed = Arc::new(AtomicBool::new(false));

    // Send initial snapshot
    let initial_event = if options.resume_from.is_some() {
        SubscriptionEvent::Update {
            rows: initial_rows.clone(),
            changes: Vec::new(),
            cursor,
        }
    } else {
        SubscriptionEvent::Initial {
            rows: initial_rows.clone(),
            cursor,
        }
    };

    queue.push(initial_event, options.buffer_size.max(1), cursor);

    let (listener_id, wake_rx) = sub_mgr.register(&options.tables);
    let worker_closed = closed.clone();
    let worker_sub_mgr = sub_mgr.clone();
    let worker_fetch = fetch.clone();
    let worker_key_fn = key_fn.clone();
    let worker_queue = queue.clone();

    let handle = thread::Builder::new()
        .name(format!("murmur-sub-{}", listener_id))
        .spawn(move || {
            let mut last_rows = initial_rows;
            let mut last_seq = current_seq;

            while !worker_closed.load(Ordering::SeqCst) {
                let wake = match wake_rx.recv_timeout(Duration::from_millis(250)) {
                    Ok(seq) => seq,
                    Err(mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                };

                if wake == u64::MAX {
                    // Reset signal
                    let c = worker_sub_mgr.cursor(last_seq);
                    worker_queue.push_reset(c, "schema or storage reset".into());
                    break;
                }

                // Derived search indexes can publish after the source commit,
                // using the same application cursor. Re-evaluate equal cursors.
                if wake < last_seq {
                    continue;
                }

                let f = worker_fetch.clone();
                let new_rows = match f() {
                    Ok(r) => r,
                    Err(e) => {
                        let c = worker_sub_mgr.cursor(last_seq);
                        worker_queue.push_reset(c, format!("query error: {e}"));
                        break;
                    }
                };

                let changes = diff_rows(&last_rows, &new_rows, &*worker_key_fn);
                let changed = !changes.is_empty();
                last_seq = wake;
                let c = worker_sub_mgr.cursor(last_seq);

                if changed || options.emit_unchanged {
                    let event = SubscriptionEvent::Update {
                        rows: new_rows.clone(),
                        changes,
                        cursor: c,
                    };

                    if !worker_queue.push(event, options.buffer_size.max(1), c) {
                        // Slow consumer buffer overflow: Reset event sent, terminate worker
                        worker_closed.store(true, Ordering::SeqCst);
                        break;
                    }
                }

                last_rows = new_rows;
            }

            worker_sub_mgr.unregister(listener_id);
        })
        .map_err(|e| Error::Storage(format!("spawn subscription worker: {e}")))?;

    Ok(Subscription {
        queue,
        closed,
        worker: Some(handle),
    })
}

fn diff_rows<T: Clone + PartialEq, K: Fn(&T) -> Vec<u8>>(
    old_rows: &[T],
    new_rows: &[T],
    key_fn: &K,
) -> Vec<RowChange<T>> {
    let mut old_by_key = HashMap::with_capacity(old_rows.len());
    for row in old_rows {
        old_by_key.insert(key_fn(row), row);
    }

    let mut new_by_key = HashMap::with_capacity(new_rows.len());
    for row in new_rows {
        new_by_key.insert(key_fn(row), row);
    }

    let mut changes = Vec::new();

    // Check additions and updates
    for (key, row) in &new_by_key {
        match old_by_key.get(key) {
            None => {
                changes.push(RowChange {
                    change_type: ChangeType::Added,
                    key: key.clone(),
                    row: Some((*row).clone()),
                });
            }
            Some(old_row) => {
                if *old_row != *row {
                    changes.push(RowChange {
                        change_type: ChangeType::Updated,
                        key: key.clone(),
                        row: Some((*row).clone()),
                    });
                }
            }
        }
    }

    // Check removals
    for key in old_by_key.keys() {
        if !new_by_key.contains_key(key) {
            changes.push(RowChange {
                change_type: ChangeType::Removed,
                key: key.clone(),
                row: None,
            });
        }
    }

    changes.sort_by(|a, b| a.key.cmp(&b.key));
    changes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_reports_added_updated_removed() {
        let key = |s: &String| s.as_bytes().to_vec();
        let old = vec!["a".to_string(), "b".to_string()];
        let new = vec!["b2".to_string(), "c".to_string()];
        // Keys differ per value here; use first-byte keys for a stable shape.
        let key1 = |s: &String| vec![s.as_bytes()[0]];
        let changes = diff_rows(&old, &new, &key1);
        let kinds: Vec<_> = changes.iter().map(|c| c.change_type).collect();
        // Sorted by key: [a] removed, [b] updated, [c] added.
        assert_eq!(kinds, vec![ChangeType::Removed, ChangeType::Updated, ChangeType::Added]);
        assert_eq!(changes[1].key, vec![b'b']);
        assert_eq!(changes[1].row, Some("b2".to_string()));
        assert_eq!(changes[0].row, None);
        let _ = key;
    }

    #[test]
    fn queue_overflow_emits_reset_and_closes() {
        let q: EventQueue<String> = EventQueue::new();
        let cursor = SubscriptionCursor {
            database_id: [7; 16],
            epoch: 1,
            sequence: 1,
        };
        let event = SubscriptionEvent::Initial {
            rows: vec!["x".to_string()],
            cursor,
        };
        assert!(q.push(event.clone(), 1, cursor));
        assert!(!q.push(event, 1, cursor));
        match q.recv().unwrap() {
            SubscriptionEvent::Reset { reason, .. } => assert!(reason.contains("buffer overflow")),
            other => panic!("expected Reset, got {other:?}"),
        }
        assert!(matches!(q.try_recv(), Err(Error::Closed)));
    }

    #[test]
    fn resume_validation_rejects_stale_and_future_cursors() {
        let mgr = SubscriptionManager::new([9; 16], 4);
        for seq in 10..14 {
            mgr.notify_commit(seq, &[]);
        }
        let good = mgr.cursor(12);
        assert!(mgr.validate_resume(good, 13).is_ok());
        let purged = SubscriptionCursor {
            database_id: [9; 16],
            epoch: mgr.epoch,
            sequence: 9,
        };
        assert!(mgr.validate_resume(purged, 13).is_err());
        let future = SubscriptionCursor {
            database_id: [9; 16],
            epoch: mgr.epoch,
            sequence: 99,
        };
        assert!(mgr.validate_resume(future, 13).is_err());
        let foreign = SubscriptionCursor {
            database_id: [8; 16],
            epoch: mgr.epoch,
            sequence: 12,
        };
        assert!(mgr.validate_resume(foreign, 13).is_err());
    }

    #[test]
    fn live_subscription_snapshots_then_diffs() -> Result<()> {
        let mgr = Arc::new(SubscriptionManager::new([3; 16], 16));
        let rows = Arc::new(Mutex::new(vec!["a".to_string()]));
        let worker_rows = rows.clone();
        let fetch = move || Ok(worker_rows.lock().unwrap().clone());
        let mut sub = start_subscription(
            &mgr,
            1,
            fetch,
            |s: &String| s.as_bytes().to_vec(),
            SubscriptionOptions::default(),
        )?;
        match sub.recv_timeout(Duration::from_secs(5))? {
            SubscriptionEvent::Initial { rows, .. } => assert_eq!(rows, vec!["a".to_string()]),
            other => panic!("expected Initial, got {other:?}"),
        }
        rows.lock().unwrap().push("b".to_string());
        mgr.notify_commit(2, &["docs".to_string()]);
        match sub.recv_timeout(Duration::from_secs(5))? {
            SubscriptionEvent::Update { changes, cursor, .. } => {
                assert_eq!(cursor.sequence, 2);
                assert_eq!(changes.len(), 1);
                assert_eq!(changes[0].change_type, ChangeType::Added);
            }
            other => panic!("expected Update, got {other:?}"),
        }
        sub.close();
        Ok(())
    }

    #[test]
    fn table_filter_mutes_unrelated_commits() -> Result<()> {
        let mgr = Arc::new(SubscriptionManager::new([4; 16], 16));
        let mut sub = start_subscription(
            &mgr,
            1,
            || Ok(vec!["a".to_string()]),
            |s: &String| s.as_bytes().to_vec(),
            SubscriptionOptions {
                tables: vec!["watched".to_string()],
                ..SubscriptionOptions::default()
            },
        )?;
        assert!(matches!(
            sub.recv_timeout(Duration::from_secs(5))?,
            SubscriptionEvent::Initial { .. }
        ));
        mgr.notify_commit(2, &["other".to_string()]);
        assert!(matches!(
            sub.recv_timeout(Duration::from_millis(600)),
            Err(Error::Busy)
        ));
        sub.close();
        Ok(())
    }
}
