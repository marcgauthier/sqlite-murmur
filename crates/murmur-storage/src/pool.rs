//! Buffer pools and the background encoding pool, ported from
//! `src/sqlite_murmur.cpp` (`ReusePool`, `PageBuffer`, `MemFile`,
//! `EncodingPool`, `physicalCoreCount`).
//!
//! The C++ pools return buffers on last-release via `shared_ptr` custom
//! deleters; the Rust mirror is an `Arc` whose inner `Drop` pushes the
//! buffer back. Limits and retention rules match exactly (they shape the
//! steady-state allocation rate the benchmark measures).

use std::collections::HashSet;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Condvar, Mutex, Weak, atomic::{AtomicUsize, Ordering}};
use std::thread::JoinHandle;
use crate::crypto::{BLOCK, DecodeWorkspace, SCRATCH_SIZE};
use crate::ffi_zstd::Cctx;

/// Physical core count from `/sys` topology, else parallelism, clamped
/// to 1..=256. Memoized like the C++ function-static.
pub fn physical_core_count() -> u32 {
    static COUNT: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
    *COUNT.get_or_init(|| {
        let mut cores = HashSet::new();
        if let Ok(entries) = std::fs::read_dir("/sys/devices/system/cpu") {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                let rest = match name.strip_prefix("cpu") {
                    Some(rest) if !rest.is_empty() && rest.bytes().all(|c| c.is_ascii_digit()) => rest,
                    _ => continue,
                };
                let _ = rest;
                let package = std::fs::read_to_string(entry.path().join("topology/physical_package_id"));
                let core = std::fs::read_to_string(entry.path().join("topology/core_id"));
                if let (Ok(p), Ok(c)) = (package, core) {
                    if let (Ok(p), Ok(c)) = (p.trim().parse::<i32>(), c.trim().parse::<i32>()) {
                        cores.insert((p, c));
                    }
                }
            }
        }
        let n = if cores.is_empty() {
            std::thread::available_parallelism().map(|n| n.get() as u32).unwrap_or(1)
        } else {
            cores.len() as u32
        };
        n.clamp(1, 256)
    })
}

/// Fixed-size pending-write plaintext copy. Wiped on reset/drop.
pub struct PageBuffer {
    pub bytes: [u8; BLOCK],
}

impl PageBuffer {
    pub fn new() -> Self {
        Self { bytes: [0u8; BLOCK] }
    }

    pub fn wipe(&mut self) {
        crate::secure_wipe(&mut self.bytes);
    }
}

impl Default for PageBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for PageBuffer {
    fn drop(&mut self) {
        self.wipe();
    }
}

/// RAM journal/temp file. Shared across VFS handles by name; the mutex
/// serializes them like the C++ member mutex.
pub struct MemFile {
    mu: Mutex<MemFileInner>,
}

pub struct MemFileInner {
    pub bytes: Vec<u8>,
    pub size: usize,
    pub dirty_end: usize,
}

impl MemFile {
    pub fn new() -> Self {
        Self {
            mu: Mutex::new(MemFileInner { bytes: Vec::new(), size: 0, dirty_end: 0 }),
        }
    }

    pub fn lock(&self) -> std::sync::MutexGuard<'_, MemFileInner> {
        self.mu.lock().expect("memfile mutex poisoned")
    }

    /// Capacity gate for pool retention (bytes Vec capacity).
    fn bytes_capacity(&self) -> usize {
        self.lock().bytes.capacity()
    }
}

impl Default for MemFile {
    fn default() -> Self {
        Self::new()
    }
}

/// Logical-file-length journal retention cap (mirrors `kJournalRetainBytes`).
const JOURNAL_RETAIN_BYTES: usize = 1024 * 1024;

pub trait Poolable {
    fn reset(&mut self);
    fn retain(&self) -> bool;
}

impl Poolable for Vec<u8> {
    fn reset(&mut self) {
        self.clear();
    }

    fn retain(&self) -> bool {
        self.capacity() <= 2 * (SCRATCH_SIZE + 69)
    }
}

impl Poolable for PageBuffer {
    fn reset(&mut self) {
        self.wipe();
    }

    fn retain(&self) -> bool {
        true
    }
}

impl Poolable for DecodeWorkspace {
    fn reset(&mut self) {
        DecodeWorkspace::reset(self);
    }

    fn retain(&self) -> bool {
        true
    }
}

impl Poolable for MemFile {
    fn reset(&mut self) {
        let mut inner = self.lock();
        if inner.dirty_end > 0 {
            let n = inner.dirty_end.min(inner.bytes.len());
            crate::secure_wipe(&mut inner.bytes[..n]);
        }
        inner.dirty_end = 0;
        inner.size = 0;
    }

    fn retain(&self) -> bool {
        self.bytes_capacity() <= JOURNAL_RETAIN_BYTES
    }
}

pub struct ReusePool<T: Poolable> {
    mu: Mutex<Vec<Box<T>>>,
    limit: usize,
}

impl<T: Poolable> ReusePool<T> {
    pub fn new(limit: usize) -> Self {
        Self { mu: Mutex::new(Vec::new()), limit }
    }

    fn push_return(&self, mut value: Box<T>) {
        value.reset();
        let mut idle = self.mu.lock().expect("pool mutex poisoned");
        if idle.len() < self.limit && value.retain() {
            idle.push(value);
        }
    }

    fn pop(&self) -> Option<Box<T>> {
        self.mu.lock().expect("pool mutex poisoned").pop()
    }

    pub fn acquire_unique(self: &Arc<Self>) -> PoolLease<T>
    where
        T: Default,
    {
        let buf = self.pop().unwrap_or_else(|| Box::default());
        PoolLease { buf: Some(buf), pool: Arc::clone(self) }
    }

    pub fn acquire_shared(self: &Arc<Self>) -> Pooled<T>
    where
        T: Default,
    {
        self.acquire_unique().share()
    }
}

/// Unique lease: returns to the pool on drop (C++ `Lease`).
pub struct PoolLease<T: Poolable> {
    buf: Option<Box<T>>,
    pool: Arc<ReusePool<T>>,
}

impl<T: Poolable> PoolLease<T> {
    /// Publish into a shared handle (C++ `shared_ptr` from `Lease`).
    pub fn share(mut self) -> Pooled<T> {
        let buf = self.buf.take().expect("lease always holds a buffer");
        Pooled {
            inner: Arc::new(PooledInner { cell: Mutex::new(Some(buf)), pool: Arc::downgrade(&self.pool) }),
        }
    }
}

impl<T: Poolable> Deref for PoolLease<T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.buf.as_ref().expect("lease always holds a buffer")
    }
}

impl<T: Poolable> DerefMut for PoolLease<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.buf.as_mut().expect("lease always holds a buffer")
    }
}

impl<T: Poolable> Drop for PoolLease<T> {
    fn drop(&mut self) {
        if let Some(buf) = self.buf.take() {
            self.pool.push_return(buf);
        }
    }
}

struct PooledInner<T: Poolable> {
    cell: Mutex<Option<Box<T>>>,
    pool: Weak<ReusePool<T>>,
}

impl<T: Poolable> Drop for PooledInner<T> {
    fn drop(&mut self) {
        // Last release: the C++ custom deleter path.
        if let Some(buf) = self.cell.lock().expect("pooled mutex poisoned").take() {
            if let Some(pool) = self.pool.upgrade() {
                pool.push_return(buf);
            }
        }
    }
}

/// Shared pooled buffer (C++ `shared_ptr<const T, Return>`). `Clone`
/// shares; the buffer returns to the pool at refcount zero. Read-only
/// after publish, like the C++ `const`.
pub struct Pooled<T: Poolable> {
    inner: Arc<PooledInner<T>>,
}

// Manual `Clone` (derive would demand `T: Clone`; only the `Arc` clones).
impl<T: Poolable> Clone for Pooled<T> {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }
}

impl<T: Poolable> Pooled<T> {
    pub fn ptr_eq(a: &Self, b: &Self) -> bool {
        Arc::ptr_eq(&a.inner, &b.inner)
    }
}

impl<T: Poolable + Sync> Deref for Pooled<T> {
    type Target = T;

    fn deref(&self) -> &T {
        // The cell is populated from publish until the last release. The
        // guard is intentionally dropped before returning: the referent
        // is stable while any `Arc` clone is alive.
        let guard = self.inner.cell.lock().expect("pooled mutex poisoned");
        let ptr = guard.as_ref().expect("pooled cell populated").as_ref() as *const T;
        // SAFETY: the `Box` is only taken in `PooledInner::drop`, which
        // runs at refcount zero; the returned borrow keeps `self` (hence
        // the `Arc`) alive, so no take can run during the borrow. `T:
        // Sync` covers concurrent readers.
        unsafe { &*ptr }
    }
}

// The pool instantiations are all `Sync`.
fn _assert_sync() {
    fn is_sync<T: Sync>() {}
    is_sync::<Pooled<Vec<u8>>>();
    is_sync::<Pooled<PageBuffer>>();
    is_sync::<Pooled<MemFile>>();
}

pub type RecordPool = ReusePool<Vec<u8>>;
pub type PagePool = ReusePool<PageBuffer>;
pub type WorkspacePool = ReusePool<DecodeWorkspace>;
pub type JournalPool = ReusePool<MemFile>;

/// A cache/pending entry: an encrypted record or a pending plaintext
/// copy. Mirrors the C++ `Record` (either handle set, pointer equality).
#[derive(Default)]
pub enum Record {
    #[default]
    Vacant,
    Encrypted(Pooled<Vec<u8>>),
    Plain(Pooled<PageBuffer>),
}

impl Clone for Record {
    fn clone(&self) -> Self {
        match self {
            Record::Vacant => Record::Vacant,
            Record::Encrypted(buf) => Record::Encrypted(buf.clone()),
            Record::Plain(buf) => Record::Plain(buf.clone()),
        }
    }
}

// Redacting Debug: record bytes (ciphertext or plaintext) never print.
impl std::fmt::Debug for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Record::Vacant => write!(f, "Vacant"),
            Record::Encrypted(buf) => write!(f, "Encrypted({} bytes)", buf.len()),
            Record::Plain(_) => write!(f, "Plain"),
        }
    }
}

impl Record {
    pub fn is_some(&self) -> bool {
        !matches!(self, Record::Vacant)
    }

    /// The encrypted bytes. Panics when plain/vacant — the C++ `operator*`
    /// dereferences null there (UB); every call site holds an encrypted
    /// record by construction.
    pub fn encrypted(&self) -> &[u8] {
        match self {
            Record::Encrypted(buf) => buf,
            _ => panic!("encrypted bytes requested from a non-encrypted record"),
        }
    }

    pub fn plain(&self) -> Option<&PageBuffer> {
        match self {
            Record::Plain(buf) => Some(buf),
            _ => None,
        }
    }
}

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Record::Vacant, Record::Vacant) => true,
            (Record::Encrypted(a), Record::Encrypted(b)) => Pooled::ptr_eq(a, b),
            (Record::Plain(a), Record::Plain(b)) => Pooled::ptr_eq(a, b),
            _ => false,
        }
    }
}

/// Per-worker reusable context/buffer set.
pub struct EncoderWorkspace {
    pub decode: DecodeWorkspace,
    pub compression: Option<Cctx>,
    pub scratch: [u8; SCRATCH_SIZE],
}

impl EncoderWorkspace {
    pub fn new() -> Self {
        Self { decode: DecodeWorkspace::new(), compression: None, scratch: [0u8; SCRATCH_SIZE] }
    }

    pub fn wipe(&mut self) {
        self.decode.reset();
        crate::secure_wipe(&mut self.scratch);
    }
}

impl Default for EncoderWorkspace {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for EncoderWorkspace {
    fn drop(&mut self) {
        self.wipe();
    }
}

type Task = dyn Fn(usize, &mut EncoderWorkspace) + Send + Sync;

struct PoolState {
    mu: Mutex<State>,
    work: Condvar,
    done: Condvar,
    next: AtomicUsize,
}

struct State {
    // Owned task of the active run (or none). Owned (rather than the
    // C++ borrowed `std::function`) so no raw-pointer protocol is needed;
    // callers package shared job state in an `Arc` (see `persist`).
    task: Option<Arc<Task>>,
    count: usize,
    active: usize,
    epoch: usize,
    stopping: bool,
    error: Option<Box<dyn std::any::Any + Send>>,
}

/// Persistent preparation workers, one reusable context/buffer set per
/// worker. Mirrors `EncodingPool`: `run` publishes a task, workers claim
/// indices atomically, the coordinator waits for all threads and then
/// resumes the first panic (C++: rethrows the first exception).
pub struct EncodingPool {
    state: Arc<PoolState>,
    threads: Vec<JoinHandle<()>>,
}

impl EncodingPool {
    pub fn new(number: u32) -> Result<Self, ()> {
        let state = Arc::new(PoolState {
            mu: Mutex::new(State { task: None, count: 0, active: 0, epoch: 0, stopping: false, error: None }),
            work: Condvar::new(),
            done: Condvar::new(),
            next: AtomicUsize::new(0),
        });
        let mut threads = Vec::new();
        for _ in 0..number {
            let thread_state = Arc::clone(&state);
            let mut workspace = EncoderWorkspace::new();
            match std::thread::Builder::new().spawn(move || {
                let state = thread_state;
                let mut seen = 0usize;
                let mut lock = state.mu.lock().expect("pool mutex poisoned");
                loop {
                    lock = state.work.wait_while(lock, |s| !s.stopping && s.epoch == seen).expect("pool mutex poisoned");
                    if lock.stopping {
                        return;
                    }
                    seen = lock.epoch;
                    let task = lock.task.clone().expect("task published for active epoch");
                    // The epoch observation synchronizes the run's
                    // publication; `count`/`task` are stable for the run.
                    let count = lock.count;
                    drop(lock);
                    loop {
                        let index = state.next.fetch_add(1, Ordering::Relaxed);
                        if index >= count {
                            break;
                        }
                        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            task(index, &mut workspace);
                        }));
                        if let Err(payload) = outcome {
                            let mut guard = state.mu.lock().expect("pool mutex poisoned");
                            if guard.error.is_none() {
                                guard.error = Some(payload);
                            }
                        }
                        workspace.wipe();
                    }
                    lock = state.mu.lock().expect("pool mutex poisoned");
                    lock.active -= 1;
                    if lock.active == 0 {
                        state.done.notify_one();
                    }
                }
            }) {
                Ok(handle) => threads.push(handle),
                Err(_) => {
                    state.mu.lock().expect("pool mutex poisoned").stopping = true;
                    state.work.notify_all();
                    for thread in threads {
                        let _ = thread.join();
                    }
                    return Err(());
                }
            }
        }
        Ok(Self { state, threads })
    }

    pub fn run<F>(&self, number: usize, task: Arc<F>)
    where
        F: Fn(usize, &mut EncoderWorkspace) + Send + Sync + 'static,
    {
        if number == 0 {
            return;
        }
        let mut lock = self.state.mu.lock().expect("pool mutex poisoned");
        self.state.next.store(0, Ordering::Relaxed);
        lock.task = Some(task as Arc<Task>);
        lock.count = number;
        lock.active = self.threads.len();
        lock.error = None;
        lock.epoch = lock.epoch.wrapping_add(1);
        self.state.work.notify_all();
        lock = self.state.done.wait_while(lock, |s| s.active != 0).expect("pool mutex poisoned");
        lock.task = None;
        lock.count = 0;
        if let Some(payload) = lock.error.take() {
            drop(lock);
            std::panic::resume_unwind(payload);
        }
    }

    fn stop(&mut self) {
        {
            let mut lock = self.state.mu.lock().expect("pool mutex poisoned");
            lock.stopping = true;
            self.state.work.notify_all();
        }
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

impl Drop for EncodingPool {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pools_round_trip_and_cap() {
        let pool: Arc<RecordPool> = Arc::new(RecordPool::new(2));
        {
            let mut a = pool.acquire_unique();
            a.extend_from_slice(b"hello");
            let shared = a.share();
            assert_eq!(&shared[..], b"hello");
            let clone = shared.clone();
            assert!(Pooled::ptr_eq(&shared, &clone));
        }
        // Returned to the pool, reset.
        let b = pool.acquire_unique();
        assert!(b.is_empty());
        assert!(b.capacity() >= 5);
    }

    #[test]
    fn records_compare_by_handle() {
        let pool: Arc<RecordPool> = Arc::new(RecordPool::new(8));
        let a = Record::Encrypted(pool.acquire_shared());
        let b = a.clone();
        let c = Record::Encrypted(pool.acquire_shared());
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_ne!(a, Record::Vacant);
        assert_eq!(Record::Vacant, Record::default());
    }

    #[test]
    fn encoding_pool_runs_all_indices() {
        let pool = EncodingPool::new(4).expect("pool");
        let hits = Arc::new(Mutex::new(vec![0u32; 37]));
        let job = Arc::clone(&hits);
        pool.run(37, Arc::new(move |index, ws: &mut EncoderWorkspace| {
            ws.scratch[0] = index as u8;
            job.lock().unwrap()[index] += 1;
        }));
        assert!(hits.lock().unwrap().iter().all(|&n| n == 1));
        // Empty runs are no-ops.
        pool.run(0, Arc::new(|_, _: &mut EncoderWorkspace| panic!("must not run")));
    }

    #[test]
    fn encoding_pool_resumes_first_panic() {
        let pool = EncodingPool::new(2).expect("pool");
        let done = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let job = Arc::clone(&done);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            pool.run(8, Arc::new(move |index, _: &mut EncoderWorkspace| {
                job.fetch_add(1, Ordering::Relaxed);
                if index == 3 {
                    panic!("boom");
                }
            }));
        }));
        assert!(outcome.is_err());
        // Every index still ran (errors don't stop the batch).
        assert_eq!(done.load(Ordering::Relaxed), 8);
    }

    #[test]
    fn core_count_in_range() {
        let n = physical_core_count();
        assert!((1..=256).contains(&n));
        assert_eq!(n, physical_core_count());
    }
}
