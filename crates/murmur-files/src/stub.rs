//! In-memory [`murmur_tx::Backend`] for unit tests.
//!
//! murmur-tx's own `test_support` is `#[cfg(test)] pub(crate)`, so external
//! crates cannot reuse it. This stub covers the kv surface the file and
//! bridge modules touch (`get`/`keys`/`commit`/`publish`, bridge import) plus
//! inert implementations of the remaining trait methods.

use murmur_tx::{Backend, NodeId, Record, Result};
use std::collections::BTreeMap;
use std::sync::Mutex;

pub struct Stub {
    kv: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
    imported: Mutex<Vec<Vec<u8>>>,
    identity: (NodeId, NodeId),
}

impl Stub {
    pub fn new() -> Self {
        Self {
            kv: Mutex::new(BTreeMap::new()),
            imported: Mutex::new(Vec::new()),
            identity: ([0xdb; 16], [0x11; 16]),
        }
    }

    /// Stub reporting a custom `(node, database)` identity. The fetch
    /// tests use it to prove the transport rejects a certificate minted
    /// for a different node.
    pub fn with_identity(identity: (NodeId, NodeId)) -> Self {
        Self {
            kv: Mutex::new(BTreeMap::new()),
            imported: Mutex::new(Vec::new()),
            identity,
        }
    }

    fn apply(&self, records: Vec<Record>) {
        let mut kv = self.kv.lock().unwrap();
        for record in records {
            match record.value {
                Some(value) => {
                    kv.insert(record.key, value);
                }
                None => {
                    kv.remove(&record.key);
                }
            }
        }
    }

    pub fn imported(&self) -> Vec<Vec<u8>> {
        self.imported.lock().unwrap().clone()
    }
}

impl Default for Stub {
    fn default() -> Self {
        Self::new()
    }
}

impl Backend for Stub {
    fn identity(&self) -> (NodeId, NodeId) {
        self.identity
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.kv.lock().unwrap().get(key).cloned())
    }

    fn keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(self
            .kv
            .lock()
            .unwrap()
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect())
    }

    fn commit(&self, records: Vec<Record>) -> Result<()> {
        self.apply(records);
        Ok(())
    }

    fn publish(&self, records: Vec<Record>) -> Result<()> {
        self.apply(records);
        Ok(())
    }

    fn import_bridge(&self, _header: murmur_tx::bridge::Header, payload: Vec<u8>) -> Result<bool> {
        self.imported.lock().unwrap().push(payload);
        Ok(true)
    }

    fn release_ownership(
        &self,
        _table: String,
        _key: Vec<u8>,
        _column: Option<String>,
    ) -> Result<bool> {
        Ok(true)
    }

    fn snapshot(&self) -> Result<Vec<u8>> {
        Ok(b"stub-snapshot".to_vec())
    }

    fn snapshot_with_progress(&self) -> Result<(Vec<u8>, Vec<murmur_tx::protocol::Progress>)> {
        Ok((b"stub-snapshot".to_vec(), Vec::new()))
    }

    fn retention_checkpoint(&self) -> Result<()> {
        Ok(())
    }

    fn prune_history(&self, _origin: NodeId, _through: u64) -> Result<u64> {
        Ok(0)
    }

    fn reclaim_retention(&self) -> Result<()> {
        Ok(())
    }

    fn schema(&self) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }

    fn apply(&self, _payloads: Vec<Vec<u8>>, records: Vec<Record>) -> Result<bool> {
        self.apply(records);
        Ok(true)
    }

    fn observe(&self, _millis: u64, _logical: u32) -> Result<()> {
        Ok(())
    }

    fn accept_schema(&self, _schema: Vec<u8>) -> Result<bool> {
        Ok(true)
    }
}
