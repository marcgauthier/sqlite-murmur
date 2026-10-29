//! Internal SHA-256 adapter backed by AWS-LC.
#![allow(dead_code)] // Consumers use different subsets of this shared adapter.
use aws_lc_rs::digest;

#[derive(Clone)]
pub(crate) struct Sha256(digest::Context);

impl Sha256 {
    pub(crate) fn new() -> Self {
        Self(digest::Context::new(&digest::SHA256))
    }

    pub(crate) fn update(&mut self, bytes: impl AsRef<[u8]>) {
        self.0.update(bytes.as_ref());
    }

    pub(crate) fn finalize(self) -> [u8; 32] {
        self.0.finish().as_ref().try_into().expect("SHA-256 length")
    }

    pub(crate) fn digest(bytes: impl AsRef<[u8]>) -> [u8; 32] {
        digest::digest(&digest::SHA256, bytes.as_ref())
            .as_ref()
            .try_into()
            .expect("SHA-256 length")
    }
}
