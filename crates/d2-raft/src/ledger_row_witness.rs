//! Private exact-byte witnesses, never persisted or deserialized authority.
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RowWitness {
    pub(super) first: u64,
    pub(super) bytes: u64,
    digest: [u8; 32],
}
impl RowWitness {
    pub(super) fn new(first: u64, key: &[u8], value: &[u8]) -> Self {
        Self {
            first,
            bytes: (key.len() + value.len()) as u64,
            digest: digest(key, value),
        }
    }
    pub(super) fn matches(&self, key: &[u8], value: &[u8]) -> bool {
        self.bytes == (key.len() + value.len()) as u64 && self.digest == digest(key, value)
    }
}
fn digest(key: &[u8], value: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"VelaGraph/private-row-witness/v1\0");
    hash.update((key.len() as u64).to_be_bytes());
    hash.update(key);
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
    hash.finalize().into()
}
