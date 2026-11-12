use sha2::{Digest, Sha256};
use std::path::Path;
pub fn fixture(path: &Path, payload: &[u8]) {
    std::fs::create_dir(path).unwrap();
    for dir in ["raft.db", "state.db"] {
        std::fs::create_dir(path.join(dir)).unwrap();
    }
    let files = [
        ("raft.db/000001.sst", b"log".as_slice()),
        ("state.db/000002.sst", payload),
    ];
    let records: Vec<_> = files.into_iter().map(|(name,data)| {
        std::fs::write(path.join(name),data).unwrap();
        serde_json::json!({"name":name,"size":data.len(),"sha256":format!("{:x}",Sha256::digest(data))})
    }).collect();
    // Synthetic bytes, not a real RocksDB checkpoint: d2-raft owns that test.
    let value = serde_json::json!({"format":1,"group_id":1,"node_id":1,"files":records});
    std::fs::write(
        path.join("checkpoint.json"),
        serde_json::to_vec(&value).unwrap(),
    )
    .unwrap();
}
